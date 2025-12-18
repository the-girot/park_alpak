import concurrent.futures
import hashlib
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

import requests
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import config, logger
from src.croner import DAG

# Создаем DAG
dag = DAG(
    dag_id="timepad_sales_sync",
    schedule_interval="0 */4 * * *",  # Ежедневно в 2:00 утра
)

# Глобальные переменные для потокобезопасности
thread_local = threading.local()


def get_session():
    """
    Создает отдельную сессию для каждого потока
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def get_all_orders_for_event(event_id: str, headers: dict) -> List[Dict]:
    """
    Получает все заказы для события без пропусков
    """
    session = get_session()
    all_orders = []
    skip = 0
    limit = 250

    # print(
    #     f"[{threading.current_thread().name}] Загружаем заказы для события {event_id}..."
    # )

    while True:
        url = f"https://api.timepad.ru/v1/events/{event_id}/orders?skip={skip}&limit={limit}"

        try:
            response = session.get(url=url, headers=headers, timeout=30)

            if response.status_code != 200:
                print(
                    f"[{threading.current_thread().name}] Ошибка {response.status_code} для события {event_id}, пропуск {skip}"
                )
                break

            data = response.json()
            orders = data.get("values", [])

            if not orders:
                break

            all_orders.extend(orders)
            # print(
            #     f"[{threading.current_thread().name}] Загружено {len(orders)} заказов (всего: {len(all_orders)})"
            # )

            # Проверяем, есть ли еще заказы
            if len(orders) < limit:
                break

            skip += limit
            time.sleep(0.1)  # Небольшая задержка чтобы не перегружать API

        except requests.exceptions.RequestException as e:
            print(f"[{threading.current_thread().name}] Ошибка сети: {e}")
            time.sleep(5)  # Пауза при ошибке сети
            continue
        except Exception as e:
            print(
                f"[{threading.current_thread().name}] Ошибка при получении заказов: {e}"
            )
            break

    print(
        f"[{threading.current_thread().name}] Всего загружено {len(all_orders)} заказов для события {event_id}"
    )
    return all_orders


def calculate_timepad_commission(amount: float) -> float:
    """
    Рассчитывает комиссию TimePad (2.5%)
    """
    return round(amount * 0.025, 2)


def calculate_acquiring_commission(amount: float) -> float:
    """
    Рассчитывает комиссию эквайринга (2.2%)
    """
    return round(amount * 0.022, 2)


def process_orders_simple(orders: List[Dict], event_id: str) -> List[Dict]:
    """
    Обрабатывает заказы - только нужные поля
    Каждый билет в заказе обрабатывается как отдельная строка
    Комиссии рассчитываются от общей суммы заказа (payment_amount) и делятся на количество билетов
    """
    export_data = []

    for order in orders:
        # Берем только нужные поля из заказа
        order_id = order.get("id", "")  # Уникальный ID заказа
        event_session_id = order.get("eventSessionId", "")  # ID сессии события
        created_at_str = order.get("created_at", "")
        order_status = order.get("status", {})
        status_name = order_status.get("name", "")
        status_title = order_status.get("title", "")
        payment = order.get("payment", {})
        payment_amount = float(payment.get("amount", 0))
        payment_discount = float(payment.get("discount", 0))

        if not created_at_str:
            continue

        # Парсим дату создания заказа
        try:
            if "+" in created_at_str:
                created_dt = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%S%z")
            else:
                created_dt = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%S")
            registration_date = created_dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        # Рассчитываем комиссии для всего заказа от payment_amount
        timepad_commission_total = 0
        acquiring_commission_total = 0

        # Комиссии считаем только для оплаченных заказов
        if status_name in ["paid", "return_payment_reject", "paid_ur", "paid_offline"]:
            timepad_commission_total = calculate_timepad_commission(payment_amount)
            acquiring_commission_total = calculate_acquiring_commission(payment_amount)

        # Обрабатываем КАЖДЫЙ билет в заказе как отдельную строку
        tickets = order.get("tickets", [])
        tickets_count = len(tickets)

        for ticket in tickets:
            # Берем только нужные поля из билета
            ticket_id = ticket.get("id", "")
            price_nominal = float(ticket.get("price_nominal", 0))
            ticket_type = ticket.get("ticket_type", {})
            ticket_type_id = ticket_type.get("id", "")
            ticket_type_name = ticket_type.get("name", "")
            ticket_price = float(ticket_type.get("price", 0))

            # Комиссии делятся на количество билетов в заказе
            timepad_commission = (
                round(timepad_commission_total / tickets_count, 2)
                if tickets_count > 0
                else 0
            )
            acquiring_commission = (
                round(acquiring_commission_total / tickets_count, 2)
                if tickets_count > 0
                else 0
            )

            # Создаем упрощенную запись
            export_record = {
                "order_id": order_id,
                "event_session_id": event_session_id,
                "created_at": registration_date,
                "status_name": status_name,
                "status_title": status_title,
                "payment_amount": payment_amount,  # Общая сумма заказа
                "payment_discount": payment_discount,
                "ticket_id": ticket_id,
                "price_nominal": price_nominal,
                "ticket_type_id": ticket_type_id,
                "ticket_type_name": ticket_type_name,
                "ticket_price": ticket_price,  # Цена конкретного билета
                "event_id": event_id,
                "timepad_commission": timepad_commission,  # Комиссия от общей суммы / на кол-во билетов
                "acquiring_commission": acquiring_commission,  # Комиссия от общей суммы / на кол-во билетов
            }

            export_data.append(export_record)

        # Если в заказе нет билетов, создаем одну строку с общей информацией
        if not tickets:
            export_record = {
                "order_id": order_id,
                "event_session_id": event_session_id,
                "created_at": registration_date,
                "status_name": status_name,
                "status_title": status_title,
                "payment_amount": payment_amount,
                "payment_discount": payment_discount,
                "ticket_id": "",
                "price_nominal": 0,
                "ticket_type_id": "",
                "ticket_type_name": "",
                "ticket_price": 0,
                "event_id": event_id,
                "timepad_commission": timepad_commission_total,
                "acquiring_commission": acquiring_commission_total,
            }

            export_data.append(export_record)

    return export_data


def process_single_event(event: Dict, headers: dict) -> Dict[str, Any]:
    """
    Обрабатывает одно событие и возвращает результат
    """
    event_id = event["event_id"]
    city = event["city"]

    print(
        f"[{threading.current_thread().name}] Начата обработка {city} (ID: {event_id})"
    )

    try:
        # Получаем все заказы
        orders = get_all_orders_for_event(event_id, headers)

        # Обрабатываем в упрощенном формате
        export_data = process_orders_simple(orders, event_id)

        # print(
        #     f"[{threading.current_thread().name}] Завершена обработка {city}: {len(export_data)} позиций"
        # )

        return {
            "city": city,
            "event_id": event_id,
            "data": export_data,
            "status": "success",
            "orders_count": len(orders),
            "tickets_count": len(export_data),
        }

    except Exception as e:
        logger.error(
            message=f"[{threading.current_thread().name}] Ошибка при обработке {city}",
            exc_info=str(e),
        )
        return {
            "city": city,
            "event_id": event_id,
            "data": [],
            "status": "error",
            "error": str(e),
        }


def insert_simple_format(data_list):
    """
    Вставляет данные в упрощенном формате в БД с использованием SQLAlchemy
    с обработкой конфликтов через on_conflict_do_update
    """
    # Создаем engine
    engine = create_engine(config.db_config.get_url())

    records_to_insert = []

    # Подготавливаем данные
    for data in data_list:
        hash_string = (
            f"{data['event_id']}"
            f"{data['order_id']}"
            f"{data['event_session_id']}"
            f"{data['created_at']}"
            f"{data['ticket_id']}"
            f"{data['ticket_type_id']}"
        )
        record = {
            "order_hash": hashlib.sha256(hash_string.encode("utf-8")).hexdigest(),
            "order_id": data["order_id"],
            "event_session_id": data["event_session_id"],
            "created_at": data["created_at"],
            "status_name": data["status_name"],
            "status_title": data["status_title"],
            "payment_amount": data["payment_amount"],
            "payment_discount": data["payment_discount"],
            "ticket_id": data["ticket_id"],
            "price_nominal": data["price_nominal"],
            "ticket_type_id": data["ticket_type_id"],
            "ticket_type_name": data["ticket_type_name"],
            "ticket_price": data["ticket_price"],
            "event_id": data["event_id"],
            "timepad_commission": data["timepad_commission"],
            "acquiring_commission": data["acquiring_commission"],
        }
        records_to_insert.append(record)

    try:
        # Получаем метаданные и таблицу
        metadata = MetaData()
        table = Table(
            "online_sales_simple", metadata, autoload_with=engine, schema="public"
        )

        # Определяем колонки для обновления при конфликте
        update_columns = {
            "event_session_id": table.c.event_session_id,
            "status_name": table.c.status_name,
            "status_title": table.c.status_title,
            "payment_amount": table.c.payment_amount,
            "payment_discount": table.c.payment_discount,
            "price_nominal": table.c.price_nominal,
            "ticket_type_name": table.c.ticket_type_name,
            "ticket_price": table.c.ticket_price,
            "timepad_commission": table.c.timepad_commission,
            "acquiring_commission": table.c.acquiring_commission,
            "created_at": table.c.created_at,
        }

        # Создаем insert statement с обработкой конфликтов
        stmt = pg_insert(table).values(records_to_insert)
        stmt = stmt.on_conflict_do_update(
            index_elements=["order_hash"],  # Уникальный индекс
            set_=update_columns,
        )

        # Выполняем запрос
        with engine.begin() as connection:
            result = connection.execute(stmt)
            logger.info(
                f"Успешно обработано {result.rowcount} записей в БД (вставлено + обновлено)"
            )
            return result.rowcount

    except Exception as e:
        print(f"Ошибка при вставке данных в БД: {e}")
        raise
    finally:
        engine.dispose()


def get_cities_from_db():
    """Получение списка активных городов из базы данных"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.connect() as conn:
            query = text("""
                SELECT name_en, event_id 
                FROM dds.cities 
                WHERE is_active = true
            """)

            result = conn.execute(query)
            cities = [{"city": row.name_en, "event_id": row.event_id} for row in result]

        print(f"Получено {len(cities)} активных городов из базы данных")
        return cities

    except Exception as e:
        print(f"Ошибка при получении городов из базы данных: {e}")
        raise


@dag.task
def sync_timepad_sales():
    """
    Основная задача DAG для синхронизации продаж с TimePad
    """
    # Конфигурация
    headers = {
        "Authorization": f"Bearer {config.timepad_api}",
        "Accept": "application/json",
    }

    # Список событий
    events = get_cities_from_db()

    all_export_data = []
    results = []

    logger.info(f"Начинаем параллельную обработку {len(events)} событий...")

    # Обрабатываем события параллельно
    max_workers = min(11, len(events))

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="CityWorker"
    ) as executor:
        future_to_event = {
            executor.submit(process_single_event, event, headers): event
            for event in events
        }

        for future in concurrent.futures.as_completed(future_to_event):
            event = future_to_event[future]
            try:
                result = future.result(timeout=300)
                results.append(result)

                if result["status"] == "success":
                    insert_simple_format(result["data"])
                    logger.info(
                        f"✓ Успешно обработан {result['city']}: {result['orders_count']} заказов, {result['tickets_count']} позиций"
                    )
                else:
                    logger.critical(
                        f"✗ Ошибка в {result['city']}: {result.get('error', 'Unknown error')}"
                    )

            except concurrent.futures.TimeoutError:
                print(f"✗ Таймаут при обработке {event['city']}")
            except Exception as e:
                print(f"✗ Неожиданная ошибка при обработке {event['city']}: {e}")
