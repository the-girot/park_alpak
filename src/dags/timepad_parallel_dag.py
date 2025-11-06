import concurrent.futures
import csv
import hashlib
import json
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
import requests
from psycopg2.extras import execute_batch

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

    print(
        f"[{threading.current_thread().name}] Загружаем заказы для события {event_id}..."
    )

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
            print(
                f"[{threading.current_thread().name}] Загружено {len(orders)} заказов (всего: {len(all_orders)})"
            )

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

        print(
            f"[{threading.current_thread().name}] Завершена обработка {city}: {len(export_data)} позиций"
        )

        return {
            "city": city,
            "event_id": event_id,
            "data": export_data,
            "status": "success",
            "orders_count": len(orders),
            "tickets_count": len(export_data),
        }

    except Exception as e:
        print(f"[{threading.current_thread().name}] Ошибка при обработке {city}: {e}")
        return {
            "city": city,
            "event_id": event_id,
            "data": [],
            "status": "error",
            "error": str(e),
        }


def save_to_csv(data: List[Dict], filename: str):
    """
    Сохраняет данные в CSV файл
    """
    if not data:
        print("Нет данных для сохранения")
        return

    fieldnames = [
        "order_id",  # Уникальный ID заказа
        "event_session_id",  # ID сессии события
        "created_at",
        "status_name",
        "status_title",
        "payment_amount",
        "payment_discount",
        "ticket_id",
        "price_nominal",
        "ticket_type_id",
        "ticket_type_name",
        "ticket_price",
        "event_id",
        "timepad_commission",
        "acquiring_commission",
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(data)

    print(f"Данные сохранены в {filename}")



def insert_simple_format(data_list, db_config):
    """
    Вставляет данные в упрощенном формате в БД
    """
    insert_query = """
    INSERT INTO public.online_sales_simple 
    (order_hash, order_id, event_session_id, created_at, status_name, status_title, payment_amount, 
     payment_discount, ticket_id, price_nominal, ticket_type_id, ticket_type_name, 
     ticket_price, event_id, timepad_commission, acquiring_commission)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """  # Добавлен один %s для order_hash

    records_to_insert = []
    for data in data_list:
        hash_string = (
            f"{data['event_id']}"
            f"{data['order_id']}"
            f"{data['event_session_id']}"
            f"{data['created_at']}"
            f"{data['ticket_id']}"
            f"{data['ticket_type_id']}"
        )
        record = (
            hashlib.sha256(hash_string.encode("utf-8")).hexdigest(),  # order_hash
            data["order_id"],
            data["event_session_id"],
            data["created_at"],
            data["status_name"],
            data["status_title"],
            data["payment_amount"],
            data["payment_discount"],
            data["ticket_id"],
            data["price_nominal"],
            data["ticket_type_id"],
            data["ticket_type_name"],
            data["ticket_price"],
            data["event_id"],
            data["timepad_commission"],
            data["acquiring_commission"],
        )
        records_to_insert.append(record)

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        execute_batch(cursor, insert_query, records_to_insert)
        conn.commit()
        print(f"Успешно вставлено {len(records_to_insert)} записей в БД")

    except Exception as e:
        print(f"Ошибка при вставке данных в БД: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    # Конфигурация

    # Список событий
    events = [
        {"city": "Ufa", "event_id": "3568845"},
        {"city": "Tumen", "event_id": "3581805"},
        {"city": "Volgograd", "event_id": "3172355"},
        {"city": "Rostov_On_Don", "event_id": "3252357"},
        {"city": "Ekaterinburg", "event_id": "3252401"},
        {"city": "Kazan", "event_id": "3252418"},
        {"city": "Moscow", "event_id": "3161245"},
        {"city": "Saint_Petersburg", "event_id": "3124166"},
        {"city": "Chegem", "event_id": "2986996"},
        {"city": "Elbrus_region", "event_id": "2987963"},
        {"city": "Omsk", "event_id": "3344106"},
        {"city": "Vorobyovy_Gory", "event_id": "3391090"},
    ]

    all_export_data = []
    results = []

    print(f"Начинаем параллельную обработку {len(events)} событий...")

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
                    all_export_data.extend(result["data"])
                    print(
                        f"✓ Успешно обработан {result['city']}: {result['orders_count']} заказов, {result['tickets_count']} позиций"
                    )
                else:
                    print(
                        f"✗ Ошибка в {result['city']}: {result.get('error', 'Unknown error')}"
                    )

            except concurrent.futures.TimeoutError:
                print(f"✗ Таймаут при обработке {event['city']}")
            except Exception as e:
                print(f"✗ Неожиданная ошибка при обработке {event['city']}: {e}")

    # Сохраняем результаты
    if all_export_data:
        # Сортируем по дате создания
        all_export_data.sort(key=lambda x: x["created_at"])

        # Сохраняем в CSV
        save_to_csv(all_export_data, "timepad_export_all_cities.csv")

        # Сохраняем в JSON для проверки
        with open("timepad_export_all_cities.json", "w", encoding="utf-8") as f:
            json.dump(all_export_data, f, indent=2, ensure_ascii=False, default=str)

        # Сохраняем отчет по результатам
        with open("processing_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)

        # Вставляем в БД
        insert_simple_format(all_export_data, DB_CONFIG)

        # Выводим статистику по статусам
        successful_cities = [r for r in results if r["status"] == "success"]
        failed_cities = [r for r in results if r["status"] == "error"]

        print("\n=== ОБЩАЯ СТАТИСТИКА ===")
        print(f"Успешно обработано: {len(successful_cities)} городов")
        print(f"С ошибками: {len(failed_cities)} городов")

        if failed_cities:
            print("\nГорода с ошибками:")
            for failed in failed_cities:
                print(f"  - {failed['city']}: {failed.get('error', 'Unknown error')}")
    else:
        print("Нет данных для сохранения")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\nОбщее время выполнения: {(end_time - start_time):.2f} секунд")
