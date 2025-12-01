import locale
from datetime import datetime

import pandas as pd
import psycopg2
import requests

from src.config import config
from src.croner import DAG

# cron (каждую минуту с 9 до 18 по будням)
bot_week_dag = DAG("bot_week_dag", schedule_interval="10 14 * * 0")


# Настройки подключения к PostgreSQL


# Конфигурация для Telegram
TELEGRAM_TOKEN = config.TG_TOKEN
TELEGRAM_CHAT_ID = config.CHAT_ID


def get_db_connection():
    """Создает подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(**(config.db_config.get_config()))
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None


def format_percentage(change):
    """Форматирует процентное изменение"""
    if change is None or change == "—":
        return "—"

    try:
        if change > 0:
            return f"+{change}%"
        else:
            return f"{change}%"
    except:
        return "—"


def format_number(num):
    """Форматирует число с разделителями"""
    return f"{num:,}".replace(",", " ")


def get_emoji(change):
    """Возвращает эмодзи в зависимости от изменения"""
    if change is None or change == "—":
        return ""

    try:
        # Убираем знак + и преобразуем в число
        change_value = float(str(change).rstrip("%").replace("+", ""))
        if change_value > 0:
            return "🟢"  # Зеленый круг для положительных изменений
        elif change_value < 0:
            return "🔴"  # Красный круг для отрицательных изменений
        else:
            return ""  # Нет изменений
    except:
        return ""


def convert_to_serializable(obj):
    """Конвертирует объекты в сериализуемые для JSON"""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    elif hasattr(obj, "item"):  # Для numpy типов
        return obj.item()
    else:
        return obj


def get_sales_data():
    """Получает данные о продажах из базы данных"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        query = """SELECT * FROM bot_view_week"""
        df = pd.read_sql_query(query, conn)

        # Логируем количество полученных записей
        print(f"Получено {len(df)} записей из базы данных")

        # Конвертируем DataFrame в список словарей с сериализуемыми значениями
        serializable_data = []
        for record in df.to_dict("records"):
            serializable_record = {}
            for key, value in record.items():
                serializable_record[key] = convert_to_serializable(value)
            serializable_data.append(serializable_record)

        return serializable_data

    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        conn.close()


def generate_report(data):
    if not data:
        return "Нет данных для формирования отчета"

    # Преобразуем данные обратно в DataFrame
    df = pd.DataFrame(data)

    # Получаем дату из данных
    date_str = df["week_range"].iloc[0]

    # Группируем по городам и типам данных
    city_data = {}
    full_data = {}  # Для хранения общих данных ("По всем городам")

    for _, row in df.iterrows():
        city = row["sklad"]
        source = row["source"]
        amount = row["Сумма вся выручка"]
        change = row["Изменение к прошлой неделе %"]

        # Обрабатываем общие данные ("По всем городам")
        if city == "По всем городам":
            full_data[source] = {
                "amount": round(amount),
                "change": round(change) if change is not None else "—",
            }
            continue

        # Обрабатываем данные по городам
        if city not in city_data:
            city_data[city] = {"all": None, "offline": None, "online": None}

        if source == "all":
            city_data[city]["all"] = {
                "amount": round(amount),
                "change": round(change) if change is not None else "—",
            }
        elif source == "offline":
            city_data[city]["offline"] = {
                "amount": round(amount),
                "change": round(change) if change is not None else "—",
            }
        elif source == "online":
            city_data[city]["online"] = {
                "amount": round(amount),
                "change": round(change) if change is not None else "—",
            }

    # Формируем отчет
    report = []
    report.append(f"Отчет по продажам за {date_str}")
    report.append("")

    # Используем общие данные из full_data
    total_all = full_data.get("all", {}).get("amount", 0)
    total_offline = full_data.get("offline", {}).get("amount", 0)
    total_online = full_data.get("online", {}).get("amount", 0)

    all_change = full_data.get("all", {}).get("change", "—")
    offline_change = full_data.get("offline", {}).get("change", "—")
    online_change = full_data.get("online", {}).get("change", "—")

    report.append("По всем городам:")
    report.append(
        f"{get_emoji(all_change)} all: {format_number(total_all)} ({format_percentage(all_change)})"
    )
    report.append(
        f"{get_emoji(online_change)} on: {format_number(total_online)} ({format_percentage(online_change)})"
    )
    report.append(
        f"{get_emoji(offline_change)} off: {format_number(total_offline)} ({format_percentage(offline_change)})"
    )
    report.append("")

    # Данные по каждому городу
    for city in sorted(city_data.keys(), key=lambda x: x.lower()):
        data = city_data[city]

        if not data["all"]:
            continue

        all_amount = data["all"]["amount"]
        all_change = data["all"]["change"]
        all_change_formatted = format_percentage(all_change)

        online_amount = data["online"]["amount"] if data["online"] else 0
        online_change = data["online"]["change"] if data["online"] else "—"
        online_change_formatted = format_percentage(online_change)

        offline_amount = data["offline"]["amount"] if data["offline"] else 0
        offline_change = data["offline"]["change"] if data["offline"] else "—"
        offline_change_formatted = format_percentage(offline_change)

        report.append(f"{city}")
        report.append(
            f"{get_emoji(all_change)} all: {format_number(all_amount)} ({all_change_formatted})"
        )
        report.append(
            f"{get_emoji(online_change)} on: {format_number(online_amount)} ({online_change_formatted})"
        )
        report.append(
            f"{get_emoji(offline_change)} off: {format_number(offline_amount)} ({offline_change_formatted})"
        )
        report.append("")

    report_text = "\n".join(report)

    # Сохраняем отчет в XCom для следующей задачи
    return report_text


def send_telegram_report(report):
    """Отправляет отчет в Telegram"""

    if not report:
        print("Нет отчета для отправки")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": report, "parse_mode": "HTML"}

        response = requests.post(url, json=data)

        if response.status_code == 200:
            print("Отчет успешно отправлен в Telegram")
        else:
            print(
                f"Ошибка отправки в Telegram: {response.status_code} - {response.text}"
            )

    except Exception as e:
        print(f"Ошибка при отправке в Telegram: {e}")


def setup_locale():
    """Настраивает локаль"""
    try:
        locale.setlocale(locale.LC_ALL, "ru_RU.UTF-8")
    except:
        try:
            locale.setlocale(locale.LC_ALL, "Russian_Russia.1251")
        except:
            print("Предупреждение: не удалось установить русскую локаль")


@bot_week_dag.task
def main():
    # Определяем порядок выполнения задач
    setup_locale()
    data = get_sales_data()
    report = generate_report(data)
    send_telegram_report(report)
