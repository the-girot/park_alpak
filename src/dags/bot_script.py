import locale
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine, text

# Каждые 10 секунд
from src.croner import DAG
from src.config import config

# cron (каждую минуту с 9 до 18 по будням)
bot_dag = DAG("bot_dag", schedule_interval="10 12 * * *")


# Настройки подключения к PostgreSQL


# Конфигурация для Telegram
TELEGRAM_TOKEN = config.TG_TOKEN
TELEGRAM_CHAT_ID = config.CHAT_ID
# TELEGRAM_CHAT_ID = ""


def get_db_connection():
    """Создает подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(**(config.db_config.get_config()))
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None


def get_sqlalchemy_connection():
    """Создает подключение через SQLAlchemy для погодных данных"""
    try:
        engine = create_engine(
            config.db_config.get_url()
        )
        return engine.connect()
    except Exception as e:
        print(f"Ошибка подключения к БД через SQLAlchemy: {e}")
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
    return f"{num:,}".replace(",", ".")


def format_temperature(temp):
    """Форматирует температуру"""
    if temp is None:
        return "—"
    return f"{round(temp)}°C"


def get_emoji(change):
    """Возвращает эмодзи в зависимости от изменения выручки"""
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


def get_weather_emoji(pictocode, is_daylight=True):
    """Возвращает эмодзи в зависимости от кода погоды"""
    # Базовые коды погоды Meteoblue
    weather_emojis_detailed = {
        1: "☀️",  # Ясно и безоблачно
        2: "🌤️",  # Ясно, с отдельными перистыми облаками
        3: "🌤️",  # Ясно, перистая облачность
        4: "⛅",  # Ясно, немного низких облаков
        5: "⛅",  # Ясно, с небольшим количеством низких облаков и отдельными перистыми облаками
        6: "⛅",  # Ясно, с небольшим количеством низких облаков и перистых облаков
        7: "🌥️",  # Переменная облачность
        8: "🌥️",  # Переменная облачность, отдельные перистые облака
        9: "🌥️",  # Переменная облачность, перистые облака
        10: "🌩️",  # Неустойчивая погода, возможны отдельные грозовые тучи
        11: "🌩️",  # Неустойчивая погода, немного перистых облаков, возможны отдельные грозовые тучи
        12: "🌩️",  # Неустойчивая погода, перистые облака, возможны отдельные грозовые тучи
        13: "☀️🌫️",  # Ясно, но слегка туманно
        14: "☀️🌫️",  # Ясно, но слегка туманно, немного перистых облаков
        15: "☀️🌫️",  # Ясно, но слегка туманно, перистые облака
        16: "🌫️☁️",  # Туман / низкие слоистые облака
        17: "🌫️☁️",  # Туман/низкие слоистые облака с перистой облачностью
        18: "🌫️☁️",  # Туман / низкие слоистые и перистые облака
        19: "☁️",  # Преимущественно облачно
        20: "☁️",  # Преимущественно облачно, немного перистых облаков
        21: "☁️",  # Преимущественно облачно, перистые облака
        22: "☁️",  # Пасмурно
        23: "🌧️",  # Пасмурно, дождь
        24: "🌨️",  # Пасмурно, снег
        25: "💧",  # Пасмурно, ливень
        26: "❄️",  # Пасмурно, снегопад
        27: "⛈️",  # Дождь, вероятны грозы
        28: "⛈️",  # Легкий дождь, вероятны грозы
        29: "⛈️🌨️",  # Гроза с сильным снегопадом
        30: "⛈️💧",  # Ливни, возможны грозы
        31: "🌦️",  # Переменная облачность, временами легкий дождь
        32: "🌨️",  # Переменная облачность, временами снег
        33: "🌧️",  # Пасмурно, легкий дождь
        34: "🌨️",  # Пасмурно, легкий снег
        35: "🌧️❄️",  # Пасмурно, дождь со снегом
        36: "〰️",  # Не используется
        37: "〰️",  # Не используется
    }

    return weather_emojis_detailed.get(pictocode, "�")


def get_weather_data():
    """Получает данные о погоде из базы данных только на 6:00 и 15:00"""
    conn = get_sqlalchemy_connection()
    if not conn:
        return None

    try:
        # Получаем данные на сегодня только на 6:00 и 15:00
        query = text("""
            SELECT 
                hwd.city_id,
                c.name_en as city_name,
                hwd.forecast_time,
                hwd.temperature,
                hwd.pictocode,
                hwd.is_daylight
            FROM raw.hourly_weather_data hwd
            JOIN dds.cities c ON hwd.city_id = c.id
            WHERE DATE(hwd.forecast_time) = CURRENT_DATE - interval '1 day'
                AND (
                    EXTRACT(HOUR FROM hwd.forecast_time) = 6 
                    OR EXTRACT(HOUR FROM hwd.forecast_time) = 15
                )
            ORDER BY hwd.city_id, hwd.forecast_time
        """)

        result = conn.execute(query)
        rows = result.fetchall()

        # Группируем данные по городам и времени
        weather_data = {}
        for row in rows:
            city_id = row[0]
            city_name = row[1]
            forecast_time = row[2]
            temperature = (
                float(str(row[3]).replace(",", ".")) if row[3] is not None else None
            )
            pictocode = row[4]
            is_daylight = row[5]

            if city_id not in weather_data:
                weather_data[city_id] = {"city_name": city_name, "forecasts": {}}

            # Сохраняем прогноз
            weather_data[city_id]["forecasts"][forecast_time] = {
                "temperature": temperature,
                "pictocode": pictocode,
                "is_daylight": is_daylight,
            }

        return weather_data

    except Exception as e:
        print(f"Ошибка выполнения запроса погоды: {e}")
        return None
    finally:
        conn.close()


def get_sales_data():
    """Получает данные о продажах из базы данных"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        query = """SELECT * FROM bot_view"""
        df = pd.read_sql_query(query, conn)
        return df.to_dict("records")

    except Exception as e:
        print(f"Ошибка выполнения запроса продаж: {e}")
        return None
    finally:
        conn.close()


def generate_combined_report(sales_data, weather_data):
    """Генерирует объединенный отчет с погодой и выручкой"""
    if not sales_data:
        return "Нет данных для формирования отчета"

    # Преобразуем данные о продажах в DataFrame
    df = pd.DataFrame(sales_data)

    # Получаем дату из данных
    date_str = df["date_sale"].iloc[0]
    try:
        report_date = datetime.strptime(str(date_str), "%Y-%m-%d %H:%M:%S.%f")
    except:
        report_date = datetime.strptime(str(date_str), "%Y-%m-%d %H:%M:%S")

    # Определяем день недели
    days = [
        "понедельник",
        "вторник",
        "среда",
        "четверг",
        "пятница",
        "суббота",
        "воскресенье",
    ]
    day_of_week = days[report_date.weekday()]

    # Группируем данные о продажах по городам
    sales_by_city = {}
    full_data = {}

    for _, row in df.iterrows():
        city = row["sklad"]
        source = row["source"]
        amount = row["Сумма вся выручка"]
        change = (
            0
            if pd.isna(row["Изменение к прошлой неделе %"])
            else row["Изменение к прошлой неделе %"]
        )

        # Обрабатываем общие данные ("По всем городам")
        if city == "По всем городам":
            full_data[source] = {"amount": round(amount), "change": round(change)}
            continue

        # Обрабатываем данные по городам
        if city not in sales_by_city:
            sales_by_city[city] = {"all": None, "offline": None, "online": None}

        if source == "all":
            sales_by_city[city]["all"] = {
                "amount": round(amount),
                "change": round(change),
            }
        elif source == "offline":
            sales_by_city[city]["offline"] = {
                "amount": round(amount),
                "change": round(change),
            }
        elif source == "online":
            sales_by_city[city]["online"] = {
                "amount": round(amount),
                "change": round(change),
            }

    # Формируем объединенный отчет
    report = []
    report.append(f"📊 Отчет за {report_date.strftime('%d.%m.%Y')} ({day_of_week})")
    report.append("")

    # Общие данные по всем городам
    total_all = full_data.get("all", {}).get("amount", 0)
    total_offline = full_data.get("offline", {}).get("amount", 0)
    total_online = full_data.get("online", {}).get("amount", 0)

    all_change = full_data.get("all", {}).get("change", "—")
    offline_change = full_data.get("offline", {}).get("change", "—")
    online_change = full_data.get("online", {}).get("change", "—")

    report.append("📈 По всем городам:")
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
    for city in sorted(sales_by_city.keys(), key=lambda x: x.lower()):
        sales_data = sales_by_city[city]

        if not sales_data["all"]:
            continue

        # Данные о продажах
        all_amount = sales_data["all"]["amount"]
        all_change = sales_data["all"]["change"]
        online_amount = sales_data["online"]["amount"] if sales_data["online"] else 0
        online_change = sales_data["online"]["change"] if sales_data["online"] else "—"
        offline_amount = sales_data["offline"]["amount"] if sales_data["offline"] else 0
        offline_change = (
            sales_data["offline"]["change"] if sales_data["offline"] else "—"
        )

        # Данные о погоде для этого города
        city_weather = None
        if weather_data:
            for city_id, city_info in weather_data.items():
                if city_info["city_name"] == city:
                    city_weather = city_info["forecasts"]
                    break

        report.append(f"🏙️ {city}")
        report.append("🌍 Погода")

        # Погода на 6:00 и 15:00
        morning_str = "—"
        afternoon_str = "—"

        if city_weather:
            for forecast_time, forecast in city_weather.items():
                print(forecast_time, forecast)
                hour = forecast_time.hour
                emoji = get_weather_emoji(
                    forecast["pictocode"], forecast["is_daylight"]
                )
                temp = format_temperature(forecast["temperature"])

                if hour == 6:
                    morning_str = f"{emoji} {temp}"
                elif hour == 15:
                    afternoon_str = f"{emoji} {temp}"

        report.append(f"   🕕 6:00: {morning_str}")
        report.append(f"   🕒 15:00: {afternoon_str}")

        # Выручка
        report.append("💰 Выручка")
        report.append(
            f"{get_emoji(all_change)} all: {format_number(all_amount)} ({format_percentage(all_change)})"
        )
        report.append(
            f"{get_emoji(online_change)} on: {format_number(online_amount)} ({format_percentage(online_change)})"
        )
        report.append(
            f"{get_emoji(offline_change)} off: {format_number(offline_amount)} ({format_percentage(offline_change)})"
        )
        report.append("")

    return "\n".join(report)

@bot_dag.task
def main():
    """Основная функция"""
    try:
        # Устанавливаем локаль
        locale.setlocale(locale.LC_ALL, "ru_RU.UTF-8")
    except:
        try:
            locale.setlocale(locale.LC_ALL, "Russian_Russia.1251")
        except:
            print("Предупреждение: не удалось установить русскую локаль")

    # Получаем данные
    sales_data = get_sales_data()
    weather_data = get_weather_data()

    if sales_data:
        # Генерируем объединенный отчет
        report = generate_combined_report(sales_data, weather_data)
        # print(report)

        # Отправка в Telegram
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": report}

        response = requests.post(url, json=data)
        # print(response.json())

        # Сохраняем в файл
        with open("combined_report.txt", "w", encoding="utf-8") as f:
            f.write(report)
        print("\nОтчет сохранен в файл combined_report.txt")
    else:
        print("Не удалось получить данные из базы данных")

