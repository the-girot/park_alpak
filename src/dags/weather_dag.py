import csv
import os
from datetime import datetime

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import config, logger
from src.croner import DAG



# DAG для ежедневного обновления погодных данных
weather_dag = DAG(
    "hourly_weather_data_update", schedule_interval="*/5 * * * *"
)  # Каждые 3 минуты


def get_cities_from_db():
    """Получение списка активных городов из базы данных"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.connect() as conn:
            query = text("""
                SELECT id, name_en, lan, lat 
                FROM dds.cities 
                WHERE is_active = true
            """)

            result = conn.execute(query)
            cities = [
                {"id": row.id, "name_en": row.name_en, "lan": row.lan, "lat": row.lat}
                for row in result
            ]

        logger.info(f"Получено {len(cities)} активных городов из базы данных")
        return cities

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при получении городов из базы данных: {e}")
        return []


def get_weather_data(cities):
    """Получение почасовых данных о погоде для всех активных городов"""
    if not cities:
        logger.warning("Нет активных городов для получения данных о погоде")
        return []

    weather_data = []

    for city in cities:
        try:
            city_id = city["id"]
            city_name = city["name_en"]
            lat = city["lan"]
            lon = city["lat"]

            # Получаем данные о погоде
            url = "https://my.meteoblue.com/packages/basic-3h_current_clouds-3h_wind-3h_snowice-3h"
            params = {
                "lat": lat,
                "lon": lon,
                "apikey": "3pdJaYLXCy5QTkZ7",
                "forecast_days": 1,
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Извлекаем нужные данные из ответа API
            metadata = data.get("metadata", {})
            data_3h = data.get("data_3h", {})

            # Обрабатываем 3-часовые прогнозные данные
            times = data_3h.get("time", [])
            if times:
                for i in range(len(times)):
                    hourly_record = create_hourly_record(city_id, metadata, data_3h, i)
                    if hourly_record:
                        weather_data.append(hourly_record)

            logger.info(
                f"Данные погоды получены для города {city_name} (ID: {city_id}): "
                f"{len([r for r in weather_data if r['city_id'] == city_id])} записей"
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка API для города {city_name}: {e}")
            continue
        except Exception as e:
            logger.error(f"Неожиданная ошибка для города {city_name}: {e}")
            continue

    return weather_data


def create_hourly_record(city_id, metadata, data_3h, index):
    """Создание записи из 3-часовых данных по индексу"""
    try:
        record = {
            "city_id": city_id,
            "forecast_time": get_list_value(data_3h, "time", index),
            "model_run_timestamp": metadata.get("modelrun_utc"),
            "data_updated_timestamp": metadata.get("modelrun_updatetime_utc"),
            "timezone_abbreviation": metadata.get("timezone_abbrevation"),
            "utc_timeoffset": metadata.get("utc_timeoffset"),
            # Основные параметры
            "temperature": get_list_value(data_3h, "temperature", index),
            "felt_temperature": get_list_value(data_3h, "felttemperature", index),
            "relative_humidity": get_list_value(data_3h, "relativehumidity", index),
            "sea_level_pressure": get_list_value(data_3h, "sealevelpressure", index),
            # Ветер
            "wind_speed": get_list_value(data_3h, "windspeed", index),
            "wind_direction": get_list_value(data_3h, "winddirection", index),
            # Осадки
            "precipitation": get_list_value(data_3h, "precipitation", index),
            "convective_precipitation": get_list_value(
                data_3h, "convective_precipitation", index
            ),
            "precipitation_probability": get_list_value(
                data_3h, "precipitation_probability", index
            ),
            # Снег
            "snow_fraction": get_list_value(data_3h, "snowfraction", index),
            # Солнечная радиация
            "uv_index": get_list_value(data_3h, "uvindex", index),
            "is_daylight": bool(get_list_value(data_3h, "isdaylight", index, 0)),
            # Коды погоды
            "pictocode": get_list_value(data_3h, "pictocode", index),
        }

        return record

    except Exception as e:
        logger.error(
            f"Ошибка при создании записи из 3-часовых данных (индекс {index}): {e}"
        )
        return None


def get_list_value(data, key, index, default=None):
    """Безопасное получение значения из списка по индексу"""
    if key in data and isinstance(data[key], list) and len(data[key]) > index:
        return data[key][index]
    return default


def save_weather_to_db(weather_data):
    """Сохранение почасовых данных о погоде в базу данных"""
    if not weather_data:
        logger.warning("Нет данных о погоде для сохранения")
        return 0

    try:
        engine = create_engine(config.db_config.get_url())

        with engine.connect() as conn:
            # SQL запрос для вставки данных с обработкой конфликтов
            insert_query = text("""
                INSERT INTO raw.hourly_weather_data (
                    city_id, forecast_time, model_run_timestamp, data_updated_timestamp,
                     timezone_abbreviation, utc_timeoffset,
                    temperature, felt_temperature, relative_humidity, sea_level_pressure,
                    wind_speed, wind_direction,
                    precipitation, convective_precipitation, precipitation_probability,
                    snow_fraction,
                    uv_index, is_daylight,
                    pictocode
                ) VALUES (
                    :city_id, :forecast_time, :model_run_timestamp, :data_updated_timestamp,
                     :timezone_abbreviation, :utc_timeoffset,
                    :temperature, :felt_temperature, :relative_humidity, :sea_level_pressure,
                    :wind_speed, :wind_direction,
                    :precipitation, :convective_precipitation, :precipitation_probability,
                    :snow_fraction,
                    :uv_index, :is_daylight,
                    :pictocode
                )
                ON CONFLICT (city_id, forecast_time) DO NOTHING
            """)

            # Выполняем вставку
            conn.execute(insert_query, weather_data)
            conn.commit()

            logger.info(
                f"Выполнена попытка сохранения {len(weather_data)} записей о погоде"
            )
            return len(weather_data)

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при сохранении в базу данных: {e}")
        return 0


def save_weather_to_csv(weather_data, filename=None):
    """Сохранение почасовых данных о погоде в CSV файл"""
    if not weather_data:
        logger.warning("Нет данных о погоде для сохранения в CSV")
        return False

    try:
        # Создаем имя файла с timestamp, если не указано
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"weather_data_{timestamp}.csv"

        # Создаем директорию для CSV файлов, если её нет
        csv_dir = "weather_data_csv"
        os.makedirs(csv_dir, exist_ok=True)

        filepath = os.path.join(csv_dir, filename)

        # Определяем порядок полей для CSV
        fieldnames = [
            "city_id",
            "forecast_time",
            "model_run_timestamp",
            "data_updated_timestamp",
            "timezone_abbreviation",
            "utc_timeoffset",
            "temperature",
            "felt_temperature",
            "relative_humidity",
            "sea_level_pressure",
            "wind_speed",
            "wind_direction",
            "precipitation",
            "convective_precipitation",
            "precipitation_probability",
            "snow_fraction",
            "uv_index",
            "is_daylight",
            "pictocode",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Записываем данные
            for record in weather_data:
                # Конвертируем специальные типы данных для CSV
                csv_record = {}
                for key in fieldnames:
                    value = record.get(key)
                    # Конвертируем bool в int для лучшей читаемости
                    if isinstance(value, bool):
                        csv_record[key] = 1 if value else 0
                    else:
                        csv_record[key] = value
                writer.writerow(csv_record)

        logger.info(f"Данные успешно сохранены в CSV файл: {filepath}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении в CSV файл: {e}")
        return False


def log_weather_update(records_count):
    """Логирование завершения обновления погодных данных"""
    if records_count > 0:
        message = f"Обновление почасовых погодных данных завершено. Сохранено {records_count} записей"
        logger.info(message)
    else:
        message = "Обновление почасовых погодных данных завершено. Записей не сохранено"
        logger.warning(message)

    return message


@weather_dag.task
def start_weather_dag():
    cities = get_cities_from_db()
    weather_data = get_weather_data(cities)
    records_count = save_weather_to_db(weather_data)
    csv_saved = save_weather_to_csv(weather_data)
    log_weather_update(records_count)
