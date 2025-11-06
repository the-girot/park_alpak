import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import config, logger
from src.croner import DAG

# DAG для ежедневного обновления погодных данных
weather_dag = DAG(
    "weather_data_update", schedule_interval="* 9 * * *"
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
    """Получение данных о погоде для всех активных городов"""
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

            # Получаем текущие данные о погоде
            url = "https://my.meteoblue.com/packages/basic-day_current_clouds-day_wind-day_snowice-day"
            params = {
                "lat": lat,
                "lon": lon,
                "apikey": config.API_KEY,
                "forecast_days": 1,
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Извлекаем нужные данные из ответа API
            metadata = data.get("metadata", {})
            data_day = data.get("data_day", {})

            # Формируем запись для базы данных в соответствии с DDL
            record = {
                "city_id": city_id,
                "forecast_date": data_day.get("time", [None])[0],
                "model_run_timestamp": metadata.get("modelrun_utc"),
                "data_updated_timestamp": metadata.get("modelrun_updatetime_utc"),
                "sigma_level": metadata.get("sigmalevel"),
                "predictability": data_day.get("predictability", [None])[0],
                "predictability_class": data_day.get("predictability_class", [None])[0],
                "temperature_min": data_day.get("temperature_min", [None])[0],
                "temperature_max": data_day.get("temperature_max", [None])[0],
                "temperature_mean": data_day.get("temperature_mean", [None])[0],
                "temperature_instant": data_day.get("temperature_instant", [None])[0],
                "felt_temperature_min": data_day.get("felttemperature_min", [None])[0],
                "felt_temperature_max": data_day.get("felttemperature_max", [None])[0],
                "felt_temperature_mean": data_day.get("felttemperature_mean", [None])[
                    0
                ],
                "precipitation": data_day.get("precipitation", [None])[0],
                "precipitation_probability": data_day.get(
                    "precipitation_probability", [None]
                )[0],
                "precipitation_hours": data_day.get("precipitation_hours", [None])[0],
                "convective_precipitation": data_day.get(
                    "convective_precipitation", [None]
                )[0],
                "precipitation_type": data_day.get("precipitationtype", [None])[0],
                "precipitation_type_probability_rain": data_day.get(
                    "precipitationtypeprobability_rain", [None]
                )[0],
                "precipitation_type_probability_snow": data_day.get(
                    "precipitationtypeprobability_snow", [None]
                )[0],
                "precipitation_type_probability_freezing_rain": data_day.get(
                    "precipitationtypeprobability_freezingrain", [None]
                )[0],
                "precipitation_type_probability_ice_pellets": data_day.get(
                    "precipitationtypeprobability_icepellets", [None]
                )[0],
                "windspeed_min": data_day.get("windspeed_min", [None])[0],
                "windspeed_max": data_day.get("windspeed_max", [None])[0],
                "windspeed_mean": data_day.get("windspeed_mean", [None])[0],
                "winddirection": data_day.get("winddirection", [None])[0],
                "windspeed_80m_min": data_day.get("windspeed_80m_min", [None])[0],
                "windspeed_80m_max": data_day.get("windspeed_80m_max", [None])[0],
                "windspeed_80m_mean": data_day.get("windspeed_80m_mean", [None])[0],
                "winddirection_80m": data_day.get("winddirection_80m", [None])[0],
                "gust_min": data_day.get("gust_min", [None])[0],
                "gust_max": data_day.get("gust_max", [None])[0],
                "gust_mean": data_day.get("gust_mean", [None])[0],
                "total_cloudcover_min": data_day.get("totalcloudcover_min", [None])[0],
                "total_cloudcover_max": data_day.get("totalcloudcover_max", [None])[0],
                "total_cloudcover_mean": data_day.get("totalcloudcover_mean", [None])[
                    0
                ],
                "low_clouds_min": data_day.get("lowclouds_min", [None])[0],
                "low_clouds_max": data_day.get("lowclouds_max", [None])[0],
                "low_clouds_mean": data_day.get("lowclouds_mean", [None])[0],
                "mid_clouds_min": data_day.get("midclouds_min", [None])[0],
                "mid_clouds_max": data_day.get("midclouds_max", [None])[0],
                "mid_clouds_mean": data_day.get("midclouds_mean", [None])[0],
                "high_clouds_min": data_day.get("highclouds_min", [None])[0],
                "high_clouds_max": data_day.get("highclouds_max", [None])[0],
                "high_clouds_mean": data_day.get("highclouds_mean", [None])[0],
                "surface_air_pressure_min": data_day.get(
                    "surfaceairpressure_min", [None]
                )[0],
                "surface_air_pressure_max": data_day.get(
                    "surfaceairpressure_max", [None]
                )[0],
                "surface_air_pressure_mean": data_day.get(
                    "surfaceairpressure_mean", [None]
                )[0],
                "sea_level_pressure_min": data_day.get("sealevelpressure_min", [None])[
                    0
                ],
                "sea_level_pressure_max": data_day.get("sealevelpressure_max", [None])[
                    0
                ],
                "sea_level_pressure_mean": data_day.get(
                    "sealevelpressure_mean", [None]
                )[0],
                "relative_humidity_min": data_day.get("relativehumidity_min", [None])[
                    0
                ],
                "relative_humidity_max": data_day.get("relativehumidity_max", [None])[
                    0
                ],
                "relative_humidity_mean": data_day.get("relativehumidity_mean", [None])[
                    0
                ],
                "humidity_greater_90_hours": data_day.get(
                    "humiditygreater90_hours", [None]
                )[0],
                "visibility_min": data_day.get("visibility_min", [None])[0],
                "visibility_max": data_day.get("visibility_max", [None])[0],
                "visibility_mean": data_day.get("visibility_mean", [None])[0],
                "sunshine_time": data_day.get("sunshine_time", [None])[0],
                "uv_index": data_day.get("uvindex", [None])[0],
                "snow_depth_min": data_day.get("snowdepth_min", [None])[0],
                "snow_depth_max": data_day.get("snowdepth_max", [None])[0],
                "snow_depth_mean": data_day.get("snowdepth_mean", [None])[0],
                "snow_accumulation": data_day.get("snowaccumulation", [None])[0],
                "snow_melt": data_day.get("snowmelt", [None])[0],
                "snow_fraction": data_day.get("snowfraction", [None])[0],
                "icing_on_surface": data_day.get("icing_on_surface", [None])[0],
                "icing_on_wire": data_day.get("icing_on_wire", [None])[0],
                "freezing_rain": data_day.get("freezingrain", [None])[0],
                "pictocode": data_day.get("pictocode", [None])[0],
                "fog_probability": data_day.get("fog_probability", [None])[0],
                "air_density_min": data_day.get("airdensity_min", [None])[0],
                "air_density_max": data_day.get("airdensity_max", [None])[0],
                "air_density_mean": data_day.get("airdensity_mean", [None])[0],
            }

            weather_data.append(record)
            logger.info(
                f"Данные погоды получены для города {city_name} (ID: {city_id})"
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка API для города {city_name}: {e}")
            continue
        except Exception as e:
            logger.error(f"Неожиданная ошибка для города {city_name}: {e}")
            continue

    return weather_data


def save_weather_to_db(weather_data):
    """Сохранение данных о погоде в базу данных"""
    if not weather_data:
        logger.warning("Нет данных о погоде для сохранения")
        return 0

    try:
        engine = create_engine(config.db_config.get_url())

        with engine.connect() as conn:
            # SQL запрос для вставки данных в соответствии с DDL
            insert_query = text("""
                INSERT INTO raw.weather_data (
                    city_id, forecast_date, model_run_timestamp, data_updated_timestamp,
                    sigma_level, predictability, predictability_class,
                    temperature_min, temperature_max, temperature_mean, temperature_instant,
                    felt_temperature_min, felt_temperature_max, felt_temperature_mean,
                    precipitation, precipitation_probability, precipitation_hours,
                    convective_precipitation, precipitation_type,
                    precipitation_type_probability_rain, precipitation_type_probability_snow,
                    precipitation_type_probability_freezing_rain, precipitation_type_probability_ice_pellets,
                    windspeed_min, windspeed_max, windspeed_mean, winddirection,
                    windspeed_80m_min, windspeed_80m_max, windspeed_80m_mean, winddirection_80m,
                    gust_min, gust_max, gust_mean,
                    total_cloudcover_min, total_cloudcover_max, total_cloudcover_mean,
                    low_clouds_min, low_clouds_max, low_clouds_mean,
                    mid_clouds_min, mid_clouds_max, mid_clouds_mean,
                    high_clouds_min, high_clouds_max, high_clouds_mean,
                    surface_air_pressure_min, surface_air_pressure_max, surface_air_pressure_mean,
                    sea_level_pressure_min, sea_level_pressure_max, sea_level_pressure_mean,
                    relative_humidity_min, relative_humidity_max, relative_humidity_mean,
                    humidity_greater_90_hours,
                    visibility_min, visibility_max, visibility_mean,
                    sunshine_time, uv_index,
                    snow_depth_min, snow_depth_max, snow_depth_mean,
                    snow_accumulation, snow_melt, snow_fraction,
                    icing_on_surface, icing_on_wire, freezing_rain,
                    pictocode, fog_probability,
                    air_density_min, air_density_max, air_density_mean
                ) VALUES (
                    :city_id, :forecast_date, :model_run_timestamp, :data_updated_timestamp,
                    :sigma_level, :predictability, :predictability_class,
                    :temperature_min, :temperature_max, :temperature_mean, :temperature_instant,
                    :felt_temperature_min, :felt_temperature_max, :felt_temperature_mean,
                    :precipitation, :precipitation_probability, :precipitation_hours,
                    :convective_precipitation, :precipitation_type,
                    :precipitation_type_probability_rain, :precipitation_type_probability_snow,
                    :precipitation_type_probability_freezing_rain, :precipitation_type_probability_ice_pellets,
                    :windspeed_min, :windspeed_max, :windspeed_mean, :winddirection,
                    :windspeed_80m_min, :windspeed_80m_max, :windspeed_80m_mean, :winddirection_80m,
                    :gust_min, :gust_max, :gust_mean,
                    :total_cloudcover_min, :total_cloudcover_max, :total_cloudcover_mean,
                    :low_clouds_min, :low_clouds_max, :low_clouds_mean,
                    :mid_clouds_min, :mid_clouds_max, :mid_clouds_mean,
                    :high_clouds_min, :high_clouds_max, :high_clouds_mean,
                    :surface_air_pressure_min, :surface_air_pressure_max, :surface_air_pressure_mean,
                    :sea_level_pressure_min, :sea_level_pressure_max, :sea_level_pressure_mean,
                    :relative_humidity_min, :relative_humidity_max, :relative_humidity_mean,
                    :humidity_greater_90_hours,
                    :visibility_min, :visibility_max, :visibility_mean,
                    :sunshine_time, :uv_index,
                    :snow_depth_min, :snow_depth_max, :snow_depth_mean,
                    :snow_accumulation, :snow_melt, :snow_fraction,
                    :icing_on_surface, :icing_on_wire, :freezing_rain,
                    :pictocode, :fog_probability,
                    :air_density_min, :air_density_max, :air_density_mean
                )
            """)

            # Выполняем вставку для каждой записи
            result = conn.execute(insert_query, weather_data)
            conn.commit()

            logger.info(f"Успешно сохранено {len(weather_data)} записей о погоде")
            return len(weather_data)

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при сохранении в базу данных: {e}")
        return 0


def log_weather_update(records_count):
    """Логирование завершения обновления погодных данных"""
    if records_count > 0:
        message = (
            f"Обновление погодных данных завершено. Сохранено {records_count} записей"
        )
        logger.info(message)
    else:
        message = "Обновление погодных данных завершено. Записей не сохранено"
        logger.warning(message)

    return message


@weather_dag.task
def start_weather_dag():
    cities = get_cities_from_db()
    weather_data = get_weather_data(cities)
    records_count = save_weather_to_db(weather_data)
    log_weather_update(records_count)
