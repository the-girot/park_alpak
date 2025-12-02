# Каждые 10 секунд
import hashlib
import os
import shutil
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert

from src.config import config, logger
from src.croner import DAG

# cron (каждую минуту с 9 до 18 по будням)
add_ads_dag = DAG("add_ads_dag", schedule_interval="*/30 * * * *")

cities_mapping = {
    "source_omsk/ads": "Omsk",
    "source_ekb/ads": "Ekaterinburg",
    "source_kazan/ads": "Kazan",
    "source_rostov/ads": "Rostov-On-Don",
    "source_spb/ads": "Saint-Petersburg",
    "source_volgograd/ads": "Volgograd",
    "source_vorobyovy/ads": "Vorobyovy_Gory",
    "sources_elbrus/ads": "Elbrus_region",
    "sources_moscow/ads": "Moscow",
    "source_tumen/ads": "Tumen",
    "source_ufa/ads": "Ufa",
}


def calculate_hash(row):
    hash_string = f"{row['date']}{row['value']}{row['city']}"
    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


@add_ads_dag.task
def load_data():
    engine = create_engine(config.db_config.get_url())
    load_dttm = datetime.now()
    files = [x for x in os.listdir(config.dir_config.get_adds_dir()) if "xlsx" in x]
    logger.info(f"Получено {len(files)} файлов")
    for file in files:
        try:
            df = pd.read_excel(os.path.normpath(config.dir_config.get_adds_dir() + f"/{file}"))
            logger.debug(f"Читаем файл {file}", file=file)
            df = df[df["Период"].notna()]
            df = df.rename(
                columns={
                    "Период": "date",
                    "Значение": "value",
                    "Количество": "qty",
                    "Склад": "city",
                }
            )
            # df = df[df.groupby(df.columns[0]).cumcount() != 0]
            df["date"] = pd.to_datetime(df["date"], dayfirst=True)
            df['file_name'] = file

            df["load_dttm"] = load_dttm
            df["id"] = df.apply(calculate_hash, axis=1)

            data = df.to_dict("records")
            logger.debug(
                f"Всего записей в файле {len(data)}", file=file, len_=len(file)
            )

            # Создаем метаданные и таблицу
            metadata = MetaData()
            table = Table(
                "ads", metadata, autoload_with=engine, schema="raw"
            )
            # Создаем insert statement с обработкой конфликтов
            stmt = insert(table).values(data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["id"])

            # Выполняем запрос
            with engine.begin() as connection:
                result = connection.execute(stmt)
                logger.info(
                    f"Успешно вставлено {result.rowcount} записей",
                    insert_rows=result.rowcount,
                )

            logger.info("Файл обработан", file=file)
            time.sleep(1)

        except Exception as e:
            logger.error("Ошибка при обработке файлов ответов", e, file=file)
