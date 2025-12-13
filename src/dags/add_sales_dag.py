# Каждые 10 секунд
import hashlib
import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert

from src.config import config, logger
from src.croner import DAG

# cron (каждую минуту с 9 до 18 по будням)
add_sales_dag = DAG("add_sales_dag", schedule_interval="*/10 * * * *")


def calculate_hash(row):
    hash_string = (
        f"{row['sale_date']}{row['kkm']}{row['receipt_name']}{row['itenm_id']}"
    )
    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


@add_sales_dag.task
def load_data():
    engine = create_engine(config.db_config.get_url())
    load_dttm = datetime.now()
    files = [x for x in os.listdir(config.dir_config.get_sales_dir()) if "xlsx" in x]
    logger.info(f"Получено {len(files)} файлов")
    for file in files:
        try:
            df = pd.read_excel(config.dir_config.get_sales_dir() + "/" + file)
            logger.debug(f"Читаем файл {file}", file=file)
            df = df[df["Запасы.Сумма"].notna()]
            df["load_dttm"] = load_dttm

            df = df.rename(
                columns={
                    "Запасы.Сумма": "amount",
                    "Дата": "sale_date",
                    "Касса ККМ": "kkm",
                    "Идентификатор чека в очереди": "receipt_id",
                    "Чек ККМ": "receipt_name",
                    "Запасы.Номенклатура.Категория": "item_category",
                    "Запасы.Номенклатура.Код": "itenm_id",
                    "Запасы.Номенклатура": "item",
                    "Запасы.Количество": "qty",
                }
            )

            # Преобразуем дату из формата "13.10.2025 14:21:58" в datetime
            df["sale_date"] = pd.to_datetime(
                df["sale_date"], format="%d.%m.%Y %H:%M:%S"
            )

            # Добавляем хеш
            df["hash_256"] = df.apply(calculate_hash, axis=1)

            # Конвертируем DataFrame в список словарей
            data = df.to_dict("records")
            logger.debug(
                f"Всего записей в файле {len(data)}", file=file, len_=len(file)
            )

            # Создаем метаданные и таблицу
            metadata = MetaData()
            table = Table("receipts", metadata, autoload_with=engine, schema="raw")

            # Создаем insert statement с обработкой конфликтов
            stmt = insert(table).values(data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["hash_256"])

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
            logger.error("Ошибка при обработке файлов продаж", e, file=file)


@add_sales_dag.task
def call_load_offline_sales():
    """Задача для вызова SQL процедуры загрузки оффлайн продаж"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.begin() as connection:
            # Вызываем хранимую процедуру
            result = connection.execute(text("CALL dds.load_offline_sales();"))
            logger.info("Успешно выполнена процедура dds.load_offline_sales()")

    except Exception as e:
        logger.error("Ошибка при выполнении процедуры dds.load_offline_sales()", e)


@add_sales_dag.task
def call_load_sales_data():
    """Задача для вызова SQL процедуры загрузки оффлайн продаж"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.begin() as connection:
            # Вызываем хранимую процедуру
            result = connection.execute(text("call dds.load_sales_data();"))
            logger.info("Успешно выполнена процедура dds.load_sales_data()")

    except Exception as e:
        logger.error("Ошибка при выполнении процедуры dds.load_sales_data()", e)

@add_sales_dag.task
def call_load_sales_month_data():
    """Задача для вызова SQL процедуры загрузки оффлайн продаж"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.begin() as connection:
            # Вызываем хранимую процедуру
            result = connection.execute(text("call dds.load_sales_month_data();"))
            logger.info("Успешно выполнена процедура dds.load_sales_month_data()")

    except Exception as e:
        logger.error("Ошибка при выполнении процедуры dds.load_sales_month_data()", e)
