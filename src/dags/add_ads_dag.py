# Каждые 10 секунд
import hashlib
import os
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert

from src.config import config, logger
from src.croner import DAG

# cron (каждую минуту с 9 до 18 по будням)
add_ads_dag = DAG("add_ads_dag", schedule_interval="0 * * * *")


def calculate_hash(row):
    hash_string = (
        f"{row['sale_date']}{row['kkm']}{row['receipt_name']}{row['itenm_id']}"
    )
    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


@add_ads_dag.task
def load_data():
    engine = create_engine(config.db_config.get_url())
    load_dttm = datetime.now()
    files = [x for x in os.listdir("N:\\source_cherkessk\\sales_raw") if "xlsx" in x]
    logger.info(f"Получено {len(files)} файлов")
    for file in files:
        try:
            df = pd.read_excel(f"N:\\source_cherkessk\\sales_raw\\{file}")
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
