# Каждые 10 секунд
import hashlib

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert

from src.config import config, logger
from src.croner import DAG

# Словарь для соответствия русских названий городов английским
CITY_MAPPING = {
    "Москва": "Moscow",
    "Яхрома": "Moscow",
    "Санкт-Петербург": "Saint_Petersburg",
    "Ростов-на-Дону": "Rostov_On_Don",
    "Екатеринбург": "Ekaterinburg",
    "Казань": "Kazan",
    "Волгоград": "Volgograd",
    "Эльбрус": "Elbrus_region",
    "Чегем": "Chegem",
    "Омск": "Omsk",
    "Воробьевы Горы": "Vorobyovy_Gory",
    "Тюмень": "Tumen",
    "Уфа": "Ufa",
    "Саратов": "Saratov",
    "Набережные Челны": "Nab_chelny",
}

google_dag = DAG("google_dag", schedule_interval="*/40 * * * *")


def generate_id(channel, city, date_from):
    """
    Генерирует уникальный ID на основе хеша полей channel, city, dateFrom
    """
    # Создаем строку для хеширования
    data_string = f"{channel}_{city}_{date_from}"

    # Создаем хеш SHA-256
    hash_object = hashlib.sha256(data_string.encode("utf-8"))

    # Берем первые 12 символов хеша для удобства
    hash_hex = hash_object.hexdigest()

    return hash_hex


def convert_to_numeric(value):
    """
    Преобразует строку с числом в различных форматах в числовой формат float
    """
    if pd.isna(value) or value == "":
        return None

    # Если значение уже числовое, проверяем не было ли оно неправильно сконвертировано
    if isinstance(value, (int, float)):
        # Если число слишком большое (например, 986268 вместо 9862,68),
        # возможно оно было неправильно сконвертировано

        return float(value)

    value_str = str(value).strip()
    original_value = value_str

    # Удаляем все пробелы (и неразрывные тоже)
    value_str = value_str.replace("\xa0", "").replace(" ", "")

    # Определяем формат числа
    if "." in value_str and "," in value_str:
        # Формат: 16,359.43 (запятая - тысячи, точка - десятичные)
        value_str = value_str.replace(",", "")
    elif "," in value_str:
        # Формат: 16359,43 (запятая - десятичные)
        # Проверяем позицию запятой
        comma_pos = value_str.rfind(",")
        if (
            comma_pos == len(value_str) - 3
        ):  # Запятая на позиции десятичного разделителя
            value_str = value_str.replace(",", ".")
        else:
            # Запятая в другой позиции - удаляем её
            value_str = value_str.replace(",", "")

    try:
        result = float(value_str)
        # print(f"Преобразовано: '{original_value}' -> {result}")
        return result
    except (ValueError, TypeError) as e:
        print(f"Ошибка преобразования: '{original_value}' -> '{value_str}': {e}")
        return None


def get_google_sheet_data():
    """
    Получает данные из Google Sheets используя API и преобразует в нужный формат
    """
    # Настройка авторизации
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    SERVICE_ACCOUNT_FILE = "cert.json"
    sheet_url = "https://docs.google.com/spreadsheets/d/1orw8ZNfjvzofxnlzHlKQEuiKSUZlvtkDPHWeoIz5BTY/edit"

    try:
        # Авторизация
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        client = gspread.authorize(creds)

        # Открываем таблицу по URL
        sheet = client.open_by_url(sheet_url)
        sheets = []
        worksheets = sheet.worksheets()
        # print("Доступные листы:")
        for ws in worksheets:
            # print(f"- {ws.title} (id: {ws.id})")
            sheets.append(ws.id)

        all_transformed_data = []

        # Получаем данные с нужных листов
        for i in sheets:
            try:
                worksheet = sheet.get_worksheet_by_id(i)

                # ВАЖНО: получаем сырые значения как строки
                raw_data = worksheet.get_all_values()

                # Первая строка - заголовки
                headers = raw_data[0]

                # Остальные строки - данные
                rows = raw_data[1:]

                # Создаем DataFrame с сырыми строковыми данными
                df = pd.DataFrame(rows, columns=headers)

                print(f"\nЛист {i}: получено {len(df)} строк")
                # print("Пример сырых данных:")
                # print(df.head(2))

                # Определяем название канала из первой строки
                if not df.empty:
                    channel_name = (
                        df.iloc[0]["Канал"] if "Канал" in df.columns else f"Channel_{i}"
                    )

                    # Преобразуем данные
                    transformed_data = transform_data(df, channel_name)
                    all_transformed_data.extend(transformed_data)

            except Exception as e:
                print(f"Ошибка при обработке листа {i}: {e}")
                continue

        # Создаем итоговый DataFrame
        if all_transformed_data:
            engine = create_engine(config.db_config.get_url())
            final_df = pd.DataFrame(all_transformed_data)

            # Проверяем уникальность ID
            unique_ids = final_df["id"].nunique()
            total_records = len(final_df)
            if unique_ids == total_records:
                print(f"✓ Все ID уникальны ({unique_ids}/{total_records})")
            else:
                print(f"⚠ Внимание: есть дубликаты ID ({unique_ids}/{total_records})")

            # Проверяем данные
            print(f"\nТип данных в колонке budget: {final_df['budget'].dtype}")
            print("Примеры значений budget:")
            for i, (idx, row) in enumerate(final_df.head(10).iterrows()):
                print(f"  {i + 1}. {row['city']}: {row['budget']}")

            data = final_df.to_dict("records")
            # Создаем метаданные и таблицу
            metadata = MetaData()
            table = Table("budgets", metadata, autoload_with=engine, schema="raw")
            # print(data)
            # Создаем insert statement с обработкой конфликтов
            stmt = insert(table).values(data)
            # stmt = stmt.on_conflict_do_update(index_elements=["id"])
            stmt = stmt.on_conflict_do_update(
                constraint="budgets_pkey",  # или укажите название первичного ключа
                set_={
                    "channel": stmt.excluded.channel,
                    "dateFrom": stmt.excluded.dateFrom,
                    "dateTo": stmt.excluded.dateTo,
                    "city": stmt.excluded.city,
                    "budget": stmt.excluded.budget,
                },
            )
            # Выполняем запрос
            with engine.begin() as connection:
                result = connection.execute(stmt)
                print(f"Успешно вставлено {result.rowcount} записей")

            print("\nИтоговый файл создан: transformed_budget_data.csv")
            print(f"Всего записей: {len(final_df)}")

            return final_df
        else:
            print("Нет данных для преобразования")
            return None

    except Exception as e:
        logger.error("Ошибка", exc_info=str(e))
        return None


def transform_data(df, channel_name):
    """
    Преобразует данные из широкого формата в длинный
    """
    transformed_data = []

    # Определяем колонки с городами (исключаем служебные колонки)
    city_columns = [
        col for col in df.columns if col not in ["Канал", "Неделя", "Дата с", "Дата по"]
    ]

    for _, row in df.iterrows():
        date_from = row["Дата с"]
        date_to = row["Дата по"]

        for city_ru in city_columns:
            budget = row[city_ru]

            # Пропускаем пустые значения бюджета
            if pd.isna(budget) or budget == "" or budget is None:
                continue

            # Преобразуем бюджет в числовой формат
            budget_numeric = convert_to_numeric(budget)

            # Если преобразование не удалось, пропускаем запись
            if budget_numeric is None:
                print(f"Пропущено значение: '{budget}' для города {city_ru}")
                continue

            # Преобразуем название города
            city_en = CITY_MAPPING.get(city_ru, city_ru)

            # Генерируем ID
            record_id = generate_id(channel_name, city_en, date_from)

            transformed_data.append(
                {
                    "id": record_id,
                    "channel": channel_name,
                    "dateFrom": date_from,
                    "dateTo": date_to,
                    "city": city_en,
                    "budget": budget_numeric,
                }
            )

    return transformed_data


@google_dag.task
def main():
    result_df = get_google_sheet_data()
    if result_df is not None:
        logger.info("\nДанные успешно преобразованы и сохранены!")
        print(f"Общее количество записей: {len(result_df)}")
    else:
        logger.error("Не удалось получить или преобразовать данные")


@google_dag.task
def call_load_offline_sales():
    """Задача для вызова SQL процедуры загрузки оффлайн продаж"""
    try:
        engine = create_engine(config.db_config.get_url())

        with engine.begin() as connection:
            # Вызываем хранимую процедуру
            result = connection.execute(text("CALL dds.load_prepared_budgets();"))
            logger.info("Успешно выполнена процедура dds.load_prepared_budgets()")

    except Exception as e:
        logger.error("Ошибка при выполнении процедуры dds.load_prepared_budgets()", e)
