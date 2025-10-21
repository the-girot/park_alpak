import logging
import os

from dotenv import load_dotenv

from .config_models import Config, DbConfig
from .logger import PostgresLoggerConfig

load_dotenv()


def get_config() -> Config:
    db_host: str = os.environ.get("DB_HOST")
    db_name: str = os.environ.get("DB_NAME")
    db_user: str = os.environ.get("DB_USER")
    db_pass: str = os.environ.get("DB_PASS")
    db_port: str = os.environ.get("DB_PORT")

    logger_db_name: str = os.environ.get("logger_db_name")

    return Config(
        db_config=DbConfig(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_pass=db_pass,
            db_port=db_port,
        ),
        logger_config=PostgresLoggerConfig(
            host=db_host,
            port=db_port,
            database=logger_db_name,
            username=db_user,
            password=db_pass,
            table_name="app_logs",
            min_level=logging.DEBUG,
            error_log_file="postgres_logger_errors.log",  # Специальный файл для ошибок модуля
            max_error_log_size=5 * 1024 * 1024,  # 5MB
            error_log_backup_count=3,
        ),
    )


config = PostgresLoggerConfig(
    host="localhost",
    port=5432,
    database="logs_db",
    username="postgres",
    password="your_password",
    table_name="app_logs",
    min_level=logging.DEBUG,
    error_log_file="postgres_logger_errors.log",  # Специальный файл для ошибок модуля
    max_error_log_size=5 * 1024 * 1024,  # 5MB
    error_log_backup_count=3,
)
