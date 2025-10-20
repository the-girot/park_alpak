# postgres_logger.py
import logging
from dataclasses import dataclass


@dataclass
class PostgresLoggerConfig:
    """Конфигурация для PostgreSQL логгера"""

    host: str = "localhost"
    port: int = 5432
    database: str = "logs_db"
    username: str = "postgres"
    password: str = "password"
    table_name: str = "application_logs"
    min_level: int = logging.INFO
    max_retries: int = 3
    batch_size: int = 10
    error_log_file: str = "error.log"  # Файл для логирования ошибок модуля
    max_error_log_size: int = 10 * 1024 * 1024  # 10MB
    error_log_backup_count: int = 5
