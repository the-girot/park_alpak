# postgres_logger.py
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    def __str__(self):
        return self.name


@dataclass
class PostgresLoggerConfig:
    """Конфигурация для PostgreSQL логгера"""

    host: str
    port: int
    database: str
    username: str
    password: str
    table_name: str = "logs"
    schema: str = "dqc"
    batch_size: int = 100
    max_retries: int = 3
    error_log_file: str = "postgres_logger_errors.log"
    max_error_log_size: int = 10 * 1024 * 1024  # 10MB
    error_log_backup_count: int = 5


class LogRecord:
    """Класс для представления записи лога"""

    def __init__(
        self,
        name: str,
        level: LogLevel,
        message: str,
        timestamp: Optional[datetime] = None,
        module: Optional[str] = None,
        function_name: Optional[str] = None,
        line_number: Optional[int] = None,
        exc_info: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.level = level
        self.message = message
        self.timestamp = timestamp or datetime.now()
        self.module = module
        self.function_name = function_name
        self.line_number = line_number
        self.exc_info = exc_info
        self.extra_data = extra_data or {}
        self.process_id = self._get_process_id()
        self.thread_id = self._get_thread_id()
        self.thread_name = self._get_thread_name()

    def _get_process_id(self) -> int:
        """Получение ID процесса"""
        try:
            import os

            return os.getpid()
        except:
            return 0

    def _get_thread_id(self) -> int:
        """Получение ID потока"""
        try:
            return threading.get_ident()
        except:
            return 0

    def _get_thread_name(self) -> str:
        """Получение имени потока"""
        try:
            return threading.current_thread().name
        except:
            return "unknown"
