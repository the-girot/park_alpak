# postgres_logger.py
import json
import logging
import threading
from datetime import datetime
from typing import Dict

import psycopg2

from .schema import PostgresLoggerConfig


class ErrorFileHandler:
    """Обработчик для записи ошибок модуля в файл"""

    def __init__(self, config: PostgresLoggerConfig):
        self.config = config
        self._setup_error_logger()

    def _setup_error_logger(self):
        """Настройка логгера для ошибок модуля"""
        # Создаем отдельный логгер для ошибок модуля
        self.error_logger = logging.getLogger("postgres_logger_errors")
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.propagate = False  # Отключаем распространение

        # Обработчик для файла с ротацией
        file_handler = logging.handlers.RotatingFileHandler(
            filename=self.config.error_log_file,
            maxBytes=self.config.max_error_log_size,
            backupCount=self.config.error_log_backup_count,
            encoding="utf-8",
        )

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s\n%(exc_info)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)

        # Очищаем существующие обработчики и добавляем новый
        self.error_logger.handlers.clear()
        self.error_logger.addHandler(file_handler)

    def log_error(self, message: str, exc_info=None, context: Dict = None):
        """Логирование ошибки модуля"""
        extra_info = ""
        if context:
            extra_info = f" | Context: {context}"

        full_message = f"{message}{extra_info}"

        if exc_info:
            self.error_logger.error(full_message, exc_info=exc_info)
        else:
            self.error_logger.error(full_message)


class PostgresHandler(logging.Handler):
    """Обработчик логов для PostgreSQL"""

    def __init__(self, config: PostgresLoggerConfig, error_handler: ErrorFileHandler):
        super().__init__()
        self.config = config
        self.error_handler = error_handler
        self._buffer = []
        self._lock = threading.Lock()
        self._setup_log_table()

    def _get_connection(self):
        """Создание подключения к PostgreSQL"""
        try:
            return psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connect_timeout=10,
            )
        except Exception:
            self.error_handler.log_error(
                "Ошибка подключения к PostgreSQL",
                exc_info=True,
                context={
                    "host": self.config.host,
                    "port": self.config.port,
                    "database": self.config.database,
                },
            )
            raise

    def _setup_log_table(self):
        """Создание таблицы для логов, если она не существует"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.table_name} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR(10) NOT NULL,
            logger_name VARCHAR(255) NOT NULL,
            message TEXT NOT NULL,
            module VARCHAR(255),
            function_name VARCHAR(255),
            line_number INTEGER,
            process_id INTEGER,
            thread_id BIGINT,
            thread_name VARCHAR(255),
            exc_info TEXT,
            extra_data JSONB
        );
        
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON {self.config.table_name} (timestamp);
        CREATE INDEX IF NOT EXISTS idx_logs_level ON {self.config.table_name} (level);
        CREATE INDEX IF NOT EXISTS idx_logs_logger_name ON {self.config.table_name} (logger_name);
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    conn.commit()
        except Exception:
            self.error_handler.log_error(
                "Ошибка при создании таблицы логов",
                exc_info=True,
                context={"table_name": self.config.table_name},
            )

    def emit(self, record):
        """Запись лога в базу данных"""
        try:
            log_entry = self._format_record(record)

            with self._lock:
                self._buffer.append(log_entry)

                # Пакетная вставка при достижении размера буфера
                if len(self._buffer) >= self.config.batch_size:
                    self._flush_buffer()

        except Exception:
            self.error_handler.log_error(
                "Ошибка при форматировании записи лога",
                exc_info=True,
                context={"record": str(record)},
            )

    def _format_record(self, record):
        """Форматирование записи лога для базы данных"""
        # Преобразование исключения в строку
        exc_info = None
        if record.exc_info:
            exc_info = self.formatException(record.exc_info)

        # Преобразование extra данных в JSON
        extra_data = {}
        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            ]:
                try:
                    json.dumps(value)  # Проверка возможности сериализации
                    extra_data[key] = value
                except (TypeError, ValueError):
                    extra_data[key] = str(value)

        return {
            "timestamp": datetime.fromtimestamp(record.created),
            "level": record.levelname,
            "logger_name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function_name": record.funcName,
            "line_number": record.lineno,
            "process_id": record.process,
            "thread_id": record.thread,
            "thread_name": record.threadName,
            "exc_info": exc_info,
            "extra_data": json.dumps(extra_data) if extra_data else None,
        }

    def _flush_buffer(self):
        """Пакетная вставка логов из буфера"""
        if not self._buffer:
            return

        insert_sql = f"""
        INSERT INTO {self.config.table_name} 
        (timestamp, level, logger_name, message, module, function_name, 
         line_number, process_id, thread_id, thread_name, exc_info, extra_data)
        VALUES (%(timestamp)s, %(level)s, %(logger_name)s, %(message)s, 
                %(module)s, %(function_name)s, %(line_number)s, %(process_id)s, 
                %(thread_id)s, %(thread_name)s, %(exc_info)s, %(extra_data)s)
        """

        for attempt in range(self.config.max_retries):
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.executemany(insert_sql, self._buffer)
                        conn.commit()

                # Очистка буфера после успешной вставки
                self._buffer.clear()
                break

            except Exception:
                error_context = {
                    "attempt": attempt + 1,
                    "max_retries": self.config.max_retries,
                    "buffer_size": len(self._buffer),
                    "table": self.config.table_name,
                }

                if attempt == self.config.max_retries - 1:
                    self.error_handler.log_error(
                        f"Не удалось записать логи после {self.config.max_retries} попыток",
                        exc_info=True,
                        context=error_context,
                    )
                else:
                    self.error_handler.log_error(
                        f"Попытка {attempt + 1} записи логов не удалась",
                        exc_info=True,
                        context=error_context,
                    )

    def close(self):
        """Закрытие обработчика с сохранением оставшихся логов"""
        try:
            with self._lock:
                if self._buffer:
                    self._flush_buffer()
            super().close()
        except Exception:
            self.error_handler.log_error(
                "Ошибка при закрытии обработчика", exc_info=True
            )
