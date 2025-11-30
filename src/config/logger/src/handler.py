import json
import threading
import time
import traceback
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import psycopg2

from .schema import LogLevel, LogRecord, PostgresLoggerConfig


class JSONEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки datetime объектов"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class ErrorFileHandler:
    """Обработчик для записи ошибок модуля в файл"""

    def __init__(self, config: PostgresLoggerConfig):
        self.config = config
        self._lock = threading.Lock()
        self._setup_error_log_file()

    def _setup_error_log_file(self):
        """Настройка файла для ошибок"""
        try:
            # Создаем директорию если нужно
            import os

            os.makedirs(os.path.dirname(self.config.error_log_file), exist_ok=True)
        except:
            pass

    def _rotate_if_needed(self):
        """Ротация лог-файла при превышении размера"""
        try:
            import os

            if os.path.exists(self.config.error_log_file):
                file_size = os.path.getsize(self.config.error_log_file)
                if file_size >= self.config.max_error_log_size:
                    self._rotate_files()
        except Exception:
            pass

    def _rotate_files(self):
        """Ротация файлов логов"""
        import os

        try:
            # Удаляем самый старый backup
            oldest_backup = (
                f"{self.config.error_log_file}.{self.config.error_log_backup_count}"
            )
            if os.path.exists(oldest_backup):
                os.remove(oldest_backup)

            # Сдвигаем существующие backups
            for i in range(self.config.error_log_backup_count - 1, 0, -1):
                old_name = f"{self.config.error_log_file}.{i}"
                new_name = f"{self.config.error_log_file}.{i + 1}"
                if os.path.exists(old_name):
                    os.rename(old_name, new_name)

            # Переименовываем текущий файл
            if os.path.exists(self.config.error_log_file):
                os.rename(self.config.error_log_file, f"{self.config.error_log_file}.1")
        except Exception:
            pass

    def log_error(
        self,
        message: str,
        exc_info: Optional[Exception] = None,
        context: Optional[Dict] = None,
    ):
        """Логирование ошибки модуля"""
        with self._lock:
            try:
                self._rotate_if_needed()

                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_entry = f"{timestamp} - postgres_logger - ERROR - {message}"

                if context:
                    # Сериализуем контекст с помощью кастомного encoder
                    try:
                        context_str = json.dumps(
                            context, cls=JSONEncoder, ensure_ascii=False
                        )
                        log_entry += f" | Context: {context_str}"
                    except Exception as json_error:
                        log_entry += f" | Context serialization error: {json_error}"

                if exc_info:
                    exc_text = "".join(
                        traceback.format_exception(
                            type(exc_info), exc_info, exc_info.__traceback__
                        )
                    )
                    log_entry += f"\n{exc_text}"

                with open(self.config.error_log_file, "a", encoding="utf-8") as f:
                    f.write(log_entry + "\n\n")

            except Exception:
                # Если не можем записать в лог, выводим в stderr
                import sys

                print(
                    f"CRITICAL: Cannot write to error log: {message}", file=sys.stderr
                )
                if exc_info:
                    traceback.print_exc()


class PostgresHandler:
    """Обработчик логов для PostgreSQL"""

    def __init__(self, config: PostgresLoggerConfig, error_handler: ErrorFileHandler):
        self.config = config
        self.error_handler = error_handler
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._closed = False
        self.json_encoder = JSONEncoder()  # Создаем экземпляр encoder

        # Запускаем фоновый поток для периодической записи
        self._flush_thread = threading.Thread(
            target=self._background_flush, daemon=True
        )
        self._flush_thread.start()

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
        except Exception as e:
            self.error_handler.log_error(
                "Ошибка подключения к PostgreSQL",
                exc_info=e,
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
        CREATE TABLE IF NOT EXISTS {self.config.schema}.{self.config.table_name} (
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
        
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON {self.config.schema}.{self.config.table_name} (timestamp);
        CREATE INDEX IF NOT EXISTS idx_logs_level ON {self.config.schema}.{self.config.table_name} (level);
        CREATE INDEX IF NOT EXISTS idx_logs_logger_name ON {self.config.schema}.{self.config.table_name} (logger_name);
        """

        for attempt in range(self.config.max_retries):
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(create_table_sql)
                        conn.commit()
                break
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    self.error_handler.log_error(
                        "Ошибка при создании таблицы логов",
                        exc_info=e,
                        context={"table_name": self.config.table_name},
                    )

    def _format_record(self, record: LogRecord) -> Dict[str, Any]:
        """Форматирование записи лога для базы данных"""
        try:
            # Сериализуем extra_data с помощью кастомного encoder
            extra_data_json = None
            if record.extra_data:
                extra_data_json = self.json_encoder.encode(record.extra_data)

            return {
                "timestamp": record.timestamp,
                "level": str(record.level),
                "logger_name": record.name,
                "message": record.message,
                "module": record.module,
                "function_name": record.function_name,
                "line_number": record.line_number,
                "process_id": record.process_id,
                "thread_id": record.thread_id,
                "thread_name": record.thread_name,
                "exc_info": record.exc_info,
                "extra_data": extra_data_json,
            }
        except Exception as e:
            self.error_handler.log_error(
                "Ошибка при форматировании записи лога",
                exc_info=e,
                context={"record": str(record.__dict__)},
            )
            # Возвращаем базовую структуру без extra_data в случае ошибки
            return {
                "timestamp": record.timestamp,
                "level": str(record.level),
                "logger_name": record.name,
                "message": record.message,
                "module": record.module,
                "function_name": record.function_name,
                "line_number": record.line_number,
                "process_id": record.process_id,
                "thread_id": record.thread_id,
                "thread_name": record.thread_name,
                "exc_info": record.exc_info,
                "extra_data": None,
            }

    def emit(self, record: LogRecord):
        """Добавление записи в буфер для последующей записи"""
        if self._closed:
            return

        try:
            log_entry = self._format_record(record)

            with self._lock:
                self._buffer.append(log_entry)

                # Пакетная вставка при достижении размера буфера
                if len(self._buffer) >= self.config.batch_size:
                    self._flush_buffer()

        except Exception as e:
            self.error_handler.log_error(
                "Ошибка при форматировании записи лога",
                exc_info=e,
                context={"record": str(record.__dict__)},
            )

    def _flush_buffer(self):
        """Пакетная вставка логов из буфера"""
        if not self._buffer or self._closed:
            return

        insert_sql = f"""
        INSERT INTO {self.config.schema}.{self.config.table_name} 
        (timestamp, level, logger_name, message, module, function_name, 
         line_number, process_id, thread_id, thread_name, exc_info, extra_data)
        VALUES (%(timestamp)s, %(level)s, %(logger_name)s, %(message)s, 
                %(module)s, %(function_name)s, %(line_number)s, %(process_id)s, 
                %(thread_id)s, %(thread_name)s, %(exc_info)s, %(extra_data)s)
        """

        buffer_to_flush = []
        with self._lock:
            if self._buffer:
                buffer_to_flush = self._buffer.copy()
                self._buffer.clear()

        if not buffer_to_flush:
            return

        for attempt in range(self.config.max_retries):
            try:
                with self._get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.executemany(insert_sql, buffer_to_flush)
                        conn.commit()
                break

            except Exception as e:
                error_context = {
                    "attempt": attempt + 1,
                    "max_retries": self.config.max_retries,
                    "buffer_size": len(buffer_to_flush),
                    "table": self.config.table_name,
                }

                if attempt == self.config.max_retries - 1:
                    self.error_handler.log_error(
                        f"Не удалось записать логи после {self.config.max_retries} попыток",
                        exc_info=e,
                        context=error_context,
                    )
                    # Возвращаем записи обратно в буфер для следующей попытки
                    with self._lock:
                        self._buffer.extend(buffer_to_flush)
                else:
                    self.error_handler.log_error(
                        f"Попытка {attempt + 1} записи логов не удалась",
                        exc_info=e,
                        context=error_context,
                    )

    def _background_flush(self):
        """Фоновая периодическая запись логов"""
        while not self._closed:
            time.sleep(30)  # Записываем каждые 30 секунд
            if self._buffer:
                self._flush_buffer()

    def close(self):
        """Закрытие обработчика с сохранением оставшихся логов"""
        self._closed = True
        try:
            if self._buffer:
                self._flush_buffer()
        except Exception as e:
            self.error_handler.log_error("Ошибка при закрытии обработчика", exc_info=e)


class Logger:
    """Класс логгера"""

    def __init__(self, name: str, handler: PostgresHandler):
        self.name = name
        self.handler = handler
        self.extra_data: Dict[str, Any] = {}

    def _get_caller_info(self):
        """Получение информации о вызывающем коде"""
        try:
            import inspect

            frame = inspect.currentframe()
            # Поднимаемся на 3 фрейма: _log -> debug/info/etc. -> вызывающий код
            for _ in range(3):
                frame = frame.f_back
                if frame is None:
                    break

            if frame:
                module = frame.f_globals.get("__name__", "unknown")
                function_name = frame.f_code.co_name
                line_number = frame.f_lineno
                return module, function_name, line_number
        except:
            pass
        return "unknown", "unknown", 0

    def _log(
        self,
        level: LogLevel,
        message: str,
        exc_info: Optional[Exception] = None,
        **kwargs,
    ):
        """Основной метод логирования"""

        module, function_name, line_number = self._get_caller_info()

        # Форматируем исключение если есть
        exc_text = None
        if exc_info:
            exc_text = "".join(
                traceback.format_exception(
                    type(exc_info), exc_info, exc_info.__traceback__
                )
            )

        # Объединяем дополнительные данные
        extra_data = {**self.extra_data, **kwargs}

        record = LogRecord(
            name=self.name,
            level=level,
            message=message,
            module=module,
            function_name=function_name,
            line_number=line_number,
            exc_info=exc_text,
            extra_data=extra_data,
        )

        self.handler.emit(record)

    def debug(self, message: str, **kwargs):
        """Логирование на уровне DEBUG"""
        self._log(LogLevel.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        """Логирование на уровне INFO"""
        self._log(LogLevel.INFO, message, **kwargs)
        print(message)

    def warning(self, message: str, **kwargs):
        """Логирование на уровне WARNING"""
        self._log(LogLevel.WARNING, message, **kwargs)

    def error(self, message: str, exc_info: Optional[Exception] = None, **kwargs):
        """Логирование на уровне ERROR"""
        self._log(LogLevel.ERROR, message, exc_info, **kwargs)
        print(message, exc_info)

    def critical(self, message: str, exc_info: Optional[Exception] = None, **kwargs):
        """Логирование на уровне CRITICAL"""
        self._log(LogLevel.CRITICAL, message, exc_info, **kwargs)

    def set_extra(self, **kwargs):
        """Установка дополнительных данных для всех сообщений логгера"""
        self.extra_data.update(kwargs)
