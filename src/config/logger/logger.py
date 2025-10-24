# postgres_logger.py
from typing import Dict

from .src.handler import ErrorFileHandler, Logger, PostgresHandler
from .src.schema import PostgresLoggerConfig


class PostgresLogger:
    """Основной класс для логирования в PostgreSQL"""

    def __init__(self, config: PostgresLoggerConfig):
        self.config = config
        self.error_handler = ErrorFileHandler(config)
        self.handler = PostgresHandler(config, self.error_handler)
        self._loggers: Dict[str, Logger] = {}

    def get_logger(self, name: str = "root") -> Logger:
        """Получение логгера с указанным именем"""
        if name not in self._loggers:
            self._loggers[name] = Logger(name, self.handler)
        return self._loggers[name]

    def close(self):
        """Закрытие логгера"""
        try:
            if self.handler:
                self.handler.close()
        except Exception as e:
            self.error_handler.log_error("Ошибка при закрытии логгера", exc_info=e)
