# postgres_logger.py
import logging

from .src.handler import ErrorFileHandler, PostgresHandler
from .src.schema import PostgresLoggerConfig


class PostgresLogger:
    """Основной класс для логирования в PostgreSQL"""

    def __init__(self, config: PostgresLoggerConfig):
        self.config = config
        self.error_handler = ErrorFileHandler(config)
        self.handler = None
        self._setup_logger()

    def _setup_logger(self):
        """Настройка логгера"""
        try:
            self.handler = PostgresHandler(self.config, self.error_handler)
            self.handler.setLevel(self.config.min_level)

            # Форматтер для консольного вывода (опционально)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            self.handler.setFormatter(formatter)

        except Exception:
            self.error_handler.log_error("Ошибка при настройке логгера", exc_info=True)
            raise

    def get_logger(self, name: str = None) -> logging.Logger:
        """Получение логгера с указанным именем"""
        try:
            logger = logging.getLogger(name)
            logger.setLevel(self.config.min_level)

            # Убираем дублирующиеся обработчики
            if not any(isinstance(h, PostgresHandler) for h in logger.handlers):
                logger.addHandler(self.handler)

            return logger
        except Exception:
            self.error_handler.log_error(
                f"Ошибка при создании логгера {name}", exc_info=True
            )
            # Возвращаем базовый логгер в случае ошибки
            return logging.getLogger(name)

    def close(self):
        """Закрытие логгера"""
        try:
            if self.handler:
                self.handler.close()
        except Exception:
            self.error_handler.log_error("Ошибка при закрытии логгера", exc_info=True)
