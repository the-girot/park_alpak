from .config_loader import get_config
from .logger import PostgresLogger

config = get_config()
pg_logger = PostgresLogger(config)
logger = pg_logger.get_logger("my_application")
