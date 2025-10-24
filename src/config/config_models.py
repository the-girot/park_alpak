from pydantic import BaseModel

from .logger import PostgresLoggerConfig


class DbConfig(BaseModel):
    db_host: str
    db_name: str
    db_user: str
    db_pass: str
    db_port: str

    def get_url(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_migrations_url(self) -> str:
        return self.get_url() + "?async_fallback=True"


class Config(BaseModel):
    db_config: DbConfig
    logger_config: PostgresLoggerConfig
