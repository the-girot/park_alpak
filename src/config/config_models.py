from pydantic import BaseModel

from .logger import PostgresLoggerConfig


class DbConfig(BaseModel):
    db_host: str
    db_name: str
    db_user: str
    db_pass: str
    db_port: str

    def get_url(self) -> str:
        return f"postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_migrations_url(self) -> str:
        return self.get_url() + "?async_fallback=True"

    def get_config(self):
        return {
            "host": self.db_host,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_pass,
            "port": self.db_port,
        }


class DirConfig(BaseModel):
    base_dir: str

    def get_sales_dir(self):
        return self.base_dir + "/sales_raw"

    def get_adds_dir(self):
        return self.base_dir + "/ads"


class Config(BaseModel):
    db_config: DbConfig
    logger_config: PostgresLoggerConfig
    dir_config: DirConfig
    API_KEY: str
    TG_TOKEN: str
    CHAT_ID: str
    timepad_api: str
