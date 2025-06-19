from pydantic_settings import BaseSettings
from typing import List
import os
from functools import lru_cache


class Settings(BaseSettings):
    # PostgreSQL Connection Settings
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: str = "5432"
    DB_SCHEMAS: List[str] = ["public"]

    # Connection pool settings
    DB_MIN_POOL_SIZE: int = 10
    DB_MAX_POOL_SIZE: int = 50
    DB_POOL_TIMEOUT: int = 30

    # LLM Configuration
    LLM_API_KEY: str
    LLM_MODEL: str = "gpt-4.1-mini"
    LLM_TEMPERATURE: float = 0.2

    # Agent Configuration
    MAX_CONVERSATIONS: int = 40

    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    LOG_LEVEL: str = "info"

    # METADATA Configuration
    METADATA_AVAILABLE: bool 
    DD_TABLE_NAME_ONLY: str 
    DD_COLUMN_NAME_ONLY: str 

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Parse schemas from comma-separated string to list
        if isinstance(self.DB_SCHEMAS, str):
            self.DB_SCHEMAS = [schema.strip() for schema in self.DB_SCHEMAS.split(",")]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache
def get_settings() -> Settings:
    """
    Get application settings with caching to avoid repeated parsing.
    Returns a singleton Settings instance.
    """
    return Settings()