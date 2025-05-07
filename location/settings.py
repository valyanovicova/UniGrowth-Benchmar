from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    """
    Класс для хранения настроек приложения, загружаемых из переменных окружения.
    """
    FILE_PATH: Path
    GEOCODING_API_KEY: str

    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()
