from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    # Odoo connection
    odoo_url: str = Field(default="http://localhost:8069")
    odoo_db: str = Field(default="odoo")
    odoo_username: str = Field(default="admin")
    odoo_password: str = Field(default="admin")

    # Oracle JDE connection — used in Phase 4 only
    oracle_dsn: str = Field(default="")
    oracle_user: str = Field(default="")
    oracle_password: str = Field(default="")

    # Pipeline behavior
    dry_run: bool = Field(default=True)
    batch_size: int = Field(default=50)
    log_level: str = Field(default="INFO")
    mock_data_path: str = Field(default="mock_data/F0101.csv")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()