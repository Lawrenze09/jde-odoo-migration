"""
config/settings.py

Role in pipeline: Foundation layer — loaded first by every other module.
Reads all environment variables from .env once, validates their types,
and exposes them as a single typed Settings object.

Input:  .env file at project root (or system environment variables)
Output: Settings instance accessible via get_settings()
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """
    Central configuration object for the JDE-to-Odoo migration pipeline.
    All fields map directly to environment variables in .env.
    Pydantic validates types automatically — a non-boolean DRY_RUN
    will raise a ValidationError at startup, not silently mid-pipeline.
    """

    # --- Odoo connection ---
    odoo_url: str = Field(default="http://localhost:8069")
    odoo_db: str = Field(default="odoo")
    odoo_username: str = Field(default="admin")
    odoo_password: str = Field(default="admin")

    # --- Oracle JDE connection — not used until Phase 4 ---
    oracle_dsn: str = Field(default="")
    oracle_user: str = Field(default="")
    oracle_password: str = Field(default="")

    # --- Pipeline behavior ---
    # dry_run defaults to True — safety first.
    # A migration tool that defaults to writing production data is dangerous.
    # Developer must explicitly set DRY_RUN=false to enable live writes.
    dry_run: bool = Field(default=True)

    # Number of records sent to Odoo per API call.
    # 50 is safe for most Odoo instances — raise if performance allows.
    batch_size: int = Field(default=50)

    log_level: str = Field(default="INFO")
    mock_data_path: str = Field(default="mock_data/F0101.csv")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Allows DRY_RUN in .env to map to dry_run in Python
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """
    Return the singleton Settings instance for the entire pipeline run.

    Uses lru_cache so the .env file is read exactly once regardless of
    how many modules call this function. All modules share the same
    Settings object — changes to .env after startup are not picked up.

    Returns:
        Settings: Validated configuration object with all pipeline settings.
    """
    return Settings()
