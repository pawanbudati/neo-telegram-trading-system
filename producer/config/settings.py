import os
from pathlib import Path
from pydantic import BaseModel, Field
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


class Settings(BaseModel):
    telegram_api_id: int = Field(default_factory=lambda: int(os.getenv("TELEGRAM_API_ID", "0")))
    telegram_api_hash: str = Field(default_factory=lambda: os.getenv("TELEGRAM_API_HASH", ""))
    telegram_phone: str = Field(default_factory=lambda: os.getenv("TELEGRAM_PHONE", ""))
    telegram_session_name: str = Field(default_factory=lambda: os.getenv("TELEGRAM_SESSION_NAME", "kotak_trader_session"))
    telegram_channels: list[str] = Field(default_factory=lambda: [
        ch.strip() for ch in os.getenv("TELEGRAM_CHANNELS", "").split(",") if ch.strip()
    ])
    gemini_api_key: str = Field(default_factory=lambda: os.getenv("GEMINI_API_KEY", ""))
    gemini_model: str = Field(default_factory=lambda: os.getenv("GEMINI_MODEL", "gemini-3.8-flash"))
    gemini_temperature: float = Field(default_factory=lambda: float(os.getenv("GEMINI_TEMPERATURE", "0.1")))
    gemini_timeout_seconds: int = Field(default_factory=lambda: int(os.getenv("GEMINI_TIMEOUT_SECONDS", "30")))
    message_history_context_size: int = Field(default_factory=lambda: int(os.getenv("MESSAGE_HISTORY_CONTEXT_SIZE", "5")))
    kafka_bootstrap_servers: str = Field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    kafka_topic_signals: str = Field(default_factory=lambda: os.getenv("KAFKA_TOPIC_SIGNALS", "kotak_trading_signals"))
    kafka_security_protocol: str = Field(default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    kafka_sasl_mechanism: str | None = Field(default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM"))
    kafka_sasl_username: str | None = Field(default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME"))
    kafka_sasl_password: str | None = Field(default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD"))


settings = Settings()
