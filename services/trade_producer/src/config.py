from typing import Optional
from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):
    kafka_broker_address: str
    kafka_topic: str
    product_id: str

    live_or_historical: Optional[str] = None
    last_n_days: Optional[int] = None

    # One way:
    class Config:
        env_file = ".env"
    # Another method:
    # model_config = {"env_file": ".env"}


config = AppConfig()