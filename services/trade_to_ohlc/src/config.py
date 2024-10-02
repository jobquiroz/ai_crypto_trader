from typing import Optional
from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):
    kafka_broker_address: str
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_consumer_group: str
    ohlcv_window_seconds: int

    # One way:
    class Config:
        env_file = ".env"
    # Another method:
    # model_config = {"env_file": ".env"}

config = AppConfig()