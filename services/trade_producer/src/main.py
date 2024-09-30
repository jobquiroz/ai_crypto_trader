
from typing import List

from loguru import logger
from quixstreams import Application

#from src.kraken_websocket_api import (KrakenWebsocketAPI, Trade)
#from kraken_websocket_api import (KrakenWebsocketAPI, Trade)

from src.trade_data_source import Trade, TradeSource

def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    trade_data_source: TradeSource,
):
    """
    Reads trades from the Kraken Websocket API and saves them in the given `kafka_topic`

    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_topic (str): The Kafka topic to save the trades
        trade_data_source (TradeSource): The source of the trades

    Returns:
        None
    """   
    # Create an Application instance with Kafka config
    app = Application(broker_address=kafka_broker_address)

    # Define a topic "my_topic" with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create a Producer instance
    with app.get_producer() as producer:

        while not trade_data_source.is_done():
            
            trades : List[Trade] = trade_data_source.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                # transform it into a sequence of bytes
                message = topic.serialize(
                    key = trade.product_id,   # Key is a way to partition the data
                    value = trade.model_dump())  # Transform pydantic model to dictionary
                
                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.debug(f"Pushed trade to Kafka: {trade}")

if __name__ == "__main__":

    # Load configuration. 
    #from src.config import config
    from src.config import config
    from src.trade_data_source.kraken_websocket_api import KrakenWebsocketAPI

    kraken_api = KrakenWebsocketAPI(product_id=config.product_id)

    produce_trades(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic,
        trade_data_source = kraken_api,
    )