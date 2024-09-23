
from quixstreams import Application
from datetime import timedelta
from loguru import logger


def init_ohlcv_candle(trade: dict):
    """
    Returns the initial OHLCV candle when the first trade in that window is received
    """
    return {
        "open": trade["price"],
        "high": trade["price"],
        "low": trade["price"],
        "close": trade["price"],
        "volume": trade["quantity"],
        #"timestamp_ms": 
    }

def update_ohlcv_candle(ohlcv_candle: dict, trade: dict):
    """
    Updates the OHLCV candle with the new trade data
    """
    ohlcv_candle["high"] = max(ohlcv_candle["high"], trade["price"])
    ohlcv_candle["low"] = min(ohlcv_candle["low"], trade["price"])
    ohlcv_candle["close"] = trade["price"]
    ohlcv_candle["volume"] += trade["quantity"]

    return ohlcv_candle

def transform_trade_to_ohlcv(
        kafka_broker_address: str,
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_consumer_group: str,
        ohlcv_window_seconds: int,
):
    """
    Reads incoming trades from the given `kafka_input_topic`, aggregates them into OHLC data
    and outputs them to the given `kafka_output_topic`
    
    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_input_topic (str): The Kafka topic to read the trades from
        kafka_output_topic (str): The Kafka topic to save the OHLC data
        kafka_consumer_group (str): The Kafka consumer group to read the trades from
    
    Returns:
        None
    """

    app = Application(
        broker_address=kafka_broker_address, 
        consumer_group=kafka_consumer_group)
    
    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Create a Quix Streams DataFrame
    sdf = app.dataframe(input_topic)

    # Check if we are actually receiving the data
    sdf.update(logger.debug)

    # Aggregate the trades into OHLCV candles (1 minute)
    sdf = (
         sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds))
         .reduce(reducer = update_ohlcv_candle, initializer = init_ohlcv_candle)
         .final()
         #.current()
     )

   

    # Flatten the dictionary
    sdf['open'] = sdf['value']['open']  
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['volume'] = sdf['value']['volume']
    sdf['timestamp_ms'] = sdf['end']

    sdf = sdf[['timestamp_ms', 'open', 'high', 'low', 'close', 'volume']]

    # Print the output to the console
    sdf.update(logger.debug)

    # Push the data to the output topic
    sdf = sdf.to_topic(output_topic)

    # Kick off the application
    app.run(sdf)


if __name__ == "__main__":

    # Load configuration
    #from src.config import config

    # transform_trade_to_ohlc(
    #     kafka_broker_address=config.kafka_broker_address,
    #     kafka_input_topic=config.kafka_topic,
    #     kafka_output_topic=config.ohlcv_topic,
    #     kafka_consumer_group=config.consumer_group,
    # )

    transform_trade_to_ohlcv(
         kafka_broker_address='localhost:19092',
         kafka_input_topic='trade',
         kafka_output_topic='ohlcv',
         kafka_consumer_group='consumer_group_trade_to_ohlcv',
         ohlcv_window_seconds = 60
     )