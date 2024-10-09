from typing import List
from quixstreams import Application

from loguru import logger
import json
from src.hopsworks_api import push_value_to_feature_group

def topic_to_feature_store(
    kafka_broker_address: str,
    kafka_input_topic: str,
    kafka_consumer_group: str,
    feature_group_name: str,
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
    start_offline_materialization: bool,
    batch_size: int,
):
    """
    Reads incoming messages from the given `kafka_input_topic`, and pushes them to the given
    `feature_group_name` in the feature store
    
    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_input_topic (str): The Kafka topic to read the messages from
        kafka_consumer_group (str): The Kafka consumer group to read the messages from
        feature_group_name (str): The name of the feature group in the feature store
        feature_group_version (int): The version of the feature group in the feature store
        feature_group_primary_keys (List[str]): The primary keys of the feature group
        feature_group_event_time (str): The event time of the feature group
        start_offline_materialization (bool): Whether to start offline materialization or not
        batch_size (int): The number of messages to batch (in memory) before pushing to the feature store
    Returns:
        None
    """

    # Configure an Application
    # The config params will be used for the Consumer instance too
    app = Application(
        broker_address=kafka_broker_address, 
        consumer_group=kafka_consumer_group,
        #auto_offset_reset='earliest', # by default it is 'latest'
    )
    
    batch = []

    # Create a consumer and start a polling loop
    with app.get_consumer() as consumer:

        # Subscribe to the input topic
        consumer.subscribe(topics = [kafka_input_topic])

        # Poll for new messages
        while True:
            msg = consumer.poll(timeout=0.1)

            if msg is None:
                continue
            elif msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            value = msg.value()
            # Decode the message bytes into a dictionary
            value = json.loads(value.decode('utf-8'))

            # Append the message to the batch
            batch.append(value)

            # If the batch is not full, continue
            if len(batch) < batch_size:
                logger.debug(f'Batch has size {len(batch)} < {batch_size:,}')
                continue

            logger.debug(f'Batch has size {len(batch)} >= {batch_size:,}... Pushing data to the feature store')
            # We need to push the value to the feature store
            push_value_to_feature_group(
                batch,
                feature_group_name,
                feature_group_version,
                feature_group_primary_keys,
                feature_group_event_time,
                start_offline_materialization,
            )

            # Clear the batch
            batch = []

            consumer.store_offsets(message=msg)


if __name__ == "__main__":
    
    from src.config import config

    topic_to_feature_store(
        kafka_broker_address = config.kafka_broker_address,
        kafka_input_topic = config.kafka_input_topic,
        kafka_consumer_group = config.kafka_consumer_group,
        feature_group_name = config.feature_group_name,
        feature_group_version = config.feature_group_version,
        feature_group_primary_keys = config.feature_group_primary_keys,
        feature_group_event_time = config.feature_group_event_time,
        start_offline_materialization = config.start_offline_materialization,
        batch_size = config.batch_size,
    )