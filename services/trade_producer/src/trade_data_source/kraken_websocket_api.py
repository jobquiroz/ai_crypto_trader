from datetime import datetime, timezone
from typing import List
import json 
from websocket import create_connection
from loguru import logger


# from pydantic import BaseModel

# class Trade(BaseModel):
#     product_id: str
#     quantity: float
#     price: float
#     timestamp_ms: int

from src.trade_data_source.trade import Trade
from src.trade_data_source.base import TradeSource

class KrakenWebsocketAPI:
    """
    Class for reading real-time trades from the Kraken Websocket API
    """
    URL = 'wss://ws.kraken.com/v2' 

    def __init__(self, product_id: str):
        """
            Initializes the KrakenWebsocketAPI instance

            Args:
                product_id (str): The product id to get the trades from
        """
        self.product_id = product_id

        # establish connection to the Kraken websocket API -> "Subscribing"
        self._ws = create_connection(self.URL)
        logger.debug('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)

    def get_trades(self) -> List[Trade]:
        """
            Returns the latest batch of trades from the Kraken Websocket API

            Args:
                None

            Returns
                List[Trade]: A list of Trade objects
        """

        message = self._ws.recv()

        if 'heartbeat' in message:
            logger.debug('Heartbeat received')
            # when I get a heartbeat, I return an empty list
            return []

        # parse the message string as a dictionary
        message = json.loads(message)

        # extract trades from the message['data'] field
        trades = []
        for trade in message['data']:
            # Extract the following fields:
            # - product_id
            # - quantity
            # - price
            # - timestamp in milliseconds
            # transform the timestamp from Kraken which is a string
            # like '2024-06-17T09:45:38.494012Z' into Unix
            # milliseconds
            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=trade['price'],
                    quantity=trade['qty'],
                    timestamp_ms=self.to_ms(trade['timestamp']),
                )
            )
            
        return trades

    def is_done(self) -> bool:
        """
            Returns True if the KrakenWebsocketAPI instance is done
        """
        False
    
    def _subscribe(self, product_id: str):
        """
        Establish connection to the Kraken websocket API and subscribe to the trades for the given `product_id`.
        """
        logger.info(f'Subscribing to trades for {product_id}')
        # let's subscribe to the trades for the given `product_id`
        msg = {
            'method': 'subscribe',
            'params': {
                'channel': 'trade',
                'symbol': [product_id],
                'snapshot': False,
            },
        }
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # For each product_id we dump
        # the first 2 messages we got from the websocket, because they contain
        # no trade data, just confirmation on their end that the subscription was successful
        for product_id in [product_id]:
            _ = self._ws.recv()
            _ = self._ws.recv()

    @staticmethod
    def to_ms(timestamp: str) -> int:
        """
        A function that transforms a timestamps expressed
        as a string like this '2024-06-17T09:36:39.467866Z'
        into a timestamp expressed in milliseconds.

        Args:
            timestamp (str): A timestamp expressed as a string.

        Returns:
            int: A timestamp expressed in milliseconds.
        """
        # parse a string like this '2024-06-17T09:36:39.467866Z'
        # into a datetime object assuming UTC timezone
        # and then transform this datetime object into Unix timestamp
        # expressed in milliseconds
        from datetime import datetime, timezone

        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)