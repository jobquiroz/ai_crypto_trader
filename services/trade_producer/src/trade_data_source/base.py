from abc import ABC, abstractmethod
from typing import List

# observ how I am using absolute imports here
# if you know how to use relative imports, please use them
from src.trade_data_source.trade import Trade

class TradeSource(ABC):
    """
    Abstract base class for a trade
    """

    @abstractmethod
    def get_trades(self) -> List[Trade]:
        """
        Retrieve the trades from whatever source you connect to
        """
        pass

    @abstractmethod
    def is_done(self) -> bool:
        """
        Returns True if the source has no more trades to give, False otherwise
        """
        pass