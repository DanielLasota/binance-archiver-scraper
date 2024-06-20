from enum import Enum, auto


class StreamType(Enum):
    ORDERBOOK = auto()
    TRANSACTIONS = auto()
    ORDERBOOK_SNAPSHOT = auto()
    ...
