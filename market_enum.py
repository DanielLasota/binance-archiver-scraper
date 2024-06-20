from enum import Enum, auto


class Market(Enum):
    SPOT = auto()
    USD_M_FUTURES = auto()
    COIN_M_FUTURES = auto()
    ...
