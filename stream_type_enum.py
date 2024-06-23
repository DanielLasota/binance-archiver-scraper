from enum import Enum, auto


class StreamType(Enum):
    DIFFERENCE_DEPTH = auto()
    TRADE = auto()
    DEPTH_SNAPSHOT = auto()
    ...
