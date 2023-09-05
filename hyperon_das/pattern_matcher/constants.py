from enum import Enum, auto


class CompatibilityStatus(int, Enum):
    """
    Enum for validate_match_only() warning messages.
    """

    INCOMPATIBLE = auto()
    NO_COVERING = auto()
    FIRST_COVERS_SECOND = auto()
    SECOND_COVERS_FIRST = auto()
    EQUAL = auto()
