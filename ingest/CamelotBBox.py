from dataclasses import dataclass, field
from typing import Union

@dataclass(frozen=True)
class CamelotBBox:
    """
    A class to store and manage bounding box values extracted from a PDF using Camelot.
    """
    x0: Union[int, float] = 0
    x1: Union[int, float] = 0
    y0: Union[int, float] = 0
    y1: Union[int, float] = 0