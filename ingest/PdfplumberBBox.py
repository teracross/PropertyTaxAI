from dataclasses import dataclass, field
from typing import Union

@dataclass
class PdfPlumberBBox:
    """
    A class to store and manage bounding box values extracted from a PDF using PDF Plumber.
    """
    x0: Union[int, float] = 0
    x1: Union[int, float] = 0
    top: Union[int, float] = 0
    bottom: Union[int, float] = 0