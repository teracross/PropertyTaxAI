from dataclasses import dataclass, field
from typing import List
from PdfplumberBBox import PdfPlumberBBox

@dataclass
class TableInfo:
    """
    A dataclass to store metadata about a database table schema parsed from PDF file.
    """
    table_name: str = ''
    csv_location: str = ''
    primary_key_fields: List[str] = field(default_factory=list)
    Pdfplumber_bboxes: List[PdfPlumberBBox] = field(default_factory=list)
    