from dataclasses import dataclass, field
from typing import List, Union

@dataclass
class TableInfo:
    """
    A dataclass to store metadata about a database table populated from a CSV file.
    """
    table_name: str = ''
    csv_location: str = ''
    x0: Union[int, float] = 0
    x1: Union[int, float] = 0
    top: Union[int, float] = 0
    bottom: Union[int, float] = 0
    primary_key_fields: List[str] = field(default_factory=list)
    