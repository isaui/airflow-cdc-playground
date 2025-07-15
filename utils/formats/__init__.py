"""Format handlers for different data storage formats."""

from .base import FormatHandler
from .json_format import JsonFormatHandler
from .parquet_format import ParquetFormatHandler
from .csv_format import CsvFormatHandler

# Register all format handlers
FORMAT_HANDLERS = {
    "json": JsonFormatHandler,
    "parquet": ParquetFormatHandler,
    "csv": CsvFormatHandler
}

__all__ = [
    'FormatHandler', 
    'JsonFormatHandler', 
    'ParquetFormatHandler', 
    'CsvFormatHandler',
    'FORMAT_HANDLERS'
]
