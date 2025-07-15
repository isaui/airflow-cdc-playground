"""Parquet format handler implementation."""

import json
import logging
import pandas as pd
from io import BytesIO
from typing import Dict, Any, Tuple, Optional

from .base import FormatHandler
from .json_format import JsonFormatHandler

logger = logging.getLogger(__name__)


class ParquetFormatHandler(FormatHandler):
    """Handler for Parquet format"""
    
    @staticmethod
    def store(data: Dict[str, Any], **kwargs) -> Tuple[BytesIO, int, str, Optional[Tuple]]:
        """Store data as Parquet
        
        Args:
            data: Data to store
            **kwargs: Additional keyword arguments
            
        Returns:
            Tuple of (data_stream, size, content_type, metadata)
        """
        # Check if data contains a list in 'data' field
        if "data" in data and isinstance(data["data"], list):
            # Extract metadata
            metadata = {k: v for k, v in data.items() if k != "data"}
            metadata_str = json.dumps(metadata)
            metadata_bytes = metadata_str.encode('utf-8')
            metadata_stream = BytesIO(metadata_bytes)
            
            # Convert data list to DataFrame and then to Parquet
            df = pd.DataFrame(data["data"])
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            parquet_size = parquet_buffer.getbuffer().nbytes
            
            return parquet_buffer, parquet_size, 'application/octet-stream', (metadata_stream, len(metadata_bytes))
        else:
            # Fallback to JSON for non-data payloads
            logger.warning("Cannot convert to Parquet: data is not in expected format")
            return JsonFormatHandler.store(data)
    
    @staticmethod
    def retrieve(data_bytes: bytes, metadata_bytes: Optional[bytes] = None, **kwargs) -> Dict[str, Any]:
        """Retrieve data from Parquet
        
        Args:
            data_bytes: Raw bytes data
            metadata_bytes: Optional metadata bytes
            **kwargs: Additional keyword arguments
            
        Returns:
            Deserialized data
        """
        df = pd.read_parquet(BytesIO(data_bytes))
        result = {"data": df.to_dict(orient="records")}
        
        # Add metadata if available
        if metadata_bytes:
            metadata = json.loads(metadata_bytes.decode('utf-8'))
            result.update(metadata)
        
        return result
