"""JSON format handler implementation."""

import json
from io import BytesIO
from typing import Dict, Any, Tuple, Optional

from .base import FormatHandler


class JsonFormatHandler(FormatHandler):
    """Handler for JSON format"""
    
    @staticmethod
    def store(data: Dict[str, Any], **kwargs) -> Tuple[BytesIO, int, str, None]:
        """Store data as JSON
        
        Args:
            data: Data to store
            **kwargs: Additional keyword arguments
            
        Returns:
            Tuple of (data_stream, size, content_type, metadata)
        """
        data_str = json.dumps(data)
        data_bytes = data_str.encode('utf-8')
        data_stream = BytesIO(data_bytes)
        return data_stream, len(data_bytes), 'application/json', None
    
    @staticmethod
    def retrieve(data_bytes: bytes, **kwargs) -> Dict[str, Any]:
        """Retrieve data from JSON
        
        Args:
            data_bytes: Raw bytes data
            **kwargs: Additional keyword arguments
            
        Returns:
            Deserialized JSON data
        """
        data_str = data_bytes.decode('utf-8')
        return json.loads(data_str)
