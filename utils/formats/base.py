"""Base class for data format handlers."""

from typing import Dict, Any, Optional, Tuple


class FormatHandler:
    """Base class for different data format handlers"""
    
    @staticmethod
    def store(data: Dict[str, Any], **kwargs) -> Tuple[Any, int, str, Optional[Tuple]]:
        """Store data in specific format
        
        Args:
            data: Data to store
            **kwargs: Additional keyword arguments
            
        Returns:
            Tuple containing:
            - Data stream object
            - Size in bytes
            - Content type
            - Optional metadata tuple (stream, size) or None
        """
        raise NotImplementedError("Subclasses must implement store method")
    
    @staticmethod
    def retrieve(data_bytes: bytes, **kwargs) -> Dict[str, Any]:
        """Retrieve data from specific format
        
        Args:
            data_bytes: Raw bytes data
            **kwargs: Additional keyword arguments
            
        Returns:
            Deserialized data dictionary
        """
        raise NotImplementedError("Subclasses must implement retrieve method")
