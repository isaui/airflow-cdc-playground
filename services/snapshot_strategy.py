import logging
import abc
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class SnapshotStrategy(abc.ABC):
    """Abstract base class for snapshot storage strategies."""
    
    def __init__(self, storage_manager):
        """Initialize the snapshot strategy.
        
        Args:
            storage_manager: Storage manager instance
        """
        self.storage_manager = storage_manager
    
    @abc.abstractmethod
    def save_snapshot(
        self, 
        table_name: str, 
        datasource_name: str, 
        changes: Dict[str, Any], 
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Save snapshot data to storage.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            changes: Changes data (added, modified, deleted)
            timestamp: Timestamp for the snapshot
            
        Returns:
            Result of the save operation
        """
        pass
    
    @abc.abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this strategy.
        
        Returns:
            File extension (e.g., 'json', 'parquet', 'csv')
        """
        pass
    
    def _generate_filename(
        self, 
        table_name: str, 
        datasource_name: str, 
        timestamp: datetime,
        suffix: str = ""
    ) -> str:
        """Generate filename for snapshot.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            timestamp: Timestamp for the snapshot
            suffix: Additional suffix for filename
            
        Returns:
            Generated filename
        """
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        extension = self.get_file_extension()
        
        if suffix:
            return f"snapshots/{datasource_name}/{table_name}/{timestamp_str}_{suffix}.{extension}"
        else:
            return f"snapshots/{datasource_name}/{table_name}/{timestamp_str}.{extension}"


class SnapshotStrategyFactory:
    """Factory class for creating snapshot strategy instances."""
    
    @staticmethod
    def create_strategy(
        format_type: str, 
        storage_manager
    ) -> Optional[SnapshotStrategy]:
        """Create a snapshot strategy based on the specified format.
        
        Args:
            format_type: Snapshot format type (json, parquet, csv)
            storage_manager: Storage manager instance
            
        Returns:
            Snapshot strategy instance or None if format not supported
        """
        format_type = format_type.lower()
        
        if format_type == "json":
            from services.strategies.json_strategy import JsonSnapshotStrategy
            return JsonSnapshotStrategy(storage_manager)
        elif format_type == "parquet":
            from services.strategies.parquet_strategy import ParquetSnapshotStrategy
            return ParquetSnapshotStrategy(storage_manager)
        elif format_type == "csv":
            from services.strategies.csv_strategy import CsvSnapshotStrategy
            return CsvSnapshotStrategy(storage_manager)
            
        logger.error(f"Unsupported snapshot format: {format_type}")
        return None