import logging
import abc
from typing import Dict, Any, Optional

from utils.database import DatabaseManager
from utils.storage import StorageManager

logger = logging.getLogger(__name__)


class CDCStrategy(abc.ABC):
    """Abstract base class for CDC strategies."""
    
    def __init__(self, db_manager: DatabaseManager, storage_manager: StorageManager):
        """Initialize the CDC strategy.
        
        Args:
            db_manager: Database manager instance
            storage_manager: Storage manager instance
        """
        self.db_manager = db_manager
        self.storage_manager = storage_manager
    
    @abc.abstractmethod
    def process(self, table_name: str, table_config: Dict[str, Any], datasource_name: str) -> Dict[str, Any]:
        """Process a table using this CDC strategy.
        
        Args:
            table_name: Name of the table
            table_config: Table configuration
            datasource_name: Name of the datasource
            
        Returns:
            Results of the operation
        """
        pass


class CDCStrategyFactory:
    """Factory class for creating CDC strategy instances."""
    
    @staticmethod
    def create_strategy(
        method: str, 
        db_manager: DatabaseManager, 
        storage_manager: StorageManager
    ) -> Optional[CDCStrategy]:
        """Create a CDC strategy based on the specified method.
        
        Args:
            method: CDC method name
            db_manager: Database manager instance
            storage_manager: Storage manager instance
            
        Returns:
            CDC strategy instance or None if method not supported
        """
        method = method.lower()
        
        if method == "timestamp":
            from services.strategies.timestamp_strategy import TimestampCDCStrategy
            return TimestampCDCStrategy(db_manager, storage_manager)
        elif method == "hash":
            from services.strategies.hash_strategy import HashCDCStrategy
            return HashCDCStrategy(db_manager, storage_manager)
        elif method in ["hash-partition", "hashpartition"]:
            from services.strategies.hash_partition_strategy import HashPartitionCDCStrategy
            return HashPartitionCDCStrategy(db_manager, storage_manager)
            
        logger.error(f"Unsupported CDC method: {method}")
        return None