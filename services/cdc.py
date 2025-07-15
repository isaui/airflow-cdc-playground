import logging
from typing import Dict, Any
from services.cdc_strategy import CDCStrategyFactory

logger = logging.getLogger(__name__)

class CDCService:
    """Service for implementing Change Data Capture (CDC) with snapshot diffs.
    
    This service follows the Strategy pattern by using different CDC strategies
    for different CDC methods (timestamp-based, hash-based, hash-partition-based).
    """
    
    def __init__(self, db_manager, storage_manager):
        """Initialize the CDC service.
        
        Args:
            db_manager: Database manager instance
            storage_manager: Storage manager instance
        """
        self.db_manager = db_manager
        self.storage_manager = storage_manager
    
    def process_table(self, table_name: str) -> Dict[str, Any]:
        """Process a table according to its CDC method.
        
        Args:
            table_name: Name of the table to process
            
        Returns:
            Dictionary with processing results
        """
        table_config = self.db_manager.get_table_config(table_name)
        if not table_config:
            logger.error(f"No configuration found for table {table_name}")
            return {"status": "error", "message": f"No configuration for table {table_name}"}
            
        method = table_config.get("method", "").lower()
        datasource_name = table_config.get("datasource")
        
        if not datasource_name:
            logger.error(f"No datasource specified for table {table_name}")
            return {"status": "error", "message": "No datasource specified"}
            
        logger.info(f"Processing table {table_name} using method: {method}")
        
        try:
            # Use the strategy factory to create the appropriate CDC strategy
            strategy = CDCStrategyFactory.create_strategy(method, self.db_manager, self.storage_manager)
            
            if not strategy:
                return {
                    "status": "error", 
                    "message": f"Unsupported CDC method: {method}"
                }
                
            # Use the strategy to process the table
            return strategy.process(table_name, table_config, datasource_name)
        except Exception as e:
            logger.exception(f"Error processing table {table_name}: {str(e)}")
            return {"status": "error", "message": f"Error: {str(e)}"}
    
    def process_all_tables(self) -> Dict[str, Any]:
        """Process all tables defined in the configuration.
        
        Returns:
            Results for all tables
        """
        table_configs = self.db_manager.get_all_table_configs()
        results = {}
        
        for table_name in table_configs:
            logger.info(f"Processing table {table_name}")
            result = self.process_table(table_name)
            results[table_name] = result
            
        return results
    
    def close(self) -> None:
        """Clean up resources."""
        self.db_manager.close_all_connections()
    
    def __enter__(self):
        """Support for context manager usage."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up when exiting context."""
        self.close()
