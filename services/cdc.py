import logging
from typing import Dict, Any, List
from datetime import datetime
from services.cdc_strategy import CDCStrategyFactory
from services.snapshot import SnapshotService

logger = logging.getLogger(__name__)

class CDCService:
    """Service for implementing Change Data Capture (CDC) with snapshot diffs.
    
    This service follows the Strategy pattern by using different CDC strategies
    for different CDC methods (timestamp-based, hash-based, hash-partition-based).
    
    Now includes integration with SnapshotService for saving timestamped files
    for ETL staging processes.
    """
    
    def __init__(self, db_manager, storage_manager, snapshot_config: Dict[str, Any] = None):
        """Initialize the CDC service.
        
        Args:
            db_manager: Database manager instance
            storage_manager: Storage manager instance
            snapshot_config: Configuration for snapshot service
        """
        self.db_manager = db_manager
        self.storage_manager = storage_manager
        
        # Initialize snapshot service
        self.snapshot_service = SnapshotService(storage_manager, snapshot_config)
        
        # Get snapshot settings from global config
        self.global_config = db_manager.config.get("global_settings", {})
        self.snapshot_enabled = self.global_config.get("snapshot", {}).get("enabled", True)
        self.snapshot_format = self.global_config.get("snapshot", {}).get("format", "json")
    
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
            result = strategy.process(table_name, table_config, datasource_name)
            
            # If CDC processing was successful, save snapshot
            if result.get("status") == "success" and self.snapshot_enabled:
                snapshot_result = self._save_snapshot(table_name, datasource_name, result)
                
                # Add snapshot info to result
                result["snapshot"] = snapshot_result
            
            return result
            
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
    
    def _save_snapshot(self, table_name: str, datasource_name: str, cdc_result: Dict[str, Any]) -> Dict[str, Any]:
        """Save CDC changes as snapshot files.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            cdc_result: Result from CDC processing
            
        Returns:
            Result of snapshot save operation
        """
        try:
            # Extract changes from CDC result
            changes = {
                "added": cdc_result.get("added", []),
                "modified": cdc_result.get("modified", []),
                "deleted": cdc_result.get("deleted", [])
            }
            
            # Get table-specific snapshot format if configured
            table_config = self.db_manager.get_table_config(table_name)
            table_snapshot_format = table_config.get("snapshot_format", self.snapshot_format)
            
            # Save snapshot
            snapshot_result = self.snapshot_service.save_changes_snapshot(
                table_name=table_name,
                datasource_name=datasource_name,
                changes=changes,
                format_type=table_snapshot_format,
                timestamp=datetime.now()
            )
            
            if snapshot_result.get("status") == "success":
                logger.info(f"Snapshot saved for {table_name}: {snapshot_result.get('total_files', 0)} files")
            else:
                logger.warning(f"Failed to save snapshot for {table_name}: {snapshot_result.get('message', 'Unknown error')}")
            
            return snapshot_result
            
        except Exception as e:
            logger.error(f"Error saving snapshot for {table_name}: {str(e)}")
            return {
                "status": "error",
                "message": f"Snapshot save failed: {str(e)}"
            }
    
    def list_table_snapshots(self, table_name: str, datasource_name: str) -> List[str]:
        """List snapshots for a specific table.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            
        Returns:
            List of snapshot file keys
        """
        return self.snapshot_service.list_snapshots(table_name, datasource_name)
    
    def get_snapshot_info(self, file_key: str) -> Dict[str, Any]:
        """Get information about a specific snapshot.
        
        Args:
            file_key: Key of the snapshot file
            
        Returns:
            Snapshot information
        """
        return self.snapshot_service.get_snapshot_info(file_key)
    
    def close(self) -> None:
        """Clean up resources."""
        self.db_manager.close_all_connections()
        self.snapshot_service.close()
    
    def __enter__(self):
        """Support for context manager usage."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up when exiting context."""
        self.close()