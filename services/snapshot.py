import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from services.snapshot_strategy import SnapshotStrategyFactory

logger = logging.getLogger(__name__)


class SnapshotService:
    """Service for managing snapshot storage with different formats.
    
    This service handles saving CDC changes as timestamped snapshot files
    for ETL staging processes. It supports multiple formats (JSON, Parquet, CSV)
    using the Strategy pattern.
    """
    
    def __init__(self, storage_manager, config: Optional[Dict[str, Any]] = None):
        """Initialize the snapshot service.
        
        Args:
            storage_manager: Storage manager instance
            config: Optional configuration for snapshot settings
        """
        self.storage_manager = storage_manager
        self.config = config or {}
        
        # Default snapshot configuration
        self.default_format = self.config.get("default_format", "json")
        self.include_metadata = self.config.get("include_metadata", True)
        self.compress_files = self.config.get("compress_files", False)
    
    def save_changes_snapshot(
        self, 
        table_name: str, 
        datasource_name: str, 
        changes: Dict[str, Any],
        format_type: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Save CDC changes as snapshot files.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            changes: Changes data containing 'added', 'modified', 'deleted'
            format_type: Format type (json, parquet, csv). If None, uses default
            timestamp: Timestamp for the snapshot. If None, uses current time
            
        Returns:
            Result of the save operation
        """
        # Use provided format or default
        if format_type is None:
            format_type = self.default_format
        
        # Use provided timestamp or current time
        if timestamp is None:
            timestamp = datetime.now()
        
        logger.info(f"Saving snapshot for {datasource_name}.{table_name} in {format_type} format")
        
        # Validate changes data
        if not self._validate_changes(changes):
            return {
                "status": "error",
                "message": "Invalid changes data structure"
            }
        
        # Skip if no changes
        if not self._has_changes(changes):
            logger.info(f"No changes detected for {table_name}, skipping snapshot")
            return {
                "status": "skipped",
                "message": "No changes to save"
            }
        
        try:
            # Create strategy based on format
            strategy = SnapshotStrategyFactory.create_strategy(format_type, self.storage_manager)
            
            if not strategy:
                return {
                    "status": "error",
                    "message": f"Unsupported format: {format_type}"
                }
            
            # Save snapshot using strategy
            result = strategy.save_snapshot(table_name, datasource_name, changes, timestamp)
            
            # Add metadata to result
            if result.get("status") == "success":
                result.update({
                    "table_name": table_name,
                    "datasource": datasource_name,
                    "timestamp": timestamp.isoformat(),
                    "changes_summary": {
                        "added": len(changes.get("added", [])),
                        "modified": len(changes.get("modified", [])),
                        "deleted": len(changes.get("deleted", []))
                    }
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error saving snapshot: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to save snapshot: {str(e)}"
            }
    
    def save_multiple_snapshots(
        self,
        snapshots: List[Dict[str, Any]],
        format_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Save multiple snapshots in batch.
        
        Args:
            snapshots: List of snapshot data, each containing:
                - table_name: str
                - datasource_name: str
                - changes: Dict[str, Any]
                - timestamp: Optional[datetime]
            format_type: Format type for all snapshots
            
        Returns:
            Results of batch save operation
        """
        results = []
        successful = 0
        failed = 0
        
        for snapshot_data in snapshots:
            result = self.save_changes_snapshot(
                table_name=snapshot_data["table_name"],
                datasource_name=snapshot_data["datasource_name"],
                changes=snapshot_data["changes"],
                format_type=format_type,
                timestamp=snapshot_data.get("timestamp")
            )
            
            results.append(result)
            
            if result.get("status") == "success":
                successful += 1
            else:
                failed += 1
        
        return {
            "status": "completed",
            "total": len(snapshots),
            "successful": successful,
            "failed": failed,
            "results": results
        }
    
    def list_snapshots(
        self,
        table_name: Optional[str] = None,
        datasource_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[str]:
        """List available snapshots with optional filtering.
        
        Args:
            table_name: Filter by table name
            datasource_name: Filter by datasource name
            start_date: Filter by start date
            end_date: Filter by end date
            
        Returns:
            List of snapshot file keys
        """
        try:
            prefix = "snapshots/"
            
            if datasource_name:
                prefix += f"{datasource_name}/"
                
            if table_name:
                prefix += f"{table_name}/"
            
            # Get all snapshot files
            snapshot_files = self.storage_manager.list_states(prefix)
            
            # Apply date filtering if provided
            if start_date or end_date:
                filtered_files = []
                for file_key in snapshot_files:
                    # Extract timestamp from filename
                    try:
                        parts = file_key.split('/')
                        if len(parts) >= 4:
                            timestamp_part = parts[3].split('_')[0] + '_' + parts[3].split('_')[1]
                            file_timestamp = datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
                            
                            if start_date and file_timestamp < start_date:
                                continue
                            if end_date and file_timestamp > end_date:
                                continue
                                
                            filtered_files.append(file_key)
                    except (ValueError, IndexError):
                        # Skip files with invalid timestamp format
                        continue
                
                snapshot_files = filtered_files
            
            return snapshot_files
            
        except Exception as e:
            logger.error(f"Error listing snapshots: {str(e)}")
            return []
    
    def get_snapshot_info(self, file_key: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific snapshot file.
        
        Args:
            file_key: Key of the snapshot file
            
        Returns:
            Snapshot information or None if not found
        """
        try:
            # Try to get summary file first
            summary_key = file_key.replace('.json', '_summary.json').replace('.parquet', '_summary.json').replace('.csv', '_summary.json')
            summary_data = self.storage_manager.retrieve_state(summary_key)
            
            if summary_data:
                return summary_data
            
            # If no summary, extract info from filename
            parts = file_key.split('/')
            if len(parts) >= 4:
                datasource = parts[1]
                table = parts[2]
                filename = parts[3]
                
                # Extract timestamp and operation
                timestamp_part = filename.split('_')[0] + '_' + filename.split('_')[1]
                operation = filename.split('_')[2].split('.')[0] if '_' in filename else 'unknown'
                
                return {
                    "datasource": datasource,
                    "table_name": table,
                    "timestamp": timestamp_part,
                    "operation": operation,
                    "file_key": file_key
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting snapshot info: {str(e)}")
            return None
    
    def _validate_changes(self, changes: Dict[str, Any]) -> bool:
        """Validate changes data structure.
        
        Args:
            changes: Changes data to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(changes, dict):
            return False
        
        # Check required keys
        required_keys = ['added', 'modified', 'deleted']
        for key in required_keys:
            if key not in changes:
                return False
            if not isinstance(changes[key], list):
                return False
        
        return True
    
    def _has_changes(self, changes: Dict[str, Any]) -> bool:
        """Check if there are any changes to save.
        
        Args:
            changes: Changes data
            
        Returns:
            True if there are changes, False otherwise
        """
        return (
            len(changes.get("added", [])) > 0 or
            len(changes.get("modified", [])) > 0 or
            len(changes.get("deleted", [])) > 0
        )
    
    def close(self) -> None:
        """Clean up resources."""
        # Storage manager cleanup handled by caller
        pass
    
    def __enter__(self):
        """Support for context manager usage."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up when exiting context."""
        self.close()