import logging
import json
from typing import Dict, Any
from datetime import datetime
from services.snapshot_strategy import SnapshotStrategy

logger = logging.getLogger(__name__)


class JsonSnapshotStrategy(SnapshotStrategy):
    """Snapshot strategy for JSON format."""
    
    def save_snapshot(
        self, 
        table_name: str, 
        datasource_name: str, 
        changes: Dict[str, Any], 
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Save snapshot data as JSON files.
        
        Args:
            table_name: Name of the table
            datasource_name: Name of the datasource
            changes: Changes data (added, modified, deleted)
            timestamp: Timestamp for the snapshot
            
        Returns:
            Result of the save operation
        """
        saved_files = []
        
        try:
            # Save added records
            if changes.get("added"):
                filename = self._generate_filename(table_name, datasource_name, timestamp, "added")
                data = {
                    "table_name": table_name,
                    "datasource": datasource_name,
                    "timestamp": timestamp.isoformat(),
                    "operation": "added",
                    "count": len(changes["added"]),
                    "data": changes["added"]
                }
                
                if self._save_json_file(filename, data):
                    saved_files.append(filename)
            
            # Save modified records
            if changes.get("modified"):
                filename = self._generate_filename(table_name, datasource_name, timestamp, "modified")
                data = {
                    "table_name": table_name,
                    "datasource": datasource_name,
                    "timestamp": timestamp.isoformat(),
                    "operation": "modified",
                    "count": len(changes["modified"]),
                    "data": changes["modified"]
                }
                
                if self._save_json_file(filename, data):
                    saved_files.append(filename)
            
            # Save deleted records
            if changes.get("deleted"):
                filename = self._generate_filename(table_name, datasource_name, timestamp, "deleted")
                data = {
                    "table_name": table_name,
                    "datasource": datasource_name,
                    "timestamp": timestamp.isoformat(),
                    "operation": "deleted",
                    "count": len(changes["deleted"]),
                    "data": changes["deleted"]
                }
                
                if self._save_json_file(filename, data):
                    saved_files.append(filename)
            
            # Save summary/manifest file
            summary_filename = self._generate_filename(table_name, datasource_name, timestamp, "summary")
            summary_data = {
                "table_name": table_name,
                "datasource": datasource_name,
                "timestamp": timestamp.isoformat(),
                "format": "json",
                "files": saved_files,
                "summary": {
                    "added": len(changes.get("added", [])),
                    "modified": len(changes.get("modified", [])),
                    "deleted": len(changes.get("deleted", []))
                }
            }
            
            if self._save_json_file(summary_filename, summary_data):
                saved_files.append(summary_filename)
            
            return {
                "status": "success",
                "format": "json",
                "files_saved": saved_files,
                "total_files": len(saved_files)
            }
            
        except Exception as e:
            logger.error(f"Error saving JSON snapshot: {str(e)}")
            return {
                "status": "error",
                "format": "json",
                "message": str(e)
            }
    
    def get_file_extension(self) -> str:
        """Get file extension for JSON format."""
        return "json"
    
    def _save_json_file(self, filename: str, data: Dict[str, Any]) -> bool:
        """Save data as JSON file to storage.
        
        Args:
            filename: Filename to save
            data: Data to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use storage manager to save file using the proper format handler
            return self.storage_manager.store_snapshot(filename, data, content_type='application/json')
        except Exception as e:
            logger.error(f"Error saving JSON file {filename}: {str(e)}")
            return False