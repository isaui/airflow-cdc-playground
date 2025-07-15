import logging
import json
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from io import BytesIO
from services.snapshot_strategy import SnapshotStrategy

logger = logging.getLogger(__name__)


class ParquetSnapshotStrategy(SnapshotStrategy):
    """Snapshot strategy for Parquet format."""
    
    def save_snapshot(
        self, 
        table_name: str, 
        datasource_name: str, 
        changes: Dict[str, Any], 
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Save snapshot data as Parquet files.
        
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
                df = pd.DataFrame(changes["added"])
                
                # Add metadata columns
                df['_cdc_operation'] = 'added'
                df['_cdc_timestamp'] = timestamp.isoformat()
                df['_cdc_table'] = table_name
                df['_cdc_datasource'] = datasource_name
                
                if self._save_parquet_file(filename, df):
                    saved_files.append(filename)
            
            # Save modified records
            if changes.get("modified"):
                filename = self._generate_filename(table_name, datasource_name, timestamp, "modified")
                df = pd.DataFrame(changes["modified"])
                
                # Add metadata columns
                df['_cdc_operation'] = 'modified'
                df['_cdc_timestamp'] = timestamp.isoformat()
                df['_cdc_table'] = table_name
                df['_cdc_datasource'] = datasource_name
                
                if self._save_parquet_file(filename, df):
                    saved_files.append(filename)
            
            # Save deleted records
            if changes.get("deleted"):
                filename = self._generate_filename(table_name, datasource_name, timestamp, "deleted")
                df = pd.DataFrame(changes["deleted"])
                
                # Add metadata columns
                df['_cdc_operation'] = 'deleted'
                df['_cdc_timestamp'] = timestamp.isoformat()
                df['_cdc_table'] = table_name
                df['_cdc_datasource'] = datasource_name
                
                if self._save_parquet_file(filename, df):
                    saved_files.append(filename)
            
            # Save summary as JSON (parquet tidak cocok untuk summary)
            summary_filename = self._generate_filename(table_name, datasource_name, timestamp, "summary").replace('.parquet', '.json')
            summary_data = {
                "table_name": table_name,
                "datasource": datasource_name,
                "timestamp": timestamp.isoformat(),
                "format": "parquet",
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
                "format": "parquet",
                "files_saved": saved_files,
                "total_files": len(saved_files)
            }
            
        except Exception as e:
            logger.error(f"Error saving Parquet snapshot: {str(e)}")
            return {
                "status": "error",
                "format": "parquet",
                "message": str(e)
            }
    
    def get_file_extension(self) -> str:
        """Get file extension for Parquet format."""
        return "parquet"
    
    def _save_parquet_file(self, filename: str, df: pd.DataFrame) -> bool:
        """Save DataFrame as Parquet file to storage.
        
        Args:
            filename: Filename to save
            df: DataFrame to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert DataFrame to data dict for the storage manager
            # Include metadata about the dataframe structure
            data = {
                "data": df.to_dict(orient='records'),
                "columns": df.columns.tolist(),
                "row_count": len(df)
            }
            
            # Use storage manager to save file
            return self.storage_manager.store_snapshot(filename, data, content_type='application/octet-stream')
            
        except Exception as e:
            logger.error(f"Error saving Parquet file {filename}: {str(e)}")
            return False
    
    def _save_json_file(self, filename: str, data: Dict[str, Any]) -> bool:
        """Save data as JSON file to storage (for summary).
        
        Args:
            filename: Filename to save
            data: Data to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use storage manager to save file
            return self.storage_manager.store_snapshot(filename, data, content_type='application/json')
            
        except Exception as e:
            logger.error(f"Error saving JSON file {filename}: {str(e)}")
            return False