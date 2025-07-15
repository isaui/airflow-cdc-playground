import logging
import datetime
from typing import Dict, Any

from services.cdc_strategy import CDCStrategy

logger = logging.getLogger(__name__)


class TimestampCDCStrategy(CDCStrategy):
    """CDC strategy using timestamp-based change detection."""
    
    def process(self, table_name: str, table_config: Dict[str, Any], datasource_name: str) -> Dict[str, Any]:
        """Process a table using timestamp-based CDC method.
        
        Args:
            table_name: Name of the table
            table_config: Table configuration
            datasource_name: Name of the datasource
            
        Returns:
            Results of the operation
        """
        timestamp_column = table_config.get("timestamp_column")
        if not timestamp_column:
            return {"status": "error", "message": "No timestamp column specified"}
            
        # Get last processed timestamp
        state_key = f"{datasource_name}/{table_name}/timestamp_state"
        last_state = self.storage_manager.retrieve_state(state_key)
        last_timestamp = last_state.get("last_timestamp") if last_state else None
        
        # Get changes since last timestamp
        where_clause = None
        if last_timestamp:
            where_clause = f"{timestamp_column} > '{last_timestamp}'"
            
        # Process data in batches
        changes = []
        latest_timestamp = last_timestamp
        
        for batch in self.db_manager.fetch_data_in_batches(
            datasource_name, table_name, where_clause=where_clause
        ):
            if batch.empty:
                continue
                
            # Track the latest timestamp
            if timestamp_column in batch.columns:
                batch_max_timestamp = str(batch[timestamp_column].max())
                if not latest_timestamp or batch_max_timestamp > latest_timestamp:
                    latest_timestamp = batch_max_timestamp
            
            # Convert batch to dictionaries for processing
            records = batch.to_dict("records")
            changes.extend(records)
        
        # Store the latest timestamp as the new state
        if latest_timestamp and latest_timestamp != last_timestamp:
            new_state = {
                "last_timestamp": latest_timestamp,
                "processed_at": datetime.datetime.now().isoformat()
            }
            self.storage_manager.store_state(state_key, new_state)
        
        return {
            "status": "success",
            "table_name": table_name,
            "method": "timestamp",
            "last_timestamp": last_timestamp,
            "new_timestamp": latest_timestamp,
            "changes_count": len(changes),
            "changes": changes
        }
