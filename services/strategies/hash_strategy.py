import logging
import hashlib
import datetime
from typing import Dict, List, Any

from services.cdc_strategy import CDCStrategy

logger = logging.getLogger(__name__)


class HashCDCStrategy(CDCStrategy):
    """CDC strategy using hash-based change detection with backend hash calculation."""
    
    def process(self, table_name: str, table_config: Dict[str, Any], datasource_name: str) -> Dict[str, Any]:
        """Process a table using hash-based CDC method.
        
        Args:
            table_name: Name of the table
            table_config: Table configuration
            datasource_name: Name of the datasource
            
        Returns:
            Results of the operation
        """
        hash_columns = table_config.get("hash_columns", [])
        primary_key = table_config.get("primary_key")
        
        if not hash_columns:
            return {"status": "error", "message": "No hash columns specified"}
        if not primary_key:
            return {"status": "error", "message": "No primary key specified"}
            
        # Get previous state with row hashes
        state_key = f"{datasource_name}/{table_name}/hash_state"
        previous_state = self.storage_manager.retrieve_state(state_key)
        previous_hashes = previous_state.get("row_hashes", {}) if previous_state else {}
        
        # Process current data
        current_hashes = {}
        changes = {
            "added": [],
            "modified": [],
            "deleted": []
        }
        
        # Process data in batches - SIMPLE SELECT * query saja
        for batch in self.db_manager.fetch_data_in_batches(datasource_name, table_name):
            if batch.empty:
                continue
                
            # Calculate hash for each row di BACKEND (bukan di DB)
            for _, row in batch.iterrows():
                row_dict = row.to_dict()
                pk_value = str(row_dict.get(primary_key, ""))
                
                if not pk_value:
                    logger.warning(f"Row missing primary key value: {row_dict}")
                    continue
                
                # Hash calculation di BACKEND - bukan di database
                row_hash = self._calculate_row_hash(row_dict, hash_columns)
                current_hashes[pk_value] = row_hash
                
                # Compare with previous hash
                if pk_value in previous_hashes:
                    if row_hash != previous_hashes[pk_value]:
                        changes["modified"].append(row_dict)
                else:
                    changes["added"].append(row_dict)
        
        # Find deleted rows
        for pk_value in previous_hashes:
            if pk_value not in current_hashes:
                changes["deleted"].append({"primary_key": primary_key, "value": pk_value})
        
        # Store the new state
        new_state = {
            "row_hashes": current_hashes,
            "processed_at": datetime.datetime.now().isoformat()
        }
        self.storage_manager.store_state(state_key, new_state)
        
        return {
            "status": "success",
            "table_name": table_name,
            "method": "hash",
            "changes": {
                "added": len(changes["added"]),
                "modified": len(changes["modified"]),
                "deleted": len(changes["deleted"])
            },
            "added": changes["added"],
            "modified": changes["modified"],
            "deleted": changes["deleted"]
        }
        
    def _calculate_row_hash(self, row_dict: Dict[str, Any], hash_columns: List[str]) -> str:
        """Calculate hash value for a row based on specified columns.
        
        IMPORTANT: Hash calculation dilakukan di BACKEND (Python level), 
        bukan di database level untuk menghindari query yang berat.
        
        Args:
            row_dict: Row data as dictionary
            hash_columns: List of column names to include in hash
            
        Returns:
            Hash string
        """
        hash_values = []
        
        # Special case: if hash_columns contains "*", use all columns
        if "*" in hash_columns:
            for col, val in sorted(row_dict.items()):
                # Convert to string and handle None values
                hash_values.append(str(val if val is not None else ""))
        else:
            for col in hash_columns:
                if col in row_dict:
                    # Convert to string and handle None values
                    val = row_dict.get(col)
                    hash_values.append(str(val if val is not None else ""))
        
        # Join with delimiter and calculate MD5 hash di BACKEND
        hash_string = "|".join(hash_values)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()