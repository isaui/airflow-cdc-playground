import logging
import hashlib
import datetime
from typing import Dict, Any

from services.cdc_strategy import CDCStrategy

logger = logging.getLogger(__name__)


class HashPartitionCDCStrategy(CDCStrategy):
    """CDC strategy using hash-partition method for large tables with backend hash calculation."""
    
    def process(self, table_name: str, table_config: Dict[str, Any], datasource_name: str) -> Dict[str, Any]:
        """Process a table using hash-partition CDC method.
        
        Args:
            table_name: Name of the table
            table_config: Table configuration
            datasource_name: Name of the datasource
            
        Returns:
            Results of the operation
        """
        hash_columns = table_config.get("hash_columns", [])
        primary_key = table_config.get("primary_key")
        partition_size = table_config.get("partition_size", 10000)
        
        if not hash_columns:
            return {"status": "error", "message": "No hash columns specified"}
        if not primary_key:
            return {"status": "error", "message": "No primary key specified"}
            
        # Get total count to determine partitions - SIMPLE COUNT query
        table_config_obj = self.db_manager.get_table_config(table_name)
        schema = table_config_obj.get("schema", "") if table_config_obj else ""
        qualified_table_name = f"{schema}.{table_name}" if schema else table_name
        
        count_query = f"SELECT COUNT(*) as count FROM {qualified_table_name}"
        result = self.db_manager.execute_query(datasource_name, count_query)
        row = result.fetchone()
        total_rows = row[0] if row else 0
        
        # Calculate partitions
        num_partitions = max(1, (total_rows + partition_size - 1) // partition_size)
        
        changes = {
            "added": [],
            "modified": [],
            "deleted": []
        }
        
        # Process each partition
        for partition_id in range(num_partitions):
            partition_changes = self._process_partition(
                table_name, 
                table_config, 
                datasource_name, 
                partition_id, 
                num_partitions
            )
            
            # Merge changes
            changes["added"].extend(partition_changes.get("added", []))
            changes["modified"].extend(partition_changes.get("modified", []))
            changes["deleted"].extend(partition_changes.get("deleted", []))
            
        return {
            "status": "success",
            "table_name": table_name,
            "method": "hash-partition",
            "partitions": num_partitions,
            "changes": {
                "added": len(changes["added"]),
                "modified": len(changes["modified"]),
                "deleted": len(changes["deleted"])
            },
            "added": changes["added"],
            "modified": changes["modified"],
            "deleted": changes["deleted"]
        }
    
    def _process_partition(
        self, 
        table_name: str, 
        table_config: Dict[str, Any], 
        datasource_name: str,
        partition_id: int,
        total_partitions: int
    ) -> Dict[str, Any]:
        """Process a specific partition of a table.
        
        Args:
            table_name: Name of the table
            table_config: Table configuration
            datasource_name: Name of the datasource
            partition_id: ID of the partition to process
            total_partitions: Total number of partitions
            
        Returns:
            Dictionary with changes for this partition
        """
        hash_columns = table_config.get("hash_columns", [])
        primary_key = table_config.get("primary_key")
        
        # Get previous state with row hashes for this partition
        state_key = f"{datasource_name}/{table_name}/partition_{partition_id}_of_{total_partitions}"
        previous_state = self.storage_manager.retrieve_state(state_key)
        previous_hashes = previous_state.get("row_hashes", {}) if previous_state else {}
        
        # Get table config for schema
        table_config_obj = self.db_manager.get_table_config(table_name)
        schema = table_config_obj.get("schema", "") if table_config_obj else ""
        qualified_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # Build partition query - SIMPLE SELECT * dengan WHERE partition clause
        partition_clause = f"MOD(ABS(CAST(COALESCE({primary_key}, 0) AS INTEGER)), {total_partitions}) = {partition_id}"
        query = f"SELECT * FROM {qualified_table_name} WHERE {partition_clause}"
        
        # Process current data
        current_hashes = {}
        changes = {
            "added": [],
            "modified": [],
            "deleted": []
        }
        
        # Execute simple SELECT query
        result = self.db_manager.execute_query(datasource_name, query)
        
        # Process rows dan calculate hash di BACKEND (bukan di database)
        for row in result:
            row_dict = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
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
        
        return changes
    
    def _calculate_row_hash(self, row_dict: Dict[str, Any], hash_columns: list) -> str:
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