import logging
import json
from typing import Dict, Any, Optional, List

from minio import Minio
from minio.error import S3Error

from utils.formats import FORMAT_HANDLERS

logger = logging.getLogger(__name__)

class StorageManager:
    """Storage manager for CDC state using MinIO/S3."""
    
    def __init__(self, config_path: str):
        """Initialize storage manager with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.storage_config = self.config.get("storage", {})
        self.client = self._initialize_client()
        self._ensure_bucket_exists()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        logger.info(f"Loading configuration from {self.config_path}")
        try:
            with open(self.config_path, 'r') as config_file:
                return json.load(config_file)
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
            
    def _initialize_client(self) -> Optional[Minio]:
        """Initialize MinIO client from configuration."""
        try:
            endpoint = self.storage_config.get("endpoint")
            access_key = self.storage_config.get("access_key")
            secret_key = self.storage_config.get("secret_key")
            secure = self.storage_config.get("secure", False)
            
            logger.info(f"Initializing MinIO client for endpoint: {endpoint}")
            
            if not all([endpoint, access_key, secret_key]):
                logger.error("Missing required MinIO configuration")
                return None
                
            return Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {str(e)}")
            return None
            
    def _ensure_bucket_exists(self) -> None:
        """Ensure the configured bucket exists."""
        if not self.client:
            return
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return
            
        try:
            if not self.client.bucket_exists(bucket):
                logger.info(f"Creating bucket: {bucket}")
                self.client.make_bucket(bucket)
            else:
                logger.info(f"Bucket already exists: {bucket}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket {bucket}: {str(e)}")
    
    def _get_format_handler(self, state_key: str):
        """Get the appropriate format handler based on configuration."""
        # Check if this is a snapshot and has table-specific format preference
        table_format = None
        if "/snapshot" in state_key:
            # Extract table name from state_key (format: datasource/table_name/snapshot)
            parts = state_key.split("/")
            if len(parts) >= 2:
                table_name = parts[1]
                tables_config = self.config.get("tables", {})
                if table_name in tables_config and "snapshot_format" in tables_config[table_name]:
                    table_format = tables_config[table_name]["snapshot_format"].lower()
                    logger.info(f"Using table-specific format {table_format} for {state_key}")
        
        # Get format type (default is JSON)
        format_type = table_format or self.storage_config.get("format", "json").lower()
        
        # Return appropriate handler
        handler_class = FORMAT_HANDLERS.get(format_type)
        if not handler_class:
            logger.warning(f"Unknown format type: {format_type}, falling back to JSON")
            handler_class = FORMAT_HANDLERS["json"]
            
        return handler_class
    
    def store_state(self, state_key: str, data: Dict[str, Any]) -> bool:
        """Store CDC state data in MinIO.
        
        Args:
            state_key: Unique key for the state (e.g., "table_name/timestamp")
            data: State data to store
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return False
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return False
        
        # Get appropriate format handler
        handler = self._get_format_handler(state_key)
        logger.info(f"Using {handler.__name__} for storing state {state_key}")
        
        try:
            # Store the data using the format handler
            result = handler.store(data)
            
            # Unpack the results - main data stream, size, content type, and optional metadata
            if len(result) == 4 and result[3] is not None:
                data_stream, data_size, content_type, metadata_tuple = result
                metadata_stream, metadata_size = metadata_tuple
                
                # Store metadata
                metadata_key = f"{state_key}_metadata"
                self.client.put_object(
                    bucket,
                    metadata_key,
                    data=metadata_stream,
                    length=metadata_size,
                    content_type='application/json'
                )
            else:
                data_stream, data_size, content_type = result[:3]
            
            # Store main data
            self.client.put_object(
                bucket,
                state_key,
                data=data_stream,
                length=data_size,
                content_type=content_type
            )
            
            logger.info(f"Stored state at {bucket}/{state_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing state at {bucket}/{state_key}: {str(e)}")
            return False
    
    def retrieve_state(self, state_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve CDC state data from MinIO.
        
        Args:
            state_key: Key of the state to retrieve
            
        Returns:
            State data or None if not found/error
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return None
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return None
        
        # Get appropriate format handler
        handler = self._get_format_handler(state_key)
        
        try:
            # Check if this format uses metadata
            metadata_bytes = None
            try:
                metadata_key = f"{state_key}_metadata"
                metadata_response = self.client.get_object(bucket, metadata_key)
                metadata_bytes = metadata_response.read()
                metadata_response.close()
                metadata_response.release_conn()
            except S3Error as e:
                if "NoSuchKey" not in str(e):
                    raise
            
            # Get main data
            response = self.client.get_object(bucket, state_key)
            data_bytes = response.read()
            response.close()
            response.release_conn()
            
            # Use handler to retrieve data
            return handler.retrieve(data_bytes, metadata_bytes=metadata_bytes)
                
        except S3Error as e:
            if "NoSuchKey" in str(e):
                logger.info(f"No state found at {bucket}/{state_key}")
            else:
                logger.error(f"Error retrieving state from {bucket}/{state_key}: {str(e)}")
            return None
            
    def list_states(self, prefix: str = "") -> List[str]:
        """List available state objects with a given prefix.
        
        Args:
            prefix: Prefix to filter objects by (e.g., "table_name/")
            
        Returns:
            List of state keys
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return []
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return []
            
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            # Filter out metadata files
            return [obj.object_name for obj in objects if not obj.object_name.endswith("_metadata")]
        except S3Error as e:
            logger.error(f"Error listing states with prefix {prefix}: {str(e)}")
            return []
    
    def delete_state(self, state_key: str) -> bool:
        """Delete a state object from storage.
        
        Args:
            state_key: Key of the state to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return False
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return False
            
        try:
            # Delete main object
            self.client.remove_object(bucket, state_key)
            logger.info(f"Deleted state at {bucket}/{state_key}")
            
            # Try to delete metadata if it exists
            try:
                metadata_key = f"{state_key}_metadata"
                self.client.remove_object(bucket, metadata_key)
                logger.info(f"Deleted metadata at {bucket}/{metadata_key}")
            except S3Error as e:
                if "NoSuchKey" not in str(e):
                    logger.warning(f"Error deleting metadata: {str(e)}")
            
            return True
        except S3Error as e:
            logger.error(f"Error deleting state at {bucket}/{state_key}: {str(e)}")
            return False
            
    def store_snapshot(self, file_path: str, data: Dict[str, Any], content_type: str = None) -> bool:
        """Store snapshot data using the appropriate format handler.
        
        Args:
            file_path: Path/key to store the snapshot at
            data: Data to store
            content_type: Optional content type override
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            logger.error("MinIO client not initialized")
            return False
            
        bucket = self.storage_config.get("bucket")
        if not bucket:
            logger.error("Bucket name not specified in configuration")
            return False
            
        # Determine the format from the file extension
        format_type = "json"  # Default
        if "." in file_path:
            extension = file_path.split(".")[-1].lower()
            if extension in ["json", "parquet", "csv"]:
                format_type = extension
                
        # Get the appropriate format handler
        handler_class = FORMAT_HANDLERS.get(format_type)
        if not handler_class:
            logger.warning(f"Unknown format type: {format_type}, falling back to JSON")
            handler_class = FORMAT_HANDLERS["json"]
            
        logger.info(f"Using {handler_class.__name__} for storing snapshot at {file_path}")
        
        try:
            # Store the data using the format handler
            result = handler_class.store(data)
            
            # Unpack the results - main data stream, size, content type, and optional metadata
            if len(result) == 4 and result[3] is not None:
                data_stream, data_size, default_content_type, metadata_tuple = result
                metadata_stream, metadata_size = metadata_tuple
                
                # Store metadata
                metadata_path = f"{file_path}_metadata"
                self.client.put_object(
                    bucket,
                    metadata_path,
                    data=metadata_stream,
                    length=metadata_size,
                    content_type='application/json'
                )
                logger.info(f"Stored snapshot metadata at {bucket}/{metadata_path}")
            else:
                data_stream, data_size, default_content_type = result[:3]
            
            # Use provided content type or default from format handler
            content_type = content_type or default_content_type
            
            # Store main data
            self.client.put_object(
                bucket,
                file_path,
                data=data_stream,
                length=data_size,
                content_type=content_type
            )
            
            logger.info(f"Stored snapshot at {bucket}/{file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing snapshot at {bucket}/{file_path}: {str(e)}")
            return False
