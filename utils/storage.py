import logging
import json
from typing import Dict, Any, Optional, List
from minio import Minio
from minio.error import S3Error

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
            
        try:
            format_type = self.storage_config.get("format", "json").lower()
            
            if format_type == "json":
                data_str = json.dumps(data)
                data_bytes = data_str.encode('utf-8')
                
                self.client.put_object(
                    bucket,
                    state_key,
                    data=data_bytes,
                    length=len(data_bytes),
                    content_type='application/json'
                )
                
                logger.info(f"Stored state at {bucket}/{state_key}")
                return True
            else:
                logger.error(f"Unsupported format type: {format_type}")
                return False
        except S3Error as e:
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
            
        try:
            response = self.client.get_object(bucket, state_key)
            data_str = response.read().decode('utf-8')
            response.close()
            response.release_conn()
            
            format_type = self.storage_config.get("format", "json").lower()
            
            if format_type == "json":
                return json.loads(data_str)
            else:
                logger.error(f"Unsupported format type: {format_type}")
                return None
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
            return [obj.object_name for obj in objects]
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
            self.client.remove_object(bucket, state_key)
            logger.info(f"Deleted state at {bucket}/{state_key}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting state at {bucket}/{state_key}: {str(e)}")
            return False
