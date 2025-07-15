import logging
import json
from typing import Dict, Any, Optional, Generator
import pandas as pd
from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.engine import Engine, Connection

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for CDC operations using SQLAlchemy with simple SELECT queries."""
    
    def __init__(self, config_path: str):
        """Initialize the database manager with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.global_settings = self.config.get("global_settings", {})
        self.engines: Dict[str, Engine] = {}
        self.metadata = MetaData()
        self._initialize_engines()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        logger.info(f"Loading configuration from {self.config_path}")
        try:
            with open(self.config_path, 'r') as config_file:
                return json.load(config_file)
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
            
    def _initialize_engines(self) -> None:
        """Initialize SQLAlchemy engines for all datasources."""
        datasources_config = self.config.get("datasources", {})
        pool_settings = self.global_settings.get("connection_pool", {})
        
        for name, config in datasources_config.items():
            try:
                logger.info(f"Initializing engine for datasource: {name}")
                url = config["url"]
                
                engine = create_engine(
                    url,
                    pool_size=pool_settings.get("pool_size", 5),
                    max_overflow=pool_settings.get("max_overflow", 10),
                    pool_timeout=pool_settings.get("timeout", 30)
                )
                
                self.engines[name] = engine
            except Exception as e:
                logger.error(f"Failed to initialize engine for {name}: {str(e)}")
                
    def get_connection(self, datasource_name: str) -> Optional[Connection]:
        """Get a connection for the specified datasource."""
        engine = self.engines.get(datasource_name)
        if engine:
            return engine.connect()
        return None
    
    def get_table_info(self, datasource_name: str, table_name: str) -> Dict[str, Any]:
        """Get table schema information."""
        engine = self.engines.get(datasource_name)
        if not engine:
            logger.error(f"Datasource {datasource_name} not found")
            return {}
            
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        primary_keys = pk_constraint.get('constrained_columns', [])
        
        return {
            "columns": columns,
            "primary_keys": primary_keys
        }
    
    def fetch_data_in_batches(
        self, 
        datasource_name: str, 
        table_name: str, 
        batch_size: Optional[int] = None,
        where_clause: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """Fetch data from table in batches using pandas with SIMPLE SELECT queries.
        """
        if batch_size is None:
            batch_size = self.global_settings.get("batch_size", 10000)
            
        engine = self.engines.get(datasource_name)
        if not engine:
            logger.error(f"Datasource {datasource_name} not found")
            return
        
        # Get table configuration to access schema
        table_config = self.get_table_config(table_name)
        schema = table_config.get("schema", "") if table_config else ""
        
        # Build fully qualified table name with schema if present
        qualified_table_name = f"{schema}.{table_name}" if schema else table_name
        
        # SIMPLE SELECT * query - no computation di database level
        query = f"SELECT * FROM {qualified_table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        logger.info(f"Fetching data from {datasource_name}.{qualified_table_name} in batches of {batch_size}")
        logger.info(f"Query: {query}")
        
        # Use pandas to handle the batching - pandas akan handle chunking
        try:
            for chunk in pd.read_sql(query, engine, chunksize=batch_size):
                yield chunk
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            raise
            
    def execute_query(self, datasource_name: str, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a raw SQL query on the datasource.
        
        IMPORTANT: Pastikan query yang dijalankan adalah query sederhana
        untuk menghindari beban berlebih di database.
        """
        engine = self.engines.get(datasource_name)
        if not engine:
            logger.error(f"Datasource {datasource_name} not found")
            return None
            
        logger.info(f"Executing query on {datasource_name}: {query}")
        
        with engine.connect() as conn:
            try:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                raise
    
    def get_table_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific table."""
        tables_config = self.config.get("tables", {})
        return tables_config.get(table_name)
    
    def get_all_table_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configuration for all tables."""
        return self.config.get("tables", {})
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration."""
        return self.config.get("storage", {})
    
    def close_all_connections(self) -> None:
        """Close all database connections."""
        for name, engine in self.engines.items():
            try:
                logger.info(f"Disposing engine for: {name}")
                engine.dispose()
            except Exception as e:
                logger.error(f"Error disposing engine {name}: {str(e)}")
                
    def __enter__(self):
        """Support for context manager usage."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close all connections when exiting context."""
        self.close_all_connections()