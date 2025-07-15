# CDC Snapshot Diff System

A flexible, high-performance Change Data Capture (CDC) system for detecting changes between database snapshots with timestamped file output for ETL staging processes.

## Features

- **Performance Optimized**: Database-friendly queries (simple SELECT *) with backend hash calculations
- **Multiple Datasources**: Support for MySQL, PostgreSQL, and other SQL databases
- **Flexible CDC Methods**: 
  - Timestamp-based CDC for tables with update timestamps
  - Hash-based CDC with backend hash calculation
  - Hash-partition CDC for large tables with partitioning
- **Snapshot Storage**: Timestamped snapshot files in multiple formats (JSON, Parquet, CSV)
- **Persistent State**: CDC state storage in MinIO for reliability
- **ETL Ready**: Structured output files for downstream ETL processes
- **Scalable Architecture**: Strategy pattern for extensibility
- **Docker Environment**: Complete containerized setup with Airflow orchestration

## System Architecture

### Core Components

- **CDC Service**: Main orchestrator for change detection
- **Snapshot Service**: Handles timestamped file generation for ETL staging
- **Strategy Pattern**: 
  - CDC strategies for different detection methods
  - Snapshot strategies for different output formats
- **Database Manager**: Optimized database abstraction with connection pooling
- **Storage Manager**: MinIO/S3 integration for state and snapshot storage

### Performance Optimization

**Database Level** (Lightweight):
```sql
-- Simple, database-friendly queries
SELECT * FROM table_name;
SELECT COUNT(*) FROM table_name;
```

**Backend Level** (Processing):
```python
# Hash calculations in Python backend
hash_value = hashlib.md5(row_data.encode()).hexdigest()
# Change detection logic
# File generation and storage
```

## System Structure

```
├── services/                           # Core CDC implementation
│   ├── cdc.py                         # Main CDC service orchestrator
│   ├── cdc_strategy.py                # CDC strategy base and factory
│   ├── snapshot_service.py            # Snapshot file generation service
│   ├── snapshot_strategy.py           # Snapshot strategy base and factory
│   ├── strategies/                    # CDC method implementations
│   │   ├── timestamp_strategy.py      # Timestamp-based CDC
│   │   ├── hash_strategy.py           # Hash-based CDC (backend calculation)
│   │   └── hash_partition_strategy.py # Partitioned hash CDC for large tables
│   └── snapshot_strategies/           # Snapshot format implementations
│       ├── json_strategy.py           # JSON format output
│       ├── parquet_strategy.py        # Parquet format output
│       └── csv_strategy.py            # CSV format output
├── utils/                             # Utility modules
│   ├── database.py                    # Database abstraction with connection pooling
│   └── storage.py                     # MinIO/S3 storage manager
├── scripts/                           # Execution scripts
│   └── run_cdc.py                     # Command-line CDC execution
├── dags/                              # Airflow DAGs
│   └── cdc_dag.py                     # Airflow scheduling and orchestration
└── docker-compose.yml                 # Complete Docker environment
```

## Configuration

The system uses a central `config.json` file with enhanced configuration options:

```json
{
  "global_settings": {
    "batch_size": 10000,
    "connection_pool": {
      "pool_size": 5,
      "max_overflow": 10,
      "timeout": 30
    },
    "scheduling": {
      "enabled": true,
      "interval_seconds": 3600,
      "max_retries": 3,
      "retry_delay_seconds": 300,
      "timeout_seconds": 1800
    },
    "snapshot": {
      "enabled": true,
      "format": "json",
      "include_metadata": true,
      "compress_files": false
    }
  },
  "datasources": {
    "mysql_prod": {
      "url": "mysql+pymysql://user:pass@host:3306/db",
      "type": "mysql"
    },
    "postgres_analytics": {
      "url": "postgresql://user:pass@host:5432/db",
      "type": "postgresql"
    }
  },
  "tables": {
    "users": {
      "datasource": "mysql_prod",
      "schema": "my_database_schema",
      "primary_key": "id",
      "method": "timestamp",
      "timestamp_column": "updated_at",
      "snapshot_format": "json"
    },
    "products": {
      "datasource": "mysql_prod",
      "schema": "my_database_schema",
      "primary_key": "product_id",
      "method": "hash",
      "hash_columns": ["product_id", "name", "price", "category"],
      "snapshot_format": "parquet"
    },
    "large_transactions": {
      "datasource": "postgres_analytics",
      "schema": "analytics",
      "primary_key": "transaction_id",
      "method": "hash-partition",
      "partition_size": 10000,
      "hash_columns": ["*"],
      "snapshot_format": "csv"
    }
  },
  "storage": {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": false,
    "bucket": "cdc-state",
    "format": "json"
  }
}
```

## Setup

1. **Environment Configuration**:
```bash
cp .env.example .env
# Edit .env to set CDC_CONFIG_PATH
```

2. **Configuration Setup**:
```bash
# Configure your config.json with database connections and table settings
cp config.json.example config.json
```

3. **Docker Environment**:
```bash
# Start the complete environment
docker-compose up -d

# Check service status
docker-compose ps
```

## Docker Services

- **Airflow**: Single container with webserver and scheduler (LocalExecutor)
- **MinIO**: Object storage for CDC state and snapshot files
- **PostgreSQL**: Airflow metadata database

### Service Access

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MinIO API**: http://localhost:9000

## Dependencies

```bash
# Core dependencies
sqlalchemy>=1.4.24,<2.0
pymysql>=1.0.3
psycopg2-binary>=2.9.6
minio>=7.1.15
pandas>=2.0.0

# Snapshot format support
pyarrow>=10.0.0      # For Parquet files
fastparquet>=0.8.3   # Alternative Parquet engine

# Airflow dependencies
apache-airflow>=2.7.1
```

## Usage

### Using Airflow DAGs

The included DAG schedules CDC operations for all configured tables:

```python
# Access Airflow UI at http://localhost:8080
# Enable and trigger the 'cdc_snapshot_diff' DAG
```

### Command-Line Usage

```bash
# Process all tables
python scripts/run_cdc.py --config /path/to/config.json

# Process specific tables
python scripts/run_cdc.py --tables users products --config /path/to/config.json
```

### Programmatic Usage

#### CDC with Automatic Snapshots

```python
from utils.database import DatabaseManager
from utils.storage import StorageManager
from services.cdc import CDCService

# Initialize managers
db_manager = DatabaseManager("/path/to/config.json")
storage_manager = StorageManager("/path/to/config.json")

# Configure snapshot settings
snapshot_config = {
    "default_format": "parquet",
    "include_metadata": True
}

# Initialize CDC service with snapshot support
cdc_service = CDCService(db_manager, storage_manager, snapshot_config)

# Process table - automatically saves snapshots
result = cdc_service.process_table("users")

# Result includes snapshot information
print(result)
# {
#   "status": "success",
#   "table_name": "users",
#   "method": "hash",
#   "changes": {
#     "added": 5,
#     "modified": 3,
#     "deleted": 1
#   },
#   "snapshot": {
#     "status": "success",
#     "format": "parquet",
#     "files_saved": [
#       "snapshots/mysql_prod/users/20241215_143022_added.parquet",
#       "snapshots/mysql_prod/users/20241215_143022_modified.parquet",
#       "snapshots/mysql_prod/users/20241215_143022_deleted.parquet",
#       "snapshots/mysql_prod/users/20241215_143022_summary.json"
#     ],
#     "total_files": 4
#   }
# }
```

#### Direct Snapshot Service Usage

```python
from services.snapshot import SnapshotService

# Initialize snapshot service
snapshot_service = SnapshotService(storage_manager)

# Save changes in different formats
changes = {
    "added": [{"id": 1, "name": "New User"}],
    "modified": [{"id": 2, "name": "Updated User"}],
    "deleted": [{"primary_key": "id", "value": "3"}]
}

# Save as JSON
result = snapshot_service.save_changes_snapshot(
    table_name="users",
    datasource_name="mysql_prod",
    changes=changes,
    format_type="json"
)

# Save as Parquet
result = snapshot_service.save_changes_snapshot(
    table_name="users",
    datasource_name="mysql_prod",
    changes=changes,
    format_type="parquet"
)

# List available snapshots
snapshots = snapshot_service.list_snapshots(
    table_name="users",
    datasource_name="mysql_prod"
)
```

### Snapshot File Structure

Snapshot files are organized for easy ETL consumption:

```
MinIO Bucket (cdc-state)/
└── snapshots/
    └── {datasource_name}/
        └── {table_name}/
            ├── YYYYMMDD_HHMMSS_added.{format}
            ├── YYYYMMDD_HHMMSS_modified.{format}
            ├── YYYYMMDD_HHMMSS_deleted.{format}
            └── YYYYMMDD_HHMMSS_summary.json
```

**Example files:**
```
snapshots/mysql_prod/users/
├── 20241215_143022_added.parquet
├── 20241215_143022_modified.parquet
├── 20241215_143022_deleted.parquet
└── 20241215_143022_summary.json
```

**File content includes:**
- Original table data
- CDC metadata columns (`_cdc_operation`, `_cdc_timestamp`, `_cdc_table`, `_cdc_datasource`)
- Summary file with processing metadata and file listings

## Performance Benefits

### Database Optimization

**Before (Database-Heavy)**:
```sql
-- Complex queries that stress the database
SELECT *, MD5(CONCAT(col1, col2, col3)) as hash FROM large_table;
SELECT COUNT(*), SUM(hash_calculations) FROM complex_view;
```

**After (Database-Friendly)**:
```sql
-- Simple queries that are database-friendly
SELECT * FROM large_table;
SELECT COUNT(*) FROM large_table;
```

### Backend Processing

- **Hash calculations**: Moved to Python backend using `hashlib`
- **Change detection**: Optimized comparison logic in memory
- **Batch processing**: Configurable batch sizes for large datasets
- **Connection pooling**: Efficient database connection management

## Extending the System

### Adding New CDC Methods

```python
from services.cdc_strategy import CDCStrategy

class MyCustomCDCStrategy(CDCStrategy):
    def process(self, table_name, table_config, datasource_name):
        # Custom implementation
        pass

# Register in CDCStrategyFactory
```

### Adding New Snapshot Formats

```python
from services.snapshot_strategy import SnapshotStrategy

class MyCustomSnapshotStrategy(SnapshotStrategy):
    def save_snapshot(self, table_name, datasource_name, changes, timestamp):
        # Custom format implementation
        pass
    
    def get_file_extension(self):
        return "myformat"

# Register in SnapshotStrategyFactory
```

## Monitoring and Troubleshooting

### Airflow Monitoring

- **Web UI**: Monitor DAG execution and task status
- **Logs**: Detailed logging for each CDC operation
- **Alerts**: Configure email alerts for failures

### Database Performance

- **Query monitoring**: All queries are logged for performance analysis
- **Connection pooling**: Monitor connection usage and timeouts
- **Batch processing**: Adjust batch sizes based on system capacity

### Storage Monitoring

- **MinIO Console**: Monitor storage usage and file organization
- **State management**: Track CDC state files and snapshot files
- **Cleanup**: Regular cleanup of old snapshot files

## Multiple Storage Format Support

The CDC system supports multiple storage formats for both state information and snapshot data:

### Supported Formats

- **JSON**: Human-readable format, good for debugging and small datasets
- **Parquet**: Columnar format with efficient compression and fast query performance
- **CSV**: Universal compatibility format for integration with various systems

### Configuration Options

Storage formats can be configured at both the global and table-specific levels:

1. **Global storage format** (applies to all tables):
```json
"storage": {
  "endpoint": "minio:9000",
  "access_key": "minioadmin",
  "secret_key": "minioadmin",
  "secure": false,
  "bucket": "cdc-state",
  "format": "json"  // Global format: "json", "parquet", or "csv"
}
```

2. **Table-specific format** (overrides global format for snapshots):
```json
"tables": {
  "users": {
    "datasource": "mysql_prod",
    "schema": "my_database_schema",
    "primary_key": "id",
    "method": "hash",
    "hash_columns": ["*"],
    "snapshot_format": "parquet"  // Table-specific format
  },
  "products": {
    "datasource": "mysql_prod",
    "schema": "my_database_schema",
    "primary_key": "id",
    "method": "timestamp",
    "timestamp_column": "updated_at",
    "snapshot_format": "csv"  // Another table with different format
  }
}
```

### Storage Format Details

#### JSON Format
- State information and metadata are stored directly in JSON
- Snapshot data is stored as JSON arrays of records
- Pros: Human-readable, easy to debug
- Cons: Larger file size, slower processing for large datasets

#### Parquet Format
- State information and metadata stored separately from snapshot data
- Snapshot data stored in efficient columnar Parquet format
- Metadata stored as accompanying JSON file (`{state_key}_metadata`)
- Pros: Efficient compression, faster processing, better for analytical queries
- Cons: Requires additional libraries (pyarrow/fastparquet), not human-readable

#### CSV Format
- State information and metadata stored separately from snapshot data
- Snapshot data stored as CSV files for universal compatibility
- Metadata stored as accompanying JSON file (`{state_key}_metadata`)
- Pros: Universal compatibility with virtually all systems and tools
- Cons: Less efficient than Parquet, limited type information

### Format Selection Guidelines

- **JSON**: Best for development, debugging, and small tables
- **Parquet**: Recommended for production and large tables (>100K rows)
- **CSV**: Ideal when interoperating with legacy systems or tools without Parquet support

### Implementation Notes

The storage manager automatically handles the appropriate format based on configuration:

1. For hash state storage (internal use), JSON is recommended
2. For snapshot data (ETL staging), choose based on your downstream needs:
   - Use Parquet for data warehouse loading
   - Use CSV for legacy system compatibility
   - Use JSON for debugging or when working with document databases

### Storage Paths

Format-specific data is stored in MinIO with consistent paths:

```
cdc-state/
├── {datasource_name}/
│   └── {table_name}/
│       ├── hash_state             # Always JSON format
│       ├── snapshot               # Uses configured format (json/parquet/csv)
│       └── snapshot_metadata      # For parquet/csv formats
```

## Common Issues and Solutions

### Database Connection Issues
- Verify connection strings in `config.json`
- Check database accessibility from Docker containers
- Monitor connection pool usage

### MinIO Access Issues
- Ensure MinIO is running and accessible
- Verify credentials and bucket configuration
- Check network connectivity between services

### Performance Issues
- Adjust batch sizes for large tables
- Use hash-partition method for very large datasets
- Monitor memory usage during processing

### Snapshot File Issues
- Verify MinIO bucket permissions
- Check available storage space
- Monitor file format compatibility

## Contributing

When contributing to this project:

1. **Performance First**: Ensure database queries remain lightweight
2. **Strategy Pattern**: Follow existing patterns for extensibility
3. **Testing**: Test with various data sizes and formats
4. **Documentation**: Update README and code comments
5. **Logging**: Include appropriate logging for debugging

## License

This project is licensed under the MIT License - see the LICENSE file for details.