# CDC Snapshot Diff System

A flexible and maintainable Change Data Capture (CDC) system for detecting changes between database snapshots.

## Features

- Support for multiple heterogeneous datasources (MySQL, PostgreSQL)
- Multiple CDC methods: timestamp-based, hash-based, and hash-partition for large tables
- Storage of CDC state in MinIO for persistence and retrieval
- Clean separation of concerns using Strategy pattern for CDC methods
- Efficient batch processing of large datasets

## System Structure

- **services/**: Core CDC implementation
  - `cdc.py`: Main CDC service orchestrator
  - `cdc_strategy.py`: Base strategy and factory implementation
  - **strategies/**: CDC method implementations
    - `timestamp_strategy.py`: Timestamp-based CDC
    - `hash_strategy.py`: Hash-based CDC
    - `hash_partition_strategy.py`: Partitioned hash-based CDC for large tables
- **utils/**: 
  - `database.py`: Database abstraction for multiple databases
  - `storage.py`: MinIO/S3 storage manager for CDC state
- **scripts/**: 
  - `run_cdc.py`: Operator script to execute CDC operations

## Setup

1. Copy the example environment file and configure your paths:

```bash
cp .env.example .env
```

2. Edit the `.env` file to set the path to your `config.json`

3. Ensure your `config.json` is properly configured with:
   - Database connection details
   - Table configurations with CDC methods
   - MinIO/S3 storage settings

## Docker Environment

The system includes a Docker Compose setup for running the complete CDC environment with Airflow orchestration and MinIO storage.

```bash
# Start the environment
docker-compose up -d

# Check the status of the services
docker-compose ps
```

### Accessing Services

- **Airflow UI**: 
  - URL: http://localhost:8080
  - Username: admin
  - Password: admin

- **MinIO Console**:
  - URL: http://localhost:9001
  - Username: minioadmin
  - Password: minioadmin

- **MinIO API**:
  - Endpoint: http://localhost:9000
  - Used by the CDC service for state storage

### Docker Services

- **airflow**: Single container running both Airflow webserver and scheduler with LocalExecutor
- **minio**: Object storage for CDC state persistence
- **postgres**: Database for Airflow metadata and serves as the Airflow backend

## Configuration

The system uses a central `config.json` file with the following example structure:

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
      "timestamp_column": "updated_at"
    },
    "products": {
      "datasource": "mysql_prod",
      "schema": "my_database_schema",
      "primary_key": "product_id",
      "method": "hash-partition",
      "partition_size": 10000,
      "hash_columns": ["product_id", "name", "price", "category"]
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

## Running CDC Operations

### Using Airflow DAGs

The included DAG (`cdc_dag.py`) schedules CDC operations for all configured tables. You can access and trigger this DAG from the Airflow UI.

### Command-Line Usage

You can also run CDC operations directly from the command line:

```bash
# Process all tables
python scripts/run_cdc.py --config /path/to/config.json

# Process specific tables
python scripts/run_cdc.py --tables table1 table2 --config /path/to/config.json
```

## Extending the System

### Adding New CDC Methods

To add a new CDC method:

1. Create a new strategy class in `services/strategies/` directory
2. Implement the required methods from `BaseCDCStrategy`
3. Register the strategy in the `CDCStrategyFactory`

Example:

```python
from services.cdc_strategy import BaseCDCStrategy, register_strategy

@register_strategy("my_new_method")
class MyNewCDCStrategy(BaseCDCStrategy):
    def detect_changes(self, table_name, table_config):
        # Implementation
        pass
```

## Troubleshooting

Common issues and solutions:

- **Connection errors**: Verify your database connection strings in `config.json`
- **MinIO access issues**: Ensure MinIO is running and credentials are correct
- **Airflow task failures**: Check Airflow logs for detailed error messages
