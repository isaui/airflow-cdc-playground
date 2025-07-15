#!/usr/bin/env python
"""
CDC Operator Script

This script runs CDC operations on specified tables or all tables
based on the configuration.
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database import DatabaseManager
from utils.storage import StorageManager
from services.cdc import CDCService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cdc_operator")


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration as a dictionary
    """
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        sys.exit(1)


def run_cdc(
    config_path: str,
    config: Dict[str, Any],
    table_names: Optional[List[str]] = None
) -> None:
    """Run CDC operations on specified tables or all tables.
    
    Args:
        config_path: Path to the configuration file
        config: Configuration dictionary
        table_names: List of table names to process, or None for all tables
    """
    # Initialize managers
    db_manager = DatabaseManager(config_path)
    storage_manager = StorageManager(config_path)
    
    # Initialize CDC service
    cdc_service = CDCService(db_manager, storage_manager)
    
    # Get table configs
    all_tables = config.get("tables", {})
    
    # Filter tables if names provided
    tables_to_process = {}
    if table_names:
        for name in table_names:
            if name in all_tables:
                tables_to_process[name] = all_tables[name]
            else:
                logger.warning(f"Table '{name}' not found in configuration")
    else:
        tables_to_process = all_tables
    
    # Process tables
    for table_name, _ in tables_to_process.items():
        logger.info(f"Processing table: {table_name}")
        try:
            result = cdc_service.process_table(table_name)
            
            # Log summary
            if result.get("status") == "success":
                changes = result.get("changes", {})
                logger.info(    
                    f"Table {table_name} processed successfully with method {result.get('method')}: "
                    f"Added={changes.get('added', 0)}, "
                    f"Modified={changes.get('modified', 0)}, "
                    f"Deleted={changes.get('deleted', 0)}"
                )
            else:
                logger.error(f"Failed to process table {table_name}: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}", exc_info=True)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run CDC operations")
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Specific table names to process. If not provided, all tables will be processed."
    )
    parser.add_argument(
        "--config",
        help="Path to config.json file (overrides .env setting)"
    )
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get config path from arguments or environment
    config_path = args.config or os.environ.get("CDC_CONFIG_PATH")
    if not config_path:
        logger.error("No configuration path provided. Set CDC_CONFIG_PATH in .env or use --config option.")
        sys.exit(1)
    
    # Load configuration
    config = load_config(config_path)
    
    # Run CDC
    run_cdc(config_path, config, args.tables)
    
    logger.info("CDC operations completed.")


if __name__ == "__main__":
    main()
