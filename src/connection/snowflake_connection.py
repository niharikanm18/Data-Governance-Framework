"""
Snowflake Connection Manager
Handles connections to Snowflake and provides query execution utilities.
"""

import os
from typing import Optional, Dict, Any, List
import snowflake.connector
from snowflake.connector import DictCursor
from loguru import logger
import yaml
from dotenv import load_dotenv


class SnowflakeConnection:
    """Manages Snowflake database connections and query execution."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize Snowflake connection.
        
        Args:
            config_path: Path to configuration file
        """
        load_dotenv()
        self.config = self._load_config(config_path)
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except FileNotFoundError:
            logger.warning(f"Config file not found at {config_path}, using defaults")
            return {}
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake.
        
        Returns:
            Snowflake connection object
        """
        try:
            sf_config = self.config.get('snowflake', {})
            
            self.connection = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT', sf_config.get('account')),
                user=os.getenv('SNOWFLAKE_USER', sf_config.get('user')),
                password=os.getenv('SNOWFLAKE_PASSWORD', sf_config.get('password')),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', sf_config.get('warehouse')),
                database=os.getenv('SNOWFLAKE_DATABASE', sf_config.get('database')),
                schema=os.getenv('SNOWFLAKE_SCHEMA', sf_config.get('schema')),
                role=os.getenv('SNOWFLAKE_ROLE', sf_config.get('role'))
            )
            
            logger.info("Successfully connected to Snowflake")
            return self.connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for parameterized queries
            
        Returns:
            List of dictionaries containing query results
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor(DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            
            logger.debug(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_many(self, query: str, data: List[tuple]) -> None:
        """
        Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query to execute
            data: List of tuples containing parameter values
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Batch insert completed: {len(data)} rows")
            
        except Exception as e:
            logger.error(f"Batch execution failed: {str(e)}")
            self.connection.rollback()
            raise
    
    def create_table_if_not_exists(self, table_name: str, schema_ddl: str) -> None:
        """
        Create a table if it doesn't exist.
        
        Args:
            table_name: Name of the table
            schema_ddl: DDL statement for table creation
        """
        try:
            self.execute_query(schema_ddl)
            logger.info(f"Table {table_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
