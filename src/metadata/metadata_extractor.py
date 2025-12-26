"""
Metadata Extraction Module
Extracts and catalogs metadata from Snowflake databases, tables, and columns.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from loguru import logger
import json

from ..connection import SnowflakeConnection


class MetadataExtractor:
    """Extracts metadata from Snowflake objects."""
    
    def __init__(self, connection: SnowflakeConnection):
        """
        Initialize metadata extractor.
        
        Args:
            connection: Snowflake connection instance
        """
        self.connection = connection
        self.metadata_catalog = []
    
    def extract_database_metadata(self, database_name: str) -> Dict[str, Any]:
        """
        Extract metadata for a specific database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            Dictionary containing database metadata
        """
        query = f"""
        SHOW DATABASES LIKE '{database_name}'
        """
        
        try:
            results = self.connection.execute_query(query)
            if results:
                db_info = results[0]
                metadata = {
                    'database_name': db_info.get('name'),
                    'created_on': str(db_info.get('created_on')),
                    'owner': db_info.get('owner'),
                    'comment': db_info.get('comment'),
                    'retention_time': db_info.get('retention_time'),
                    'extracted_at': datetime.utcnow().isoformat()
                }
                logger.info(f"Extracted metadata for database: {database_name}")
                return metadata
        except Exception as e:
            logger.error(f"Failed to extract database metadata: {str(e)}")
            raise
    
    def extract_schema_metadata(self, database_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Extract metadata for schemas in a database.
        
        Args:
            database_name: Name of the database
            schema_name: Optional specific schema name
            
        Returns:
            List of dictionaries containing schema metadata
        """
        if schema_name:
            query = f"SHOW SCHEMAS LIKE '{schema_name}' IN DATABASE {database_name}"
        else:
            query = f"SHOW SCHEMAS IN DATABASE {database_name}"
        
        try:
            results = self.connection.execute_query(query)
            schemas = []
            
            for schema in results:
                schema_metadata = {
                    'database_name': database_name,
                    'schema_name': schema.get('name'),
                    'created_on': str(schema.get('created_on')),
                    'owner': schema.get('owner'),
                    'comment': schema.get('comment'),
                    'extracted_at': datetime.utcnow().isoformat()
                }
                schemas.append(schema_metadata)
            
            logger.info(f"Extracted metadata for {len(schemas)} schemas in {database_name}")
            return schemas
            
        except Exception as e:
            logger.error(f"Failed to extract schema metadata: {str(e)}")
            raise
    
    def extract_table_metadata(self, database_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Extract metadata for tables in a database/schema.
        
        Args:
            database_name: Name of the database
            schema_name: Optional specific schema name
            
        Returns:
            List of dictionaries containing table metadata
        """
        if schema_name:
            query = f"SHOW TABLES IN {database_name}.{schema_name}"
        else:
            query = f"SHOW TABLES IN DATABASE {database_name}"
        
        try:
            results = self.connection.execute_query(query)
            tables = []
            
            for table in results:
                table_metadata = {
                    'database_name': table.get('database_name'),
                    'schema_name': table.get('schema_name'),
                    'table_name': table.get('name'),
                    'table_type': table.get('kind'),
                    'row_count': table.get('rows'),
                    'bytes': table.get('bytes'),
                    'created_on': str(table.get('created_on')),
                    'owner': table.get('owner'),
                    'comment': table.get('comment'),
                    'cluster_by': table.get('cluster_by'),
                    'extracted_at': datetime.utcnow().isoformat()
                }
                tables.append(table_metadata)
            
            logger.info(f"Extracted metadata for {len(tables)} tables")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to extract table metadata: {str(e)}")
            raise
    
    def extract_column_metadata(self, database_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract metadata for columns in a table.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of dictionaries containing column metadata
        """
        query = f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name}"
        
        try:
            results = self.connection.execute_query(query)
            columns = []
            
            for col in results:
                column_metadata = {
                    'database_name': database_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'column_name': col.get('name'),
                    'data_type': col.get('type'),
                    'nullable': col.get('null?') == 'Y',
                    'default_value': col.get('default'),
                    'primary_key': col.get('primary key') == 'Y',
                    'unique_key': col.get('unique key') == 'Y',
                    'comment': col.get('comment'),
                    'extracted_at': datetime.utcnow().isoformat()
                }
                columns.append(column_metadata)
            
            logger.info(f"Extracted metadata for {len(columns)} columns in {table_name}")
            return columns
            
        except Exception as e:
            logger.error(f"Failed to extract column metadata: {str(e)}")
            raise
    
    def extract_table_statistics(self, database_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Extract statistics for a table.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Dictionary containing table statistics
        """
        query = f"""
        SELECT 
            COUNT(*) as row_count,
            COUNT(DISTINCT *) as distinct_rows
        FROM {database_name}.{schema_name}.{table_name}
        """
        
        try:
            results = self.connection.execute_query(query)
            if results:
                stats = results[0]
                statistics = {
                    'database_name': database_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'row_count': stats.get('ROW_COUNT'),
                    'distinct_rows': stats.get('DISTINCT_ROWS'),
                    'extracted_at': datetime.utcnow().isoformat()
                }
                logger.info(f"Extracted statistics for {table_name}")
                return statistics
        except Exception as e:
            logger.warning(f"Failed to extract table statistics: {str(e)}")
            return {}
    
    def extract_full_metadata(self, databases: List[str]) -> Dict[str, Any]:
        """
        Extract complete metadata catalog for specified databases.
        
        Args:
            databases: List of database names to extract metadata from
            
        Returns:
            Complete metadata catalog
        """
        catalog = {
            'databases': [],
            'schemas': [],
            'tables': [],
            'columns': [],
            'extraction_timestamp': datetime.utcnow().isoformat()
        }
        
        for db in databases:
            try:
                # Extract database metadata
                db_metadata = self.extract_database_metadata(db)
                catalog['databases'].append(db_metadata)
                
                # Extract schema metadata
                schemas = self.extract_schema_metadata(db)
                catalog['schemas'].extend(schemas)
                
                # Extract table and column metadata
                tables = self.extract_table_metadata(db)
                catalog['tables'].extend(tables)
                
                for table in tables:
                    schema = table['schema_name']
                    table_name = table['table_name']
                    
                    # Extract column metadata
                    columns = self.extract_column_metadata(db, schema, table_name)
                    catalog['columns'].extend(columns)
                
                logger.info(f"Completed metadata extraction for database: {db}")
                
            except Exception as e:
                logger.error(f"Error extracting metadata for {db}: {str(e)}")
                continue
        
        return catalog
    
    def save_metadata_to_snowflake(self, catalog: Dict[str, Any], output_table: str) -> None:
        """
        Save metadata catalog to Snowflake table.
        
        Args:
            catalog: Metadata catalog to save
            output_table: Name of the output table
        """
        # Create metadata table if not exists
        create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            metadata_type VARCHAR,
            database_name VARCHAR,
            schema_name VARCHAR,
            object_name VARCHAR,
            metadata_json VARIANT,
            extracted_at TIMESTAMP_NTZ,
            PRIMARY KEY (metadata_type, database_name, schema_name, object_name, extracted_at)
        )
        """
        
        self.connection.create_table_if_not_exists(output_table, create_table_ddl)
        
        # Prepare data for insertion
        insert_data = []
        
        for db in catalog.get('databases', []):
            insert_data.append((
                'DATABASE',
                db['database_name'],
                None,
                db['database_name'],
                json.dumps(db),
                datetime.utcnow()
            ))
        
        for schema in catalog.get('schemas', []):
            insert_data.append((
                'SCHEMA',
                schema['database_name'],
                schema['schema_name'],
                schema['schema_name'],
                json.dumps(schema),
                datetime.utcnow()
            ))
        
        for table in catalog.get('tables', []):
            insert_data.append((
                'TABLE',
                table['database_name'],
                table['schema_name'],
                table['table_name'],
                json.dumps(table),
                datetime.utcnow()
            ))
        
        for column in catalog.get('columns', []):
            insert_data.append((
                'COLUMN',
                column['database_name'],
                column['schema_name'],
                f"{column['table_name']}.{column['column_name']}",
                json.dumps(column),
                datetime.utcnow()
            ))
        
        # Insert data
        insert_query = f"""
        INSERT INTO {output_table} 
        (metadata_type, database_name, schema_name, object_name, metadata_json, extracted_at)
        VALUES (%s, %s, %s, %s, PARSE_JSON(%s), %s)
        """
        
        self.connection.execute_many(insert_query, insert_data)
        logger.info(f"Saved {len(insert_data)} metadata records to {output_table}")
