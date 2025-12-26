"""
Data Lineage Tracking Module
Tracks data lineage through Snowflake query history and object dependencies.
"""

from typing import List, Dict, Any, Set, Tuple
from datetime import datetime, timedelta
from loguru import logger
import json
import networkx as nx

from ..connection import SnowflakeConnection


class LineageTracker:
    """Tracks data lineage in Snowflake."""
    
    def __init__(self, connection: SnowflakeConnection):
        """
        Initialize lineage tracker.
        
        Args:
            connection: Snowflake connection instance
        """
        self.connection = connection
        self.lineage_graph = nx.DiGraph()
    
    def extract_table_dependencies(self, database_name: str, schema_name: str = None) -> List[Dict[str, Any]]:
        """
        Extract table dependencies from views and materialized views.
        
        Args:
            database_name: Name of the database
            schema_name: Optional schema name
            
        Returns:
            List of table dependencies
        """
        if schema_name:
            query = f"""
            SELECT 
                table_catalog as database_name,
                table_schema as schema_name,
                table_name,
                referenced_database,
                referenced_schema,
                referenced_object_name,
                referenced_object_domain
            FROM {database_name}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
            WHERE table_schema = '{schema_name}'
            """
        else:
            query = f"""
            SELECT 
                table_catalog as database_name,
                table_schema as schema_name,
                table_name,
                referenced_database,
                referenced_schema,
                referenced_object_name,
                referenced_object_domain
            FROM {database_name}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
            """
        
        try:
            results = self.connection.execute_query(query)
            dependencies = []
            
            for dep in results:
                dependency = {
                    'source_database': dep.get('REFERENCED_DATABASE'),
                    'source_schema': dep.get('REFERENCED_SCHEMA'),
                    'source_object': dep.get('REFERENCED_OBJECT_NAME'),
                    'source_type': dep.get('REFERENCED_OBJECT_DOMAIN'),
                    'target_database': dep.get('DATABASE_NAME'),
                    'target_schema': dep.get('SCHEMA_NAME'),
                    'target_object': dep.get('TABLE_NAME'),
                    'dependency_type': 'DIRECT',
                    'extracted_at': datetime.utcnow().isoformat()
                }
                dependencies.append(dependency)
                
                # Add to graph
                source = f"{dep.get('REFERENCED_DATABASE')}.{dep.get('REFERENCED_SCHEMA')}.{dep.get('REFERENCED_OBJECT_NAME')}"
                target = f"{dep.get('DATABASE_NAME')}.{dep.get('SCHEMA_NAME')}.{dep.get('TABLE_NAME')}"
                self.lineage_graph.add_edge(source, target, type='DIRECT')
            
            logger.info(f"Extracted {len(dependencies)} table dependencies")
            return dependencies
            
        except Exception as e:
            logger.error(f"Failed to extract table dependencies: {str(e)}")
            raise
    
    def extract_query_history_lineage(self, days: int = 7, limit: int = 10000) -> List[Dict[str, Any]]:
        """
        Extract lineage from query history.
        
        Args:
            days: Number of days to look back
            limit: Maximum number of queries to analyze
            
        Returns:
            List of lineage relationships from query history
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        query = f"""
        SELECT 
            query_id,
            query_text,
            database_name,
            schema_name,
            user_name,
            role_name,
            execution_status,
            start_time,
            end_time,
            total_elapsed_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE start_time >= '{start_time.isoformat()}'
          AND start_time <= '{end_time.isoformat()}'
          AND execution_status = 'SUCCESS'
          AND query_type IN ('INSERT', 'CREATE_TABLE_AS_SELECT', 'MERGE', 'UPDATE')
        ORDER BY start_time DESC
        LIMIT {limit}
        """
        
        try:
            results = self.connection.execute_query(query)
            lineage_records = []
            
            for query_record in results:
                query_text = query_record.get('QUERY_TEXT', '').upper()
                
                # Parse query to extract source and target tables
                source_tables = self._extract_source_tables(query_text)
                target_tables = self._extract_target_tables(query_text)
                
                for target in target_tables:
                    for source in source_tables:
                        lineage = {
                            'query_id': query_record.get('QUERY_ID'),
                            'source_table': source,
                            'target_table': target,
                            'query_type': self._determine_query_type(query_text),
                            'user_name': query_record.get('USER_NAME'),
                            'role_name': query_record.get('ROLE_NAME'),
                            'execution_time': str(query_record.get('START_TIME')),
                            'elapsed_time_ms': query_record.get('TOTAL_ELAPSED_TIME'),
                            'extracted_at': datetime.utcnow().isoformat()
                        }
                        lineage_records.append(lineage)
                        
                        # Add to graph
                        self.lineage_graph.add_edge(source, target, 
                                                   type='QUERY_BASED',
                                                   query_id=query_record.get('QUERY_ID'))
            
            logger.info(f"Extracted {len(lineage_records)} lineage records from query history")
            return lineage_records
            
        except Exception as e:
            logger.error(f"Failed to extract query history lineage: {str(e)}")
            raise
    
    def _extract_source_tables(self, query_text: str) -> Set[str]:
        """Extract source table names from query text."""
        sources = set()
        
        # Simple pattern matching (can be enhanced with SQL parser)
        keywords = ['FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']
        
        for keyword in keywords:
            if keyword in query_text:
                parts = query_text.split(keyword)
                for i in range(1, len(parts)):
                    # Extract table name (simplified)
                    tokens = parts[i].strip().split()
                    if tokens:
                        table_name = tokens[0].strip('(),;')
                        if '.' in table_name:
                            sources.add(table_name)
        
        return sources
    
    def _extract_target_tables(self, query_text: str) -> Set[str]:
        """Extract target table names from query text."""
        targets = set()
        
        # Look for INSERT INTO, CREATE TABLE, MERGE INTO patterns
        if 'INSERT INTO' in query_text:
            parts = query_text.split('INSERT INTO')
            for i in range(1, len(parts)):
                tokens = parts[i].strip().split()
                if tokens:
                    targets.add(tokens[0].strip('(),;'))
        
        if 'CREATE TABLE' in query_text:
            parts = query_text.split('CREATE TABLE')
            for i in range(1, len(parts)):
                tokens = parts[i].strip().split()
                if tokens and tokens[0] not in ['IF', 'OR']:
                    targets.add(tokens[0].strip('(),;'))
        
        if 'MERGE INTO' in query_text:
            parts = query_text.split('MERGE INTO')
            for i in range(1, len(parts)):
                tokens = parts[i].strip().split()
                if tokens:
                    targets.add(tokens[0].strip('(),;'))
        
        return targets
    
    def _determine_query_type(self, query_text: str) -> str:
        """Determine the type of query."""
        if 'INSERT' in query_text:
            return 'INSERT'
        elif 'CREATE TABLE' in query_text:
            return 'CREATE_TABLE_AS_SELECT'
        elif 'MERGE' in query_text:
            return 'MERGE'
        elif 'UPDATE' in query_text:
            return 'UPDATE'
        else:
            return 'UNKNOWN'
    
    def get_upstream_lineage(self, table_name: str, max_depth: int = 5) -> Dict[str, Any]:
        """
        Get upstream lineage for a table.
        
        Args:
            table_name: Fully qualified table name
            max_depth: Maximum depth to traverse
            
        Returns:
            Upstream lineage tree
        """
        if table_name not in self.lineage_graph:
            logger.warning(f"Table {table_name} not found in lineage graph")
            return {}
        
        upstream = {
            'table': table_name,
            'ancestors': []
        }
        
        try:
            # Get all ancestors up to max_depth
            for predecessor in self.lineage_graph.predecessors(table_name):
                upstream['ancestors'].append({
                    'table': predecessor,
                    'relationship': 'DIRECT_UPSTREAM'
                })
            
            logger.info(f"Retrieved upstream lineage for {table_name}")
            return upstream
            
        except Exception as e:
            logger.error(f"Failed to get upstream lineage: {str(e)}")
            return upstream
    
    def get_downstream_lineage(self, table_name: str, max_depth: int = 5) -> Dict[str, Any]:
        """
        Get downstream lineage for a table.
        
        Args:
            table_name: Fully qualified table name
            max_depth: Maximum depth to traverse
            
        Returns:
            Downstream lineage tree
        """
        if table_name not in self.lineage_graph:
            logger.warning(f"Table {table_name} not found in lineage graph")
            return {}
        
        downstream = {
            'table': table_name,
            'descendants': []
        }
        
        try:
            # Get all descendants up to max_depth
            for successor in self.lineage_graph.successors(table_name):
                downstream['descendants'].append({
                    'table': successor,
                    'relationship': 'DIRECT_DOWNSTREAM'
                })
            
            logger.info(f"Retrieved downstream lineage for {table_name}")
            return downstream
            
        except Exception as e:
            logger.error(f"Failed to get downstream lineage: {str(e)}")
            return downstream
    
    def get_full_lineage(self, table_name: str) -> Dict[str, Any]:
        """
        Get complete lineage (both upstream and downstream) for a table.
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            Complete lineage information
        """
        return {
            'table': table_name,
            'upstream': self.get_upstream_lineage(table_name),
            'downstream': self.get_downstream_lineage(table_name),
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def save_lineage_to_snowflake(self, lineage_records: List[Dict[str, Any]], output_table: str) -> None:
        """
        Save lineage records to Snowflake table.
        
        Args:
            lineage_records: List of lineage records
            output_table: Name of the output table
        """
        # Create lineage table if not exists
        create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            lineage_id VARCHAR,
            source_table VARCHAR,
            target_table VARCHAR,
            lineage_type VARCHAR,
            query_id VARCHAR,
            user_name VARCHAR,
            execution_time TIMESTAMP_NTZ,
            lineage_json VARIANT,
            extracted_at TIMESTAMP_NTZ,
            PRIMARY KEY (source_table, target_table, extracted_at)
        )
        """
        
        self.connection.create_table_if_not_exists(output_table, create_table_ddl)
        
        # Prepare data for insertion
        insert_data = []
        
        for record in lineage_records:
            insert_data.append((
                record.get('query_id', ''),
                record.get('source_table', ''),
                record.get('target_table', ''),
                record.get('query_type', 'UNKNOWN'),
                record.get('query_id', ''),
                record.get('user_name', ''),
                datetime.fromisoformat(record.get('execution_time', datetime.utcnow().isoformat())),
                json.dumps(record),
                datetime.utcnow()
            ))
        
        # Insert data
        insert_query = f"""
        INSERT INTO {output_table}
        (lineage_id, source_table, target_table, lineage_type, query_id, user_name, execution_time, lineage_json, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s)
        """
        
        self.connection.execute_many(insert_query, insert_data)
        logger.info(f"Saved {len(insert_data)} lineage records to {output_table}")
    
    def export_lineage_graph(self, output_file: str = "lineage_graph.json") -> None:
        """
        Export lineage graph to JSON file.
        
        Args:
            output_file: Path to output file
        """
        try:
            graph_data = {
                'nodes': list(self.lineage_graph.nodes()),
                'edges': [
                    {
                        'source': edge[0],
                        'target': edge[1],
                        'attributes': self.lineage_graph.edges[edge]
                    }
                    for edge in self.lineage_graph.edges()
                ]
            }
            
            with open(output_file, 'w') as f:
                json.dump(graph_data, f, indent=2)
            
            logger.info(f"Exported lineage graph to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to export lineage graph: {str(e)}")
