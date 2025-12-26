"""
Data Quality Validation Module
Performs comprehensive data quality checks on Snowflake tables.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
import json

from ..connection import SnowflakeConnection


class DataQualityValidator:
    """Performs data quality validations."""
    
    def __init__(self, connection: SnowflakeConnection, config: Dict[str, Any] = None):
        """
        Initialize data quality validator.
        
        Args:
            connection: Snowflake connection instance
            config: Configuration dictionary
        """
        self.connection = connection
        self.config = config or {}
        self.validation_results = []
    
    def check_completeness(self, database: str, schema: str, table: str, 
                          columns: List[str] = None, threshold: float = 0.95) -> Dict[str, Any]:
        """
        Check data completeness (null values).
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            columns: List of columns to check (None for all)
            threshold: Acceptable completeness threshold
            
        Returns:
            Completeness check results
        """
        full_table_name = f"{database}.{schema}.{table}"
        
        try:
            # Get all columns if not specified
            if not columns:
                col_query = f"DESCRIBE TABLE {full_table_name}"
                col_results = self.connection.execute_query(col_query)
                columns = [col['name'] for col in col_results]
            
            # Get total row count
            count_query = f"SELECT COUNT(*) as total_rows FROM {full_table_name}"
            total_rows_result = self.connection.execute_query(count_query)
            total_rows = total_rows_result[0]['TOTAL_ROWS']
            
            completeness_results = {
                'check_type': 'COMPLETENESS',
                'table': full_table_name,
                'total_rows': total_rows,
                'columns': [],
                'overall_status': 'PASSED',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            for column in columns:
                # Count null values
                null_query = f"""
                SELECT 
                    COUNT(*) as null_count,
                    COUNT(*) * 100.0 / {total_rows} as null_percentage
                FROM {full_table_name}
                WHERE {column} IS NULL
                """
                
                null_result = self.connection.execute_query(null_query)
                null_count = null_result[0]['NULL_COUNT']
                null_percentage = null_result[0]['NULL_PERCENTAGE']
                
                completeness = 1 - (null_percentage / 100.0)
                status = 'PASSED' if completeness >= threshold else 'FAILED'
                
                if status == 'FAILED':
                    completeness_results['overall_status'] = 'FAILED'
                
                completeness_results['columns'].append({
                    'column_name': column,
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2),
                    'completeness': round(completeness, 4),
                    'threshold': threshold,
                    'status': status
                })
            
            logger.info(f"Completeness check completed for {full_table_name}")
            return completeness_results
            
        except Exception as e:
            logger.error(f"Completeness check failed: {str(e)}")
            return {
                'check_type': 'COMPLETENESS',
                'table': full_table_name,
                'status': 'ERROR',
                'error': str(e)
            }
    
    def check_uniqueness(self, database: str, schema: str, table: str, 
                        columns: List[str]) -> Dict[str, Any]:
        """
        Check data uniqueness (duplicate values).
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            columns: List of columns to check for uniqueness
            
        Returns:
            Uniqueness check results
        """
        full_table_name = f"{database}.{schema}.{table}"
        
        try:
            uniqueness_results = {
                'check_type': 'UNIQUENESS',
                'table': full_table_name,
                'columns': [],
                'overall_status': 'PASSED',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            for column in columns:
                # Check for duplicates
                dup_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(DISTINCT {column}) as distinct_count,
                    COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count
                FROM {full_table_name}
                """
                
                dup_result = self.connection.execute_query(dup_query)
                total_count = dup_result[0]['TOTAL_COUNT']
                distinct_count = dup_result[0]['DISTINCT_COUNT']
                duplicate_count = dup_result[0]['DUPLICATE_COUNT']
                
                uniqueness_ratio = distinct_count / total_count if total_count > 0 else 0
                status = 'PASSED' if duplicate_count == 0 else 'FAILED'
                
                if status == 'FAILED':
                    uniqueness_results['overall_status'] = 'FAILED'
                
                uniqueness_results['columns'].append({
                    'column_name': column,
                    'total_count': total_count,
                    'distinct_count': distinct_count,
                    'duplicate_count': duplicate_count,
                    'uniqueness_ratio': round(uniqueness_ratio, 4),
                    'status': status
                })
            
            logger.info(f"Uniqueness check completed for {full_table_name}")
            return uniqueness_results
            
        except Exception as e:
            logger.error(f"Uniqueness check failed: {str(e)}")
            return {
                'check_type': 'UNIQUENESS',
                'table': full_table_name,
                'status': 'ERROR',
                'error': str(e)
            }
    
    def check_validity(self, database: str, schema: str, table: str,
                      validation_rules: Dict[str, str]) -> Dict[str, Any]:
        """
        Check data validity against custom rules.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            validation_rules: Dictionary of column -> validation SQL condition
            
        Returns:
            Validity check results
        """
        full_table_name = f"{database}.{schema}.{table}"
        
        try:
            validity_results = {
                'check_type': 'VALIDITY',
                'table': full_table_name,
                'rules': [],
                'overall_status': 'PASSED',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            for column, rule in validation_rules.items():
                # Count invalid records
                invalid_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    SUM(CASE WHEN NOT ({rule}) THEN 1 ELSE 0 END) as invalid_count
                FROM {full_table_name}
                WHERE {column} IS NOT NULL
                """
                
                invalid_result = self.connection.execute_query(invalid_query)
                total_count = invalid_result[0]['TOTAL_COUNT']
                invalid_count = invalid_result[0]['INVALID_COUNT']
                
                validity_ratio = (total_count - invalid_count) / total_count if total_count > 0 else 0
                status = 'PASSED' if invalid_count == 0 else 'FAILED'
                
                if status == 'FAILED':
                    validity_results['overall_status'] = 'FAILED'
                
                validity_results['rules'].append({
                    'column_name': column,
                    'rule': rule,
                    'total_count': total_count,
                    'invalid_count': invalid_count,
                    'validity_ratio': round(validity_ratio, 4),
                    'status': status
                })
            
            logger.info(f"Validity check completed for {full_table_name}")
            return validity_results
            
        except Exception as e:
            logger.error(f"Validity check failed: {str(e)}")
            return {
                'check_type': 'VALIDITY',
                'table': full_table_name,
                'status': 'ERROR',
                'error': str(e)
            }
    
    def check_consistency(self, database: str, schema: str, table: str,
                         consistency_checks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Check data consistency (referential integrity, business rules).
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            consistency_checks: List of consistency check definitions
            
        Returns:
            Consistency check results
        """
        full_table_name = f"{database}.{schema}.{table}"
        
        try:
            consistency_results = {
                'check_type': 'CONSISTENCY',
                'table': full_table_name,
                'checks': [],
                'overall_status': 'PASSED',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            for check in consistency_checks:
                check_name = check.get('name', 'unnamed_check')
                check_query = check.get('query')
                
                if not check_query:
                    continue
                
                # Execute consistency check query
                result = self.connection.execute_query(check_query)
                inconsistent_count = result[0].get('INCONSISTENT_COUNT', 0) if result else 0
                
                status = 'PASSED' if inconsistent_count == 0 else 'FAILED'
                
                if status == 'FAILED':
                    consistency_results['overall_status'] = 'FAILED'
                
                consistency_results['checks'].append({
                    'check_name': check_name,
                    'inconsistent_count': inconsistent_count,
                    'status': status
                })
            
            logger.info(f"Consistency check completed for {full_table_name}")
            return consistency_results
            
        except Exception as e:
            logger.error(f"Consistency check failed: {str(e)}")
            return {
                'check_type': 'CONSISTENCY',
                'table': full_table_name,
                'status': 'ERROR',
                'error': str(e)
            }
    
    def check_timeliness(self, database: str, schema: str, table: str,
                        timestamp_column: str, max_age_hours: int = 24) -> Dict[str, Any]:
        """
        Check data timeliness (freshness).
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            timestamp_column: Column containing timestamp
            max_age_hours: Maximum acceptable age in hours
            
        Returns:
            Timeliness check results
        """
        full_table_name = f"{database}.{schema}.{table}"
        
        try:
            # Get latest timestamp
            freshness_query = f"""
            SELECT 
                MAX({timestamp_column}) as latest_timestamp,
                DATEDIFF('hour', MAX({timestamp_column}), CURRENT_TIMESTAMP()) as age_hours
            FROM {full_table_name}
            """
            
            freshness_result = self.connection.execute_query(freshness_query)
            latest_timestamp = freshness_result[0]['LATEST_TIMESTAMP']
            age_hours = freshness_result[0]['AGE_HOURS']
            
            status = 'PASSED' if age_hours <= max_age_hours else 'FAILED'
            
            timeliness_results = {
                'check_type': 'TIMELINESS',
                'table': full_table_name,
                'timestamp_column': timestamp_column,
                'latest_timestamp': str(latest_timestamp),
                'age_hours': age_hours,
                'max_age_hours': max_age_hours,
                'status': status,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Timeliness check completed for {full_table_name}")
            return timeliness_results
            
        except Exception as e:
            logger.error(f"Timeliness check failed: {str(e)}")
            return {
                'check_type': 'TIMELINESS',
                'table': full_table_name,
                'status': 'ERROR',
                'error': str(e)
            }
    
    def run_comprehensive_validation(self, database: str, schema: str, table: str,
                                   validation_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run comprehensive data quality validation.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            validation_config: Custom validation configuration
            
        Returns:
            Comprehensive validation results
        """
        config = validation_config or self.config.get('data_quality', {})
        
        results = {
            'table': f"{database}.{schema}.{table}",
            'validation_timestamp': datetime.utcnow().isoformat(),
            'checks': [],
            'overall_status': 'PASSED'
        }
        
        try:
            # Completeness check
            if config.get('rules', {}).get('completeness', {}).get('enabled', True):
                completeness = self.check_completeness(
                    database, schema, table,
                    threshold=config.get('rules', {}).get('completeness', {}).get('threshold', 0.95)
                )
                results['checks'].append(completeness)
                if completeness.get('overall_status') == 'FAILED':
                    results['overall_status'] = 'FAILED'
            
            # Additional checks can be added based on config
            
            logger.info(f"Comprehensive validation completed for {database}.{schema}.{table}")
            
        except Exception as e:
            logger.error(f"Comprehensive validation failed: {str(e)}")
            results['overall_status'] = 'ERROR'
            results['error'] = str(e)
        
        return results
    
    def save_validation_results(self, results: List[Dict[str, Any]], output_table: str) -> None:
        """
        Save validation results to Snowflake table.
        
        Args:
            results: List of validation results
            output_table: Name of the output table
        """
        # Create DQ results table if not exists
        create_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
            validation_id VARCHAR,
            table_name VARCHAR,
            check_type VARCHAR,
            check_status VARCHAR,
            validation_json VARIANT,
            validation_timestamp TIMESTAMP_NTZ,
            PRIMARY KEY (validation_id)
        )
        """
        
        self.connection.create_table_if_not_exists(output_table, create_table_ddl)
        
        # Prepare data for insertion
        insert_data = []
        
        for result in results:
            validation_id = f"{result.get('table', 'unknown')}_{result.get('check_type', 'unknown')}_{datetime.utcnow().timestamp()}"
            
            insert_data.append((
                validation_id,
                result.get('table', ''),
                result.get('check_type', ''),
                result.get('status', result.get('overall_status', 'UNKNOWN')),
                json.dumps(result),
                datetime.utcnow()
            ))
        
        # Insert data
        insert_query = f"""
        INSERT INTO {output_table}
        (validation_id, table_name, check_type, check_status, validation_json, validation_timestamp)
        VALUES (%s, %s, %s, %s, PARSE_JSON(%s), %s)
        """
        
        self.connection.execute_many(insert_query, insert_data)
        logger.info(f"Saved {len(insert_data)} validation results to {output_table}")
    
    def generate_dq_report(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a summary report from validation results.
        
        Args:
            results: List of validation results
            
        Returns:
            Summary report
        """
        report = {
            'report_timestamp': datetime.utcnow().isoformat(),
            'total_checks': len(results),
            'passed_checks': 0,
            'failed_checks': 0,
            'error_checks': 0,
            'check_summary': []
        }
        
        for result in results:
            status = result.get('status', result.get('overall_status', 'UNKNOWN'))
            
            if status == 'PASSED':
                report['passed_checks'] += 1
            elif status == 'FAILED':
                report['failed_checks'] += 1
            elif status == 'ERROR':
                report['error_checks'] += 1
            
            report['check_summary'].append({
                'table': result.get('table'),
                'check_type': result.get('check_type'),
                'status': status
            })
        
        report['success_rate'] = report['passed_checks'] / report['total_checks'] if report['total_checks'] > 0 else 0
        
        logger.info("Data quality report generated")
        return report
