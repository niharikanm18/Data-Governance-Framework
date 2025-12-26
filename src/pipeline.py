"""
Main Data Governance Pipeline
Orchestrates metadata extraction, lineage tracking, and data quality validation.
"""

import sys
from typing import Dict, Any, List
from datetime import datetime
from loguru import logger
import yaml
import json

from connection import SnowflakeConnection
from metadata import MetadataExtractor
from lineage import LineageTracker
from quality import DataQualityValidator


class GovernancePipeline:
    """Main pipeline orchestrator for data governance framework."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize governance pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self._setup_logging()
        
        # Initialize connection
        self.connection = None
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_file = log_config.get('file', 'logs/governance.log')
        
        # Remove default handler
        logger.remove()
        
        # Add console handler
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
            level=log_level
        )
        
        # Add file handler
        logger.add(
            log_file,
            rotation="100 MB",
            retention="30 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
        )
        
        logger.info("Logging configured")
    
    def run_metadata_extraction(self) -> Dict[str, Any]:
        """
        Run metadata extraction pipeline.
        
        Returns:
            Metadata catalog
        """
        logger.info("Starting metadata extraction pipeline...")
        
        try:
            metadata_config = self.config.get('metadata', {})
            output_config = self.config.get('output', {})
            
            # Initialize metadata extractor
            extractor = MetadataExtractor(self.connection)
            
            # Get tracked databases
            tracked_databases = metadata_config.get('tracked_databases', [])
            
            # Extract full metadata
            catalog = extractor.extract_full_metadata(tracked_databases)
            
            # Save to Snowflake
            output_table = output_config.get('metadata_table', 'METADATA_CATALOG')
            extractor.save_metadata_to_snowflake(catalog, output_table)
            
            # Export to JSON if configured
            if output_config.get('export_json', False):
                output_file = f"output/metadata_catalog_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w') as f:
                    json.dump(catalog, f, indent=2)
                logger.info(f"Metadata catalog exported to {output_file}")
            
            logger.info("Metadata extraction pipeline completed successfully")
            return catalog
            
        except Exception as e:
            logger.error(f"Metadata extraction pipeline failed: {str(e)}")
            raise
    
    def run_lineage_tracking(self) -> Dict[str, Any]:
        """
        Run lineage tracking pipeline.
        
        Returns:
            Lineage information
        """
        logger.info("Starting lineage tracking pipeline...")
        
        try:
            lineage_config = self.config.get('lineage', {})
            output_config = self.config.get('output', {})
            metadata_config = self.config.get('metadata', {})
            
            # Initialize lineage tracker
            tracker = LineageTracker(self.connection)
            
            # Get tracked databases
            tracked_databases = metadata_config.get('tracked_databases', [])
            
            # Extract table dependencies
            all_dependencies = []
            for database in tracked_databases:
                dependencies = tracker.extract_table_dependencies(database)
                all_dependencies.extend(dependencies)
            
            # Extract query history lineage
            query_history_days = lineage_config.get('query_history_days', 7)
            query_lineage = tracker.extract_query_history_lineage(days=query_history_days)
            
            # Combine all lineage records
            all_lineage = all_dependencies + query_lineage
            
            # Save to Snowflake
            output_table = output_config.get('lineage_table', 'LINEAGE_GRAPH')
            tracker.save_lineage_to_snowflake(all_lineage, output_table)
            
            # Export lineage graph if configured
            if output_config.get('export_json', False):
                output_file = f"output/lineage_graph_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                tracker.export_lineage_graph(output_file)
            
            logger.info("Lineage tracking pipeline completed successfully")
            return {
                'dependencies': all_dependencies,
                'query_lineage': query_lineage,
                'total_lineage_records': len(all_lineage)
            }
            
        except Exception as e:
            logger.error(f"Lineage tracking pipeline failed: {str(e)}")
            raise
    
    def run_data_quality_validation(self, tables: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Run data quality validation pipeline.
        
        Args:
            tables: List of tables to validate (dict with database, schema, table keys)
            
        Returns:
            Validation results
        """
        logger.info("Starting data quality validation pipeline...")
        
        try:
            dq_config = self.config.get('data_quality', {})
            output_config = self.config.get('output', {})
            
            # Initialize validator
            validator = DataQualityValidator(self.connection, self.config)
            
            # If no tables specified, use metadata to get all tables
            if not tables:
                tables = self._get_tables_from_metadata()
            
            # Run validation for each table
            all_results = []
            for table_info in tables:
                database = table_info.get('database')
                schema = table_info.get('schema')
                table = table_info.get('table')
                
                logger.info(f"Validating table: {database}.{schema}.{table}")
                
                # Run comprehensive validation
                result = validator.run_comprehensive_validation(database, schema, table)
                all_results.append(result)
                
                # Run specific checks based on config
                if dq_config.get('rules', {}).get('uniqueness', {}).get('enabled', False):
                    # Example: Check primary key uniqueness
                    uniqueness_result = validator.check_uniqueness(
                        database, schema, table,
                        columns=[table_info.get('primary_key', 'id')]
                    )
                    all_results.append(uniqueness_result)
                
                if dq_config.get('rules', {}).get('timeliness', {}).get('enabled', False):
                    # Example: Check data freshness
                    max_age = dq_config.get('rules', {}).get('timeliness', {}).get('max_age_hours', 24)
                    timeliness_result = validator.check_timeliness(
                        database, schema, table,
                        timestamp_column=table_info.get('timestamp_column', 'created_at'),
                        max_age_hours=max_age
                    )
                    all_results.append(timeliness_result)
            
            # Save results to Snowflake
            output_table = output_config.get('dq_results_table', 'DQ_VALIDATION_RESULTS')
            validator.save_validation_results(all_results, output_table)
            
            # Generate report
            report = validator.generate_dq_report(all_results)
            
            # Export report if configured
            if output_config.get('export_json', False):
                output_file = f"output/dq_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w') as f:
                    json.dump(report, f, indent=2)
                logger.info(f"DQ report exported to {output_file}")
            
            logger.info("Data quality validation pipeline completed successfully")
            return report
            
        except Exception as e:
            logger.error(f"Data quality validation pipeline failed: {str(e)}")
            raise
    
    def _get_tables_from_metadata(self) -> List[Dict[str, str]]:
        """
        Get list of tables from metadata catalog.
        
        Returns:
            List of table information dictionaries
        """
        # This is a placeholder - in a real implementation, you would query
        # the metadata catalog table to get the list of tables
        metadata_config = self.config.get('metadata', {})
        tracked_databases = metadata_config.get('tracked_databases', [])
        
        tables = []
        # Example: return empty list if no specific tables configured
        # In production, this would query the metadata catalog
        
        return tables
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete data governance pipeline.
        
        Returns:
            Complete pipeline results
        """
        logger.info("=" * 80)
        logger.info("Starting FULL Data Governance Pipeline")
        logger.info("=" * 80)
        
        start_time = datetime.utcnow()
        results = {
            'pipeline_start': start_time.isoformat(),
            'metadata': None,
            'lineage': None,
            'data_quality': None,
            'status': 'RUNNING'
        }
        
        try:
            # Connect to Snowflake
            self.connection = SnowflakeConnection(self.config_path)
            self.connection.connect()
            
            # Step 1: Metadata Extraction
            logger.info("STEP 1/3: Metadata Extraction")
            results['metadata'] = self.run_metadata_extraction()
            
            # Step 2: Lineage Tracking
            logger.info("STEP 2/3: Lineage Tracking")
            results['lineage'] = self.run_lineage_tracking()
            
            # Step 3: Data Quality Validation
            logger.info("STEP 3/3: Data Quality Validation")
            results['data_quality'] = self.run_data_quality_validation()
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results['pipeline_end'] = end_time.isoformat()
            results['duration_seconds'] = duration
            results['status'] = 'COMPLETED'
            
            logger.info("=" * 80)
            logger.info(f"Data Governance Pipeline completed successfully in {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return results
            
        except Exception as e:
            results['status'] = 'FAILED'
            results['error'] = str(e)
            logger.error(f"Pipeline failed: {str(e)}")
            raise
            
        finally:
            # Close connection
            if self.connection:
                self.connection.close()


def main():
    """Main entry point."""
    try:
        # Initialize and run pipeline
        pipeline = GovernancePipeline()
        results = pipeline.run_full_pipeline()
        
        # Print summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Status: {results['status']}")
        print(f"Duration: {results.get('duration_seconds', 0):.2f} seconds")
        print(f"Start Time: {results['pipeline_start']}")
        print(f"End Time: {results.get('pipeline_end', 'N/A')}")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
