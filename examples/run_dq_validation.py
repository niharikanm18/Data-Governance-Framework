"""
Example: Run Data Quality Validation
Demonstrates how to perform data quality checks on Snowflake tables.
"""

import sys
sys.path.append('..')

from connection import SnowflakeConnection
from quality import DataQualityValidator
from loguru import logger


def main():
    """Run data quality validation checks."""
    
    # Initialize connection
    conn = SnowflakeConnection(config_path="../config/config.yaml")
    
    try:
        # Connect to Snowflake
        conn.connect()
        logger.info("Connected to Snowflake")
        
        # Initialize validator
        validator = DataQualityValidator(conn)
        
        # Define table to validate
        database = "PROD_DB"
        schema = "SALES"
        table = "ORDERS"
        
        print(f"\nValidating table: {database}.{schema}.{table}")
        print("=" * 60)
        
        # 1. Completeness check
        logger.info("Running completeness check...")
        completeness = validator.check_completeness(
            database, schema, table,
            columns=['order_id', 'customer_id', 'order_date', 'total_amount'],
            threshold=0.95
        )
        
        print(f"\n✓ Completeness Check: {completeness['overall_status']}")
        for col in completeness['columns']:
            print(f"  - {col['column_name']}: {col['completeness']*100:.2f}% complete "
                  f"({col['null_count']} nulls)")
        
        # 2. Uniqueness check
        logger.info("Running uniqueness check...")
        uniqueness = validator.check_uniqueness(
            database, schema, table,
            columns=['order_id']
        )
        
        print(f"\n✓ Uniqueness Check: {uniqueness['overall_status']}")
        for col in uniqueness['columns']:
            print(f"  - {col['column_name']}: {col['duplicate_count']} duplicates found")
        
        # 3. Validity check
        logger.info("Running validity check...")
        validity_rules = {
            'total_amount': 'total_amount > 0',
            'order_date': 'order_date <= CURRENT_DATE()'
        }
        validity = validator.check_validity(
            database, schema, table,
            validation_rules=validity_rules
        )
        
        print(f"\n✓ Validity Check: {validity['overall_status']}")
        for rule in validity['rules']:
            print(f"  - {rule['column_name']}: {rule['invalid_count']} invalid records")
        
        # 4. Timeliness check
        logger.info("Running timeliness check...")
        timeliness = validator.check_timeliness(
            database, schema, table,
            timestamp_column='order_date',
            max_age_hours=24
        )
        
        print(f"\n✓ Timeliness Check: {timeliness['status']}")
        print(f"  - Latest data: {timeliness['latest_timestamp']}")
        print(f"  - Age: {timeliness['age_hours']} hours")
        
        # Collect all results
        all_results = [completeness, uniqueness, validity, timeliness]
        
        # Save results to Snowflake
        validator.save_validation_results(
            all_results,
            "GOVERNANCE_DB.METADATA.DQ_VALIDATION_RESULTS"
        )
        logger.info("Validation results saved to Snowflake")
        
        # Generate report
        report = validator.generate_dq_report(all_results)
        
        print("\n" + "=" * 60)
        print("DATA QUALITY SUMMARY")
        print("=" * 60)
        print(f"Total Checks: {report['total_checks']}")
        print(f"Passed: {report['passed_checks']}")
        print(f"Failed: {report['failed_checks']}")
        print(f"Errors: {report['error_checks']}")
        print(f"Success Rate: {report['success_rate']*100:.2f}%")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
