"""
Example: Run Full Pipeline
Demonstrates how to run the complete data governance pipeline.
"""

import sys
sys.path.append('..')

from pipeline import GovernancePipeline
from loguru import logger


def main():
    """Run the complete data governance pipeline."""
    
    try:
        # Initialize pipeline
        pipeline = GovernancePipeline(config_path="../config/config.yaml")
        
        # Run full pipeline
        logger.info("Starting full data governance pipeline...")
        results = pipeline.run_full_pipeline()
        
        # Print detailed results
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION RESULTS")
        print("=" * 80)
        
        print("\n1. METADATA EXTRACTION")
        print("-" * 80)
        metadata = results.get('metadata', {})
        print(f"   Databases: {len(metadata.get('databases', []))}")
        print(f"   Schemas: {len(metadata.get('schemas', []))}")
        print(f"   Tables: {len(metadata.get('tables', []))}")
        print(f"   Columns: {len(metadata.get('columns', []))}")
        
        print("\n2. LINEAGE TRACKING")
        print("-" * 80)
        lineage = results.get('lineage', {})
        print(f"   Table Dependencies: {len(lineage.get('dependencies', []))}")
        print(f"   Query-based Lineage: {len(lineage.get('query_lineage', []))}")
        print(f"   Total Lineage Records: {lineage.get('total_lineage_records', 0)}")
        
        print("\n3. DATA QUALITY VALIDATION")
        print("-" * 80)
        dq = results.get('data_quality', {})
        print(f"   Total Checks: {dq.get('total_checks', 0)}")
        print(f"   Passed: {dq.get('passed_checks', 0)}")
        print(f"   Failed: {dq.get('failed_checks', 0)}")
        print(f"   Success Rate: {dq.get('success_rate', 0)*100:.2f}%")
        
        print("\n" + "=" * 80)
        print(f"Pipeline Status: {results['status']}")
        print(f"Total Duration: {results.get('duration_seconds', 0):.2f} seconds")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
