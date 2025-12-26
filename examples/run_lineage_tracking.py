"""
Example: Run Lineage Tracking
Demonstrates how to track data lineage in Snowflake.
"""

import sys
sys.path.append('..')

from connection import SnowflakeConnection
from lineage import LineageTracker
from loguru import logger


def main():
    """Track data lineage from Snowflake."""
    
    # Initialize connection
    conn = SnowflakeConnection(config_path="../config/config.yaml")
    
    try:
        # Connect to Snowflake
        conn.connect()
        logger.info("Connected to Snowflake")
        
        # Initialize lineage tracker
        tracker = LineageTracker(conn)
        
        # Extract table dependencies
        logger.info("Extracting table dependencies...")
        dependencies = tracker.extract_table_dependencies('PROD_DB')
        
        # Extract query history lineage
        logger.info("Extracting query history lineage...")
        query_lineage = tracker.extract_query_history_lineage(days=7)
        
        # Print summary
        print("\n" + "=" * 60)
        print("LINEAGE TRACKING SUMMARY")
        print("=" * 60)
        print(f"Table Dependencies: {len(dependencies)}")
        print(f"Query-based Lineage: {len(query_lineage)}")
        print(f"Total Lineage Records: {len(dependencies) + len(query_lineage)}")
        print("=" * 60)
        
        # Example: Get lineage for specific table
        table_name = "PROD_DB.SALES.ORDERS"
        print(f"\nLineage for {table_name}:")
        
        upstream = tracker.get_upstream_lineage(table_name)
        print(f"  Upstream tables: {len(upstream.get('ancestors', []))}")
        
        downstream = tracker.get_downstream_lineage(table_name)
        print(f"  Downstream tables: {len(downstream.get('descendants', []))}")
        
        # Save to Snowflake
        all_lineage = dependencies + query_lineage
        tracker.save_lineage_to_snowflake(all_lineage, "GOVERNANCE_DB.METADATA.LINEAGE_GRAPH")
        logger.info("Lineage data saved to Snowflake")
        
        # Export graph
        tracker.export_lineage_graph("../output/lineage_graph.json")
        logger.info("Lineage graph exported")
        
    except Exception as e:
        logger.error(f"Lineage tracking failed: {str(e)}")
        raise
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
