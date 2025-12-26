"""
Example: Run Metadata Extraction
Demonstrates how to extract metadata from Snowflake databases.
"""

import sys
sys.path.append('..')

from connection import SnowflakeConnection
from metadata import MetadataExtractor
from loguru import logger


def main():
    """Extract metadata from specified databases."""
    
    # Initialize connection
    conn = SnowflakeConnection(config_path="../config/config.yaml")
    
    try:
        # Connect to Snowflake
        conn.connect()
        logger.info("Connected to Snowflake")
        
        # Initialize metadata extractor
        extractor = MetadataExtractor(conn)
        
        # Extract metadata for specific databases
        databases = ['PROD_DB', 'ANALYTICS_DB']
        
        logger.info(f"Extracting metadata for databases: {databases}")
        catalog = extractor.extract_full_metadata(databases)
        
        # Print summary
        print("\n" + "=" * 60)
        print("METADATA EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Databases: {len(catalog['databases'])}")
        print(f"Schemas: {len(catalog['schemas'])}")
        print(f"Tables: {len(catalog['tables'])}")
        print(f"Columns: {len(catalog['columns'])}")
        print("=" * 60)
        
        # Save to Snowflake
        extractor.save_metadata_to_snowflake(catalog, "GOVERNANCE_DB.METADATA.METADATA_CATALOG")
        logger.info("Metadata saved to Snowflake")
        
    except Exception as e:
        logger.error(f"Metadata extraction failed: {str(e)}")
        raise
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
