"""
Example usage script for Informatica JSON and PySpark code vector database ingestion
"""

import os
from pathlib import Path
from informatica_vector_ingestion import InformaticaPySparkIngester
import json

# Sample Informatica JSON structure (for testing)
SAMPLE_INFORMATICA_JSON = {
    "FOLDER": {
        "NAME": "ETL_WORKFLOWS",
        "VERSION": "10.5.1",
        "REPOSITORY": "PROD_REPO",
        "MAPPING": [
            {
                "NAME": "CUSTOMER_DIM_LOAD",
                "DESCRIPTION": "Load customer dimension from source",
                "VERSION": "1.0",
                "TRANSFORMATION": [
                    {
                        "TYPE": "Source Qualifier",
                        "NAME": "SQ_CUSTOMER",
                        "ATTRIBUTE": {
                            "SQL_QUERY": "SELECT * FROM CUSTOMERS WHERE ACTIVE = 'Y'"
                        }
                    },
                    {
                        "TYPE": "Expression",
                        "NAME": "EXP_CUSTOMER_TRANSFORM",
                        "ATTRIBUTE": {
                            "EXPRESSION": "UPPER(CUSTOMER_NAME)"
                        }
                    },
                    {
                        "TYPE": "Filter",
                        "NAME": "FIL_VALID_CUSTOMERS",
                        "ATTRIBUTE": {
                            "FILTER_CONDITION": "CUSTOMER_ID IS NOT NULL"
                        }
                    }
                ],
                "CONNECTOR": ["SQ_TO_EXP", "EXP_TO_FIL", "FIL_TO_TGT"]
            }
        ],
        "WORKFLOW": {
            "NAME": "WF_CUSTOMER_LOAD",
            "DESCRIPTION": "Daily customer dimension load workflow",
            "SESSION": {
                "NAME": "S_CUSTOMER_DIM_LOAD",
                "MAPPING": "CUSTOMER_DIM_LOAD"
            }
        }
    }
}

# Sample PySpark code (for testing)
SAMPLE_PYSPARK_CODE = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerETL:
    """ETL pipeline for customer dimension processing"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def load_customer_data(self, source_path: str):
        """Load customer data from source"""
        schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("active", StringType(), True),
            StructField("created_date", StringType(), True)
        ])
        
        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(source_path)
            
        self.logger.info(f"Loaded {df.count()} customer records")
        return df
    
    def transform_customers(self, df):
        """Apply transformations to customer data"""
        # Expression transformation - uppercase customer name
        df_transformed = df.withColumn(
            "customer_name_upper",
            upper(col("customer_name"))
        )
        
        # Filter transformation - only active customers
        df_filtered = df_transformed.filter(
            (col("active") == "Y") & 
            (col("customer_id").isNotNull())
        )
        
        # Add derived columns
        df_final = df_filtered.withColumn(
            "customer_type",
            when(col("email").contains("@corporate"), "BUSINESS")
            .otherwise("INDIVIDUAL")
        )
        
        return df_final
    
    def save_to_target(self, df, target_path: str):
        """Save transformed data to target"""
        df.write \
            .mode("overwrite") \
            .partitionBy("customer_type") \
            .parquet(target_path)
        
        self.logger.info(f"Saved {df.count()} records to {target_path}")

def main():
    """Main ETL execution"""
    spark = SparkSession.builder \
        .appName("CustomerDimensionETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize ETL
        etl = CustomerETL(spark)
        
        # Extract
        df_raw = etl.load_customer_data("s3a://data-lake/raw/customers/")
        
        # Transform
        df_transformed = etl.transform_customers(df_raw)
        
        # Load
        etl.save_to_target(df_transformed, "s3a://data-lake/processed/dim_customer/")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
'''

def create_sample_files():
    """Create sample files for testing"""
    # Create sample Informatica JSON
    with open("informatica_metadata.json", "w") as f:
        json.dump(SAMPLE_INFORMATICA_JSON, f, indent=2)
    
    # Create sample PySpark code
    with open("pyspark_transformations.py", "w") as f:
        f.write(SAMPLE_PYSPARK_CODE)
    
    print("Created sample files: informatica_metadata.json and pyspark_transformations.py")

def example_chroma_ingestion():
    """Example using ChromaDB (default)"""
    print("\n=== ChromaDB Ingestion Example ===")
    
    # Initialize ingester with ChromaDB
    ingester = InformaticaPySparkIngester(db_type='chroma')
    
    # Ingest files
    num_chunks = ingester.ingest_combined(
        "informatica_metadata.json",
        "pyspark_transformations.py",
        collection_name="etl_metadata"
    )
    
    print(f"Ingested {num_chunks} chunks into ChromaDB")
    
    # Search examples
    queries = [
        "customer transformation filter",
        "expression uppercase",
        "spark sql query",
        "workflow session mapping"
    ]
    
    for query in queries:
        print(f"\nSearching for: '{query}'")
        results = ingester.search(query, k=2)
        for i, result in enumerate(results):
            print(f"\nResult {i+1}:")
            print(f"Type: {result['metadata'].get('type')}")
            print(f"Chunk Type: {result['metadata'].get('chunk_type')}")
            print(f"Score: {result['score']:.4f}")
            print(f"Content: {result['content'][:150]}...")

def example_faiss_ingestion():
    """Example using FAISS"""
    print("\n=== FAISS Ingestion Example ===")
    
    # Initialize ingester with FAISS
    ingester = InformaticaPySparkIngester(db_type='faiss')
    
    # Ingest files
    num_chunks = ingester.ingest_combined(
        "informatica_metadata.json",
        "pyspark_transformations.py",
        collection_name="etl_metadata_faiss"
    )
    
    print(f"Ingested {num_chunks} chunks into FAISS")
    
    # Save FAISS index
    ingester.db_ingester.save_faiss_index("./faiss_etl_index")
    print("FAISS index saved to ./faiss_etl_index")
    
    # Search example
    results = ingester.search("customer filter transformation", k=3)
    print(f"\nFound {len(results)} results")

def example_milvus_ingestion():
    """Example using Milvus (requires Milvus server running)"""
    print("\n=== Milvus Ingestion Example ===")
    
    try:
        # Initialize ingester with Milvus
        ingester = InformaticaPySparkIngester(db_type='milvus')
        
        # Configure Milvus connection
        connection_args = {
            "host": "localhost",
            "port": "19530"
        }
        
        # Initialize and ingest
        ingester.db_ingester.initialize_db(
            collection_name="etl_metadata_milvus",
            connection_args=connection_args
        )
        
        # Read files and create chunks
        with open("informatica_metadata.json", "r") as f:
            json_content = f.read()
        with open("pyspark_transformations.py", "r") as f:
            code_content = f.read()
        
        # Create documents
        json_docs = ingester.json_chunker.chunk(json_content, {'source': 'informatica_metadata.json'})
        code_docs = ingester.code_chunker.chunk(code_content, {'source': 'pyspark_transformations.py'})
        
        all_docs = json_docs + code_docs
        
        # Ingest
        ingester.db_ingester.ingest_documents(all_docs)
        
        print(f"Ingested {len(all_docs)} chunks into Milvus")
        
    except Exception as e:
        print(f"Milvus example failed (is Milvus server running?): {e}")

def example_advanced_search():
    """Example of advanced search with metadata filtering"""
    print("\n=== Advanced Search Example ===")
    
    # Initialize ingester
    ingester = InformaticaPySparkIngester(db_type='chroma')
    
    # Ingest if not already done
    if not Path("./chroma_db").exists():
        ingester.ingest_combined(
            "informatica_metadata.json",
            "pyspark_transformations.py"
        )
    
    # Search with different strategies
    print("\n1. Search for Informatica transformations:")
    results = ingester.search("transformation filter expression", k=3)
    informatica_results = [r for r in results if r['metadata'].get('type') == 'informatica_transformation']
    for r in informatica_results:
        print(f"- {r['metadata'].get('parent_entity')} -> {r['content'][:100]}...")
    
    print("\n2. Search for PySpark functions:")
    results = ingester.search("def transform load", k=3)
    pyspark_results = [r for r in results if r['metadata'].get('type') == 'pyspark_code']
    for r in pyspark_results:
        print(f"- {r['metadata'].get('unit_type')} {r['metadata'].get('unit_name', 'N/A')}")

def main():
    """Run all examples"""
    # Create sample files if they don't exist
    if not Path("informatica_metadata.json").exists() or not Path("pyspark_transformations.py").exists():
        create_sample_files()
    
    # Run examples
    try:
        example_chroma_ingestion()
    except Exception as e:
        print(f"ChromaDB example error: {e}")
    
    try:
        example_faiss_ingestion()
    except Exception as e:
        print(f"FAISS example error: {e}")
    
    try:
        example_milvus_ingestion()
    except Exception as e:
        print(f"Milvus example error: {e}")
    
    try:
        example_advanced_search()
    except Exception as e:
        print(f"Advanced search error: {e}")

if __name__ == "__main__":
    main()