"""
Simple test script to verify vector ingestion functionality
"""

import json
from pathlib import Path

def test_imports():
    """Test if all modules can be imported"""
    try:
        from informatica_vector_ingestion import (
            InformaticaPySparkIngester,
            InformaticaJSONChunker,
            PySparkCodeChunker,
            VectorDBIngester
        )
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_chunkers():
    """Test chunker initialization"""
    try:
        from informatica_vector_ingestion import InformaticaJSONChunker, PySparkCodeChunker
        
        json_chunker = InformaticaJSONChunker()
        code_chunker = PySparkCodeChunker()
        
        print("✓ Chunkers initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Chunker initialization error: {e}")
        return False

def test_sample_chunking():
    """Test chunking with sample data"""
    try:
        from informatica_vector_ingestion import InformaticaJSONChunker, PySparkCodeChunker
        
        # Test JSON chunking
        json_chunker = InformaticaJSONChunker()
        sample_json = {
            "MAPPING": {
                "NAME": "Test_Mapping",
                "TRANSFORMATION": [{
                    "TYPE": "Filter",
                    "NAME": "FIL_TEST"
                }]
            }
        }
        
        json_docs = json_chunker.chunk(
            json.dumps(sample_json),
            {"source": "test.json"}
        )
        
        print(f"✓ JSON chunking successful: {len(json_docs)} chunks created")
        
        # Test code chunking
        code_chunker = PySparkCodeChunker()
        sample_code = """
def test_function():
    '''Test function'''
    df = spark.read.csv('test.csv')
    return df.filter(col('value') > 0)
"""
        
        code_docs = code_chunker.chunk(
            sample_code,
            {"source": "test.py"}
        )
        
        print(f"✓ Code chunking successful: {len(code_docs)} chunks created")
        return True
        
    except Exception as e:
        print(f"✗ Chunking test error: {e}")
        return False

def test_config():
    """Test configuration module"""
    try:
        from config import get_db_config, get_embedding_model, get_chunk_config
        
        db_config = get_db_config('chroma')
        embedding = get_embedding_model('default')
        chunk_config = get_chunk_config('informatica_json')
        
        print("✓ Configuration module working")
        print(f"  - Default embedding model: {embedding}")
        print(f"  - JSON chunk size: {chunk_config['chunk_size']}")
        return True
        
    except Exception as e:
        print(f"✗ Configuration test error: {e}")
        return False

def main():
    """Run all tests"""
    print("Running Vector Ingestion Tests...\n")
    
    tests = [
        ("Import Test", test_imports),
        ("Chunker Initialization", test_chunkers),
        ("Sample Chunking", test_sample_chunking),
        ("Configuration", test_config)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1
    
    print(f"\n\nTests Summary: {passed}/{len(tests)} passed")
    
    if passed == len(tests):
        print("\n✅ All tests passed! The ingestion system is ready to use.")
        print("\nNext steps:")
        print("1. Place your informatica_metadata.json file in the current directory")
        print("2. Place your pyspark_transformations.py file in the current directory")
        print("3. Run: python usage_example.py")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    main()