# Informatica JSON and PySpark Code Vector Database Ingestion

A comprehensive solution for ingesting Informatica metadata (JSON converted from XML) and PySpark code into vector databases with proper semantic chunking and search capabilities.

## Features

- **Semantic Chunking**: Intelligent chunking that preserves context and relationships
  - Informatica-specific chunking for mappings, workflows, transformations
  - PySpark code analysis with AST parsing for functions, classes, and operations
- **Multiple Vector Database Support**:
  - ChromaDB (default, persistent local storage)
  - FAISS (high-performance, in-memory)
  - Milvus (scalable, distributed)
  - Qdrant (cloud-native)
  - Weaviate (GraphQL support)
- **Advanced Search**: Semantic search with metadata filtering and relevance scoring
- **Configurable**: Flexible configuration for chunking, embedding models, and database settings

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. For specific vector databases:
```bash
# For Milvus
docker run -d --name milvus-standalone -p 19530:19530 -p 9091:9091 milvusdb/milvus:latest

# For Qdrant
docker run -d --name qdrant -p 6333:6333 qdrant/qdrant

# For Weaviate
docker run -d --name weaviate -p 8080:8080 semitechnologies/weaviate:latest
```

## Quick Start

### Basic Usage

```python
from informatica_vector_ingestion import InformaticaPySparkIngester

# Initialize ingester (ChromaDB by default)
ingester = InformaticaPySparkIngester()

# Ingest both files into a combined collection
num_chunks = ingester.ingest_combined(
    "informatica_metadata.json",
    "pyspark_transformations.py",
    collection_name="etl_metadata"
)

# Search
results = ingester.search("customer transformation filter", k=5)
for result in results:
    print(f"Score: {result['score']}")
    print(f"Content: {result['content'][:200]}...")
```

### Run Examples

```bash
# Run all examples with sample data
python usage_example.py
```

## Architecture

### Components

1. **SemanticChunker** (Abstract Base)
   - `InformaticaJSONChunker`: Handles Informatica metadata
   - `PySparkCodeChunker`: Handles PySpark code

2. **VectorDBIngester**: Manages vector database operations
   - Database initialization
   - Document ingestion
   - Search operations

3. **InformaticaPySparkIngester**: Main orchestrator class

### Chunking Strategy

#### Informatica JSON
- Extracts entities: Mappings, Workflows, Transformations, Sources, Targets
- Creates semantic chunks for:
  - Main entity information
  - Individual transformations
  - Properties and attributes
  - Connections and relationships

#### PySpark Code
- Uses AST parsing to identify:
  - Classes and methods
  - Functions with docstrings
  - PySpark-specific operations (select, filter, join, etc.)
- Falls back to regex parsing for invalid Python syntax

## Configuration

### Using config.py

```python
from config import get_db_config, get_embedding_model

# Get database configuration
db_config = get_db_config('milvus')

# Get embedding model
model = get_embedding_model('code')  # For code-specific embeddings
```

### Environment Variables

```bash
# Milvus
export MILVUS_HOST=localhost
export MILVUS_PORT=19530

# Qdrant
export QDRANT_HOST=localhost
export QDRANT_PORT=6333
export QDRANT_API_KEY=your_api_key

# Weaviate
export WEAVIATE_URL=http://localhost:8080
export WEAVIATE_API_KEY=your_api_key
```

## Advanced Usage

### Custom Chunking Parameters

```python
from informatica_vector_ingestion import InformaticaJSONChunker, PySparkCodeChunker

# Custom JSON chunker
json_chunker = InformaticaJSONChunker(
    chunk_size=800,
    chunk_overlap=150
)

# Custom code chunker
code_chunker = PySparkCodeChunker(
    chunk_size=2000,
    chunk_overlap=400
)
```

### Different Vector Databases

```python
# FAISS (high performance)
ingester = InformaticaPySparkIngester(db_type='faiss')
ingester.ingest_combined(json_path, code_path)
ingester.db_ingester.save_faiss_index('./my_index')

# Milvus (distributed)
ingester = InformaticaPySparkIngester(db_type='milvus')
ingester.db_ingester.initialize_db(
    collection_name='etl_data',
    connection_args={'host': 'localhost', 'port': '19530'}
)

# Qdrant (cloud-native)
ingester = InformaticaPySparkIngester(db_type='qdrant')
```

### Metadata Filtering

```python
# Search and filter by metadata
results = ingester.search("transformation", k=10)

# Filter for Informatica transformations only
informatica_results = [
    r for r in results 
    if r['metadata'].get('type') == 'informatica_transformation'
]

# Filter for PySpark functions
pyspark_functions = [
    r for r in results 
    if r['metadata'].get('unit_type') == 'function'
]
```

## Informatica JSON Structure

Expected structure for Informatica JSON (converted from XML):

```json
{
  "FOLDER": {
    "NAME": "folder_name",
    "MAPPING": [{
      "NAME": "mapping_name",
      "TRANSFORMATION": [{
        "TYPE": "transformation_type",
        "NAME": "transformation_name",
        "ATTRIBUTE": {}
      }]
    }],
    "WORKFLOW": {
      "NAME": "workflow_name",
      "SESSION": {
        "NAME": "session_name",
        "MAPPING": "mapping_reference"
      }
    }
  }
}
```

## PySpark Code Best Practices

For optimal chunking and search:

1. Use docstrings for classes and functions
2. Follow PySpark naming conventions
3. Include type hints where possible
4. Structure code with clear separation of concerns

## Performance Considerations

1. **Chunk Size**: Balance between context preservation and search efficiency
   - Smaller chunks (500-1000): Better for precise searches
   - Larger chunks (1500-2500): Better for context understanding

2. **Embedding Models**:
   - `all-MiniLM-L6-v2`: Fast, general purpose (default)
   - `all-mpnet-base-v2`: Higher quality, slower
   - `microsoft/codebert-base`: Optimized for code

3. **Vector Databases**:
   - ChromaDB: Best for local development
   - FAISS: Best for performance with smaller datasets
   - Milvus: Best for large-scale production
   - Qdrant: Best for cloud deployments

## Troubleshooting

### Common Issues

1. **Memory Issues with Large Files**
   - Use smaller chunk sizes
   - Process files in batches
   - Consider using FAISS with memory mapping

2. **Poor Search Results**
   - Adjust chunk overlap
   - Try different embedding models
   - Use more specific search queries

3. **Database Connection Issues**
   - Check if database services are running
   - Verify connection parameters
   - Check firewall settings

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - See LICENSE file for details
