"""
Configuration settings for vector database ingestion
"""

import os
from typing import Dict, Any

# Embedding model configurations
EMBEDDING_MODELS = {
    "default": "all-MiniLM-L6-v2",
    "multilingual": "paraphrase-multilingual-MiniLM-L12-v2",
    "large": "all-mpnet-base-v2",
    "code": "microsoft/codebert-base"
}

# Chunking configurations
CHUNK_CONFIGS = {
    "informatica_json": {
        "chunk_size": 1000,
        "chunk_overlap": 200
    },
    "pyspark_code": {
        "chunk_size": 1500,
        "chunk_overlap": 300
    }
}

# Vector database configurations
VECTOR_DB_CONFIGS = {
    "chroma": {
        "persist_directory": "./chroma_db",
        "anonymized_telemetry": False,
        "allow_reset": True
    },
    "milvus": {
        "host": os.getenv("MILVUS_HOST", "localhost"),
        "port": os.getenv("MILVUS_PORT", "19530"),
        "user": os.getenv("MILVUS_USER", ""),
        "password": os.getenv("MILVUS_PASSWORD", ""),
        "secure": os.getenv("MILVUS_SECURE", "False").lower() == "true"
    },
    "qdrant": {
        "host": os.getenv("QDRANT_HOST", "localhost"),
        "port": int(os.getenv("QDRANT_PORT", "6333")),
        "api_key": os.getenv("QDRANT_API_KEY", None),
        "https": os.getenv("QDRANT_HTTPS", "False").lower() == "true"
    },
    "faiss": {
        "index_path": "./faiss_index",
        "index_type": "IndexFlatL2"  # Can be IndexFlatIP, IndexIVFFlat, etc.
    },
    "weaviate": {
        "url": os.getenv("WEAVIATE_URL", "http://localhost:8080"),
        "api_key": os.getenv("WEAVIATE_API_KEY", None),
        "additional_headers": {}
    }
}

# Informatica object type mappings
INFORMATICA_OBJECT_TYPES = {
    "mappings": ["MAPPING", "MAPPLET"],
    "workflows": ["WORKFLOW", "WORKLET"],
    "transformations": ["TRANSFORMATION"],
    "sources": ["SOURCE", "SOURCEDEFINITION"],
    "targets": ["TARGET", "TARGETDEFINITION"],
    "sessions": ["SESSION"],
    "connections": ["CONNECTION", "CONNECTIONREFERENCE"],
    "folders": ["FOLDER", "REPOSITORY"]
}

# PySpark pattern configurations
PYSPARK_PATTERNS = {
    "dataframe_operations": [
        (r'\.select\s*\(', 'select'),
        (r'\.filter\s*\(', 'filter'),
        (r'\.where\s*\(', 'where'),
        (r'\.groupBy\s*\(', 'groupBy'),
        (r'\.agg\s*\(', 'aggregation'),
        (r'\.join\s*\(', 'join'),
        (r'\.union\s*\(', 'union'),
        (r'\.unionAll\s*\(', 'unionAll'),
        (r'\.unionByName\s*\(', 'unionByName'),
        (r'\.distinct\s*\(', 'distinct'),
        (r'\.dropDuplicates\s*\(', 'dropDuplicates'),
        (r'\.orderBy\s*\(', 'orderBy'),
        (r'\.sort\s*\(', 'sort')
    ],
    "transformations": [
        (r'\.withColumn\s*\(', 'withColumn'),
        (r'\.withColumnRenamed\s*\(', 'withColumnRenamed'),
        (r'\.drop\s*\(', 'drop'),
        (r'\.dropna\s*\(', 'dropna'),
        (r'\.fillna\s*\(', 'fillna'),
        (r'\.replace\s*\(', 'replace'),
        (r'\.cast\s*\(', 'cast'),
        (r'\.alias\s*\(', 'alias')
    ],
    "io_operations": [
        (r'\.read\.\w+\s*\(', 'read'),
        (r'\.write\.\w+\s*\(', 'write'),
        (r'\.load\s*\(', 'load'),
        (r'\.save\s*\(', 'save'),
        (r'\.saveAsTable\s*\(', 'saveAsTable'),
        (r'\.insertInto\s*\(', 'insertInto')
    ],
    "sql_operations": [
        (r'spark\.sql\s*\(', 'spark_sql'),
        (r'\.createOrReplaceTempView\s*\(', 'createTempView'),
        (r'\.createGlobalTempView\s*\(', 'createGlobalTempView'),
        (r'\.createDataFrame\s*\(', 'createDataFrame')
    ],
    "window_operations": [
        (r'Window\.partitionBy\s*\(', 'window_partition'),
        (r'Window\.orderBy\s*\(', 'window_order'),
        (r'\.over\s*\(', 'window_over'),
        (r'row_number\s*\(', 'row_number'),
        (r'rank\s*\(', 'rank'),
        (r'dense_rank\s*\(', 'dense_rank')
    ],
    "udf_operations": [
        (r'udf\s*\(', 'udf_definition'),
        (r'@udf', 'udf_decorator'),
        (r'pandas_udf\s*\(', 'pandas_udf')
    ]
}

# Search optimization settings
SEARCH_SETTINGS = {
    "max_results": 10,
    "score_threshold": 0.7,
    "rerank": True,
    "hybrid_search": True
}

# Logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "vector_ingestion.log",
            "mode": "a"
        }
    },
    "loggers": {
        "informatica_vector_ingestion": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console"]
    }
}

def get_db_config(db_type: str) -> Dict[str, Any]:
    """Get configuration for a specific database type"""
    return VECTOR_DB_CONFIGS.get(db_type, {})

def get_embedding_model(model_type: str = "default") -> str:
    """Get embedding model name"""
    return EMBEDDING_MODELS.get(model_type, EMBEDDING_MODELS["default"])

def get_chunk_config(content_type: str) -> Dict[str, int]:
    """Get chunking configuration for content type"""
    return CHUNK_CONFIGS.get(content_type, CHUNK_CONFIGS["informatica_json"])