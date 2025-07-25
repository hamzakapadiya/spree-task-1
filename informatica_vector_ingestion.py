"""
Informatica JSON and PySpark Code Vector Database Ingestion
This module provides semantic chunking and ingestion of Informatica metadata 
and PySpark code into various vector databases.
"""

import json
import re
import ast
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import logging
from dataclasses import dataclass
from abc import ABC, abstractmethod

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.vectorstores import Chroma, Milvus, Qdrant, FAISS
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class InformaticaEntity:
    """Represents an Informatica entity with its metadata"""
    entity_type: str
    name: str
    description: str
    properties: Dict[str, Any]
    transformations: List[Dict[str, Any]]
    connections: List[str]
    metadata: Dict[str, Any]


class SemanticChunker(ABC):
    """Abstract base class for semantic chunking strategies"""
    
    @abstractmethod
    def chunk(self, content: str, metadata: Dict[str, Any]) -> List[Document]:
        pass


class InformaticaJSONChunker(SemanticChunker):
    """Specialized chunker for Informatica JSON structures"""
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
    def chunk(self, content: str, metadata: Dict[str, Any]) -> List[Document]:
        """
        Semantically chunk Informatica JSON based on logical boundaries
        """
        try:
            data = json.loads(content) if isinstance(content, str) else content
        except json.JSONDecodeError:
            logger.error("Invalid JSON content")
            return []
        
        documents = []
        
        # Extract and chunk different Informatica components
        components = self._extract_components(data)
        
        for component in components:
            # Create semantic chunks for each component
            chunks = self._create_semantic_chunks(component)
            documents.extend(chunks)
        
        return documents
    
    def _extract_components(self, data: Dict[str, Any]) -> List[InformaticaEntity]:
        """Extract Informatica components from JSON"""
        components = []
        
        # Common Informatica object types
        object_types = [
            'MAPPING', 'SESSION', 'WORKFLOW', 'SOURCE', 'TARGET',
            'TRANSFORMATION', 'MAPPLET', 'WORKLET', 'FOLDER'
        ]
        
        for obj_type in object_types:
            if obj_type in data:
                items = data[obj_type] if isinstance(data[obj_type], list) else [data[obj_type]]
                for item in items:
                    entity = self._parse_entity(obj_type, item)
                    if entity:
                        components.append(entity)
        
        # Also handle nested structures
        for key, value in data.items():
            if isinstance(value, dict) and key not in object_types:
                nested_components = self._extract_components(value)
                components.extend(nested_components)
        
        return components
    
    def _parse_entity(self, entity_type: str, data: Dict[str, Any]) -> Optional[InformaticaEntity]:
        """Parse an Informatica entity from JSON data"""
        try:
            return InformaticaEntity(
                entity_type=entity_type,
                name=data.get('NAME', data.get('name', 'Unknown')),
                description=data.get('DESCRIPTION', data.get('description', '')),
                properties=self._extract_properties(data),
                transformations=self._extract_transformations(data),
                connections=self._extract_connections(data),
                metadata={
                    'version': data.get('VERSION', data.get('version', '')),
                    'repository': data.get('REPOSITORY', data.get('repository', '')),
                    'folder': data.get('FOLDER', data.get('folder', ''))
                }
            )
        except Exception as e:
            logger.error(f"Error parsing entity: {e}")
            return None
    
    def _extract_properties(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract properties from entity data"""
        properties = {}
        property_keys = ['ATTRIBUTE', 'TABLEATTRIBUTE', 'PROPERTY', 'CONFIG']
        
        for key in property_keys:
            if key in data:
                if isinstance(data[key], list):
                    properties[key] = data[key]
                else:
                    properties[key] = [data[key]]
        
        return properties
    
    def _extract_transformations(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract transformations from entity data"""
        transformations = []
        
        if 'TRANSFORMATION' in data:
            trans_data = data['TRANSFORMATION']
            if isinstance(trans_data, list):
                transformations.extend(trans_data)
            else:
                transformations.append(trans_data)
        
        return transformations
    
    def _extract_connections(self, data: Dict[str, Any]) -> List[str]:
        """Extract connections/relationships from entity data"""
        connections = []
        
        # Look for connection-related fields
        connection_keys = ['CONNECTOR', 'CONNECTION', 'LINK', 'RELATIONSHIP']
        for key in connection_keys:
            if key in data:
                if isinstance(data[key], list):
                    connections.extend([str(conn) for conn in data[key]])
                else:
                    connections.append(str(data[key]))
        
        return connections
    
    def _create_semantic_chunks(self, entity: InformaticaEntity) -> List[Document]:
        """Create semantic chunks from an Informatica entity"""
        chunks = []
        
        # Create main entity chunk
        main_content = self._format_entity_content(entity)
        main_metadata = {
            'type': 'informatica_entity',
            'entity_type': entity.entity_type,
            'entity_name': entity.name,
            'chunk_type': 'main'
        }
        main_metadata.update(entity.metadata)
        
        chunks.append(Document(page_content=main_content, metadata=main_metadata))
        
        # Create chunks for transformations
        for i, transformation in enumerate(entity.transformations):
            trans_content = self._format_transformation_content(transformation, entity)
            trans_metadata = {
                'type': 'informatica_transformation',
                'parent_entity': entity.name,
                'parent_type': entity.entity_type,
                'transformation_index': i,
                'chunk_type': 'transformation'
            }
            chunks.append(Document(page_content=trans_content, metadata=trans_metadata))
        
        # Create chunks for complex properties
        if entity.properties:
            prop_content = self._format_properties_content(entity.properties, entity)
            prop_metadata = {
                'type': 'informatica_properties',
                'parent_entity': entity.name,
                'parent_type': entity.entity_type,
                'chunk_type': 'properties'
            }
            chunks.append(Document(page_content=prop_content, metadata=prop_metadata))
        
        return chunks
    
    def _format_entity_content(self, entity: InformaticaEntity) -> str:
        """Format entity content for embedding"""
        content = f"{entity.entity_type}: {entity.name}\n"
        if entity.description:
            content += f"Description: {entity.description}\n"
        
        content += f"\nConnections: {', '.join(entity.connections) if entity.connections else 'None'}\n"
        content += f"Number of transformations: {len(entity.transformations)}\n"
        
        # Add key metadata
        for key, value in entity.metadata.items():
            if value:
                content += f"{key.title()}: {value}\n"
        
        return content
    
    def _format_transformation_content(self, transformation: Dict[str, Any], parent: InformaticaEntity) -> str:
        """Format transformation content for embedding"""
        content = f"Transformation in {parent.entity_type} '{parent.name}'\n"
        content += f"Type: {transformation.get('TYPE', 'Unknown')}\n"
        content += f"Name: {transformation.get('NAME', 'Unknown')}\n"
        
        # Add transformation details
        for key, value in transformation.items():
            if key not in ['TYPE', 'NAME'] and value:
                content += f"{key}: {value}\n"
        
        return content
    
    def _format_properties_content(self, properties: Dict[str, Any], parent: InformaticaEntity) -> str:
        """Format properties content for embedding"""
        content = f"Properties for {parent.entity_type} '{parent.name}'\n"
        
        for prop_type, prop_list in properties.items():
            content += f"\n{prop_type}:\n"
            for prop in prop_list:
                if isinstance(prop, dict):
                    for key, value in prop.items():
                        content += f"  {key}: {value}\n"
                else:
                    content += f"  {prop}\n"
        
        return content


class PySparkCodeChunker(SemanticChunker):
    """Specialized chunker for PySpark code"""
    
    def __init__(self, chunk_size: int = 1500, chunk_overlap: int = 300):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\nclass ", "\ndef ", "\n\n", "\n", " ", ""],
            keep_separator=True
        )
    
    def chunk(self, content: str, metadata: Dict[str, Any]) -> List[Document]:
        """
        Semantically chunk PySpark code based on logical boundaries
        """
        documents = []
        
        # Parse the code to extract semantic units
        semantic_units = self._extract_semantic_units(content)
        
        for unit in semantic_units:
            # Create chunks for each semantic unit
            unit_chunks = self._create_code_chunks(unit)
            documents.extend(unit_chunks)
        
        # Also create general chunks for the entire code
        general_chunks = self.text_splitter.create_documents(
            [content],
            [{'type': 'pyspark_code', 'chunk_type': 'general', **metadata}]
        )
        documents.extend(general_chunks)
        
        return documents
    
    def _extract_semantic_units(self, code: str) -> List[Dict[str, Any]]:
        """Extract semantic units from PySpark code"""
        units = []
        
        try:
            tree = ast.parse(code)
        except SyntaxError:
            logger.warning("Could not parse code as valid Python, falling back to regex")
            return self._extract_units_regex(code)
        
        # Extract classes, functions, and important PySpark operations
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                units.append({
                    'type': 'class',
                    'name': node.name,
                    'content': ast.get_source_segment(code, node),
                    'docstring': ast.get_docstring(node),
                    'line_start': node.lineno
                })
            elif isinstance(node, ast.FunctionDef):
                units.append({
                    'type': 'function',
                    'name': node.name,
                    'content': ast.get_source_segment(code, node),
                    'docstring': ast.get_docstring(node),
                    'line_start': node.lineno
                })
        
        # Extract PySpark-specific patterns
        pyspark_patterns = self._extract_pyspark_patterns(code)
        units.extend(pyspark_patterns)
        
        return units
    
    def _extract_units_regex(self, code: str) -> List[Dict[str, Any]]:
        """Fallback regex-based extraction"""
        units = []
        
        # Extract functions
        func_pattern = r'def\s+(\w+)\s*\([^)]*\):\s*(?:"""([^"]*)""")?'
        for match in re.finditer(func_pattern, code, re.MULTILINE | re.DOTALL):
            units.append({
                'type': 'function',
                'name': match.group(1),
                'content': match.group(0),
                'docstring': match.group(2) if match.group(2) else '',
                'line_start': code[:match.start()].count('\n') + 1
            })
        
        # Extract classes
        class_pattern = r'class\s+(\w+)(?:\([^)]*\))?:\s*(?:"""([^"]*)""")?'
        for match in re.finditer(class_pattern, code, re.MULTILINE | re.DOTALL):
            units.append({
                'type': 'class',
                'name': match.group(1),
                'content': match.group(0),
                'docstring': match.group(2) if match.group(2) else '',
                'line_start': code[:match.start()].count('\n') + 1
            })
        
        return units
    
    def _extract_pyspark_patterns(self, code: str) -> List[Dict[str, Any]]:
        """Extract PySpark-specific patterns"""
        patterns = []
        
        # Common PySpark operations
        pyspark_operations = [
            (r'spark\.sql\s*\(([^)]+)\)', 'spark_sql'),
            (r'\.createDataFrame\s*\(', 'dataframe_creation'),
            (r'\.read\.\w+\s*\(', 'data_reading'),
            (r'\.write\.\w+\s*\(', 'data_writing'),
            (r'\.select\s*\(', 'select_operation'),
            (r'\.filter\s*\(', 'filter_operation'),
            (r'\.groupBy\s*\(', 'groupby_operation'),
            (r'\.join\s*\(', 'join_operation'),
            (r'\.agg\s*\(', 'aggregation'),
            (r'\.withColumn\s*\(', 'column_operation'),
            (r'udf\s*\(', 'udf_definition'),
            (r'\.rdd\s*\.', 'rdd_operation')
        ]
        
        for pattern, op_type in pyspark_operations:
            for match in re.finditer(pattern, code, re.MULTILINE):
                # Extract surrounding context
                start = max(0, match.start() - 200)
                end = min(len(code), match.end() + 200)
                context = code[start:end]
                
                patterns.append({
                    'type': 'pyspark_operation',
                    'operation': op_type,
                    'content': context,
                    'line_start': code[:match.start()].count('\n') + 1
                })
        
        return patterns
    
    def _create_code_chunks(self, unit: Dict[str, Any]) -> List[Document]:
        """Create chunks from a semantic unit"""
        chunks = []
        
        content = unit.get('content', '')
        if not content:
            return chunks
        
        # Create metadata for the unit
        metadata = {
            'type': 'pyspark_code',
            'unit_type': unit['type'],
            'unit_name': unit.get('name', 'unknown'),
            'line_start': unit.get('line_start', 0),
            'chunk_type': 'semantic_unit'
        }
        
        if unit.get('docstring'):
            metadata['docstring'] = unit['docstring']
        
        if unit['type'] == 'pyspark_operation':
            metadata['operation'] = unit.get('operation', 'unknown')
        
        # If the unit is small enough, create a single chunk
        if len(content) <= self.chunk_size:
            chunks.append(Document(page_content=content, metadata=metadata))
        else:
            # Otherwise, split it further
            sub_chunks = self.text_splitter.create_documents([content], [metadata])
            chunks.extend(sub_chunks)
        
        return chunks


class VectorDBIngester:
    """Handles ingestion into various vector databases"""
    
    def __init__(self, db_type: str = 'chroma', embedding_model: str = 'all-MiniLM-L6-v2'):
        self.db_type = db_type
        self.embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
        self.vectorstore = None
        
    def initialize_db(self, collection_name: str = 'informatica_pyspark', **kwargs):
        """Initialize the vector database"""
        if self.db_type == 'chroma':
            self.vectorstore = Chroma(
                collection_name=collection_name,
                embedding_function=self.embeddings,
                persist_directory=kwargs.get('persist_directory', './chroma_db')
            )
        elif self.db_type == 'milvus':
            self.vectorstore = Milvus(
                embedding_function=self.embeddings,
                collection_name=collection_name,
                connection_args=kwargs.get('connection_args', {"host": "localhost", "port": "19530"})
            )
        elif self.db_type == 'qdrant':
            from qdrant_client import QdrantClient
            client = QdrantClient(**kwargs.get('client_args', {}))
            self.vectorstore = Qdrant(
                client=client,
                collection_name=collection_name,
                embeddings=self.embeddings
            )
        elif self.db_type == 'faiss':
            self.vectorstore = FAISS(
                embedding_function=self.embeddings,
                index=None,
                docstore=None,
                index_to_docstore_id={}
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def ingest_documents(self, documents: List[Document], batch_size: int = 100):
        """Ingest documents into the vector database"""
        if not self.vectorstore:
            raise ValueError("Vector database not initialized")
        
        total_docs = len(documents)
        logger.info(f"Ingesting {total_docs} documents into {self.db_type}")
        
        # Process in batches
        for i in tqdm(range(0, total_docs, batch_size), desc="Ingesting documents"):
            batch = documents[i:i + batch_size]
            
            if self.db_type == 'faiss':
                if i == 0:
                    self.vectorstore = FAISS.from_documents(batch, self.embeddings)
                else:
                    self.vectorstore.add_documents(batch)
            else:
                self.vectorstore.add_documents(batch)
        
        logger.info("Ingestion completed")
        
        # Persist if supported
        if hasattr(self.vectorstore, 'persist'):
            self.vectorstore.persist()
            logger.info("Vector database persisted")
    
    def save_faiss_index(self, path: str = './faiss_index'):
        """Save FAISS index to disk"""
        if self.db_type == 'faiss' and self.vectorstore:
            self.vectorstore.save_local(path)
            logger.info(f"FAISS index saved to {path}")
    
    def search(self, query: str, k: int = 5) -> List[Document]:
        """Search the vector database"""
        if not self.vectorstore:
            raise ValueError("Vector database not initialized")
        
        return self.vectorstore.similarity_search(query, k=k)
    
    def search_with_score(self, query: str, k: int = 5) -> List[Tuple[Document, float]]:
        """Search with relevance scores"""
        if not self.vectorstore:
            raise ValueError("Vector database not initialized")
        
        return self.vectorstore.similarity_search_with_score(query, k=k)


class InformaticaPySparkIngester:
    """Main class for ingesting Informatica JSON and PySpark code"""
    
    def __init__(self, db_type: str = 'chroma', embedding_model: str = 'all-MiniLM-L6-v2'):
        self.json_chunker = InformaticaJSONChunker()
        self.code_chunker = PySparkCodeChunker()
        self.db_ingester = VectorDBIngester(db_type, embedding_model)
        
    def ingest_informatica_json(self, json_path: str, collection_name: str = 'informatica_metadata'):
        """Ingest Informatica JSON file"""
        logger.info(f"Processing Informatica JSON: {json_path}")
        
        # Read JSON file
        with open(json_path, 'r') as f:
            json_content = f.read()
        
        # Create semantic chunks
        documents = self.json_chunker.chunk(
            json_content,
            {'source': json_path, 'file_type': 'informatica_json'}
        )
        
        logger.info(f"Created {len(documents)} chunks from Informatica JSON")
        
        # Initialize database and ingest
        self.db_ingester.initialize_db(collection_name)
        self.db_ingester.ingest_documents(documents)
        
        return len(documents)
    
    def ingest_pyspark_code(self, code_path: str, collection_name: str = 'pyspark_code'):
        """Ingest PySpark code file"""
        logger.info(f"Processing PySpark code: {code_path}")
        
        # Read code file
        with open(code_path, 'r') as f:
            code_content = f.read()
        
        # Create semantic chunks
        documents = self.code_chunker.chunk(
            code_content,
            {'source': code_path, 'file_type': 'pyspark_code'}
        )
        
        logger.info(f"Created {len(documents)} chunks from PySpark code")
        
        # Initialize database and ingest
        self.db_ingester.initialize_db(collection_name)
        self.db_ingester.ingest_documents(documents)
        
        return len(documents)
    
    def ingest_combined(self, json_path: str, code_path: str, collection_name: str = 'informatica_pyspark'):
        """Ingest both Informatica JSON and PySpark code into the same collection"""
        logger.info("Processing combined ingestion")
        
        all_documents = []
        
        # Process JSON
        with open(json_path, 'r') as f:
            json_content = f.read()
        json_docs = self.json_chunker.chunk(
            json_content,
            {'source': json_path, 'file_type': 'informatica_json'}
        )
        all_documents.extend(json_docs)
        
        # Process code
        with open(code_path, 'r') as f:
            code_content = f.read()
        code_docs = self.code_chunker.chunk(
            code_content,
            {'source': code_path, 'file_type': 'pyspark_code'}
        )
        all_documents.extend(code_docs)
        
        logger.info(f"Created {len(all_documents)} total chunks")
        
        # Initialize database and ingest
        self.db_ingester.initialize_db(collection_name)
        self.db_ingester.ingest_documents(all_documents)
        
        return len(all_documents)
    
    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """Search the vector database and return formatted results"""
        results = self.db_ingester.search_with_score(query, k)
        
        formatted_results = []
        for doc, score in results:
            formatted_results.append({
                'content': doc.page_content,
                'metadata': doc.metadata,
                'score': score
            })
        
        return formatted_results


def main():
    """Example usage"""
    # Initialize the ingester
    ingester = InformaticaPySparkIngester(db_type='chroma')
    
    # Example paths (update with your actual file paths)
    json_path = "informatica_metadata.json"
    code_path = "pyspark_transformations.py"
    
    # Check if files exist
    if Path(json_path).exists() and Path(code_path).exists():
        # Ingest both files into a combined collection
        num_chunks = ingester.ingest_combined(
            json_path,
            code_path,
            collection_name='informatica_pyspark_combined'
        )
        
        print(f"Successfully ingested {num_chunks} chunks")
        
        # Example search
        results = ingester.search("transformation filter", k=3)
        for i, result in enumerate(results):
            print(f"\nResult {i+1}:")
            print(f"Score: {result['score']}")
            print(f"Type: {result['metadata'].get('type')}")
            print(f"Content preview: {result['content'][:200]}...")
    else:
        print(f"Please ensure {json_path} and {code_path} exist in the current directory")


if __name__ == "__main__":
    main()