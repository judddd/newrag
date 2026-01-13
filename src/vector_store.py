"""Vector store module with Elasticsearch integration"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog
from elasticsearch import Elasticsearch
from langchain_core.documents import Document
from langchain_elasticsearch import ElasticsearchStore

from src.config import config
from src.models import EmbeddingModel

logger = structlog.get_logger(__name__)


class VectorStore:
    """Vector store with Elasticsearch backend and hybrid search"""

    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """
        Initialize vector store
        
        Args:
            config_override: Optional config dict to override global config
        """
        # 从 config.yaml 读取 Elasticsearch 配置
        self.config = config_override or config.es_config
        
        # 初始化 Embedding 模型（从 config.yaml 读取）
        self.embedding_model = EmbeddingModel()
        
        # Initialize Elasticsearch client from config
        es_hosts = self.config.get('hosts', ['http://localhost:9200'])
        es_username = self.config.get('username', '')
        es_password = self.config.get('password', '')
        
        self.es_client = Elasticsearch(
            es_hosts,
            basic_auth=(es_username, es_password) if es_username else None,
            timeout=self.config.get('timeout', 30),
            max_retries=self.config.get('max_retries', 3),
            retry_on_timeout=self.config.get('retry_on_timeout', True),
        )
        
        self.index_name = self.config.get('index_name', 'aiops_knowledge_base')
        
        # Initialize LangChain Elasticsearch store with version compatibility
        try:
            # Try new parameter name first (langchain-elasticsearch >= 0.2.0)
            self.store = ElasticsearchStore(
                es_client=self.es_client,
                index_name=self.index_name,
                embedding=self.embedding_model.get_langchain_embeddings(),
                vector_query_field="content_vector",
            )
        except TypeError:
            # Fallback to old parameter name for backwards compatibility
            logger.warning("Using legacy es_connection parameter for ElasticsearchStore")
            self.store = ElasticsearchStore(
                es_connection=self.es_client,
                index_name=self.index_name,
                embedding=self.embedding_model.get_langchain_embeddings(),
                vector_query_field="content_vector",
            )
        
        logger.info("vector_store_initialized", index_name=self.index_name)
    
    def build_permission_filter(self, user_id: Optional[int] = None, 
                               org_id: Optional[int] = None,
                               is_superuser: bool = False) -> List[Dict[str, Any]]:
        """
        Build Elasticsearch filter clauses for permission filtering
        
        Permission logic (same as DatabaseManager):
        - Superuser with org_id: filter by org_id only
        - Superuser without org_id: no filter (sees everything)
        - Regular user: can see:
          1. Public documents (visibility=public OR visibility field missing - legacy docs)
          2. Organization documents (visibility=organization AND org_id matches)
          3. Documents they own (owner_id matches)
          4. Documents explicitly shared with them (user_id in shared_with_users)
        
        Returns:
            List of filter clauses for ES bool query
        """
        if is_superuser:
            # Superuser with org filter: only show docs from that org
            if org_id is not None:
                return [{"term": {"metadata.org_id": org_id}}]
            # Superuser without org filter: sees everything, no filter needed
            return []
        
        # Build OR conditions for permissions
        should_clauses = [
            {"term": {"metadata.visibility": "public"}},  # Public documents
            # Legacy documents without visibility field (treat as public)
            {"bool": {"must_not": {"exists": {"field": "metadata.visibility"}}}},
        ]
        
        if user_id is not None:
            # Documents owned by user
            should_clauses.append({"term": {"metadata.owner_id": user_id}})
            
            # Documents shared with user (check if user_id appears in shared_with_users array)
            should_clauses.append({"term": {"metadata.shared_with_users": user_id}})
        
        if org_id is not None:
            # Organization documents
            should_clauses.append({
                "bool": {
                    "must": [
                        {"term": {"metadata.visibility": "organization"}},
                        {"term": {"metadata.org_id": org_id}}
                    ]
                }
            })
        
        # If no user_id and not superuser, only show public documents + legacy docs
        if user_id is None:
            return [{
                "bool": {
                    "should": [
                        {"term": {"metadata.visibility": "public"}},
                        {"bool": {"must_not": {"exists": {"field": "metadata.visibility"}}}}
                    ],
                    "minimum_should_match": 1
                }
            }]
        
        # Return as a single bool should clause with minimum_should_match=1
        return [{
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }]
    
    def _promote_metadata_fields(self, doc_ids: List[str]) -> None:
        """
        Promote critical fields from metadata to top level for easier querying.
        
        Langchain's ElasticsearchStore puts all doc.metadata fields inside the 'metadata' object,
        but our ES mapping and queries expect certain fields at the top level.
        
        This method uses bulk update API to move fields from metadata.* to top level.
        """
        # Fields to promote from metadata to top level
        fields_to_promote = [
            'visual_description',
            'description_vector',
            'page_type',
            'drawing_number',
            'project_name',
            'all_components',
            'equipment_tags',
            'equipment_names',
            'table_cells',
            'all_text_tokens',
            'component_details'
        ]
        
        # Build painless script to copy fields from metadata to top level
        # Script checks if field exists in metadata before copying
        script_lines = []
        for field in fields_to_promote:
            script_lines.append(
                f"if (ctx._source.metadata?.{field} != null) {{ "
                f"ctx._source.{field} = ctx._source.metadata.{field}; "
                f"}}"
            )
        
        painless_script = " ".join(script_lines)
        
        # Use update_by_query to update all documents in batch
        try:
            response = self.es_client.update_by_query(
                index=self.index_name,
                body={
                    "query": {
                        "ids": {
                            "values": doc_ids
                        }
                    },
                    "script": {
                        "source": painless_script,
                        "lang": "painless"
                    }
                },
                refresh=True,  # Make changes immediately searchable
                conflicts="proceed"  # Continue even if there are version conflicts
            )
            
            logger.info(
                "fields_promoted",
                updated=response.get('updated', 0),
                total=response.get('total', 0),
                fields=fields_to_promote
            )
        except Exception as e:
            logger.error(
                "field_promotion_error",
                error=str(e),
                num_docs=len(doc_ids)
            )
            raise
    
    def add_documents(
        self,
        documents: List[Document],
        batch_size: int = 50
    ) -> List[str]:
        """
        Add documents to vector store
        
        Args:
            documents: List of documents to add
            batch_size: Batch size for bulk indexing
        
        Returns:
            List of document IDs
        """
        try:
            logger.info("starting_document_validation", total_documents=len(documents))
            
            # Validate and clean documents
            valid_documents = []
            for idx, doc in enumerate(documents):
                logger.debug(
                    "validating_document",
                    doc_index=idx,
                    content_type=type(doc.page_content).__name__,
                    content_length=len(doc.page_content) if hasattr(doc.page_content, '__len__') else 'N/A',
                    has_metadata=bool(doc.metadata)
                )
                
                # Ensure page_content is a non-empty string
                if not isinstance(doc.page_content, str):
                    logger.warning(
                        "converting_document_content_to_string",
                        doc_index=idx,
                        original_type=type(doc.page_content).__name__
                    )
                    doc.page_content = str(doc.page_content)
                
                # Skip empty documents
                if not doc.page_content.strip():
                    logger.warning(
                        "skipping_empty_document",
                        doc_index=idx,
                        metadata=doc.metadata
                    )
                    continue
                
                # Add indexed_at timestamp
                doc.metadata['indexed_at'] = datetime.utcnow().isoformat()
                
                # Remove complex nested objects (not needed in ES, all files stored in MinIO)
                # MinIO will hold all original data (PNG, JSON, etc.)
                if 'page_json' in doc.metadata:
                    del doc.metadata['page_json']
                
                if 'ocr_data' in doc.metadata:
                    del doc.metadata['ocr_data']
                
                # Remove minio_urls dict to avoid nested object issues
                # Individual URLs are stored in page_image_url, minio_base_url, etc.
                if 'minio_urls' in doc.metadata:
                    del doc.metadata['minio_urls']
                
                # Handle structured_content to avoid ES explosion
                # Only keep it for the first page/chunk to prevent duplication across hundreds of chunks
                if 'structured_content' in doc.metadata:
                    # Check page number (1-based)
                    page_num = doc.metadata.get('page_number', 1)
                    # Check chunk index (0-based, if available)
                    chunk_idx = doc.metadata.get('chunk_index', 0)
                    
                    # Remove structured_content from non-first pages or subsequent chunks of page 1
                    # Logic: Keep only if (Page 1 AND Chunk 0) OR (No Page info AND Chunk 0)
                    # If structured_content is massive, we only want it ONCE per file.
                    should_keep = False
                    
                    if page_num == 1:
                        if 'chunk_index' in doc.metadata:
                            if chunk_idx == 0:
                                should_keep = True
                        else:
                            # If no chunk index, assume it's the only chunk for page 1 (or first one we see)
                            should_keep = True
                    
                    if not should_keep:
                        del doc.metadata['structured_content']
                
                valid_documents.append(doc)
                logger.debug(
                    "document_validated",
                    doc_index=idx,
                    content_length=len(doc.page_content)
                )
            
            if not valid_documents:
                logger.warning("no_valid_documents_to_index")
                return []
            
            logger.info("✅ Document validation complete", valid_documents=len(valid_documents), skipped=len(documents) - len(valid_documents))
            
            # Verify ES connection and ensure index exists (auto-create if needed)
            try:
                es_client = self.store.client
                es_info = es_client.info()
                index_exists = es_client.indices.exists(index=self.index_name)
                
                if not index_exists:
                    logger.warning(f"⚠️  Index '{self.index_name}' does not exist, creating automatically...")
                    # Load mapping from file and create index
                    from pathlib import Path
                    mapping_file = Path(__file__).parent.parent / "schemas" / "elasticsearch_mapping.json"
                    with open(mapping_file, 'r') as f:
                        mapping = json.load(f)
                    es_client.indices.create(index=self.index_name, body=mapping)
                    logger.info(f"✅ Index '{self.index_name}' created successfully")
                
                logger.info(
                    "✅ Elasticsearch connection verified",
                    es_version=es_info.get('version', {}).get('number'),
                    index_name=self.index_name,
                    index_exists=index_exists
                )
                    
            except Exception as es_check_error:
                logger.error(
                    "❌ Elasticsearch connection or index check failed",
                    error=str(es_check_error),
                    error_type=type(es_check_error).__name__
                )
                raise
            
            # Generate description_vector for documents with visual_description
            logger.info("🔄 Generating description vectors for visual_description fields...")
            description_vectors_generated = 0
            for doc in valid_documents:
                visual_desc = doc.metadata.get('visual_description', '').strip()
                if visual_desc:
                    try:
                        # Generate embedding for visual_description
                        desc_vector = self.embedding_model.embed_query(visual_desc)
                        doc.metadata['description_vector'] = desc_vector
                        description_vectors_generated += 1
                    except Exception as vec_error:
                        logger.warning(
                            "failed_to_generate_description_vector",
                            error=str(vec_error),
                            visual_desc_length=len(visual_desc)
                        )
            logger.info(
                "✅ Description vector generation complete",
                generated=description_vectors_generated,
                total_docs=len(valid_documents)
            )
            
            # Add documents in batches
            ids = []
            total_batches = (len(valid_documents) + batch_size - 1) // batch_size
            logger.info(f"🔄 Starting batch indexing to Elasticsearch ({total_batches} batches)", total_batches=total_batches, batch_size=batch_size)
            
            # Log first document sample for debugging
            if valid_documents:
                sample_doc = valid_documents[0]
                logger.info(
                    "📋 Sample document to be indexed:",
                    content_length=len(sample_doc.page_content),
                    content_preview=sample_doc.page_content[:150],
                    metadata_keys=list(sample_doc.metadata.keys()) if hasattr(sample_doc, 'metadata') else [],
                    has_description_vector=('description_vector' in sample_doc.metadata)
                )
            
            for i in range(0, len(valid_documents), batch_size):
                batch = valid_documents[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                logger.info(
                    "indexing_batch",
                    batch_num=batch_num,
                    batch_size=len(batch),
                    total_docs=len(valid_documents)
                )
                
                try:
                    # Log first document in batch for debugging
                    logger.debug(
                        "batch_first_doc",
                        batch_num=batch_num,
                        content_length=len(batch[0].page_content),
                        content_preview=batch[0].page_content[:100],
                        metadata_keys=list(batch[0].metadata.keys())
                    )
                    
                    # Try to add the batch
                    logger.info(f"📤 Indexing batch {batch_num}...", batch_size=len(batch))
                    batch_ids = self.store.add_documents(batch)
                    ids.extend(batch_ids)
                    
                    logger.info(
                        f"✅ Batch {batch_num} indexed successfully",
                        batch_num=batch_num,
                        batch_size=len(batch),
                        num_ids=len(batch_ids),
                        sample_ids=batch_ids[:2] if batch_ids else []
                    )
                except Exception as batch_error:
                    # If batch fails, try adding documents one by one
                    import traceback
                    error_details = {
                        'error_type': type(batch_error).__name__,
                        'error_message': str(batch_error),
                    }
                    
                    # Extract detailed ES errors from BulkIndexError
                    if hasattr(batch_error, 'errors') and batch_error.errors:
                        # BulkIndexError.errors is a list of error details
                        error_details['num_errors'] = len(batch_error.errors)
                        # Show first 3 errors with full details
                        error_details['es_error_details'] = []
                        for i, err in enumerate(batch_error.errors[:3]):
                            if isinstance(err, dict):
                                # Extract the actual error reason from ES response
                                # Usually bulk errors are in format: {'index': {'_index': '...', 'status': 400, 'error': {...}}}
                                index_error = err.get('index', {}).get('error') or err.get('create', {}).get('error') or err
                                error_info = {
                                    'doc_index': i,
                                    'error_summary': str(index_error)[:1000]  # Limit error length
                                }
                                error_details['es_error_details'].append(error_info)
                        
                        logger.error(
                            "🚨 BATCH INDEXING FAILED - ES REJECTED ALL DOCUMENTS",
                            batch_num=batch_num,
                            **error_details
                        )
                    else:
                        # Fallback if errors not in expected format
                        if hasattr(batch_error, 'args') and len(batch_error.args) > 1:
                            error_details['error_args'] = str(batch_error.args)
                        error_details['traceback'] = traceback.format_exc()
                        logger.error(
                            "🚨 BATCH INDEXING FAILED - ES REJECTED ALL DOCUMENTS",
                            batch_num=batch_num,
                            **error_details
                        )
                    logger.warning("⚠️  Retrying documents individually to find problematic ones...", batch_size=len(batch))
                    
                    for doc_idx, doc in enumerate(batch):
                        try:
                            logger.info(f"🔍 Trying document {doc_idx+1}/{len(batch)}", doc_index=doc_idx, content_length=len(doc.page_content))
                            doc_ids = self.store.add_documents([doc])
                            ids.extend(doc_ids)
                            logger.info(f"✅ Document {doc_idx+1} indexed successfully", doc_index=doc_idx, doc_ids=doc_ids)
                        except Exception as doc_error:
                            import traceback
                            error_details = {
                                'error_type': type(doc_error).__name__,
                                'error_message': str(doc_error),
                                'content_preview': doc.page_content[:200] if doc.page_content else "(empty)",
                                'metadata_keys': list(doc.metadata.keys()) if hasattr(doc, 'metadata') else [],
                            }
                            
                            # Extract detailed ES errors from BulkIndexError
                            if hasattr(doc_error, 'errors') and doc_error.errors:
                                error_details['es_error_details'] = doc_error.errors[0] if doc_error.errors else {}
                                logger.error(
                                    f"❌ Document {doc_idx+1} FAILED TO INDEX - ES ERROR DETAILS:",
                                    doc_index=doc_idx,
                                    **error_details
                                )
                            else:
                                # Fallback
                                if hasattr(doc_error, 'args') and len(doc_error.args) > 1:
                                    error_details['error_args'] = str(doc_error.args)
                                error_details['traceback'] = traceback.format_exc()[:500]  # Limit traceback
                                logger.error(
                                    f"❌ Document {doc_idx+1} FAILED TO INDEX",
                                    doc_index=doc_idx,
                                    **error_details
                                )
            
            # Promote critical fields from metadata to top level for easier querying
            if ids:
                logger.info("🔄 Promoting fields from metadata to top level...", num_docs=len(ids))
                try:
                    self._promote_metadata_fields(ids)
                    logger.info("✅ Field promotion completed", num_docs=len(ids))
                except Exception as e:
                    logger.error("❌ Field promotion failed", error=str(e))
            
            # Verify ES write success by checking actual indexed documents
            # Query ES to confirm documents are actually there
            try:
                es_client = self.store.client
                es_client.indices.refresh(index=self.index_name)  # Refresh to make docs searchable
                
                # Count documents that were just added (by checking the document IDs)
                if ids:
                    # Verify a sample of documents actually exist in ES
                    sample_ids = ids[:min(3, len(ids))]
                    for doc_id in sample_ids:
                        try:
                            es_client.get(index=self.index_name, id=doc_id)
                        except Exception as e:
                            logger.error(f"❌ Document {doc_id} NOT FOUND in ES after indexing!", error=str(e))
                            raise RuntimeError(f"ES write verification failed: document {doc_id} not found")
            except Exception as verify_error:
                logger.error("❌ ES WRITE VERIFICATION FAILED", error=str(verify_error))
                raise RuntimeError(f"Failed to verify ES write: {verify_error}")
            
            # Check if any documents were successfully indexed
            if len(ids) == 0 and len(valid_documents) > 0:
                error_msg = f"Failed to index all {len(valid_documents)} documents to Elasticsearch"
                logger.error("❌ ALL DOCUMENTS FAILED TO INDEX", 
                            attempted=len(valid_documents), 
                            successful=0)
                raise RuntimeError(error_msg)
            elif len(ids) < len(valid_documents):
                logger.warning("⚠️  PARTIAL SUCCESS", 
                              successful=len(ids), 
                              attempted=len(valid_documents),
                              failed=len(valid_documents) - len(ids))
            
            logger.info("=" * 60)
            logger.info("✅ DOCUMENTS WRITTEN & VERIFIED IN ELASTICSEARCH", 
                       total_docs=len(ids), 
                       attempted=len(valid_documents),
                       verified_sample=min(3, len(ids)),
                       success_rate=f"{len(ids)/len(valid_documents)*100:.1f}%" if valid_documents else "N/A")
            logger.info("=" * 60)
            
            return ids
        
        except Exception as e:
            logger.error("document_indexing_failed", error=str(e), num_docs=len(documents))
            raise
    
    def similarity_search(
        self,
        query: str,
        k: int = 5,
        filter_dict: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
        org_id: Optional[int] = None,
        is_superuser: bool = False
    ) -> List[Document]:
        """
        Perform similarity search with permission filtering
        
        Args:
            query: Search query
            k: Number of results to return
            filter_dict: Metadata filters
            user_id: Current user ID for permission filtering
            org_id: Current user's organization ID
            is_superuser: Is user a superuser
        
        Returns:
            List of matching documents
        """
        try:
            # Merge permission filter with custom filters
            permission_filters = self.build_permission_filter(user_id, org_id, is_superuser)
            
            # Combine with existing filters
            if filter_dict:
                # Convert filter_dict to ES filter format and merge
                combined_filter = filter_dict.copy()
                if permission_filters:
                    # Note: This is a simplified merge. For complex cases, 
                    # we'd need to construct a proper bool query
                    if "bool" not in combined_filter:
                        combined_filter["bool"] = {}
                    if "filter" not in combined_filter["bool"]:
                        combined_filter["bool"]["filter"] = []
                    combined_filter["bool"]["filter"].extend(permission_filters)
                filter_dict = combined_filter
            elif permission_filters:
                filter_dict = {"bool": {"filter": permission_filters}}
            
            results = self.store.similarity_search(
                query=query,
                k=k,
                filter=filter_dict
            )
            
            logger.info("similarity_search_completed", query=query, num_results=len(results))
            
            return results
        
        except Exception as e:
            logger.error("similarity_search_failed", error=str(e), query=query)
            raise
    
    def hybrid_search(
        self,
        query: str,
        k: int = 5,
        filter_dict: Optional[Dict[str, Any]] = None,
        vector_weight: Optional[float] = None,
        bm25_weight: Optional[float] = None,
        user_id: Optional[int] = None,
        org_id: Optional[int] = None,
        is_superuser: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search (vector + BM25) with permission filtering
        
        Args:
            query: Search query
            k: Number of results to return
            filter_dict: Metadata filters
            vector_weight: Weight for vector search (default from config)
            bm25_weight: Weight for BM25 search (default from config)
            user_id: Current user ID for permission filtering
            org_id: Current user's organization ID
            is_superuser: Is user a superuser
        
        Returns:
            List of matching documents with scores
        """
        hybrid_config = self.config.get('hybrid_search', {})
        vector_weight = vector_weight or hybrid_config.get('vector_weight', 0.7)
        bm25_weight = bm25_weight or hybrid_config.get('bm25_weight', 0.3)
        
        # Build permission filters
        permission_filters = self.build_permission_filter(user_id, org_id, is_superuser)
        
        try:
            # Handle empty query (match_all + filters)
            if not query or not query.strip():
                query_body = {
                    "size": k,
                    "query": {
                        "bool": {
                            "must": [{"match_all": {}}]
                        }
                    }
                }
                
                # Build filter clauses (custom filters + permission filters)
                filter_clauses = []
                
                # Add custom filters if provided
                if filter_dict:
                    for k, v in filter_dict.items():
                        if k == "filename" and isinstance(v, str):
                            search_val = v if '*' in v else f"*{v}*"
                            filter_clauses.append({
                                "wildcard": {
                                    "metadata.filename": {
                                        "value": search_val,
                                        "case_insensitive": True
                                    }
                                }
                            })
                        else:
                            filter_clauses.append({"term": {f"metadata.{k}": v}})
                
                # Add permission filters
                if permission_filters:
                    filter_clauses.extend(permission_filters)
                
                if filter_clauses:
                    query_body["query"]["bool"]["filter"] = filter_clauses
                
                # Execute search
                response = self.es_client.search(
                    index=self.index_name,
                    body=query_body
                )
                
                results = []
                for hit in response['hits']['hits']:
                    source = hit['_source']
                    results.append({
                        'id': hit['_id'],
                        'score': hit['_score'],
                        'content': source.get('text', ''),
                        'content_snippet': source.get('text', '')[:500],
                        'highlighted': None,  # No highlights for match_all
                        'metadata': source.get('metadata', {}),
                        'document_name': source.get('document_name', ''),
                        'page_number': source.get('page_number', 1),
                        'total_pages': source.get('total_pages', 1),
                        'page_type': source.get('page_type', 'text'),
                        'page_json': source.get('original_content', {}),
                        'drawing_number': source.get('drawing_number', ''),
                        'project_name': source.get('project_name', ''),
                        'equipment_tags': source.get('equipment_tags', []),
                        'component_details': source.get('component_details', []),
                    })
                
                logger.info(
                    "match_all_search_completed",
                    num_results=len(results),
                    filters=filter_dict
                )
                return results

            # Get vector search results
            vector_query = self.embedding_model.embed_text(query)
            
            # Build ES query with enhanced BM25 fields
            query_body = {
                "size": k,
                "query": {
                    "bool": {
                        "should": [
                            {
                                "script_score": {
                                    "query": {"match_all": {}},
                                    "script": {
                                        "source": f"cosineSimilarity(params.query_vector, 'content_vector') * {vector_weight}",
                                        "params": {"query_vector": vector_query}
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "text^3",  # Main content (highest priority)
                                        "metadata.filename^2.5",  # Filename (high priority)
                                        "metadata.description^2",
                                        "metadata.filepath^1.5",  # File path
                                        "document_name^2",
                                        "drawing_number^2",
                                        "project_name^1.5",
                                        "equipment_tags^1.2",
                                        "component_details"
                                    ],
                                    "type": "best_fields",
                                    "boost": bm25_weight,
                                    "operator": "or",
                                    "fuzziness": "AUTO"
                                }
                            }
                        ]
                    }
                },
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 150,
                            "number_of_fragments": 3,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "metadata.filename": {
                            "fragment_size": 200,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "metadata.description": {
                            "fragment_size": 150,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "metadata.filepath": {
                            "fragment_size": 200,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "document_name": {
                            "fragment_size": 150,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "drawing_number": {
                            "fragment_size": 100,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        },
                        "project_name": {
                            "fragment_size": 150,
                            "number_of_fragments": 1,
                            "pre_tags": ["<mark>"],
                            "post_tags": ["</mark>"]
                        }
                    },
                    "require_field_match": False
                }
            }
            
            # Build filter clauses (custom filters + permission filters)
            filter_clauses = []
            
            # Add custom filters if provided
            if filter_dict:
                for k, v in filter_dict.items():
                    # Special handling for filename (wildcard search)
                    if k == "filename" and isinstance(v, str):
                        # If value contains wildcards or we want partial match, use wildcard
                        # Otherwise just wrap in wildcards
                        search_val = v if '*' in v else f"*{v}*"
                        filter_clauses.append({
                            "wildcard": {
                                "metadata.filename": {
                                    "value": search_val,
                                    "case_insensitive": True
                                }
                            }
                        })
                    # Standard exact match for other fields (file_type, etc.)
                    else:
                        filter_clauses.append({"term": {f"metadata.{k}": v}})
            
            # Add permission filters
            if permission_filters:
                filter_clauses.extend(permission_filters)
            
            if filter_clauses:
                query_body["query"]["bool"]["filter"] = filter_clauses
            
            # Execute search
            response = self.es_client.search(
                index=self.index_name,
                body=query_body
            )
            
            # Parse results
            results = []
            for hit in response['hits']['hits']:
                # Merge ALL highlighted text from all fields
                highlighted_parts = []
                if 'highlight' in hit:
                    # Priority order for highlights
                    highlight_fields = [
                        'metadata.filename',
                        'metadata.filepath', 
                        'text',
                        'metadata.description',
                        'document_name',
                        'drawing_number',
                        'project_name',
                        'equipment_tags',
                        'component_details'
                    ]
                    for field in highlight_fields:
                        if field in hit['highlight']:
                            highlighted_parts.extend(hit['highlight'][field])
                
                highlighted_text = ' ... '.join(highlighted_parts) if highlighted_parts else None
                
                source = hit['_source']
                
                results.append({
                    'id': hit['_id'],
                    'score': hit['_score'],
                    'content': source.get('text', ''),  # LangChain uses 'text' field
                    'content_snippet': source.get('text', '')[:500],  # Content snippet
                    'highlighted': highlighted_text,  # Highlighted fragments
                    'metadata': source.get('metadata', {}),
                    # Page-level fields
                    'document_name': source.get('document_name', ''),
                    'page_number': source.get('page_number', 1),
                    'total_pages': source.get('total_pages', 1),
                    'page_type': source.get('page_type', 'text'),
                    'page_json': source.get('original_content', {}),
                    # Searchable fields
                    'drawing_number': source.get('drawing_number', ''),
                    'project_name': source.get('project_name', ''),
                    'equipment_tags': source.get('equipment_tags', []),
                    'component_details': source.get('component_details', []),
                })
            
            logger.info(
                "hybrid_search_completed",
                query=query,
                num_results=len(results),
                vector_weight=vector_weight,
                bm25_weight=bm25_weight
            )
            
            return results
        
        except Exception as e:
            logger.error("hybrid_search_failed", error=str(e), query=query)
            raise
    
    def delete_by_metadata(self, filter_dict: Dict[str, Any], fallback_filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete documents by metadata filter, with optional fallback for legacy data
        
        Args:
            filter_dict: Primary metadata filters
            fallback_filters: Optional fallback filters for documents that don't have primary fields
        
        Returns:
            Number of documents deleted
        """
        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {f"metadata.{k}": v}} for k, v in filter_dict.items()
                        ]
                    }
                }
            }
            
            response = self.es_client.delete_by_query(
                index=self.index_name,
                body=query
            )
            
            deleted_count = response.get('deleted', 0)
            logger.info("documents_deleted", filter=filter_dict, count=deleted_count)
            
            # If no documents were deleted and fallback filters provided, try fallback
            if deleted_count == 0 and fallback_filters:
                logger.info("trying_fallback_deletion", fallback_filters=fallback_filters)
                fallback_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {f"metadata.{k}": v}} for k, v in fallback_filters.items()
                            ]
                        }
                    }
                }
                
                fallback_response = self.es_client.delete_by_query(
                    index=self.index_name,
                    body=fallback_query
                )
                
                fallback_deleted = fallback_response.get('deleted', 0)
                deleted_count += fallback_deleted
                logger.info("fallback_documents_deleted", filter=fallback_filters, count=fallback_deleted)
            
            return deleted_count
        
        except Exception as e:
            logger.error("delete_failed", error=str(e), filter=filter_dict)
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get index statistics
        
        Returns:
            Dictionary containing index stats
        """
        try:
            # Check if index exists
            if not self.es_client.indices.exists(index=self.index_name):
                return {
                    'document_count': 0,
                    'index_size_bytes': 0,
                    'categories': [],
                    'file_types': []
                }
            
            # Get document count
            try:
                count_response = self.es_client.count(index=self.index_name)
                doc_count = count_response['count']
            except Exception:
                doc_count = 0
            
            # Get index stats
            try:
                stats = self.es_client.indices.stats(index=self.index_name)
                index_stats = stats['indices'][self.index_name]
                size_bytes = index_stats['total']['store']['size_in_bytes']
            except Exception:
                size_bytes = 0
            
            # Get aggregations for categories (try both with and without metadata prefix)
            categories = []
            file_types = []
            
            if doc_count > 0:
                try:
                    agg_query = {
                        "size": 0,
                        "aggs": {
                            "categories": {
                                "terms": {"field": "metadata.category", "size": 10, "missing": "uncategorized"}
                            },
                            "file_types": {
                                "terms": {"field": "metadata.file_type", "size": 10, "missing": "unknown"}
                            }
                        }
                    }
                    
                    agg_response = self.es_client.search(
                        index=self.index_name,
                        body=agg_query
                    )
                    
                    categories = [
                        {'name': b['key'], 'count': b['doc_count']}
                        for b in agg_response['aggregations']['categories']['buckets']
                    ]
                    
                    file_types = [
                        {'name': b['key'], 'count': b['doc_count']}
                        for b in agg_response['aggregations']['file_types']['buckets']
                    ]
                except Exception as e:
                    logger.warning("aggregation_failed", error=str(e))
            
            return {
                'document_count': doc_count,
                'index_size_bytes': size_bytes,
                'categories': categories,
                'file_types': file_types
            }
        
        except Exception as e:
            logger.error("stats_retrieval_failed", error=str(e))
            # Return empty stats instead of raising
            return {
                'document_count': 0,
                'index_size_bytes': 0,
                'categories': [],
                'file_types': []
            }
    
    def search_component(
        self,
        component_id: str,
        k: int = 10,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for pages containing specific component
        
        Args:
            component_id: Component ID to search for (e.g., "C1", "V-2001", "R100")
            k: Number of results to return
            filter_dict: Additional metadata filters
        
        Returns:
            List of matching pages with component information
        """
        try:
            query_body = {
                "size": k,
                "query": {
                    "bool": {
                        "should": [
                            # Exact match in equipment tags
                            {"term": {"equipment_tags": component_id}},
                            # Match in component details (nested)
                            {
                                "nested": {
                                    "path": "component_details",
                                    "query": {
                                        "term": {"component_details.id": component_id}
                                    }
                                }
                            },
                            # Match in all_components field
                            {"match": {"all_components": component_id}},
                            # Fuzzy match in text
                            {
                                "match": {
                                    "text": {
                                        "query": component_id,
                                        "fuzziness": "AUTO"
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
            
            # Add additional filters
            if filter_dict:
                filter_clauses = []
                for k, v in filter_dict.items():
                    if k == "filename" and isinstance(v, str):
                        search_val = v if '*' in v else f"*{v}*"
                        filter_clauses.append({
                            "wildcard": {
                                "metadata.filename": {
                                    "value": search_val,
                                    "case_insensitive": True
                                }
                            }
                        })
                    else:
                        filter_clauses.append({"term": {f"metadata.{k}": v}})
                
                query_body["query"]["bool"]["filter"] = filter_clauses
            
            # Execute search
            response = self.es_client.search(
                index=self.index_name,
                body=query_body
            )
            
            # Parse results
            results = []
            for hit in response['hits']['hits']:
                source = hit['_source']
                
                # Find matched components in this page
                matched_components = []
                if 'component_details' in source:
                    matched_components = [
                        c for c in source['component_details']
                        if c.get('id', '').lower() == component_id.lower()
                    ]
                
                results.append({
                    'id': hit['_id'],
                    'score': hit['_score'],
                    'document_name': source.get('document_name', ''),
                    'page_number': source.get('page_number', 1),
                    'total_pages': source.get('total_pages', 1),
                    'page_type': source.get('page_type', 'text'),
                    'content_snippet': source.get('text', '')[:500],
                    'page_json': source.get('original_content', {}),
                    'matched_components': matched_components,
                    'drawing_number': source.get('drawing_number', ''),
                    'project_name': source.get('project_name', ''),
                    'metadata': source.get('metadata', {})
                })
            
            logger.info(
                "component_search_completed",
                component_id=component_id,
                num_results=len(results)
            )
            
            return results
        
        except Exception as e:
            logger.error("component_search_failed", error=str(e), component_id=component_id)
            raise

