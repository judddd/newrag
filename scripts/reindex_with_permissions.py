"""
Reindex existing documents to Elasticsearch with proper permission metadata.

This script updates Elasticsearch documents to include org_id, owner_id, and visibility fields.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import DatabaseManager
from src.vector_store import VectorStore
import structlog

logger = structlog.get_logger(__name__)


def reindex_documents_with_permissions():
    """
    Update Elasticsearch documents with permission metadata from database.
    """
    db = DatabaseManager()
    vs = VectorStore()
    
    logger.info("Starting permission metadata reindexing...")
    
    # Get all document masters
    session = db.get_session()
    try:
        from src.database import DocumentMaster, DocumentVersion
        
        # Query all document masters
        masters = session.query(DocumentMaster).all()
        logger.info(f"Found {len(masters)} document masters")
        
        updated_count = 0
        error_count = 0
        
        for master in masters:
            try:
                # Get latest version for this master
                latest_version = db.get_latest_version(master.id)
                if not latest_version or not latest_version.checksum:
                    logger.warning(f"No latest version or checksum for master {master.id}")
                    continue
                
                checksum = latest_version.checksum
                
                # Build the filter to find all ES documents for this file
                filter_query = {
                    "term": {"metadata.checksum": checksum}
                }
                
                # Search for documents with this checksum
                result = vs.es_client.search(
                    index=vs.index_name,
                    body={
                        "query": filter_query,
                        "size": 10000  # Get all chunks
                    }
                )
                
                hits = result['hits']['hits']
                logger.info(f"Found {len(hits)} ES documents for checksum {checksum[:8]}...")
                
                if not hits:
                    continue
                
                # Prepare update data
                update_data = {
                    "org_id": master.org_id,
                    "owner_id": master.owner_id,
                    "visibility": master.visibility
                }
                
                # Update each document
                for hit in hits:
                    doc_id = hit['_id']
                    try:
                        vs.es_client.update(
                            index=vs.index_name,
                            id=doc_id,
                            body={
                                "doc": {
                                    "metadata": update_data
                                }
                            }
                        )
                        updated_count += 1
                    except Exception as e:
                        logger.error(f"Failed to update ES doc {doc_id}: {e}")
                        error_count += 1
                
                logger.info(f"Updated {len(hits)} ES documents for master {master.id} ({master.filename_base})")
                
            except Exception as e:
                logger.error(f"Failed to process master {master.id}: {e}")
                error_count += 1
                continue
        
        logger.info(f"✅ Reindexing complete! Updated: {updated_count}, Errors: {error_count}")
        
    finally:
        session.close()


if __name__ == "__main__":
    reindex_documents_with_permissions()





