"""FastAPI web application for RAG Knowledge Base"""

import os
import shutil
import json
import subprocess
import sys
import threading
from pathlib import Path

# Ensure logs directory exists before importing logging modules
Path("logs").mkdir(exist_ok=True)
from typing import List, Optional
from datetime import datetime

import structlog
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.requests import Request

from src.config import config
from src.pipeline import ProcessingPipeline
from src.database import DatabaseManager
from src.logging_config import setup_logging
from src.task_manager import task_manager, TaskStatus, TaskStage

# Import routers from separate modules
from web.routes import document_router, cleanup_router
from web.routes.auth_routes import router as auth_router
from web.routes.admin_routes import router as admin_router
from web.handlers import extract_matched_bboxes_from_file
from web.middleware.auth import AuthMiddleware
from web.dependencies.auth_deps import get_optional_user, get_current_user
from src.database import User

# Initialize logging with configuration from config.yaml
setup_logging(log_config=config.logging_config)
logger = structlog.get_logger(__name__)

# Concurrent processing control (limit to 3 documents processing at the same time)
processing_semaphore = threading.Semaphore(3)

# Initialize FastAPI app
app = FastAPI(
    title="AIOps RAG Knowledge Base",
    description="AI-powered knowledge base for IT Operations and Security",
    version="1.1.0"
)

# CORS configuration
web_config = config.web_config
cors_config = web_config.get('cors', {})
if cors_config.get('enabled', True):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_config.get('allow_origins', ["*"]),
        allow_credentials=True,
        allow_methods=cors_config.get('allow_methods', ["*"]),
        allow_headers=cors_config.get('allow_headers', ["*"]),
    )

# Add JWT authentication middleware (after CORS)
security_config = config.security_config
if security_config.get('auth', {}).get('enabled', False):
    app.add_middleware(AuthMiddleware)

# Setup templates and static files
templates = Jinja2Templates(directory="web/templates")
app.mount("/static", StaticFiles(directory="web/static"), name="static")

# Initialize pipeline and database
pipeline = ProcessingPipeline()
db = DatabaseManager()

# Include routers
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(document_router)
app.include_router(cleanup_router)

# Recover any stuck tasks from previous runs
from web.handlers.document_processor import recover_stuck_tasks
try:
    logger.info("attempting_task_recovery")
    recover_stuck_tasks()
except Exception as e:
    logger.error("task_recovery_failed_at_startup", error=str(e))

# Create upload and processed folders
upload_folder = Path(web_config.get('upload_folder', './uploads'))
upload_folder.mkdir(parents=True, exist_ok=True)

processed_folder = Path('web/static/processed_docs')
processed_folder.mkdir(parents=True, exist_ok=True)


# Pydantic models
class SearchRequest(BaseModel):
    query: str
    k: int = 5
    filters: Optional[dict] = None
    use_hybrid: bool = True
    organization_id: Optional[int] = None  # Organization filter for search


class SearchResponse(BaseModel):
    results: List[dict]
    total: int


class MetadataUpdate(BaseModel):
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    author: Optional[str] = None
    description: Optional[str] = None


# Routes
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render API status page (Frontend is separate)"""
    backend_port = web_config.get('port', 8080)
    frontend_port = web_config.get('frontend_port', 3000)
    
    html_content = f"""
    <html>
        <head>
            <title>NewRAG Backend API</title>
            <style>
                body {{ font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f0f2f5; }}
                .container {{ text-align: center; padding: 2rem; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                h1 {{ color: #333; }}
                p {{ color: #666; }}
                a {{ color: #007bff; text-decoration: none; font-weight: bold; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🚀 Backend API is Running</h1>
                <p>This is the backend service port ({backend_port}).</p>
                <p>👉 Please visit the <b>React Frontend</b> at: <a href="http://localhost:{frontend_port}">http://localhost:{frontend_port}</a></p>
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.post("/search", response_model=SearchResponse)
async def search(
    request: SearchRequest,
    current_user: Optional[User] = Depends(get_optional_user)
):
    """
    Search knowledge base with permission filtering and organization filtering
    """
    try:
        # Build permission filters based on current user
        permission_filters = {}
        
        # Handle organization filtering
        target_org_id = None
        if request.organization_id is not None:
            if current_user and current_user.is_superuser:
                # Superuser can filter by any organization
                target_org_id = request.organization_id
            elif current_user:
                # Non-superuser can only filter by their own organization
                target_org_id = current_user.org_id if current_user.org_id else None
        
        if current_user:
            if not current_user.is_superuser:
                # Non-superusers can only search:
                # 1. Documents they own
                # 2. Documents visible to their organization
                # 3. Public documents
                # Note: Elasticsearch filtering will be handled by adding these to the query
                permission_filters['user_permissions'] = {
                    'user_id': current_user.id,
                    'org_id': target_org_id if target_org_id else current_user.org_id,
                    'is_superuser': False
                }
            elif target_org_id:
                # Superuser with organization filter: filter by org_id
                permission_filters['user_permissions'] = {
                    'user_id': current_user.id,
                    'org_id': target_org_id,
                    'is_superuser': True  # Still superuser, but with org filter
                }
            else:
                # Superuser without org filter: can see everything
                # IMPORTANT: Still must pass is_superuser flag to avoid being treated as anonymous
                permission_filters['user_permissions'] = {
                    'user_id': current_user.id,
                    'is_superuser': True
                }
        else:
            # Unauthenticated users can only see public documents
            permission_filters['visibility'] = 'public'
        
        # Merge user-provided filters with permission filters
        combined_filters = {**(request.filters or {}), **permission_filters}
        
        logger.info(
            "search_request", 
            query=request.query, 
            user_id=current_user.id if current_user else None,
            has_permission_filter=bool(permission_filters)
        )
        
        results = pipeline.search(
            query=request.query,
            k=request.k,
            filters=combined_filters,
            use_hybrid=request.use_hybrid
        )
        
        # Enrich results with pages_data and matched bboxes from database
        for result in results:
            metadata = result.get('metadata', {})
            checksum = metadata.get('checksum')
            
            if checksum:
                # Query database for document with this checksum
                doc = db.get_document_by_checksum(checksum)
                if doc and doc.pages_data:
                    try:
                        # Parse pages_data JSON and add to metadata
                        pages_data = json.loads(doc.pages_data) if isinstance(doc.pages_data, str) else doc.pages_data
                        metadata['pages_data'] = pages_data
                        metadata['ocr_engine'] = doc.ocr_engine
                        
                        # Extract matched bboxes for this result
                        matched_bboxes = extract_matched_bboxes_from_file(
                            doc_id=doc.id,
                            checksum=checksum,
                            page_number=metadata.get('page_number', 1),
                            query_text=request.query
                        )
                        result['matched_bboxes'] = matched_bboxes
                        
                    except json.JSONDecodeError:
                        logger.warning("failed_to_parse_pages_data", checksum=checksum)
        
        return SearchResponse(results=results, total=len(results))
    
    except Exception as e:
        logger.error("search_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/component/{component_id}")
async def search_component(component_id: str, k: int = 10):
    """
    Search for pages containing specific component
    
    Args:
        component_id: Component ID (e.g., C1, V-2001, R100)
        k: Number of results to return
    
    Returns:
        List of pages containing the component
    """
    try:
        results = pipeline.search_component(
            component_id=component_id,
            k=k
        )
        
        return {
            "component_id": component_id,
            "results": results,
            "total": len(results)
        }
    
    except Exception as e:
        logger.error("component_search_failed", error=str(e), component_id=component_id)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats(user: User = Depends(get_current_user)):
    """
    Get knowledge base statistics (filtered by user permissions)
    """
    try:
        # Get ES stats (fail gracefully if ES is down)
        try:
            es_stats = pipeline.vector_store.get_stats(
                user_id=user.id,
                org_id=user.org_id,
                is_superuser=user.is_superuser
            )
        except Exception as e:
            logger.warning("es_stats_unavailable", error=str(e))
            es_stats = {'document_count': 0, 'file_types': []}

        # Get database stats
        db_stats = db.get_stats(
            user_id=user.id,
            org_id=user.org_id,
            is_superuser=user.is_superuser
        )
        
        # Get MinIO storage stats
        from src.minio_storage import minio_storage
        minio_stats = minio_storage.get_storage_stats()
        
        # Get ES index info
        index_name = pipeline.vector_store.index_name
        index_info = {
            'name': index_name,
            'exists': False,
            'status': 'unknown',
            'document_count': 0
        }

        try:
            es_client = pipeline.vector_store.es_client
            # Check if index exists
            index_exists_response = es_client.indices.exists(index=index_name)
            # Handle both old and new ES client API responses
            if hasattr(index_exists_response, 'body'):
                index_exists = bool(index_exists_response.body)
            else:
                index_exists = bool(index_exists_response)
            
            index_info['exists'] = index_exists
            if index_exists:
                index_info['status'] = 'green'
                index_info['document_count'] = es_stats.get('document_count', 0)
            else:
                index_info['status'] = 'not_created'
                
        except Exception as e:
            logger.warning("es_index_check_failed", error=str(e))
            index_info['status'] = 'unreachable'
        
        # Build response with new stats structure
        combined_stats = {
            # Main stats for dashboard cards
            'total_documents': db_stats.get('total', 0),
            'total_pages': db_stats.get('total_pages', 0),
            'total_size_mb': minio_stats.get('total_size_mb', 0),
            
            # Breakdown by document type (from ES)
            'documents_by_type': {},
            
            # Breakdown by status (from database)
            'documents_by_status': {
                'completed': db_stats.get('completed', 0),
                'processing': db_stats.get('processing', 0),
                'queued': db_stats.get('queued', 0),
                'failed': db_stats.get('failed', 0)
            },
            
            # Additional detailed stats
            'database': db_stats,
            'minio': minio_stats,
            'elasticsearch': es_stats,
            'index': index_info
        }
        
        # Add document type distribution from ES if available
        if 'file_types' in es_stats:
            for file_type in es_stats['file_types']:
                type_name = file_type.get('name', 'unknown')
                type_count = file_type.get('count', 0)
                combined_stats['documents_by_type'][type_name] = type_count
        
        return JSONResponse(content=combined_stats)
    
    except Exception as e:
        logger.error("stats_retrieval_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    
    host = web_config.get('host', '0.0.0.0')
    port = web_config.get('port', 8000)
    
    logger.info("starting_web_server", host=host, port=port)
    
    uvicorn.run(app, host=host, port=port)
