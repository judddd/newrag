"""Document management routes"""

from pathlib import Path
from typing import Optional, List
import structlog
import hashlib
import zipfile
import os
import threading
from datetime import datetime
from fastapi import APIRouter, HTTPException, File, Form, UploadFile, Depends, Request
from fastapi.responses import JSONResponse
from src.task_manager import task_manager, TaskStatus
from src.database import DatabaseManager, User
import shutil
from src.pipeline import ProcessingPipeline
from src.config import config
from web.handlers.document_processor import process_document_background
from web.dependencies.auth_deps import get_current_user, require_permission

web_config = config.web_config
upload_folder = Path(web_config.get('upload_folder', './uploads'))
upload_folder.mkdir(parents=True, exist_ok=True)


db = DatabaseManager()
pipeline = ProcessingPipeline()

logger = structlog.get_logger(__name__)

# Create router
router = APIRouter(prefix="", tags=["documents"])


# ============================================================
# 示例路由 - 你可以把其他文档相关的路由复制到这里
# ============================================================

@router.get("/documents")
async def list_documents(
    limit: int = 50, 
    offset: int = 0, 
    status: Optional[str] = None, 
    include_archives: bool = False,
    current_user: Optional[User] = Depends(get_current_user)
):
    """
    List uploaded documents with permission filtering
    
    Requires authentication. Returns only documents the user has permission to see.
    """
    try:
        # 默认不显示 ZIP 压缩包本身，除非 include_archives=True
        exclude_types = None if include_archives else ['zip']
        
        # Apply permission filtering based on current user
        user_id = current_user.id if current_user else None
        org_id = current_user.org_id if current_user else None
        is_superuser = current_user.is_superuser if current_user else False
        
        docs = db.list_documents(
            limit=limit, 
            offset=offset, 
            status=status, 
            exclude_file_types=exclude_types,
            user_id=user_id,
            org_id=org_id,
            is_superuser=is_superuser
        )
        return JSONResponse(content={
            "documents": [doc.to_dict() for doc in docs],
            "total": len(docs)
        })
    except Exception as e:
        logger.error("list_documents_failed", error=str(e), user_id=user_id if current_user else None)
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/documents/{doc_id}/progress")
async def get_document_progress(doc_id: int, include_children: bool = False):
    """Get processing progress for a document (enhanced with task manager)"""
    try:
        # Try to get from task manager first (for active tasks)
        if include_children:
            task_dict = task_manager.get_task_with_children(doc_id)
            if task_dict:
                # Also get database info
                doc = db.get_document(doc_id)
                if doc:
                    if not task_dict.get('filename'):
                        task_dict['filename'] = doc.filename
                    task_dict['doc_id'] = doc.id
                
                return JSONResponse(content=task_dict)
        else:
            task = task_manager.get_task(doc_id)
            if task:
                task_dict = task.to_dict()
                
                # Also get database info
                doc = db.get_document(doc_id)
                if doc:
                    task_dict['filename'] = doc.filename
                    task_dict['doc_id'] = doc.id
                
                return JSONResponse(content=task_dict)
        
        # Fall back to database for completed/old tasks
        doc = db.get_document(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        
        return JSONResponse(content={
            "doc_id": doc.id,
            "status": doc.status,
            "progress_percentage": doc.progress_percentage or 0,
            "message": doc.progress_message or "",
            "total_pages": doc.total_pages or 0,
            "processed_pages": doc.processed_pages or 0,
            "filename": doc.filename,
            "is_zip_parent": False,
            "child_task_ids": [],
            "total_files": 0,
            "processed_files": 0
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_progress_failed", error=str(e), doc_id=doc_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks")
async def list_tasks(status: Optional[str] = None):
    """List all tasks with optional status filter"""
    try:
        status_filter = None
        if status:
            try:
                status_filter = TaskStatus(status)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        
        tasks = task_manager.list_tasks(status_filter)
        return JSONResponse(content={
            "tasks": tasks,
            "total": len(tasks)
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error("list_tasks_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}")
async def get_task(task_id: int):
    """Get detailed task information"""
    try:
        task = task_manager.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return JSONResponse(content=task.to_dict())
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_task_failed", error=str(e), task_id=task_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/pause")
async def pause_task(task_id: int):
    """Pause a running task"""
    try:
        success = task_manager.pause_task(task_id)
        if not success:
            raise HTTPException(status_code=400, detail="Cannot pause task. Check task status.")
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Task {task_id} pause requested",
            "task_id": task_id
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error("pause_task_failed", error=str(e), task_id=task_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/resume")
async def resume_task(task_id: int):
    """Resume a paused task"""
    try:
        success = task_manager.resume_task(task_id)
        if not success:
            raise HTTPException(status_code=400, detail="Cannot resume task. Check task status.")
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Task {task_id} resumed",
            "task_id": task_id
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error("resume_task_failed", error=str(e), task_id=task_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: int):
    """Cancel a task"""
    try:
        success = task_manager.cancel_task(task_id)
        if not success:
            raise HTTPException(status_code=400, detail="Cannot cancel task. Check task status.")
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Task {task_id} cancellation requested",
            "task_id": task_id
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error("cancel_task_failed", error=str(e), task_id=task_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/cleanup")
async def cleanup_tasks(keep_recent: int = 10):
    """Cleanup old finished tasks"""
    try:
        task_manager.cleanup_finished_tasks(keep_recent)
        return JSONResponse(content={
            "status": "success",
            "message": f"Cleaned up old tasks, keeping {keep_recent} most recent"
        })
    except Exception as e:
        logger.error("cleanup_tasks_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/documents/{doc_id}/cleanup-minio")
async def cleanup_document_minio(doc_id: int):
    """
    清理单个文档的 MinIO 数据（不删除数据库记录）
    """
    try:
        # 获取文档信息
        doc = db.get_document(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        
        checksum = doc.checksum
        filename = doc.filename
        
        # 删除 MinIO 数据
        deleted_count = 0
        try:
            from src.minio_storage import minio_storage
            if minio_storage.enabled and checksum:
                filename_base = Path(filename).stem.replace(' ', '_').replace('/', '_')
                minio_prefix = f"{filename_base}_{doc_id}_{checksum[:8]}"
                
                deleted_count = minio_storage.delete_directory(minio_prefix)
                logger.info("minio_cleaned_for_document", doc_id=doc_id, prefix=minio_prefix, count=deleted_count)
        except Exception as minio_error:
            logger.warning("minio_cleanup_failed", error=str(minio_error), doc_id=doc_id)
        
        return JSONResponse(content={
            "status": "success",
            "message": f"MinIO data cleaned for document {doc_id}",
            "doc_id": doc_id,
            "files_deleted": deleted_count
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("cleanup_document_minio_failed", error=str(e), doc_id=doc_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: int,
    current_user: User = Depends(get_current_user)
):
    """
    Delete a specific document completely from:
    - SQLite database
    - Elasticsearch index
    - MinIO storage (if enabled)
    - Local processed files
    - Original uploaded files
    - Child documents (if this is a ZIP parent)
    
    Requires authentication. Users can only delete their own documents unless they are superuser.
    """
    try:
        logger.info(f"Attempting to delete document {doc_id}", user_id=current_user.id)
        
        # 1. Try to find in DB first (bypass permission filter to get the actual document)
        # We'll check permissions separately
        session = db.get_session()
        try:
            from src.database import Document
            doc = session.query(Document).filter(Document.id == doc_id).first()
        finally:
            session.close()
        
        # Check permissions - users can only delete their own documents unless superuser
        if doc and not current_user.is_superuser:
            # Allow deletion of legacy documents (no owner_id) OR owned documents
            if doc.owner_id is not None and doc.owner_id != current_user.id:
                raise HTTPException(
                    status_code=403,
                    detail="You can only delete your own documents"
                )
        
        # If not found in DB, we might still need to clean up ES and other storages
        # But if found, we use its info
        
        checksum = doc.checksum if doc else None
        filename = doc.filename if doc else None
        file_path = doc.file_path if doc else None
        
        deletion_result = {
            "doc_id": doc_id,
            "filename": filename,
            "es_deleted": 0,
            "minio_deleted": 0,
            "local_files_deleted": False,
            "original_file_deleted": False,
            "child_docs_deleted": 0
        }
        
        # If found in DB, cancel tasks and handle children
        if doc:
            # Cancel any running task for this document
            task_manager.cancel_task(doc_id)
        
        # 1.5. If this is a ZIP parent, collect child task IDs and delete them first
        child_task_ids = []
        task = task_manager.get_task(doc_id)
        if task and task.child_task_ids:
            child_task_ids = list(task.child_task_ids)
            logger.info("found_child_tasks", parent_id=doc_id, child_ids=child_task_ids)
        
        # Delete all child documents first
        for child_id in child_task_ids:
                # ... (child deletion logic remains same) ...
            try:
                # Get child document info
                child_doc = db.get_document(child_id)
                if not child_doc:
                    logger.warning("child_doc_not_found_in_db", child_id=child_id)
                    # Still try to clean ES for child ID
                    try:
                        pipeline.vector_store.delete_by_metadata({"document_id": str(child_id)})
                    except:
                        pass
                    continue
                
                # Cancel child task
                task_manager.cancel_task(child_id)
                
                # Delete from ES
                try:
                    child_es_deleted = pipeline.vector_store.delete_by_metadata({"document_id": str(child_id)})
                    deletion_result["es_deleted"] += child_es_deleted
                except Exception as es_error:
                    logger.warning("child_es_deletion_failed", error=str(es_error), child_id=child_id)
                
                # Delete from MinIO
                try:
                    from src.minio_storage import minio_storage
                    if minio_storage.enabled and child_doc.checksum:
                        child_filename_base = Path(child_doc.filename).stem.replace(' ', '_').replace('/', '_')
                        child_minio_prefix = f"{child_filename_base}_{child_id}_{child_doc.checksum[:8]}"
                        child_minio_deleted = minio_storage.delete_directory(child_minio_prefix)
                        deletion_result["minio_deleted"] += child_minio_deleted
                except Exception as minio_error:
                    logger.warning("child_minio_deletion_failed", error=str(minio_error), child_id=child_id)
                
                # Delete local processed files
                try:
                    processed_folder = Path('web/static/processed_docs')
                    child_doc_folder = processed_folder / f"{child_id}_{child_doc.checksum[:8]}"
                    if child_doc_folder.exists():
                        import shutil
                        shutil.rmtree(child_doc_folder)
                except Exception as local_error:
                    logger.warning("child_local_deletion_failed", error=str(local_error), child_id=child_id)
                
                # Delete original file
                try:
                    if child_doc.file_path and Path(child_doc.file_path).exists():
                        Path(child_doc.file_path).unlink()
                except Exception as file_error:
                    logger.warning("child_original_file_deletion_failed", error=str(file_error), child_id=child_id)
                
                # Delete from database
                db.delete_document(child_id)
                deletion_result["child_docs_deleted"] += 1
                
            except Exception as child_error:
                logger.error("child_deletion_failed", error=str(child_error), child_id=child_id)
        
        # 2. Delete parent document from Elasticsearch by document_id
        # Even if doc is not in DB, we try to delete from ES using the ID provided
        try:
            # Try primary deletion by document_id, fallback to checksum for legacy data
            es_deleted = pipeline.vector_store.delete_by_metadata(
                {"document_id": str(doc_id)},
                fallback_filters={"checksum": checksum} if checksum else None
            )
            deletion_result["es_deleted"] += es_deleted
            logger.info("es_deleted", doc_id=doc_id, count=es_deleted)
        except Exception as es_error:
            logger.warning("es_deletion_failed", error=str(es_error), doc_id=doc_id)
        
        # 3. Delete from MinIO
        try:
            from src.minio_storage import minio_storage
            if minio_storage.enabled and filename and checksum:
                # 构建 MinIO prefix: {filename_base}_{doc_id}_{checksum[:8]}
                filename_base = Path(filename).stem.replace(' ', '_').replace('/', '_')
                minio_prefix = f"{filename_base}_{doc_id}_{checksum[:8]}"
                
                minio_deleted = minio_storage.delete_directory(minio_prefix)
                deletion_result["minio_deleted"] = minio_deleted
                logger.info("minio_deleted", doc_id=doc_id, prefix=minio_prefix, count=minio_deleted)
        except Exception as minio_error:
            logger.warning("minio_deletion_failed", error=str(minio_error), doc_id=doc_id)
        
        # 4. Delete local processed files
        if checksum:
            try:
                processed_folder = Path('web/static/processed_docs')
                doc_folder = processed_folder / f"{doc_id}_{checksum[:8]}"
                if doc_folder.exists():
                    import shutil
                    shutil.rmtree(doc_folder)
                    deletion_result["local_files_deleted"] = True
                    logger.info("local_files_deleted", doc_id=doc_id, path=str(doc_folder))
            except Exception as local_error:
                logger.warning("local_deletion_failed", error=str(local_error), doc_id=doc_id)
        
        # 5. Delete original uploaded file
        if file_path:
            try:
                if Path(file_path).exists():
                    Path(file_path).unlink()
                    deletion_result["original_file_deleted"] = True
                    logger.info("original_file_deleted", doc_id=doc_id, path=file_path)
            except Exception as file_error:
                logger.warning("original_file_deletion_failed", error=str(file_error), doc_id=doc_id)
        
        # 6. Delete from SQLite
        # Even if it wasn't found initially (race condition?), try delete one last time
        if doc:
            success = db.delete_document(doc_id)
            if not success:
                logger.warning("db_delete_failed_or_already_gone", doc_id=doc_id)
        
        logger.info("document_completely_deleted", **deletion_result)
        
        return JSONResponse(content={
            "status": "success", 
            "message": f"Document {doc_id} completely deleted",
            **deletion_result
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_document_failed", error=str(e), doc_id=doc_id)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/documents")
async def delete_all_documents():
    """
    Delete ALL documents completely from:
    - SQLite database
    - Elasticsearch index
    - MinIO storage (if enabled)
    - Local processed files
    - Original uploaded files
    """
    try:
        # 1. Get all documents info before deletion
        all_docs = db.list_documents(limit=10000)
        
        deletion_result = {
            "total_docs": len(all_docs),
            "es_deleted": 0,
            "minio_deleted": 0,
            "local_folders_deleted": 0,
            "original_files_deleted": 0
        }
        
        # 2. Delete each document's data
        for doc in all_docs:
            doc_id = doc.get('id')
            checksum = doc.get('checksum', '')
            filename = doc.get('filename', '')
            file_path = doc.get('file_path', '')
            
            # Delete from Elasticsearch (使用正确的 document_id, with fallback for legacy data)
            try:
                count = pipeline.vector_store.delete_by_metadata(
                    {"document_id": str(doc_id)},
                    fallback_filters={"checksum": checksum} if checksum else None
                )
                deletion_result["es_deleted"] += count
            except Exception as es_error:
                logger.warning("es_deletion_failed", error=str(es_error), doc_id=doc_id)
            
            # Delete from MinIO
            try:
                from src.minio_storage import minio_storage
                if minio_storage.enabled and checksum:
                    filename_base = Path(filename).stem.replace(' ', '_').replace('/', '_')
                    minio_prefix = f"{filename_base}_{doc_id}_{checksum[:8]}"
                    count = minio_storage.delete_directory(minio_prefix)
                    deletion_result["minio_deleted"] += count
            except Exception as minio_error:
                logger.warning("minio_deletion_failed", error=str(minio_error), doc_id=doc_id)
            
            # Delete local processed files
            try:
                if checksum:
                    processed_folder = Path('web/static/processed_docs')
                    doc_folder = processed_folder / f"{doc_id}_{checksum[:8]}"
                    if doc_folder.exists():
                        import shutil
                        shutil.rmtree(doc_folder)
                        deletion_result["local_folders_deleted"] += 1
            except Exception as local_error:
                logger.warning("local_deletion_failed", error=str(local_error), doc_id=doc_id)
            
            # Delete original file
            try:
                if file_path and Path(file_path).exists():
                    Path(file_path).unlink()
                    deletion_result["original_files_deleted"] += 1
            except Exception as file_error:
                logger.warning("original_file_deletion_failed", error=str(file_error), doc_id=doc_id)
        
        # 3. Delete all from SQLite (最后删除)
        db.delete_all_documents()
        
        # 4. Cancel all tasks
        task_manager.tasks.clear()
        
        logger.info("all_documents_completely_deleted", **deletion_result)
        
        return JSONResponse(content={
            "status": "success", 
            "message": "All documents completely deleted",
            **deletion_result
        })
    except Exception as e:
        logger.error("delete_all_documents_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    organization_id: Optional[int] = Form(None),
    visibility: Optional[str] = Form('organization'),
    category: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),
    author: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    ocr_engine: Optional[str] = Form('easy'),
    processing_mode: Optional[str] = Form('fast')
):
    """
    Upload and process single file
    Requires authentication.
    """
    doc_id = None
    file_path = None
    
    try:
        # Determine organization ID
        if not organization_id:
            organization_id = current_user.org_id
        
        # Validate user can upload to this organization
        if not current_user.is_superuser and organization_id != current_user.org_id:
            raise HTTPException(
                status_code=403, 
                detail="You can only upload documents to your own organization"
            )
        
        # Validate visibility setting
        valid_visibility = ['private', 'organization', 'public']
        if visibility not in valid_visibility:
            visibility = 'organization'
        
        # Only superusers can create public documents
        if visibility == 'public' and not current_user.is_superuser:
            raise HTTPException(
                status_code=403,
                detail="Only administrators can create public documents"
            )
        
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        # Check file extension
        allowed_extensions = web_config.get('allowed_extensions', [])
        file_ext = Path(file.filename).suffix.lower().lstrip('.')
        
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"File type not allowed. Allowed types: {', '.join(allowed_extensions)}"
            )
        
        # Save uploaded file
        file_path = upload_folder / file.filename
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        file_size = file_path.stat().st_size
        
        logger.info("file_uploaded", filename=file.filename, size=file_size, user_id=current_user.id, org_id=organization_id)
        
        # Calculate checksum
        import hashlib
        with open(file_path, 'rb') as f:
            checksum = hashlib.sha256(f.read()).hexdigest()
        
        # Check if already exists
        existing = db.get_document_by_checksum(checksum)
        if existing:
            if file_path.exists():
                os.remove(file_path)
            return JSONResponse(content={
                "status": "duplicate",
                "message": "File already exists",
                "document": existing.to_dict()
            })
        
        # Create database record with user and organization info
        doc = db.create_document(
            filename=file.filename,
            file_path=str(file_path),
            file_type=file_ext,
            file_size=file_size,
            checksum=checksum,
            category=category,
            tags=tags.split(',') if tags else None,
            author=author,
            description=description,
            ocr_engine=ocr_engine,
            owner_id=current_user.id,
            org_id=organization_id,
            visibility=visibility
        )
        doc_id = doc.id
        logger.info("document_created", doc_id=doc_id, file_type=file_ext)
        
        # Update status to processing
        db.update_document_status(doc_id, 'processing')
        logger.info("status_updated_to_processing", doc_id=doc_id)
        
        # Prepare metadata
        metadata = {}
        if category:
            metadata['category'] = category
        if tags:
            metadata['tags'] = tags.split(',')
        if author:
            metadata['author'] = author
        if description:
            metadata['description'] = description
        
        # Start background processing
        logger.info("starting_background_processing", doc_id=doc_id, filename=file.filename, ocr_engine=ocr_engine, file_type=file_ext)
        
        # Start background thread
        thread = threading.Thread(
            target=process_document_background,
            args=(doc_id, file_path, metadata, ocr_engine, checksum, processing_mode),
            daemon=True
        )
        
        # Register thread in task manager
        task_manager.register_thread(doc_id, thread)
        thread.start()
        
        # Return immediately with task info
        return JSONResponse(content={
            'status': 'processing',
            'message': 'Document uploaded and processing started',
            'document_id': doc_id,
            'checksum': checksum,
            'filename': file.filename
        })
    
    except Exception as e:
        logger.error("upload_failed", error=str(e))
        
        # Update database if record was created
        if doc_id:
            db.update_document_status(doc_id, 'failed', error_message=str(e))
        
        # Clean up file
        if file_path and file_path.exists():
            os.remove(file_path)
        
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/upload_batch")
async def upload_batch(
    files: List[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
    organization_id: Optional[int] = Form(None),
    visibility: Optional[str] = Form('organization'),
    category: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),
    author: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    ocr_engine: Optional[str] = Form('vision'),
    processing_mode: Optional[str] = Form('fast')
):
    """
    Upload and process multiple files asynchronously
    Requires authentication.
    """
    try:
        # Determine organization ID
        if not organization_id:
            organization_id = current_user.org_id
        
        # Validate user can upload to this organization
        if not current_user.is_superuser and organization_id != current_user.org_id:
            raise HTTPException(
                status_code=403, 
                detail="You can only upload documents to your own organization"
            )
        
        # Validate visibility setting
        valid_visibility = ['private', 'organization', 'public']
        if visibility not in valid_visibility:
            visibility = 'organization'
        
        # Only superusers can create public documents
        if visibility == 'public' and not current_user.is_superuser:
            raise HTTPException(
                status_code=403,
                detail="Only administrators can create public documents"
            )
        
        results = []
        logger.info("batch_upload_started", num_files=len(files), user_id=current_user.id, org_id=organization_id)
        
        for file in files:
            file_path = None
            try:
                # 1. Validate
                if not file.filename:
                    continue
                
                # Check file extension
                allowed_extensions = web_config.get('allowed_extensions', [])
                file_ext = Path(file.filename).suffix.lower().lstrip('.')
                
                if file_ext not in allowed_extensions:
                    results.append({
                        "filename": file.filename,
                        "status": "failed",
                        "error": f"File type not allowed. Allowed: {', '.join(allowed_extensions)}"
                    })
                    continue
                
                # 2. Save file
                file_path = upload_folder / file.filename
                with open(file_path, "wb") as f:
                    shutil.copyfileobj(file.file, f)
                
                file_size = file_path.stat().st_size
                
                # 3. Checksum
                with open(file_path, 'rb') as f:
                    checksum = hashlib.sha256(f.read()).hexdigest()
                
                # 4. Check Duplicate
                existing = db.get_document_by_checksum(checksum)
                if existing:
                    if file_path.exists():
                        os.remove(file_path)
                    results.append({
                        "filename": file.filename,
                        "status": "duplicate",
                        "document_id": existing.id,
                        "message": "File already exists"
                    })
                    continue
                
                # 5. Create DB Record with user and organization info
                doc = db.create_document(
                    filename=file.filename,
                    file_path=str(file_path),
                    file_type=file_ext,
                    file_size=file_size,
                    checksum=checksum,
                    category=category,
                    tags=tags.split(',') if tags else None,
                    author=author,
                    description=description,
                    ocr_engine=ocr_engine,
                    owner_id=current_user.id,
                    org_id=organization_id,
                    visibility=visibility
                )
        
                # Update status to processing
                db.update_document_status(doc.id, 'processing')
                
                # 6. Prepare Metadata
                metadata = {}
                if category: metadata['category'] = category
                if tags: metadata['tags'] = tags.split(',')
                if author: metadata['author'] = author
                if description: metadata['description'] = description
                
                # 7. Start Background Task
                thread = threading.Thread(
                    target=process_document_background,
                    args=(doc.id, file_path, metadata, ocr_engine, checksum, processing_mode),
                    daemon=True
                )
                task_manager.register_thread(doc.id, thread)
                thread.start()
                
                results.append({
                    "filename": file.filename,
                    "status": "processing",
                    "document_id": doc.id,
                    "checksum": checksum
                })
                
                logger.info("batch_file_processing_started", doc_id=doc.id, filename=file.filename)
                
            except Exception as file_error:
                logger.error("batch_file_failed", filename=file.filename, error=str(file_error))
                # Clean up file if it exists and we failed before starting processing
                if file_path and file_path.exists() and "document_id" not in locals():
                    try:
                        os.remove(file_path)
                    except:
                        pass
                        
                results.append({
                    "filename": file.filename,
                    "status": "failed",
                    "error": str(file_error)
                })
        
        return JSONResponse(content={"results": results})
    
    except Exception as e:
        logger.error("batch_upload_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload_zip")
async def upload_zip(
    file: UploadFile = File(...),
    category: Optional[str] = Form(None),
    tags: Optional[str] = Form(None),
    author: Optional[str] = Form(None)
):
    """
    Upload and process ZIP file
    """
    try:
        if not file.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="File must be a ZIP archive")
        
        # Save ZIP file
        zip_path = upload_folder / file.filename
        with open(zip_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        
        logger.info("zip_uploaded", filename=file.filename)
        
        # Prepare metadata
        metadata = {}
        if category:
            metadata['category'] = category
        if tags:
            metadata['tags'] = tags.split(',')
        if author:
            metadata['author'] = author
        
        # Process ZIP
        result = pipeline.process_zip(str(zip_path), metadata)
        
        # Clean up
        if zip_path.exists():
            os.remove(zip_path)
        
        # Clean up extracted files
        extract_dir = upload_folder / f"extracted_{zip_path.stem}"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        
        return JSONResponse(content=result)
    
    except Exception as e:
        logger.error("zip_upload_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# - upload_file()
# - upload_batch()
# - upload_zip()
# - get_document_progress()
# - delete_document()
# - delete_all_documents()
# - cleanup_document_minio()
# - list_tasks()
# - get_task()
# - pause_task()
# - resume_task()
# - cancel_task()
# - cleanup_tasks()
# - get_document_permissions()
# - update_document_permissions()


# ============================================================
# Document Permission Management
# ============================================================

from pydantic import BaseModel
from src.database import AuthManager


class DocumentPermissionRequest(BaseModel):
    """Document permission update request"""
    visibility: str  # "public" | "organization" | "private"
    shared_with_users: Optional[List[int]] = []
    shared_with_roles: Optional[List[str]] = []


@router.get("/documents/{doc_id}/permissions")
async def get_document_permissions(
    doc_id: int,
    current_user: Optional[User] = Depends(get_current_user)
):
    """
    Get document permission details
    
    Returns document owner, visibility, and shared users/roles.
    """
    # Get document (with permission check)
    doc = db.get_document(
        doc_id,
        user_id=current_user.id if current_user else None,
        org_id=current_user.org_id if current_user else None,
        is_superuser=current_user.is_superuser if current_user else False
    )
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found or access denied")
    
    # Initialize auth manager
    auth_manager = AuthManager(db.engine)
    
    # Get owner info
    owner = None
    if doc.owner_id:
        owner_user = auth_manager.get_user_by_id(doc.owner_id)
        if owner_user:
            owner = {
                'id': owner_user.id,
                'username': owner_user.username,
                'email': owner_user.email
            }
    
    # Get organization info
    organization = None
    if doc.org_id:
        org = auth_manager.get_organization(doc.org_id)
        if org:
            organization = {
                'id': org.id,
                'name': org.name
            }
    
    # Get shared users
    shared_users = []
    if doc.shared_with_users:
        import json
        user_ids = json.loads(doc.shared_with_users) if isinstance(doc.shared_with_users, str) else doc.shared_with_users
        for user_id in user_ids:
            user = auth_manager.get_user_by_id(user_id)
            if user:
                shared_users.append({
                    'id': user.id,
                    'username': user.username,
                    'email': user.email
                })
    
    # Get shared roles
    shared_roles = []
    if doc.shared_with_roles:
        import json
        role_codes = json.loads(doc.shared_with_roles) if isinstance(doc.shared_with_roles, str) else doc.shared_with_roles
        for role_code in role_codes:
            role = auth_manager.get_role_by_code(role_code)
            if role:
                shared_roles.append({
                    'code': role.code,
                    'name': role.name
                })
    
    return {
        'id': doc.id,
        'filename': doc.filename,
        'visibility': doc.visibility or 'private',
        'owner': owner,
        'organization': organization,
        'shared_users': shared_users,
        'shared_roles': shared_roles
    }


@router.put("/documents/{doc_id}/permissions")
async def update_document_permissions(
    doc_id: int,
    request: DocumentPermissionRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Update document permissions
    
    Only document owner or superuser can update permissions.
    Shared users must be in the same organization.
    """
    import json
    from sqlalchemy.orm import Session
    
    # Get document
    doc = db.get_document(
        doc_id,
        user_id=current_user.id,
        org_id=current_user.org_id,
        is_superuser=current_user.is_superuser
    )
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found or access denied")
    
    # Check if user is owner or superuser
    if doc.owner_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=403,
            detail="Only document owner or administrator can update permissions"
        )
    
    # Validate visibility
    if request.visibility not in ['public', 'organization', 'private']:
        raise HTTPException(status_code=400, detail="Invalid visibility value")
    
    # Initialize auth manager
    auth_manager = AuthManager(db.engine)
    
    # Validate shared users (must be in same organization)
    if request.shared_with_users:
        for user_id in request.shared_with_users:
            user = auth_manager.get_user_by_id(user_id)
            if not user:
                raise HTTPException(status_code=400, detail=f"User {user_id} not found")
            if doc.org_id and user.org_id != doc.org_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot share with user from different organization"
                )
    
    # Validate shared roles
    if request.shared_with_roles:
        for role_code in request.shared_with_roles:
            role = auth_manager.get_role_by_code(role_code)
            if not role:
                raise HTTPException(status_code=400, detail=f"Role {role_code} not found")
    
    # Update document permissions
    session: Session = db.get_session()
    try:
        from src.database import Document
        doc_obj = session.query(Document).filter_by(id=doc_id).first()
        
        if not doc_obj:
            raise HTTPException(status_code=404, detail="Document not found")
        
        doc_obj.visibility = request.visibility
        doc_obj.shared_with_users = json.dumps(request.shared_with_users) if request.shared_with_users else None
        doc_obj.shared_with_roles = json.dumps(request.shared_with_roles) if request.shared_with_roles else None
        
        session.commit()
        
        return {"message": "Permissions updated successfully"}
    except HTTPException:
        session.rollback()
        raise
    except Exception as e:
        session.rollback()
        logger.error("failed_to_update_permissions", doc_id=doc_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

