"""Document processing handlers"""

import json
import os
import shutil
import subprocess
import sys
import threading
import queue
from pathlib import Path
from typing import Optional

import structlog
from src.task_manager import task_manager, TaskStatus, TaskStage
from src.database import DatabaseManager
from src.pipeline import ProcessingPipeline
from src.config import config

logger = structlog.get_logger(__name__)

# Initialize database and pipeline
db = DatabaseManager()
pipeline = ProcessingPipeline()

# Get upload folder from config
web_config = config.web_config
upload_folder = Path(web_config.get('upload_folder', './uploads'))

# Create processed folder path globally
processed_folder = Path('web/static/processed_docs')
processed_folder.mkdir(parents=True, exist_ok=True)

# Global Task Queue
# tuple: (doc_id, file_path, metadata, ocr_engine, checksum)
task_queue = queue.Queue()

# Worker Thread Management
WORKER_COUNT = 3  # Number of concurrent workers
workers = []

def processing_worker():
    """Worker thread to process documents from queue"""
    while True:
        try:
            # Get task from queue
            task_args = task_queue.get()
            if task_args is None:
                break  # Sentinel to stop worker
            
            doc_id, file_path, metadata, ocr_engine, checksum, processing_mode = task_args
            
            try:
                logger.info("worker_picked_task", doc_id=doc_id, processing_mode=processing_mode, thread=threading.current_thread().name)
                _real_process_document(doc_id, file_path, metadata, ocr_engine, checksum, processing_mode)
            except Exception as e:
                logger.error("worker_task_failed", doc_id=doc_id, error=str(e))
                task_manager.complete_task(doc_id, success=False, error_message=str(e))
                db.update_document_status(doc_id, 'failed', error_message=str(e))
            finally:
                task_queue.task_done()
                
        except Exception as e:
            logger.error("worker_thread_error", error=str(e))

# Start workers
def start_workers():
    global workers
    if not workers:
        for i in range(WORKER_COUNT):
            t = threading.Thread(target=processing_worker, name=f"DocWorker-{i+1}", daemon=True)
            t.start()
            workers.append(t)
        logger.info("worker_threads_started", count=WORKER_COUNT)

# Ensure workers are started
start_workers()

def recover_stuck_tasks():
    """
    Recover tasks that were interrupted during shutdown.
    Reset 'processing' and 'queued' tasks to 'queued' state and re-enqueue them.
    This ensures no tasks are left in limbo after a restart.
    """
    try:
        # Find tasks that are stuck in processing or queued
        stuck_statuses = ['processing', 'queued']
        stuck_docs = db.get_documents_by_status(stuck_statuses)
        
        if not stuck_docs:
            logger.info("no_stuck_tasks_found")
            return
            
        logger.info("recovering_stuck_tasks", count=len(stuck_docs))
        
        for doc in stuck_docs:
            try:
                doc_id = doc.id
                file_path = Path(doc.file_path) if doc.file_path else None
                
                # Verify file exists
                if not file_path or not file_path.exists():
                    logger.warning("stuck_task_file_missing", doc_id=doc_id, path=str(file_path))
                    db.update_document_status(doc_id, 'failed', error_message="File not found during recovery")
                    continue
                
                # Reconstruct metadata
                metadata = {}
                if doc.category: metadata['category'] = doc.category
                if doc.tags: metadata['tags'] = doc.tags.split(',') if doc.tags else []
                if doc.author: metadata['author'] = doc.author
                if doc.description: metadata['description'] = doc.description
                
                # Default values
                ocr_engine = doc.ocr_engine or 'vision'
                checksum = doc.checksum or ''
                
                # Reset status to queued
                db.update_document_status(doc_id, 'queued', error_message="Recovered from system restart")
                
                # Re-create task in manager
                task_manager.create_task(doc_id)
                task_manager.update_task(
                    doc_id,
                    status=TaskStatus.PENDING,
                    message="Recovered task, waiting in queue...",
                    progress_percentage=0
                )
                
                # Enqueue (use default 'fast' mode for recovered tasks)
                task_queue.put((doc_id, file_path, metadata, ocr_engine, checksum, 'fast'))
                logger.info("stuck_task_recovered", doc_id=doc_id, filename=doc.filename)
                
            except Exception as doc_error:
                logger.error("failed_to_recover_single_task", doc_id=doc.id, error=str(doc_error))
                db.update_document_status(doc.id, 'failed', error_message=f"Recovery failed: {str(doc_error)}")
                
    except Exception as e:
        logger.error("task_recovery_process_failed", error=str(e))


# ============================================================
# Helper functions
# ============================================================

def extract_matched_bboxes_from_file(doc_id: int, checksum: str, page_number: int, query_text: str):
    """
    Extract matched bboxes from OCR JSON file for visualization
    
    Args:
        doc_id: Document ID
        checksum: Document checksum (first 8 chars used in folder name)
        page_number: Page number to extract bboxes from
        query_text: Query text to match against OCR text blocks
        
    Returns:
        List of matched bbox dicts with text, bbox, confidence, matched_words
    """
    import re
    
    try:
        # Build path to processed document folder
        doc_folder = processed_folder / f"{doc_id}_{checksum[:8]}"
        
        if not doc_folder.exists():
            logger.warning("doc_folder_not_found", doc_id=doc_id, folder=str(doc_folder))
            return []
        
        # Load OCR JSON file for the specific page
        ocr_json_file = doc_folder / f"page_{page_number:03d}_global_ocr.json"
        
        # 如果找不到单页的 OCR JSON，尝试查找完整的 OCR JSON (PPTX/DOCX/图片可能使用这种格式)
        if not ocr_json_file.exists():
            complete_json_file = doc_folder / "complete_adaptive_ocr.json"
            if complete_json_file.exists():
                try:
                    with open(complete_json_file, 'r', encoding='utf-8') as f:
                        complete_data = json.load(f)
                        
                    # 查找对应页面的数据
                    target_page_data = None
                    for page in complete_data.get('pages', []):
                        if page.get('page_number') == page_number:
                            # 尝试从不同阶段获取 text_blocks
                            # 优先使用 stage3_vlm (最终结果)
                            if 'stage3_vlm' in page:
                                target_page_data = page['stage3_vlm']
                            # 其次使用 stage2_ocr
                            elif 'stage2_ocr' in page:
                                target_page_data = page['stage2_ocr']
                            break
                    
                    if target_page_data:
                        # 模拟单页 JSON 结构
                        ocr_data = target_page_data
                    else:
                        logger.warning("page_not_found_in_complete_json", page=page_number, file=str(complete_json_file))
                        return []
                except Exception as e:
                    logger.error("failed_to_read_complete_json", error=str(e), file=str(complete_json_file))
                    return []
            else:
                # Also try image_ocr.json for single images
                image_ocr_file = doc_folder / "image_ocr.json"
                if image_ocr_file.exists():
                    try:
                        with open(image_ocr_file, 'r', encoding='utf-8') as f:
                            ocr_data = json.load(f)
                    except Exception as e:
                        logger.error("failed_to_read_image_ocr_json", error=str(e), file=str(image_ocr_file))
                        return []
                else:
                    logger.warning("ocr_json_not_found", page=page_number, file=str(ocr_json_file))
                    return []
        else:
            with open(ocr_json_file, 'r', encoding='utf-8') as f:
                ocr_data = json.load(f)
        
        text_blocks = ocr_data.get('text_blocks', [])
        if not text_blocks:
            return []
        
        # Normalize query for matching
        query_normalized = re.sub(r'\s+', ' ', query_text.lower().strip())
        query_words = query_normalized.split()
        
        matched_bboxes = []
        
        # Match text blocks
        for idx, block in enumerate(text_blocks):
            text = block.get('text', '')
            bbox = block.get('bbox', [])
            confidence = block.get('confidence', 0.0)
            
            if not text or not bbox or len(bbox) != 4:
                continue
            
            text_normalized = text.lower()
            
            # Check if any query word is in this text block
            matched = False
            matched_words = []
            
            for word in query_words:
                if len(word) >= 2 and word in text_normalized:
                    matched = True
                    matched_words.append(word)
            
            # Also try partial matching for longer queries
            if not matched and len(query_normalized) >= 4:
                if query_normalized in text_normalized:
                    matched = True
                    matched_words.append(query_normalized)
            
            if matched:
                matched_bboxes.append({
                    'text': text,
                    'bbox': bbox,  # [x1, y1, x2, y2]
                    'confidence': confidence,
                    'matched_words': matched_words,
                    'block_index': idx
                })
        
        # Sort by confidence (highest first)
        matched_bboxes.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Limit to top 20 matches
        result = matched_bboxes[:20]
        logger.info("extracted_matched_bboxes", page=page_number, count=len(result), total_matches=len(matched_bboxes))
        return result
        
    except Exception as e:
        logger.error("failed_to_extract_bboxes", error=str(e), doc_id=doc_id, page=page_number)
        return []


def process_single_pdf(doc_id: int, pdf_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None, processing_mode: str = 'fast'):
    """Process a single PDF file"""
    try:
        # Update task status
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.OCR_PROCESSING,
            progress_percentage=10,
            message=f"Processing {pdf_path.name} ({processing_mode} mode)...",
            filename=pdf_path.name
        )
        db.update_document_progress(doc_id, 10, f"Starting OCR for {pdf_path.name} ({processing_mode} mode)...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Prepare output directory
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Run intelligent PDF processing with VLM, directly output to final directory
        pdf_vlm_script = Path('document_ocr_pipeline/process_pdf_vlm.py')
        subprocess.run([
            sys.executable,
            str(pdf_vlm_script),
            str(pdf_path),
            '--ocr-engine', ocr_engine,
            '--output-dir', str(doc_output_dir),
            '--processing-mode', processing_mode
        ], check=True, timeout=172800)  # 48 hours timeout
        
        # Check for cancellation after OCR
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        task_manager.update_task(
            doc_id,
            progress_percentage=50,
            message="OCR completed, processing pages..."
        )
        db.update_document_progress(doc_id, 50, "OCR completed, processing pages...")
        
        # Update progress: Loading pages data
        task_manager.update_task(
            doc_id,
            stage=TaskStage.VLM_EXTRACTION,
            progress_percentage=60,
            message="Loading pages data..."
        )
        db.update_document_progress(doc_id, 60, "Loading pages data...")
        
        # Load pages data
        complete_json = doc_output_dir / 'complete_adaptive_ocr.json'
        pages_data_list = []
        total_pages = 0
        
        if complete_json.exists():
            with open(complete_json, 'r', encoding='utf-8') as f:
                complete_data = json.load(f)
            
            total_pages = len(complete_data.get('pages', []))
            task_manager.update_task(
                doc_id,
                progress_percentage=65,
                message=f"Processing {total_pages} pages...",
                total_pages=total_pages,
                processed_pages=0
            )
            db.update_document_progress(doc_id, 65, f"Processing {total_pages} pages...", 
                                       processed_pages=0, total_pages=total_pages)
            
            # Build pages data
            for idx, page in enumerate(complete_data.get('pages', []), 1):
                # Check for cancellation/pause before each page
                if not task_manager.wait_if_paused(doc_id):
                    raise InterruptedError("Task was cancelled by user")
                
                page_num = page.get('page_number', idx)
                
                # Update progress per page
                page_progress = 65 + (20 * idx / total_pages)  # 65-85% for page processing
                task_manager.update_task(
                    doc_id,
                    progress_percentage=int(page_progress),
                    message=f"Processing page {idx}/{total_pages}...",
                    current_page=idx,
                    processed_pages=idx
                )
                db.update_document_progress(
                    doc_id, 
                    int(page_progress), 
                    f"Processing page {idx}/{total_pages}...",
                    processed_pages=idx,
                    total_pages=total_pages
                )
                
                # Get text count from statistics
                stats = page.get('statistics', {})
                text_count = stats.get('total_text_blocks', 0)
                
                # Get stage1 file paths
                stage1 = page.get('stage1_global', {})
                image_filename = stage1.get('image', f'page_{page_num:03d}_300dpi.png')
                visualized_filename = stage1.get('visualized', f'page_{page_num:03d}_global_visualized.png')
                ocr_json_filename = stage1.get('ocr_json', f'page_{page_num:03d}_global_ocr.json')
                
                # Try to extract components from VLM JSON if available
                components = []
                stage3 = page.get('stage3_vlm', {})
                vlm_json_filename = stage3.get('vlm_json')
                if vlm_json_filename:
                    vlm_json_path = doc_output_dir / vlm_json_filename
                    if vlm_json_path.exists():
                        try:
                            with open(vlm_json_path, 'r', encoding='utf-8') as vf:
                                vlm_data = json.load(vf)
                                # Try different possible locations for components
                                if 'components' in vlm_data:
                                    components = vlm_data['components']
                                elif 'domain_data' in vlm_data and isinstance(vlm_data['domain_data'], dict):
                                    if 'components' in vlm_data['domain_data']:
                                        components = vlm_data['domain_data']['components']
                                    elif 'equipment' in vlm_data['domain_data']:
                                        equipment = vlm_data['domain_data']['equipment']
                                        if isinstance(equipment, list):
                                            components = [e.get('id', '') for e in equipment if isinstance(e, dict) and 'id' in e]
                        except Exception as e:
                            logger.warning("failed_to_parse_vlm_json", error=str(e), file=vlm_json_filename)
                
                page_info = {
                    'page_num': page_num,
                    'image_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/{image_filename}",
                    'visualized_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/{visualized_filename}",
                    'ocr_json_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/{ocr_json_filename}",
                    'text_count': text_count,
                    'components': components[:20] if components else []
                }
                pages_data_list.append(page_info)
        
        # Check for cancellation before indexing
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Update progress: Indexing to Elasticsearch
        task_manager.update_task(
            doc_id,
            stage=TaskStage.INDEXING,
            progress_percentage=85,
            message="Indexing to Elasticsearch..."
        )
        db.update_document_progress(doc_id, 85, "Indexing to Elasticsearch...")
        
        # Add document identifiers to metadata for MinIO naming
        metadata['document_id'] = doc_id
        metadata['filename'] = pdf_path.name
        metadata['checksum'] = checksum
        
        # Process with vector store
        result = pipeline.process_file(str(pdf_path), metadata, processed_json_dir=str(doc_output_dir))
        
        # Check for cancellation after indexing
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Update progress: Finalizing
        task_manager.update_task(
            doc_id,
            stage=TaskStage.FINALIZING,
            progress_percentage=95,
            message="Finalizing..."
        )
        db.update_document_progress(doc_id, 95, "Finalizing...")
        
        # Update database with result
        if result.get('status') == 'completed':
            if not result.get('document_ids'):
                error_msg = 'Processing completed but no documents were indexed to Elasticsearch'
                logger.error("NO_DOCUMENTS_INDEXED", 
                           num_chunks=result.get('num_chunks', 0), doc_id=doc_id)
                task_manager.complete_task(doc_id, success=False, error_message=error_msg)
                db.update_document_status(doc_id, 'failed', error_message=error_msg)
            else:
                task_manager.complete_task(doc_id, success=True)
                db.update_document_status(
                    doc_id,
                    'completed',
                    num_chunks=result.get('num_chunks', 0),
                    es_document_ids=json.dumps(result.get('document_ids', [])),
                    pages_data=json.dumps(pages_data_list)
                )
                logger.info("document_processing_completed", doc_id=doc_id, 
                          num_chunks=result.get('num_chunks', 0))
        else:
            error_msg = result.get('error', 'Unknown error')
            task_manager.complete_task(doc_id, success=False, error_message=error_msg)
            db.update_document_status(doc_id, 'failed', error_message=error_msg)
        
    except InterruptedError:
        raise
    except Exception as e:
        logger.error("pdf_processing_failed", error=str(e), doc_id=doc_id, pdf=pdf_path.name)
        raise


def process_single_pptx(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None):
    """Process a single PPTX file"""
    try:
        logger.info("processing_pptx_file", doc_id=doc_id, filename=file_path.name)
        
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.OCR_PROCESSING,
            progress_percentage=10,
            message=f"Processing PPTX: {file_path.name}...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 10, f"Starting PPTX processing for {file_path.name}...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Create output directory for this document
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Run process_pptx.py to extract text and images
        db.update_document_progress(doc_id, 20, "Extracting PPTX content...")
        
        pptx_script = Path('document_ocr_pipeline/process_pptx.py')
        result = subprocess.run([
            sys.executable,
            str(pptx_script),
            str(file_path),
            '-o', str(doc_output_dir),
            '--ocr-engine', ocr_engine
        ], capture_output=True, text=True, timeout=172800)  # 48 hours timeout
        
        if result.returncode != 0:
            logger.error("pptx_processing_failed", error=result.stderr, doc_id=doc_id)
            raise ValueError(f"PPTX processing failed: {result.stderr}")
        
        logger.info("pptx_extraction_completed", doc_id=doc_id)
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Load the generated complete_adaptive_ocr.json
        complete_json_path = doc_output_dir / "complete_adaptive_ocr.json"
        if not complete_json_path.exists():
            raise ValueError("PPTX processing did not generate complete_adaptive_ocr.json")
        
        with open(complete_json_path, 'r', encoding='utf-8') as f:
            complete_data = json.load(f)
        
        # Build pages_data for database (similar to PDF processing)
        pages_data = []
        for page in complete_data.get('pages', []):
            page_num = page['page_number']
            stage1 = page.get('stage1_global', {})
            stage3 = page.get('stage3_vlm', {})
            
            # Extract image filename from stage1
            image_filename = stage1.get('image', f'page_{page_num:03d}_300dpi.png')
            
            # Build page data structure (使用 page_num 字段名与 PDF 保持一致)
            page_data = {
                'page_num': page_num,
                'image_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/{image_filename}",
                'visualized_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_{page_num:03d}_visualized.png",
                'text_count': len(stage3.get('text_combined', '').split()),
                'components': []  # PPTX 暂无组件提取
            }
            pages_data.append(page_data)
        
        # Update database with pages_data
        db.update_document_pages_data(doc_id, pages_data)
        logger.info("pptx_pages_data_saved", doc_id=doc_id, total_pages=len(pages_data))
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Update progress
        db.update_document_progress(doc_id, 60, "Indexing to vector store...")
        task_manager.update_task(
            doc_id,
            stage=TaskStage.INDEXING,
            progress_percentage=60,
            message="Indexing to vector store..."
        )
        
        # Index to vector store using pipeline (与 PDF/DOCX 保持一致的命名)
        metadata['document_id'] = doc_id
        metadata['filename'] = file_path.name  # 使用原始文件名
        metadata['checksum'] = checksum
        metadata['pages_data'] = pages_data
        metadata['source'] = str(file_path)
        
        pipeline.process_file(
            file_path=str(file_path),
            metadata=metadata,
            processed_json_dir=str(doc_output_dir)
        )
        
        logger.info("pptx_indexed", doc_id=doc_id)
        
        # Mark as completed if not child task (parent handles its own completion)
        # But we should update DB status for this specific doc ID
        db.update_document_status(doc_id, 'completed')
        if not parent_task_id:
            db.update_document_progress(doc_id, 100, "Completed")
        task_manager.complete_task(doc_id, success=True)
        
        logger.info("pptx_processing_completed", doc_id=doc_id, filename=file_path.name)

    except InterruptedError:
        raise
    except Exception as e:
        logger.error("pptx_processing_failed", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=str(e))
        db.update_document_status(doc_id, 'failed', error_message=str(e))
        raise


def process_single_text(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None):
    """Process a single Text/Markdown file (No OCR, just text indexing)"""
    try:
        logger.info("processing_text_file", doc_id=doc_id, filename=file_path.name)
        
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.INDEXING, # Skip OCR stage
            progress_percentage=30,
            message=f"Processing text: {file_path.name}...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 30, f"Processing text content...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Create output directory (for consistency)
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Index to vector store using pipeline
        metadata['document_id'] = doc_id
        metadata['filename'] = file_path.name
        metadata['checksum'] = checksum
        metadata['source'] = str(file_path)
        
        # Direct text processing via pipeline -> DocumentProcessor -> TextLoader
        result = pipeline.process_file(
            file_path=str(file_path),
            metadata=metadata,
            processed_json_dir=str(doc_output_dir)
        )
        
        # Create a simple pages_data for frontend consistency
        # We don't have images, but we can provide text stats
        pages_data = [{
            'page_num': 1,
            'image_path': "", # No image for text files
            'visualized_path': "",
            'text_count': 0, # Will be populated if we parsed result better, but 0 is safe
            'components': []
        }]
        
        db.update_document_pages_data(doc_id, pages_data)
        
        # Mark as completed
        db.update_document_status(doc_id, 'completed')
        if not parent_task_id:
            db.update_document_progress(doc_id, 100, "Completed")
        task_manager.complete_task(doc_id, success=True)
        
        logger.info("text_processing_completed", doc_id=doc_id)

    except InterruptedError:
        raise
    except Exception as e:
        logger.error("text_processing_failed", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=str(e))
        db.update_document_status(doc_id, 'failed', error_message=str(e))
        raise


def process_single_docx(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None):
    """Process a single DOCX file"""
    try:
        logger.info("processing_docx_file", doc_id=doc_id, filename=file_path.name)
        
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.OCR_PROCESSING,
            progress_percentage=10,
            message=f"Processing DOCX: {file_path.name}...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 10, f"Starting DOCX processing for {file_path.name}...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Create output directory for this document
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Run process_docx.py to extract text and images
        db.update_document_progress(doc_id, 20, "Extracting DOCX content...")
        
        docx_script = Path('document_ocr_pipeline/process_docx.py')
        result = subprocess.run([
            sys.executable,
            str(docx_script),
            str(file_path),
            '-o', str(doc_output_dir),
            '--ocr-engine', ocr_engine
        ], capture_output=True, text=True, timeout=172800)  # 48 hours timeout
        
        if result.returncode != 0:
            logger.error("docx_processing_failed", error=result.stderr, stdout=result.stdout, doc_id=doc_id)
            raise ValueError(f"DOCX processing failed: {result.stdout} {result.stderr}")
        
        logger.info("docx_extraction_completed", doc_id=doc_id)
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Load the generated complete_document.json
        complete_doc_path = doc_output_dir / "complete_document.json"
        if not complete_doc_path.exists():
            raise ValueError("DOCX processing did not generate complete_document.json")
        
        with open(complete_doc_path, 'r', encoding='utf-8') as f:
            complete_data = json.load(f)
        
        # Build pages_data for database
        pages_data = []
        for page in complete_data.get('pages', []):
            page_num = page.get('page_number', 1)
            content = page.get('content', {})
            text_content = content.get('full_text_cleaned', '')
            
            # Build page data structure
            page_data = {
                'page_num': page_num,
                'image_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_{page_num:03d}_300dpi.png",
                'visualized_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_{page_num:03d}_visualized.png",
                'text_count': len(text_content.split()),
                'components': []  # DOCX 暂无组件提取
            }
            pages_data.append(page_data)
        
        # Update database with pages_data
        db.update_document_pages_data(doc_id, pages_data)
        logger.info("docx_pages_data_saved", doc_id=doc_id, total_pages=len(pages_data))
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Update progress
        db.update_document_progress(doc_id, 60, "Indexing to vector store...")
        task_manager.update_task(
            doc_id,
            stage=TaskStage.INDEXING,
            progress_percentage=60,
            message="Indexing to vector store..."
        )
        
        # Index to vector store using pipeline
        metadata['document_id'] = doc_id
        metadata['filename'] = file_path.name
        metadata['checksum'] = checksum
        metadata['pages_data'] = pages_data
        metadata['source'] = str(file_path)
        
        pipeline.process_file(
            file_path=str(file_path),
            metadata=metadata,
            processed_json_dir=str(doc_output_dir)
        )
        
        logger.info("docx_indexed", doc_id=doc_id)
        
        # Mark as completed
        db.update_document_status(doc_id, 'completed')
        if not parent_task_id:
            db.update_document_progress(doc_id, 100, "Completed")
        task_manager.complete_task(doc_id, success=True)
        
        logger.info("docx_processing_completed", doc_id=doc_id, filename=file_path.name)

    except InterruptedError:
        raise
    except Exception as e:
        logger.error("docx_processing_failed", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=str(e))
        db.update_document_status(doc_id, 'failed', error_message=str(e))
        raise


def process_single_excel(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None):
    """Process a single Excel file"""
    try:
        logger.info("processing_excel_file", doc_id=doc_id, filename=file_path.name)
        
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.OCR_PROCESSING,
            progress_percentage=10,
            message=f"Processing Excel: {file_path.name}...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 10, f"Starting Excel processing for {file_path.name}...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Create output directory for this document
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Run process_excel.py
        db.update_document_progress(doc_id, 20, "Extracting Excel content...")
        
        excel_script = Path('document_ocr_pipeline/process_excel.py')
        result = subprocess.run([
            sys.executable,
            str(excel_script),
            str(file_path),
            '-o', str(doc_output_dir)
        ], capture_output=True, text=True, timeout=172800)  # 48 hours timeout
        
        if result.returncode != 0:
            logger.error("excel_processing_failed", error=result.stderr, doc_id=doc_id)
            raise ValueError(f"Excel processing failed: {result.stderr}")
        
        logger.info("excel_extraction_completed", doc_id=doc_id)
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Load the generated complete_document.json
        complete_doc_path = doc_output_dir / "complete_document.json"
        if not complete_doc_path.exists():
            raise ValueError("Excel processing did not generate complete_document.json")
        
        with open(complete_doc_path, 'r', encoding='utf-8') as f:
            complete_data = json.load(f)
        
        # Build pages_data for database
        pages_data = []
        for page in complete_data.get('pages', []):
            page_num = page.get('page_number', 1)
            content = page.get('content', {})
            text_content = content.get('full_text_cleaned', '')
            
            # Build page data structure
            page_data = {
                'page_num': page_num,
                'image_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_{page_num:03d}_300dpi.png",
                'visualized_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_{page_num:03d}_visualized.png", # 如果有的话
                'text_count': len(text_content.split()),
                'components': []
            }
            pages_data.append(page_data)
        
        # Update database with pages_data
        db.update_document_pages_data(doc_id, pages_data)
        logger.info("excel_pages_data_saved", doc_id=doc_id, total_pages=len(pages_data))
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Update progress
        db.update_document_progress(doc_id, 60, "Indexing to vector store...")
        task_manager.update_task(
            doc_id,
            stage=TaskStage.INDEXING,
            progress_percentage=60,
            message="Indexing to vector store..."
        )
        
        # Index to vector store using pipeline
        metadata['document_id'] = doc_id
        metadata['filename'] = file_path.name
        metadata['checksum'] = checksum
        metadata['pages_data'] = pages_data
        metadata['source'] = str(file_path)
        
        # 重要：将 structured_content 传递给 metadata
        # structured_content 位于 complete_document.json 的顶层
        if 'structured_content' in complete_data:
            logger.info("adding_structured_content_to_metadata", doc_id=doc_id, count=len(complete_data['structured_content']))
            metadata['structured_content'] = complete_data['structured_content']
        
        pipeline.process_file(
            file_path=str(file_path),
            metadata=metadata,
            processed_json_dir=str(doc_output_dir)
        )
        
        logger.info("excel_indexed", doc_id=doc_id)
        
        # Mark as completed
        db.update_document_status(doc_id, 'completed')
        if not parent_task_id:
            db.update_document_progress(doc_id, 100, "Completed")
        task_manager.complete_task(doc_id, success=True)
        
        logger.info("excel_processing_completed", doc_id=doc_id, filename=file_path.name)

    except InterruptedError:
        raise
    except Exception as e:
        logger.error("excel_processing_failed", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=str(e))
        db.update_document_status(doc_id, 'failed', error_message=str(e))
        raise


def process_single_image(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, parent_task_id: Optional[int] = None):
    """Process a single Image file"""
    try:
        logger.info("🖼️ processing_image_file", doc_id=doc_id, filename=file_path.name, ocr_engine=ocr_engine)
        
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.OCR_PROCESSING,
            progress_percentage=10,
            message=f"Processing {file_path.name}...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 10, f"Starting intelligent OCR for {file_path.name}...")
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # 创建输出目录
        doc_output_dir = processed_folder / f"{doc_id}_{checksum[:8]}"
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        
        # 使用新的 process_image.py 脚本（支持 VLM 修正）
        logger.info("🚀 running_intelligent_image_processing", doc_id=doc_id, image=file_path.name, ocr_engine=ocr_engine)
        
        process_script = Path('document_ocr_pipeline/process_image.py')
        cmd = [
            sys.executable,
            str(process_script),
            str(file_path),
            '--ocr-engine', ocr_engine,
            '--output-dir', str(doc_output_dir)
        ]
        logger.info("📝 process_command", doc_id=doc_id, cmd=' '.join(cmd))
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=172800)  # 48 hours timeout
        logger.info("✅ image_processing_stdout", doc_id=doc_id, stdout=result.stdout[:500] if result.stdout else "")
        if result.stderr:
            logger.warning("⚠️ image_processing_stderr", doc_id=doc_id, stderr=result.stderr[:500])
        
        logger.info("image_processing_completed", doc_id=doc_id)
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # 更新进度：读取处理结果
        task_manager.update_task(
            doc_id,
            stage=TaskStage.VLM_EXTRACTION,
            progress_percentage=60,
            message="Building searchable content...",
            total_pages=1,
            processed_pages=0
        )
        db.update_document_progress(doc_id, 60, "Building searchable content...", processed_pages=0, total_pages=1)
        
        # 读取生成的 complete_adaptive_ocr.json
        complete_json_path = doc_output_dir / "complete_adaptive_ocr.json"
        if not complete_json_path.exists():
            raise RuntimeError(f"Image processing output not found: {complete_json_path}")
        
        with open(complete_json_path, 'r', encoding='utf-8') as f:
            complete_data = json.load(f)
        
        # 读取 complete_document.json (用于 ES 索引)
        complete_doc_path = doc_output_dir / "complete_document.json"
        if not complete_doc_path.exists():
            raise RuntimeError(f"Image document JSON not found: {complete_doc_path}")
        
        with open(complete_doc_path, 'r', encoding='utf-8') as f:
            doc_data = json.load(f)
        
        pages_list = doc_data.get('pages', [])
        if not pages_list:
            raise RuntimeError("No pages found in image processing output")
        
        page_data = pages_list[0]
        
        # 构建 pages_data（用于数据库）
        pages_data = [{
            'page_number': 1,
            'image_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/page_001_300dpi.png",
            'visualized_path': f"/static/processed_docs/{doc_id}_{checksum[:8]}/image_visualized.png",
            'text': page_data.get('text', ''),
            'text_count': page_data.get('avg_ocr_confidence', 0),  # Store confidence
            'components': [],
            'extraction_method': page_data.get('extraction_method', 'ocr'),
            'ocr_engine': ocr_engine
        }]
        
        logger.info("📋 pages_data_built_from_intelligent_processing",
                   doc_id=doc_id,
                   text_length=len(page_data.get('text', '')),
                   avg_confidence=page_data.get('avg_ocr_confidence', 0),
                   vlm_refined=complete_data.get('pages', [{}])[0].get('statistics', {}).get('vlm_refined', False))
        
        task_manager.update_task(
            doc_id,
            processed_pages=1
        )
        db.update_document_progress(doc_id, 70, "Processing completed", processed_pages=1, total_pages=1)
        
        # Check for cancellation before indexing
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # 更新进度：索引到 Elasticsearch
        task_manager.update_task(
            doc_id,
            stage=TaskStage.INDEXING,
            progress_percentage=80,
            message="Indexing to Elasticsearch..."
        )
        db.update_document_progress(doc_id, 80, "Indexing to Elasticsearch...")
        
        # 添加文档标识到 metadata
        metadata['document_id'] = doc_id
        metadata['filename'] = file_path.name
        metadata['checksum'] = checksum
        
        logger.info("🔄 starting_pipeline_indexing", doc_id=doc_id, metadata=metadata)
        
        # 使用 pipeline 索引（会读取 complete_document.json）
        result = pipeline.process_file(str(file_path), metadata, processed_json_dir=str(doc_output_dir))
        
        logger.info("✅ pipeline_result", doc_id=doc_id, status=result.get('status'), num_chunks=result.get('num_chunks', 0), document_ids=result.get('document_ids'))
        
        # Check for cancellation after indexing
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # 更新进度：完成
        task_manager.update_task(
            doc_id,
            stage=TaskStage.FINALIZING,
            progress_percentage=95,
            message="Finalizing..."
        )
        db.update_document_progress(doc_id, 95, "Finalizing...")
        
        # 更新数据库
        if result.get('status') == 'completed':
            if not result.get('document_ids'):
                error_msg = 'Image processing completed but no documents were indexed'
                logger.error("❌ no_documents_indexed", doc_id=doc_id)
                task_manager.complete_task(doc_id, success=False, error_message=error_msg)
                db.update_document_status(doc_id, 'failed', error_message=error_msg)
            else:
                logger.info("🎉 marking_as_completed", doc_id=doc_id, num_chunks=result.get('num_chunks', 0))
                task_manager.complete_task(doc_id, success=True)
                db.update_document_status(
                    doc_id,
                    'completed',
                    num_chunks=result.get('num_chunks', 0),
                    pages_data=json.dumps(pages_data)
                )
                logger.info("✅ image_processing_completed", doc_id=doc_id, num_chunks=result.get('num_chunks', 0))
        else:
            error_msg = result.get('error', 'Unknown error during image processing')
            logger.error("❌ pipeline_failed", doc_id=doc_id, error=error_msg)
            task_manager.complete_task(doc_id, success=False, error_message=error_msg)
            db.update_document_status(doc_id, 'failed', error_message=error_msg)
    
    except InterruptedError:
        raise
    except Exception as e:
        logger.error("image_processing_failed", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=str(e))
        db.update_document_status(doc_id, 'failed', error_message=str(e))
        raise


def _real_process_document(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, processing_mode: str = 'fast'):
    """
    Actual logic for processing documents.
    This is called by the worker thread and replaces the old process_document_background logic.
    """
    
    try:
        # Update task status
        task_manager.update_task(
            doc_id,
            status=TaskStatus.RUNNING,
            stage=TaskStage.INITIALIZING,
            progress_percentage=0,
            message=f"Initializing document processing ({processing_mode} mode)...",
            filename=file_path.name
        )
        db.update_document_progress(doc_id, 0, "Initializing...")
        logger.info("background_processing_started", doc_id=doc_id, filename=file_path.name, ocr_engine=ocr_engine, processing_mode=processing_mode)
        
        # Check for cancellation
        if not task_manager.wait_if_paused(doc_id):
            raise InterruptedError("Task was cancelled by user")
        
        # Determine file type and handle accordingly
        file_ext = file_path.suffix.lower()
        logger.info("📄 file_type_detected", doc_id=doc_id, file_ext=file_ext, ocr_engine=ocr_engine)
        
        # Handle ZIP files - extract and process all supported files
        if file_ext == '.zip':
            import zipfile
            
            task_manager.update_task(
                doc_id,
                stage=TaskStage.EXTRACTING_ZIP,
                progress_percentage=5,
                message="Extracting ZIP archive...",
                is_zip_parent=True
            )
            db.update_document_progress(doc_id, 5, "Extracting ZIP archive...")
            logger.info("extracting_zip", doc_id=doc_id, zip_file=file_path.name)
            
            # Create temporary extraction directory
            temp_extract_dir = upload_folder / f"temp_extract_{doc_id}_{checksum[:8]}"
            temp_extract_dir.mkdir(exist_ok=True)
            
            # Extract ZIP
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Handle encoding issues in ZIP filenames (common with macOS-created ZIPs)
                    for zip_info in zip_ref.infolist():
                        # Try to fix filename encoding if needed
                        try:
                            # First try: assume filename is correct UTF-8
                            corrected_name = zip_info.filename
                        except:
                            # If fails, try to decode as CP437 (DOS) and re-encode as UTF-8
                            try:
                                corrected_name = zip_info.filename.encode('cp437').decode('utf-8')
                            except:
                                # Last resort: keep original
                                corrected_name = zip_info.filename
                        
                        # Extract with corrected name
                        zip_info.filename = corrected_name
                        zip_ref.extract(zip_info, temp_extract_dir)
                
                # Find all supported files
                supported_extensions = ['.pdf', '.pptx', '.ppt', '.odp', '.docx', '.doc', '.odt', '.xlsx', '.xls', '.ods', '.jpg', '.jpeg', '.png']
                found_files = []
                
                for p in temp_extract_dir.rglob('*'):
                    if p.is_file() and p.suffix.lower() in supported_extensions:
                        # Check if any part of the path is hidden or __MACOSX
                        parts = p.relative_to(temp_extract_dir).parts
                        if not any(part.startswith('.') or part == '__MACOSX' for part in parts):
                            found_files.append(p)
                
                if not found_files:
                    raise ValueError("No supported files found in ZIP archive")
                
                logger.info("found_files_in_zip", count=len(found_files), doc_id=doc_id)
                
                # Update parent task with total files
                task_manager.update_task(
                    doc_id,
                    total_files=len(found_files),
                    processed_files=0,
                    message=f"Found {len(found_files)} files in ZIP. Initializing tasks..."
                )
                
                # Phase 1: Create all tasks and DB records first (Show all as Pending immediately)
                pending_tasks = []
                for idx, f_path in enumerate(found_files, 1):
                    f_ext = f_path.suffix.lower()
                    
                    child_checksum = f"{checksum}_{idx}"
                    
                    # Create database record for child FIRST (to get real ID)
                    child_doc = db.create_document(
                        filename=f_path.name,
                        file_path=str(f_path),
                        file_type=f_ext.lstrip('.'),
                        file_size=f_path.stat().st_size,
                        checksum=child_checksum,
                        category=metadata.get('category'),
                        tags=metadata.get('tags'),
                        author=metadata.get('author'),
                        description=f"From ZIP: {file_path.name} / {f_path.name}",
                        ocr_engine=ocr_engine
                    )
                    
                    # Use the database-generated ID as child_doc_id
                    child_doc_id = child_doc.id
                    
                    # Create child task with real ID
                    task_manager.create_task(child_doc_id)
                    task_manager.add_child_task(doc_id, child_doc_id)
                    
                    # Store info for processing
                    pending_tasks.append({
                        'child_doc_id': child_doc_id,
                        'file_path': f_path,
                        'file_ext': f_ext,
                        'checksum': child_checksum,
                        'idx': idx
                    })
                    
                    # Initialize task status as pending
                    task_manager.update_task(
                        child_doc_id,
                        status=TaskStatus.PENDING,
                        message="Waiting in queue...",
                        progress_percentage=0,
                        filename=f_path.name
                    )

                # Phase 2: Process files sequentially
                for task_info in pending_tasks:
                    idx = task_info['idx']
                    f_path = task_info['file_path']
                    f_ext = task_info['file_ext']
                    child_doc_id = task_info['child_doc_id']
                    child_checksum = task_info['checksum']
                    
                    # Check for cancellation
                    if not task_manager.wait_if_paused(doc_id):
                        raise InterruptedError("Task was cancelled by user")
                    
                    # Update parent progress
                    parent_progress = 10 + (80 * (idx - 1) / len(found_files))
                    task_manager.update_task(
                        doc_id,
                        progress_percentage=int(parent_progress),
                        message=f"Processing file {idx}/{len(found_files)}: {f_path.name}",
                        processed_files=idx - 1
                    )
                    
                    # Process the file based on type
                    try:
                        if f_ext == '.pdf':
                            process_single_pdf(child_doc_id, f_path, metadata, ocr_engine, child_checksum, parent_task_id=doc_id, processing_mode=processing_mode)
                        elif f_ext == '.pptx':
                            process_single_pptx(child_doc_id, f_path, metadata, ocr_engine, child_checksum, parent_task_id=doc_id)
                        elif f_ext in ['.docx', '.doc', '.odt', '.txt', '.md']:
                            process_single_docx(child_doc_id, f_path, metadata, ocr_engine, child_checksum, parent_task_id=doc_id)
                        elif f_ext in ['.xlsx', '.xls', '.ods', '.odp', '.ppt']:
                            # Route ODS, ODP, PPT to Excel processor (Generic LibreOffice -> PDF -> VLM)
                            process_single_excel(child_doc_id, f_path, metadata, ocr_engine, child_checksum, parent_task_id=doc_id)
                        elif f_ext in ['.jpg', '.jpeg', '.png']:
                            process_single_image(child_doc_id, f_path, metadata, ocr_engine, child_checksum, parent_task_id=doc_id)
                        
                        # Child task status is already updated in the processing function
                        # No need to update again here
                        
                    except Exception as e:
                        logger.error("child_file_failed", error=str(e), child_id=child_doc_id, file=f_path.name)
                        task_manager.complete_task(child_doc_id, success=False, error_message=str(e))
                        db.update_document_status(child_doc_id, 'failed', error_message=str(e))
                    
                    # Update parent processed count
                    task_manager.update_task(
                        doc_id,
                        processed_files=idx
                    )
                
                # All files processed
                task_manager.complete_task(doc_id, success=True)
                db.update_document_status(doc_id, 'completed')
                logger.info("zip_processing_completed", doc_id=doc_id, total_files=len(found_files))
                return
                
            except zipfile.BadZipFile:
                raise ValueError("Invalid or corrupted ZIP file")
        
        elif file_ext == '.pdf':
            # Handle single PDF file
            process_single_pdf(doc_id, file_path, metadata, ocr_engine, checksum, processing_mode=processing_mode)
        
        elif file_ext == '.pptx':
            # Handle PPTX files
            process_single_pptx(doc_id, file_path, metadata, ocr_engine, checksum)
        
        elif file_ext in ['.docx', '.doc', '.odt', '.txt', '.md']:
            # Handle DOCX and Text/Markdown files
            process_single_docx(doc_id, file_path, metadata, ocr_engine, checksum)
        
        elif file_ext in ['.xlsx', '.xls', '.ods', '.odp', '.ppt']:
            # Handle Excel/ODS/ODP/PPT files (Generic PDF conversion)
            process_single_excel(doc_id, file_path, metadata, ocr_engine, checksum)
        
        elif file_ext in ['.jpg', '.jpeg', '.png']:
            # Handle image files
            process_single_image(doc_id, file_path, metadata, ocr_engine, checksum)
        
        else:
            raise ValueError(f"Unsupported file type: {file_ext}. Supported: PDF, ZIP, JPG, PNG, PPTX, DOCX, XLSX, ODT, ODS, ODP")
    
    except InterruptedError as e:
        # Task was cancelled by user
        logger.info("task_cancelled", doc_id=doc_id, message=str(e))
        task_manager.complete_task(doc_id, success=False, error_message="Task cancelled by user")
        db.update_document_status(doc_id, 'cancelled', error_message=str(e))
    
    except subprocess.TimeoutExpired as e:
        error_msg = f"Processing timed out after {e.timeout}s: {str(e)}"
        logger.error("❌ subprocess_timeout", error=str(e), doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=error_msg)
        db.update_document_status(doc_id, 'failed', error_message=error_msg)
    
    except subprocess.CalledProcessError as e:
        error_msg = f"OCR processing failed: {str(e)}"
        logger.error("❌ subprocess_failed", error=str(e), returncode=e.returncode, 
                    cmd=' '.join(e.cmd) if hasattr(e, 'cmd') else 'unknown',
                    stdout=e.stdout[:500] if hasattr(e, 'stdout') and e.stdout else '',
                    stderr=e.stderr[:500] if hasattr(e, 'stderr') and e.stderr else '',
                    doc_id=doc_id, ocr_engine=ocr_engine)
        task_manager.complete_task(doc_id, success=False, error_message=error_msg)
        db.update_document_status(doc_id, 'failed', error_message=error_msg)
    
    except (RuntimeError, ValueError) as e:
        error_msg = f"Processing failed: {str(e)}"
        logger.error("processing_failed", error=str(e), 
                    error_type=type(e).__name__, doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=error_msg)
        db.update_document_status(doc_id, 'failed', error_message=error_msg)
    
    except Exception as e:
        error_msg = str(e)
        logger.error("background_processing_failed", error=error_msg, doc_id=doc_id)
        task_manager.complete_task(doc_id, success=False, error_message=error_msg)
        db.update_document_status(doc_id, 'failed', error_message=error_msg)
    finally:
        # Clean up temporary extraction directory
        if 'temp_extract_dir' in locals() and temp_extract_dir and temp_extract_dir.exists():
            try:
                shutil.rmtree(temp_extract_dir)
                logger.info("cleaned_up_temp_extract_dir", dir=str(temp_extract_dir))
            except Exception as e:
                logger.warning("failed_to_cleanup_temp_dir", error=str(e), dir=str(temp_extract_dir))


def process_document_background(doc_id: int, file_path: Path, metadata: dict, ocr_engine: str, checksum: str, processing_mode: str = 'fast'):
    """
    Entry point for background processing.
    Now just enqueues the task for workers.
    """
    # Create task in task manager
    task_manager.create_task(doc_id)
    
    # Update status to Queued
    task_manager.update_task(
        doc_id,
        status=TaskStatus.PENDING,
        message="Waiting in queue for available worker...",
        progress_percentage=0
    )
    db.update_document_status(doc_id, 'queued', error_message=None)
    
    # Enqueue task
    task_queue.put((doc_id, file_path, metadata, ocr_engine, checksum, processing_mode))
    logger.info("task_enqueued", doc_id=doc_id, processing_mode=processing_mode, qsize=task_queue.qsize())
