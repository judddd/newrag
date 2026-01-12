#!/usr/bin/env python3
"""
Migration script to convert existing documents to version control architecture.
This script migrates data from the old Document model to DocumentMaster + DocumentVersion.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import DatabaseManager, Document, DocumentMaster, DocumentVersion
from datetime import datetime
import uuid


def migrate_documents_to_version_control(db_path: str = "data/documents.db"):
    """
    Migrate existing documents to version control architecture.
    Each existing document becomes:
    - One DocumentMaster (with metadata)
    - One DocumentVersion (version 1, with file data)
    """
    print("=" * 80)
    print("Document Version Control Migration")
    print("=" * 80)
    
    db = DatabaseManager(db_path=db_path)
    
    with db.get_session() as session:
        # Get all existing documents
        documents = session.query(Document).all()
        total_docs = len(documents)
        
        if total_docs == 0:
            print("\n✓ No documents to migrate.")
            return
        
        print(f"\nFound {total_docs} documents to migrate.")
        print("-" * 80)
        
        migrated_count = 0
        skipped_count = 0
        error_count = 0
        
        for idx, doc in enumerate(documents, 1):
            try:
                print(f"\n[{idx}/{total_docs}] Migrating: {doc.filename}")
                
                # Check if already migrated (by checking if a DocumentMaster exists with same filename)
                existing_master = session.query(DocumentMaster).filter(
                    DocumentMaster.filename_base == doc.filename,
                    DocumentMaster.org_id == doc.org_id
                ).first()
                
                if existing_master:
                    print(f"  ⚠ Skipped: Already migrated (Master ID: {existing_master.id})")
                    skipped_count += 1
                    continue
                
                # Create DocumentMaster
                master = DocumentMaster(
                    document_group_id=str(uuid.uuid4()),
                    filename_base=doc.filename,
                    owner_id=doc.owner_id,
                    org_id=doc.org_id,
                    visibility=doc.visibility,
                    shared_with_users=doc.shared_with_users,
                    shared_with_roles=doc.shared_with_roles,
                    category=doc.category,
                    tags=doc.tags,
                    author=doc.author,
                    description=doc.description,
                    created_at=doc.uploaded_at or datetime.utcnow(),
                    updated_at=doc.processed_at or doc.uploaded_at or datetime.utcnow()
                )
                session.add(master)
                session.flush()  # Get master.id
                
                print(f"  ✓ Created DocumentMaster (ID: {master.id}, Group: {master.document_group_id})")
                
                # Create DocumentVersion (version 1)
                version = DocumentVersion(
                    document_master_id=master.id,
                    version=1,
                    file_path=doc.file_path,
                    file_type=doc.file_type,
                    file_size=doc.file_size,
                    checksum=doc.checksum,
                    status=doc.status,
                    num_chunks=doc.num_chunks,
                    error_message=doc.error_message,
                    progress_percentage=doc.progress_percentage,
                    progress_message=doc.progress_message,
                    total_pages=doc.total_pages,
                    processed_pages=doc.processed_pages,
                    es_document_ids=doc.es_document_ids,
                    ocr_engine=doc.ocr_engine,
                    pages_data=doc.pages_data,
                    version_note="Initial version (migrated from legacy system)",
                    uploaded_by_id=doc.owner_id,
                    uploaded_at=doc.uploaded_at or datetime.utcnow(),
                    processed_at=doc.processed_at,
                    is_active=True
                )
                session.add(version)
                session.flush()  # Get version.id
                
                print(f"  ✓ Created DocumentVersion (ID: {version.id}, Version: 1)")
                
                # Update master's latest_version_id
                master.latest_version_id = version.id
                
                print(f"  ✓ Linked latest version to master")
                
                # Commit this document's migration
                session.commit()
                migrated_count += 1
                
                print(f"  ✓ Migration completed successfully")
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                session.rollback()
                error_count += 1
                continue
    
    # Print summary
    print("\n" + "=" * 80)
    print("Migration Summary")
    print("=" * 80)
    print(f"Total documents:     {total_docs}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped (already migrated): {skipped_count}")
    print(f"Errors:              {error_count}")
    print("=" * 80)
    
    if error_count > 0:
        print("\n⚠ Some documents failed to migrate. Please check the errors above.")
        return False
    elif migrated_count > 0:
        print("\n✓ All documents migrated successfully!")
        print("\nNote: The old 'documents' table is still intact for rollback purposes.")
        print("You can safely delete it after verifying the new system works correctly.")
        return True
    else:
        print("\n✓ Nothing to migrate (all documents already migrated or no documents found).")
        return True


def verify_migration(db_path: str = "data/documents.db"):
    """Verify the migration completed correctly"""
    print("\n" + "=" * 80)
    print("Verification")
    print("=" * 80)
    
    db = DatabaseManager(db_path=db_path)
    
    with db.get_session() as session:
        old_doc_count = session.query(Document).count()
        master_count = session.query(DocumentMaster).count()
        version_count = session.query(DocumentVersion).count()
        
        print(f"\nOld documents table:    {old_doc_count} records")
        print(f"DocumentMasters table:  {master_count} records")
        print(f"DocumentVersions table: {version_count} records")
        
        # Check integrity
        masters_without_latest = session.query(DocumentMaster).filter(
            DocumentMaster.latest_version_id == None
        ).count()
        
        versions_without_master = session.query(DocumentVersion).filter(
            ~DocumentVersion.document_master_id.in_(
                session.query(DocumentMaster.id)
            )
        ).count()
        
        print(f"\nIntegrity checks:")
        print(f"  Masters without latest version: {masters_without_latest}")
        print(f"  Versions without master: {versions_without_master}")
        
        if masters_without_latest > 0 or versions_without_master > 0:
            print("\n⚠ Data integrity issues detected!")
            return False
        else:
            print("\n✓ Data integrity verified!")
            return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate documents to version control")
    parser.add_argument(
        '--db-path',
        default='data/documents.db',
        help='Path to database file (default: data/documents.db)'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify migration without performing it'
    )
    
    args = parser.parse_args()
    
    if args.verify_only:
        success = verify_migration(args.db_path)
    else:
        success = migrate_documents_to_version_control(args.db_path)
        if success:
            verify_migration(args.db_path)
    
    sys.exit(0 if success else 1)








