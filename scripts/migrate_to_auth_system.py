#!/usr/bin/env python3
"""
Migrate existing documents to authentication system

This script helps migrate from a non-authenticated deployment to the new
authentication and permission system. It will:

1. Check if authentication system is initialized
2. Find admin user (or create if missing)
3. Migrate all legacy documents (Document, DocumentMaster, DocumentVersion)
   to be owned by admin user
4. Update Elasticsearch metadata with permission information
5. Set appropriate visibility (public/organization/private)

Usage:
    # Dry run (preview changes)
    python scripts/migrate_to_auth_system.py --dry-run
    
    # Migrate with default settings (public visibility)
    python scripts/migrate_to_auth_system.py
    
    # Migrate with organization visibility
    python scripts/migrate_to_auth_system.py --visibility organization
    
    # Auto-confirm (no prompts)
    python scripts/migrate_to_auth_system.py --auto-confirm
"""

import os
import sys
from pathlib import Path
from typing import Optional, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import DatabaseManager, AuthManager, Document, DocumentMaster, DocumentVersion
from src.pipeline import ProcessingPipeline
from src.config import config
import structlog

logger = structlog.get_logger(__name__)


def get_or_create_admin_user(auth_manager: AuthManager, db: DatabaseManager) -> Tuple[int, int]:
    """
    Get existing admin user or create one if missing
    
    Returns:
        Tuple of (admin_user_id, default_org_id)
    """
    session = db.get_session()
    try:
        # Try to find existing admin user
        admin_user = auth_manager.get_user_by_username('admin')
        
        if admin_user:
            logger.info("found_existing_admin", user_id=admin_user.id, org_id=admin_user.org_id)
            return admin_user.id, admin_user.org_id
        
        # No admin user found - check if we have any superuser
        from src.database import User
        superuser = session.query(User).filter(User.is_superuser == True).first()
        
        if superuser:
            logger.info("found_existing_superuser", user_id=superuser.id, username=superuser.username)
            return superuser.id, superuser.org_id
        
        # No admin or superuser found - need to initialize auth system
        raise RuntimeError(
            "No admin user found. Please run 'python scripts/init_auth_system.py' first "
            "to initialize the authentication system."
        )
        
    finally:
        session.close()


def count_legacy_documents(db: DatabaseManager) -> dict:
    """Count documents without proper ownership"""
    session = db.get_session()
    try:
        counts = {
            'documents': 0,
            'document_masters': 0,
            'document_versions': 0
        }
        
        # Count legacy Document records
        counts['documents'] = session.query(Document).filter(
            Document.owner_id == None
        ).count()
        
        # Count legacy DocumentMaster records
        counts['document_masters'] = session.query(DocumentMaster).filter(
            DocumentMaster.owner_id == None
        ).count()
        
        # Count legacy DocumentVersion records
        counts['document_versions'] = session.query(DocumentVersion).filter(
            DocumentVersion.owner_id == None
        ).count()
        
        counts['total'] = sum(counts.values())
        return counts
        
    finally:
        session.close()


def migrate_documents(
    db: DatabaseManager,
    admin_user_id: int,
    default_org_id: int,
    visibility: str = 'public',
    dry_run: bool = False
) -> dict:
    """
    Migrate all legacy documents to be owned by admin user
    
    Args:
        db: Database manager
        admin_user_id: Admin user ID to assign ownership to
        default_org_id: Default organization ID
        visibility: Default visibility ('public', 'organization', 'private')
        dry_run: If True, only preview changes without applying
        
    Returns:
        Dictionary with migration statistics
    """
    session = db.get_session()
    stats = {
        'documents_updated': 0,
        'masters_updated': 0,
        'versions_updated': 0,
        'total_updated': 0
    }
    
    try:
        # 1. Migrate legacy Document records
        legacy_docs = session.query(Document).filter(
            Document.owner_id == None
        ).all()
        
        if legacy_docs:
            logger.info("migrating_documents", count=len(legacy_docs))
            for doc in legacy_docs:
                if not dry_run:
                    doc.owner_id = admin_user_id
                    doc.org_id = default_org_id
                    doc.visibility = visibility
                    
                logger.debug(
                    "updated_document",
                    doc_id=doc.id,
                    filename=doc.filename,
                    owner_id=admin_user_id,
                    org_id=default_org_id,
                    visibility=visibility
                )
                stats['documents_updated'] += 1
        
        # 2. Migrate DocumentMaster records
        legacy_masters = session.query(DocumentMaster).filter(
            DocumentMaster.owner_id == None
        ).all()
        
        if legacy_masters:
            logger.info("migrating_document_masters", count=len(legacy_masters))
            for master in legacy_masters:
                if not dry_run:
                    master.owner_id = admin_user_id
                    master.org_id = default_org_id
                    master.visibility = visibility
                    
                logger.debug(
                    "updated_document_master",
                    master_id=master.id,
                    filename=master.filename,
                    owner_id=admin_user_id,
                    org_id=default_org_id,
                    visibility=visibility
                )
                stats['masters_updated'] += 1
        
        # 3. Migrate DocumentVersion records
        legacy_versions = session.query(DocumentVersion).filter(
            DocumentVersion.owner_id == None
        ).all()
        
        if legacy_versions:
            logger.info("migrating_document_versions", count=len(legacy_versions))
            for version in legacy_versions:
                if not dry_run:
                    version.owner_id = admin_user_id
                    version.org_id = default_org_id
                    version.visibility = visibility
                    
                logger.debug(
                    "updated_document_version",
                    version_id=version.id,
                    master_id=version.document_master_id,
                    version_number=version.version_number,
                    owner_id=admin_user_id,
                    org_id=default_org_id,
                    visibility=visibility
                )
                stats['versions_updated'] += 1
        
        if not dry_run:
            session.commit()
            logger.info("migration_committed", stats=stats)
        else:
            logger.info("dry_run_complete", stats=stats)
        
        stats['total_updated'] = sum([
            stats['documents_updated'],
            stats['masters_updated'],
            stats['versions_updated']
        ])
        
        return stats
        
    except Exception as e:
        session.rollback()
        logger.error("migration_failed", error=str(e))
        raise
    finally:
        session.close()


def update_elasticsearch_metadata(
    db: DatabaseManager,
    admin_user_id: int,
    default_org_id: int,
    visibility: str,
    dry_run: bool = False
) -> int:
    """
    Update Elasticsearch documents with permission metadata
    
    Returns:
        Number of documents updated in ES
    """
    try:
        pipeline = ProcessingPipeline()
        es_client = pipeline.vector_store.es_client
        index_name = pipeline.vector_store.index_name
        
        if not es_client.indices.exists(index=index_name):
            logger.warning("es_index_not_found", index=index_name)
            return 0
        
        # Get all document IDs from database
        session = db.get_session()
        try:
            all_doc_ids = []
            
            # Get Document IDs
            docs = session.query(Document.id).all()
            all_doc_ids.extend([str(d.id) for d in docs])
            
            # Get DocumentVersion IDs
            versions = session.query(DocumentVersion.id).all()
            all_doc_ids.extend([str(v.id) for v in versions])
            
        finally:
            session.close()
        
        if not all_doc_ids:
            logger.info("no_documents_to_update_in_es")
            return 0
        
        logger.info("updating_es_metadata", document_count=len(all_doc_ids))
        
        if dry_run:
            logger.info("dry_run_es_update", would_update=len(all_doc_ids))
            return len(all_doc_ids)
        
        # Update each document's metadata in ES
        from elasticsearch.helpers import bulk
        
        def generate_updates():
            for doc_id in all_doc_ids:
                yield {
                    '_op_type': 'update',
                    '_index': index_name,
                    '_id': doc_id,
                    'script': {
                        'source': '''
                            ctx._source.metadata.owner_id = params.owner_id;
                            ctx._source.metadata.org_id = params.org_id;
                            ctx._source.metadata.visibility = params.visibility;
                        ''',
                        'params': {
                            'owner_id': admin_user_id,
                            'org_id': default_org_id,
                            'visibility': visibility
                        }
                    }
                }
        
        success_count, errors = bulk(
            es_client,
            generate_updates(),
            raise_on_error=False,
            raise_on_exception=False
        )
        
        if errors:
            logger.warning("es_update_had_errors", error_count=len(errors))
        
        logger.info("es_metadata_updated", success_count=success_count)
        return success_count
        
    except Exception as e:
        logger.error("es_update_failed", error=str(e))
        if not dry_run:
            raise
        return 0


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate existing documents to authentication system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview changes without applying
  python scripts/migrate_to_auth_system.py --dry-run
  
  # Migrate with public visibility (default)
  python scripts/migrate_to_auth_system.py
  
  # Migrate with organization visibility
  python scripts/migrate_to_auth_system.py --visibility organization
  
  # Auto-confirm without prompts
  python scripts/migrate_to_auth_system.py --auto-confirm
  
  # Skip Elasticsearch update (database only)
  python scripts/migrate_to_auth_system.py --skip-es
        """
    )
    
    parser.add_argument(
        '--visibility',
        default='public',
        choices=['public', 'organization', 'private'],
        help='Default visibility for migrated documents (default: public)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without applying them'
    )
    parser.add_argument(
        '--auto-confirm',
        action='store_true',
        help='Skip confirmation prompts'
    )
    parser.add_argument(
        '--skip-es',
        action='store_true',
        help='Skip Elasticsearch metadata update'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("🔄 Document Migration to Authentication System")
    print("=" * 80)
    
    # Initialize managers
    db = DatabaseManager()
    auth_manager = AuthManager(db.engine)
    
    # Step 1: Get or verify admin user
    print("\n📋 Step 1: Checking authentication system...")
    try:
        admin_user_id, default_org_id = get_or_create_admin_user(auth_manager, db)
        print(f"   ✓ Found admin user (ID: {admin_user_id})")
        print(f"   ✓ Default organization (ID: {default_org_id})")
    except RuntimeError as e:
        print(f"   ❌ {e}")
        sys.exit(1)
    
    # Step 2: Count legacy documents
    print("\n📊 Step 2: Analyzing legacy documents...")
    counts = count_legacy_documents(db)
    
    if counts['total'] == 0:
        print("   ✓ No legacy documents found. All documents already have ownership.")
        print("\n✅ Migration not needed - system is already up to date!")
        return
    
    print(f"   Found {counts['total']} legacy documents:")
    if counts['documents'] > 0:
        print(f"     - Document records: {counts['documents']}")
    if counts['document_masters'] > 0:
        print(f"     - DocumentMaster records: {counts['document_masters']}")
    if counts['document_versions'] > 0:
        print(f"     - DocumentVersion records: {counts['document_versions']}")
    
    # Step 3: Show migration plan
    print(f"\n📝 Step 3: Migration plan")
    print(f"   Owner: admin (ID: {admin_user_id})")
    print(f"   Organization: ID {default_org_id}")
    print(f"   Visibility: {args.visibility}")
    print(f"   Update Elasticsearch: {'No (skipped)' if args.skip_es else 'Yes'}")
    print(f"   Mode: {'DRY RUN (preview only)' if args.dry_run else 'APPLY CHANGES'}")
    
    # Step 4: Confirm
    if not args.auto_confirm and not args.dry_run:
        print("\n⚠️  This will modify your database and Elasticsearch index.")
        response = input("   Proceed with migration? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            print("\n❌ Migration cancelled.")
            return
    
    # Step 5: Migrate database
    print(f"\n🔄 Step 4: Migrating database records...")
    try:
        stats = migrate_documents(
            db=db,
            admin_user_id=admin_user_id,
            default_org_id=default_org_id,
            visibility=args.visibility,
            dry_run=args.dry_run
        )
        
        print(f"   ✓ Database migration complete:")
        if stats['documents_updated'] > 0:
            print(f"     - Document records: {stats['documents_updated']}")
        if stats['masters_updated'] > 0:
            print(f"     - DocumentMaster records: {stats['masters_updated']}")
        if stats['versions_updated'] > 0:
            print(f"     - DocumentVersion records: {stats['versions_updated']}")
        print(f"     - Total: {stats['total_updated']}")
        
    except Exception as e:
        print(f"   ❌ Database migration failed: {e}")
        sys.exit(1)
    
    # Step 6: Update Elasticsearch
    if not args.skip_es:
        print(f"\n🔍 Step 5: Updating Elasticsearch metadata...")
        try:
            es_count = update_elasticsearch_metadata(
                db=db,
                admin_user_id=admin_user_id,
                default_org_id=default_org_id,
                visibility=args.visibility,
                dry_run=args.dry_run
            )
            print(f"   ✓ Elasticsearch update complete: {es_count} documents")
        except Exception as e:
            print(f"   ⚠️  Elasticsearch update failed: {e}")
            print(f"   You can run 'python scripts/reindex_with_permissions.py' later")
    else:
        print(f"\n⏭️  Step 5: Skipped Elasticsearch update (--skip-es)")
    
    # Summary
    print("\n" + "=" * 80)
    if args.dry_run:
        print("✅ Dry run complete - no changes were made")
        print("\nTo apply these changes, run without --dry-run:")
        print("  python scripts/migrate_to_auth_system.py")
    else:
        print("✅ Migration complete!")
        print("\n📝 Next steps:")
        print("   1. Verify documents are visible in the UI")
        print("   2. Test search functionality")
        print("   3. Check organization management page for document counts")
        if args.skip_es:
            print("   4. Run: python scripts/reindex_with_permissions.py")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Migration cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)







