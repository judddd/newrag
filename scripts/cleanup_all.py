import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from src.database import DatabaseManager, Document, DocumentMaster, DocumentVersion
from src.vector_store import VectorStore
from src.minio_storage import MinIOStorage
from src.task_manager import task_manager

def cleanup():
    print("🧹 Cleaning up system...")
    db = DatabaseManager()
    
    # 1. Clean DB Documents
    session = db.get_session()
    try:
        # Delete all documents
        print("Deleting DocumentVersion...")
        session.query(DocumentVersion).delete()
        print("Deleting DocumentMaster...")
        session.query(DocumentMaster).delete()
        print("Deleting Document (Legacy)...")
        session.query(Document).delete()
        session.commit()
        print("✅ Database documents cleared")
    except Exception as e:
        print(f"❌ Database cleanup failed: {e}")
        session.rollback()
    finally:
        session.close()

    # 2. Clean ES
    try:
        vs = VectorStore()
        if vs.client.indices.exists(index=vs.index_name):
            vs.client.delete_by_query(
                index=vs.index_name,
                body={"query": {"match_all": {}}},
                wait_for_completion=True
            )
            print("✅ Elasticsearch index cleared")
        else:
            print("ℹ️ Elasticsearch index does not exist")
    except Exception as e:
        print(f"❌ ES cleanup failed: {e}")

    # 3. Clean MinIO
    try:
        ms = MinIOStorage()
        # Check if bucket exists
        if ms.client.bucket_exists(ms.bucket_name):
            # List and delete all objects in bucket
            objects = ms.client.list_objects(ms.bucket_name, recursive=True)
            for obj in objects:
                ms.client.remove_object(ms.bucket_name, obj.object_name)
            print("✅ MinIO bucket cleared")
    except Exception as e:
        print(f"❌ MinIO cleanup failed: {e}")

    # 4. Clear local processed folder
    processed_path = Path("web/static/processed_docs")
    if processed_path.exists():
        import shutil
        for item in processed_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print("✅ Local processed files cleared")

    print("🎉 Cleanup complete!")

if __name__ == "__main__":
    cleanup()

