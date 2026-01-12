"""
Test search with organization filtering
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline import ProcessingPipeline
import structlog

logger = structlog.get_logger(__name__)


def test_search():
    """Test search with different permission scenarios"""
    pipeline = ProcessingPipeline()
    
    print("\n=== Test 1: Search without filters (superuser) ===")
    results = pipeline.search(
        query="数据",
        k=5,
        filters=None,
        use_hybrid=True
    )
    print(f"Found {len(results)} results")
    if results:
        print(f"First result: {results[0].get('metadata', {}).get('filename')}")
        print(f"org_id: {results[0].get('metadata', {}).get('org_id')}")
    
    print("\n=== Test 2: Search with org_id filter (superuser) ===")
    results = pipeline.search(
        query="数据",
        k=5,
        filters={
            'user_permissions': {
                'user_id': 1,
                'org_id': 1,
                'is_superuser': True
            }
        },
        use_hybrid=True
    )
    print(f"Found {len(results)} results")
    if results:
        print(f"First result: {results[0].get('metadata', {}).get('filename')}")
        print(f"org_id: {results[0].get('metadata', {}).get('org_id')}")
    
    print("\n=== Test 3: Search with wrong org_id (superuser) ===")
    results = pipeline.search(
        query="数据",
        k=5,
        filters={
            'user_permissions': {
                'user_id': 1,
                'org_id': 999,  # Non-existent org
                'is_superuser': True
            }
        },
        use_hybrid=True
    )
    print(f"Found {len(results)} results (should be 0)")
    
    print("\n=== Test 4: Empty query with org filter ===")
    results = pipeline.search(
        query="",
        k=5,
        filters={
            'user_permissions': {
                'user_id': 1,
                'org_id': 1,
                'is_superuser': True
            }
        },
        use_hybrid=True
    )
    print(f"Found {len(results)} results")
    if results:
        print(f"First result: {results[0].get('metadata', {}).get('filename')}")


if __name__ == "__main__":
    test_search()








