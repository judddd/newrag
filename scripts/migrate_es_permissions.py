#!/usr/bin/env python3
"""
Migrate Elasticsearch index to add permission fields
Uses Reindex API to avoid data loss
"""

import os
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from elasticsearch import Elasticsearch
from src.config import config


def migrate_es_permissions():
    """Migrate ES index to add permission fields"""
    print("=" * 80)
    print("Elasticsearch Permission Fields Migration")
    print("=" * 80)
    
    # Get ES configuration
    es_config = config.es_config
    es_hosts = es_config.get('hosts', ['http://localhost:9200'])
    es_username = es_config.get('username', '')
    es_password = es_config.get('password', '')
    index_name = es_config.get('index_name', 'aiops_knowledge_base')
    
    print(f"\n📦 Elasticsearch: {es_hosts[0]}")
    print(f"📊 Index: {index_name}")
    
    # Initialize ES client
    es_client = Elasticsearch(
        es_hosts,
        basic_auth=(es_username, es_password) if es_username else None,
        timeout=30
    )
    
    # Check if index exists
    if not es_client.indices.exists(index=index_name):
        print(f"\n⚠️  Index '{index_name}' does not exist. Nothing to migrate.")
        return
    
    # Get current mapping
    print("\n🔍 Checking current mapping...")
    current_mapping = es_client.indices.get_mapping(index=index_name)
    current_properties = current_mapping[index_name]['mappings']['properties']
    
    # Check if permission fields already exist
    metadata_props = current_properties.get('metadata', {}).get('properties', {})
    if 'owner_id' in metadata_props:
        print("   ✓ Permission fields already exist. No migration needed.")
        return
    
    print("   ⚠️  Permission fields not found. Starting migration...")
    
    # Step 1: Create temporary index with new mapping
    temp_index = f"{index_name}_temp"
    print(f"\n📝 Creating temporary index: {temp_index}")
    
    # Load mapping template
    mapping_file = Path(__file__).parent.parent / "schemas" / "elasticsearch_mapping.json"
    with open(mapping_file, 'r') as f:
        new_mapping = json.load(f)
    
    # Add permission fields to metadata
    if 'metadata' not in new_mapping['mappings']['properties']:
        new_mapping['mappings']['properties']['metadata'] = {'properties': {}}
    
    new_mapping['mappings']['properties']['metadata']['properties'].update({
        'owner_id': {'type': 'keyword'},
        'org_id': {'type': 'keyword'},
        'visibility': {'type': 'keyword'},
        'shared_with_users': {'type': 'keyword'},
        'shared_with_roles': {'type': 'keyword'}
    })
    
    # Create temp index
    if es_client.indices.exists(index=temp_index):
        print(f"   ⚠️  Temporary index exists. Deleting...")
        es_client.indices.delete(index=temp_index)
    
    es_client.indices.create(index=temp_index, body=new_mapping)
    print("   ✓ Temporary index created")
    
    # Step 2: Reindex with script to add default permission fields
    print(f"\n🔄 Reindexing data from {index_name} to {temp_index}...")
    print("   Adding default permission fields (owner_id=1, org_id=1, visibility=public)...")
    
    reindex_body = {
        "source": {
            "index": index_name
        },
        "dest": {
            "index": temp_index
        },
        "script": {
            "source": """
                if (ctx._source.metadata == null) {
                    ctx._source.metadata = [:];
                }
                ctx._source.metadata.owner_id = "1";
                ctx._source.metadata.org_id = "1";
                ctx._source.metadata.visibility = "public";
                ctx._source.metadata.shared_with_users = [];
                ctx._source.metadata.shared_with_roles = [];
            """,
            "lang": "painless"
        }
    }
    
    response = es_client.reindex(body=reindex_body, wait_for_completion=True, request_timeout=300)
    
    if response.get('failures'):
        print(f"   ❌ Reindex failed with errors:")
        for failure in response['failures']:
            print(f"      {failure}")
        return
    
    total = response.get('total', 0)
    created = response.get('created', 0)
    print(f"   ✓ Reindexed {created}/{total} documents")
    
    # Step 3: Delete old index
    print(f"\n🗑️  Deleting old index: {index_name}")
    es_client.indices.delete(index=index_name)
    print("   ✓ Old index deleted")
    
    # Step 4: Create alias or rename temp index
    print(f"\n🔄 Renaming {temp_index} to {index_name}")
    
    # Create new index with original name
    es_client.indices.create(index=index_name, body=new_mapping)
    
    # Reindex from temp to original
    reindex_body = {
        "source": {"index": temp_index},
        "dest": {"index": index_name}
    }
    es_client.reindex(body=reindex_body, wait_for_completion=True, request_timeout=300)
    
    # Delete temp index
    es_client.indices.delete(index=temp_index)
    print("   ✓ Index renamed successfully")
    
    # Step 5: Verify migration
    print("\n✅ Verifying migration...")
    new_mapping = es_client.indices.get_mapping(index=index_name)
    new_properties = new_mapping[index_name]['mappings']['properties']
    metadata_props = new_properties.get('metadata', {}).get('properties', {})
    
    required_fields = ['owner_id', 'org_id', 'visibility', 'shared_with_users', 'shared_with_roles']
    missing_fields = [f for f in required_fields if f not in metadata_props]
    
    if missing_fields:
        print(f"   ⚠️  Missing fields: {missing_fields}")
    else:
        print("   ✓ All permission fields present")
    
    # Get document count
    count_response = es_client.count(index=index_name)
    doc_count = count_response['count']
    print(f"   ✓ Document count: {doc_count}")
    
    print("\n" + "=" * 80)
    print("✅ Elasticsearch migration completed successfully!")
    print("=" * 80)
    print("\n📝 Next steps:")
    print("   1. Verify search functionality")
    print("   2. Enable authentication in config.yaml")
    print("   3. Restart the application")
    print("\n")


if __name__ == "__main__":
    try:
        migrate_es_permissions()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)











