#!/usr/bin/env python3
"""
Initialize authentication system
- Create auth tables
- Initialize default organization, roles, and permissions
- Create admin user
- Migrate existing documents
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
import bcrypt as bcrypt_lib
from sqlalchemy import inspect, text

from src.database import Base, DatabaseManager, AuthManager, TokenManager
from src.config import config


def check_table_exists(engine, table_name: str) -> bool:
    """Check if table exists in database"""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def init_auth_system():
    """Initialize authentication system"""
    print("=" * 80)
    print("NewRAG Authentication System Initialization")
    print("=" * 80)
    
    # Get database configuration
    db_config = config.database_config
    db_url = os.getenv('DATABASE_URL') or db_config.get('url', 'sqlite:///data/documents.db')
    
    print(f"\n📦 Database: {db_url}")
    
    # Initialize database managers
    db = DatabaseManager(db_url=db_url)
    auth_manager = AuthManager(db.engine)
    token_manager = TokenManager(db.engine)
    
    # Tables are automatically created by DatabaseManager
    print("\n✅ Database tables ready...")
    
    # Check if auth data already exists
    with db.engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM users"))
        user_count = result.scalar()
        
        if user_count > 0:
            print(f"   ⚠️  Found {user_count} existing users. Skipping initialization.")
            print("   If you want to reinitialize, please delete the database file first.")
            return
    
    print("   ✓ Tables created successfully")
    
    # Step 1: Create default organization
    print("\n📊 Creating default organization...")
    org = auth_manager.create_organization(
        name="Default Organization",
        description="Default organization for all users"
    )
    print(f"   ✓ Organization created: {org.name} (ID: {org.id})")
    
    # Step 2: Create default permissions
    print("\n🔐 Creating default permissions...")
    permissions_data = [
        ("document:read", "document", "read", "Read documents"),
        ("document:write", "document", "write", "Create and upload documents"),
        ("document:delete", "document", "delete", "Delete documents"),
        ("search:read", "search", "read", "Search knowledge base"),
        ("stats:read", "stats", "read", "View statistics"),
        ("admin:all", "admin", "all", "Full administrative access"),
    ]
    
    permissions = {}
    for code, resource, action, description in permissions_data:
        perm = auth_manager.create_permission(code, resource, action, description)
        permissions[code] = perm
        print(f"   ✓ {code}")
    
    # Step 3: Create default roles
    print("\n👥 Creating default roles...")
    
    # Admin role
    admin_role = auth_manager.create_role(
        name="Administrator",
        code="admin",
        description="Full system access",
        is_system=True
    )
    for perm in permissions.values():
        auth_manager.assign_permission_to_role(admin_role.id, perm.id)
    print(f"   ✓ {admin_role.name} (all permissions)")
    
    # Editor role
    editor_role = auth_manager.create_role(
        name="Editor",
        code="editor",
        description="Can create, edit, and delete documents",
        is_system=True
    )
    for code in ["document:read", "document:write", "document:delete", "search:read", "stats:read"]:
        auth_manager.assign_permission_to_role(editor_role.id, permissions[code].id)
    print(f"   ✓ {editor_role.name} (document:*, search:read, stats:read)")
    
    # Viewer role
    viewer_role = auth_manager.create_role(
        name="Viewer",
        code="viewer",
        description="Read-only access",
        is_system=True
    )
    for code in ["search:read", "stats:read"]:
        auth_manager.assign_permission_to_role(viewer_role.id, permissions[code].id)
    print(f"   ✓ {viewer_role.name} (search:read, stats:read)")
    
    # Step 4: Create admin user
    print("\n👤 Creating admin user...")
    admin_username = os.getenv('ADMIN_USERNAME', 'admin')
    admin_email = os.getenv('ADMIN_EMAIL', 'admin@example.com')
    admin_password = os.getenv('ADMIN_PASSWORD', 'Admin123!@#')
    
    # Hash password (bcrypt has 72 byte limit)
    password_bytes = admin_password.encode('utf-8')[:72]
    salt = bcrypt_lib.gensalt()
    password_hash = bcrypt_lib.hashpw(password_bytes, salt).decode('utf-8')
    
    admin_user = auth_manager.create_user(
        username=admin_username,
        email=admin_email,
        password_hash=password_hash,
        org_id=org.id,
        is_superuser=True
    )
    
    # Assign admin role
    auth_manager.assign_role_to_user(admin_user.id, admin_role.id)
    
    print(f"   ✓ Admin user created:")
    print(f"     Username: {admin_username}")
    print(f"     Email: {admin_email}")
    print(f"     Password: {admin_password}")
    print(f"     ⚠️  IMPORTANT: Change the password after first login!")
    
    # Step 5: Migrate existing documents
    print("\n📄 Migrating existing documents...")
    
    # Check if documents table has owner_id column
    inspector = inspect(db.engine)
    columns = [col['name'] for col in inspector.get_columns('documents')]
    
    if 'owner_id' not in columns:
        print("   ⚠️  Adding owner_id, org_id, visibility columns to documents table...")
        with db.engine.connect() as conn:
            # SQLite doesn't support ADD COLUMN with NOT NULL, so we add as nullable first
            conn.execute(text("ALTER TABLE documents ADD COLUMN owner_id INTEGER"))
            conn.execute(text("ALTER TABLE documents ADD COLUMN org_id INTEGER"))
            conn.execute(text("ALTER TABLE documents ADD COLUMN visibility VARCHAR(20) DEFAULT 'private'"))
            conn.execute(text("ALTER TABLE documents ADD COLUMN shared_with_users TEXT DEFAULT '[]'"))
            conn.execute(text("ALTER TABLE documents ADD COLUMN shared_with_roles TEXT DEFAULT '[]'"))
            conn.commit()
        print("   ✓ Columns added")
    
    # Update existing documents to assign to admin user
    with db.engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM documents WHERE owner_id IS NULL"))
        count = result.scalar()
        
        if count > 0:
            conn.execute(text(f"""
                UPDATE documents 
                SET owner_id = {admin_user.id},
                    org_id = {org.id},
                    visibility = 'public'
                WHERE owner_id IS NULL
            """))
            conn.commit()
            print(f"   ✓ Migrated {count} existing documents to admin user")
        else:
            print("   ✓ No existing documents to migrate")
    
    print("\n" + "=" * 80)
    print("✅ Authentication system initialized successfully!")
    print("=" * 80)
    print("\n📝 Next steps:")
    print("   1. Update config.yaml: set security.auth.enabled to true")
    print("   2. Run ES migration script: python scripts/migrate_es_permissions.py")
    print("   3. Restart the application")
    print("   4. Login with admin credentials and change password")
    print("\n")


if __name__ == "__main__":
    try:
        init_auth_system()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

