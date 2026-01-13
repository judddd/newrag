"""SQLite database for document tracking and authentication"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import threading
import json
import uuid

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Text, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship, joinedload

Base = declarative_base()


# Association tables for many-to-many relationships
user_roles = Table(
    'user_roles',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete='CASCADE'), primary_key=True)
)

role_permissions = Table(
    'role_permissions',
    Base.metadata,
    Column('role_id', Integer, ForeignKey('roles.id', ondelete='CASCADE'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.id', ondelete='CASCADE'), primary_key=True)
)


class Organization(Base):
    """Organization/Tenant model for multi-tenancy support"""
    __tablename__ = 'organizations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    users = relationship('User', back_populates='organization', cascade='all, delete-orphan')
    documents = relationship('Document', back_populates='organization')
    document_masters = relationship('DocumentMaster', back_populates='organization')


class User(Base):
    """User model for authentication"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(200), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    
    # Organization relationship
    org_id = Column(Integer, ForeignKey('organizations.id', ondelete='SET NULL'))
    organization = relationship('Organization', back_populates='users')
    
    # User status
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)
    
    # Relationships
    roles = relationship('Role', secondary=user_roles, back_populates='users')
    documents = relationship('Document', back_populates='owner')
    document_masters = relationship('DocumentMaster', foreign_keys='DocumentMaster.owner_id', back_populates='owner')
    document_versions_uploaded = relationship('DocumentVersion', foreign_keys='DocumentVersion.uploaded_by_id', back_populates='uploaded_by')
    mcp_tokens = relationship('McpToken', back_populates='user', cascade='all, delete-orphan')
    refresh_tokens = relationship('RefreshToken', back_populates='user', cascade='all, delete-orphan')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'org_id': self.org_id,
            'is_active': self.is_active,
            'is_superuser': self.is_superuser,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'roles': [{'id': role.id, 'code': role.code, 'name': role.name} for role in self.roles]
        }


class Role(Base):
    """Role model for RBAC"""
    __tablename__ = 'roles'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    
    # System role or organization-specific
    org_id = Column(Integer, ForeignKey('organizations.id', ondelete='CASCADE'))
    is_system = Column(Boolean, default=False)  # System roles apply globally
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    users = relationship('User', secondary=user_roles, back_populates='roles')
    permissions = relationship('Permission', secondary=role_permissions, back_populates='roles')


class Permission(Base):
    """Permission model for RBAC"""
    __tablename__ = 'permissions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(100), unique=True, nullable=False, index=True)  # e.g., "document:write"
    resource = Column(String(50), nullable=False)  # e.g., "document"
    action = Column(String(50), nullable=False)  # e.g., "write"
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    roles = relationship('Role', secondary=role_permissions, back_populates='permissions')


class McpToken(Base):
    """MCP long-lived token model"""
    __tablename__ = 'mcp_tokens'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    token_id = Column(String(100), unique=True, nullable=False, index=True)  # UUID
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(200), nullable=False)  # e.g., "Cursor Desktop"
    token = Column(Text)  # Stored full token string for display
    
    expires_at = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)
    last_used_at = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship('User', back_populates='mcp_tokens')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'token_id': self.token_id,
            'name': self.name,
            'token': self.token,  # Include token in dict
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_active': self.is_active,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


class RefreshToken(Base):
    """Refresh token model for JWT authentication"""
    __tablename__ = 'refresh_tokens'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    token_id = Column(String(100), unique=True, nullable=False, index=True)  # UUID
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    expires_at = Column(DateTime, nullable=False)
    is_revoked = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship('User', back_populates='refresh_tokens')


class Document(Base):
    """Document model for tracking uploaded documents"""
    __tablename__ = 'documents'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(500), nullable=False)
    file_path = Column(String(1000))
    file_type = Column(String(50))
    file_size = Column(Integer)
    checksum = Column(String(64), unique=True)  # Legacy: will be migrated to version control
    
    # Ownership and permissions
    owner_id = Column(Integer, ForeignKey('users.id', ondelete='SET NULL'))
    org_id = Column(Integer, ForeignKey('organizations.id', ondelete='SET NULL'))
    visibility = Column(String(20), default='private')  # private, org, public
    shared_with_users = Column(Text, default='[]')  # JSON array: [1, 2, 3]
    shared_with_roles = Column(Text, default='[]')  # JSON array: ["analyst", "editor"]
    
    # Metadata
    category = Column(String(100))
    tags = Column(Text)  # Comma-separated
    author = Column(String(200))
    description = Column(Text)
    
    # Processing status
    status = Column(String(50), default='pending')  # pending, queued, processing, completed, failed
    num_chunks = Column(Integer, default=0)
    error_message = Column(Text)
    
    # Progress tracking
    progress_percentage = Column(Integer, default=0)  # 0-100
    progress_message = Column(String(500))  # Current step description
    total_pages = Column(Integer, default=0)
    processed_pages = Column(Integer, default=0)
    
    # ES info
    es_document_ids = Column(Text)  # JSON string of document IDs
    
    # OCR Processing info
    ocr_engine = Column(String(20))  # easy, paddle, vision
    pages_data = Column(Text)  # JSON string of pages info (image paths, ocr data, etc.)
    
    # Timestamps
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)
    
    # Relationships
    owner = relationship('User', back_populates='documents')
    organization = relationship('Organization', back_populates='documents')
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'filename': self.filename,
            'file_type': self.file_type,
            'file_size': self.file_size,
            'category': self.category,
            'tags': self.tags.split(',') if self.tags else [],
            'author': self.author,
            'status': self.status,
            'num_chunks': self.num_chunks,
            'error_message': self.error_message,
            'ocr_engine': self.ocr_engine,
            'pages_data': json.loads(self.pages_data) if self.pages_data else None,
            # Frontend expects created_at and updated_at
            'created_at': self.uploaded_at.isoformat() if self.uploaded_at else None,
            'updated_at': self.processed_at.isoformat() if self.processed_at else None,
            'progress_percentage': self.progress_percentage or 0,
            'progress_message': self.progress_message or '',
            'total_pages': self.total_pages or 0,
            'processed_pages': self.processed_pages or 0,
            # Permission fields
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'visibility': self.visibility,
            'shared_with_users': json.loads(self.shared_with_users) if self.shared_with_users else [],
            'shared_with_roles': json.loads(self.shared_with_roles) if self.shared_with_roles else []
        }


class DocumentMaster(Base):
    """Document Master model - represents a logical document with multiple versions"""
    __tablename__ = 'document_masters'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    document_group_id = Column(String(36), unique=True, nullable=False, index=True, default=lambda: str(uuid.uuid4()))
    filename_base = Column(String(500), nullable=False)
    
    # Ownership and permissions (inherited by all versions)
    owner_id = Column(Integer, ForeignKey('users.id', ondelete='SET NULL'))
    org_id = Column(Integer, ForeignKey('organizations.id', ondelete='SET NULL'))
    visibility = Column(String(20), default='private')  # private, organization, public
    shared_with_users = Column(Text, default='[]')  # JSON array: [1, 2, 3]
    shared_with_roles = Column(Text, default='[]')  # JSON array: ["analyst", "editor"]
    
    # Metadata (inherited by all versions but can be updated)
    category = Column(String(100))
    tags = Column(Text)  # Comma-separated
    author = Column(String(200))
    description = Column(Text)
    
    # Version tracking
    latest_version_id = Column(Integer, ForeignKey('document_versions.id', ondelete='SET NULL', use_alter=True))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship('User', foreign_keys=[owner_id])
    organization = relationship('Organization', foreign_keys=[org_id])
    versions = relationship('DocumentVersion', back_populates='master', foreign_keys='DocumentVersion.document_master_id')
    latest_version = relationship('DocumentVersion', foreign_keys=[latest_version_id], post_update=True)
    
    def to_dict(self, include_versions=False):
        """Convert to dictionary"""
        result = {
            'id': self.id,
            'document_group_id': self.document_group_id,
            'filename_base': self.filename_base,
            'category': self.category,
            'tags': self.tags.split(',') if self.tags else [],
            'author': self.author,
            'description': self.description,
            'owner_id': self.owner_id,
            'org_id': self.org_id,
            'visibility': self.visibility,
            'shared_with_users': json.loads(self.shared_with_users) if self.shared_with_users else [],
            'shared_with_roles': json.loads(self.shared_with_roles) if self.shared_with_roles else [],
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'latest_version_id': self.latest_version_id,
        }
        
        if include_versions and self.versions:
            result['versions'] = [v.to_dict() for v in self.versions]
        
        if self.latest_version:
            result['version'] = self.latest_version.version
            result['version_count'] = len(self.versions) if self.versions else 0
        
        return result


class DocumentVersion(Base):
    """Document Version model - represents a specific version of a document"""
    __tablename__ = 'document_versions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    document_master_id = Column(Integer, ForeignKey('document_masters.id', ondelete='CASCADE'), nullable=False)
    version = Column(Integer, nullable=False)  # 1, 2, 3, ...
    
    # Physical file information
    file_path = Column(String(1000))
    file_type = Column(String(50))
    file_size = Column(Integer)
    checksum = Column(String(64), nullable=False)  # No UNIQUE constraint - same file can exist in different masters
    
    # Processing status
    status = Column(String(50), default='pending')  # pending, queued, processing, completed, failed
    num_chunks = Column(Integer, default=0)
    error_message = Column(Text)
    
    # Progress tracking
    progress_percentage = Column(Integer, default=0)  # 0-100
    progress_message = Column(String(500))
    total_pages = Column(Integer, default=0)
    processed_pages = Column(Integer, default=0)
    
    # ES info
    es_document_ids = Column(Text)  # JSON string of document IDs
    
    # OCR Processing info
    ocr_engine = Column(String(20))  # easy, paddle, vision
    pages_data = Column(Text)  # JSON string of pages info (image paths, ocr data, etc.)
    
    # Version metadata
    version_note = Column(Text)  # User-provided note about this version
    uploaded_by_id = Column(Integer, ForeignKey('users.id', ondelete='SET NULL'))
    
    # Timestamps
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)
    
    # Soft delete
    is_active = Column(Boolean, default=True)
    
    # Relationships
    master = relationship('DocumentMaster', back_populates='versions', foreign_keys=[document_master_id])
    uploaded_by = relationship('User', foreign_keys=[uploaded_by_id])
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'document_master_id': self.document_master_id,
            'version': self.version,
            'file_path': self.file_path,
            'file_type': self.file_type,
            'file_size': self.file_size,
            'checksum': self.checksum,
            'status': self.status,
            'num_chunks': self.num_chunks,
            'error_message': self.error_message,
            'progress_percentage': self.progress_percentage or 0,
            'progress_message': self.progress_message or '',
            'total_pages': self.total_pages or 0,
            'processed_pages': self.processed_pages or 0,
            'ocr_engine': self.ocr_engine,
            'pages_data': json.loads(self.pages_data) if self.pages_data else None,
            'es_document_ids': self.es_document_ids,
            'version_note': self.version_note,
            'uploaded_by_id': self.uploaded_by_id,
            'uploaded_at': self.uploaded_at.isoformat() if self.uploaded_at else None,
            'processed_at': self.processed_at.isoformat() if self.processed_at else None,
            'is_active': self.is_active,
        }
    
    def to_combined_dict(self, master: 'DocumentMaster' = None):
        """Convert to dictionary with master metadata for API compatibility"""
        if not master:
            master = self.master
        
        return {
            # Version-specific fields
            'id': self.id,
            'filename': master.filename_base if master else '',
            'file_path': self.file_path,
            'file_type': self.file_type,
            'file_size': self.file_size,
            'checksum': self.checksum,
            'status': self.status,
            'num_chunks': self.num_chunks,
            'error_message': self.error_message,
            'progress_percentage': self.progress_percentage or 0,
            'progress_message': self.progress_message or '',
            'total_pages': self.total_pages or 0,
            'processed_pages': self.processed_pages or 0,
            'ocr_engine': self.ocr_engine,
            'pages_data': json.loads(self.pages_data) if self.pages_data else None,
            
            # Master metadata
            'document_group_id': master.document_group_id if master else None,
            'version': self.version,
            'is_latest': (master.latest_version_id == self.id) if master else False,
            'version_count': len(master.versions) if master and master.versions else 1,
            'category': master.category if master else None,
            'tags': master.tags.split(',') if (master and master.tags) else [],
            'author': master.author if master else None,
            'description': master.description if master else None,
            'owner_id': master.owner_id if master else None,
            'org_id': master.org_id if master else None,
            'visibility': master.visibility if master else 'private',
            'shared_with_users': json.loads(master.shared_with_users) if (master and master.shared_with_users) else [],
            'shared_with_roles': json.loads(master.shared_with_roles) if (master and master.shared_with_roles) else [],
            
            # Timestamps
            'created_at': self.uploaded_at.isoformat() if self.uploaded_at else None,
            'updated_at': self.processed_at.isoformat() if self.processed_at else None,
        }


class DatabaseManager:
    """Database manager for SQLite"""
    
    _db_lock = threading.Lock()  # Global lock for SQLite write operations
    
    def __init__(self, db_path: str = "data/documents.db", db_url: Optional[str] = None):
        """
        Initialize database
        
        Args:
            db_path: Path to SQLite database file (used if db_url not provided)
            db_url: Full database URL (e.g., postgresql://user:pass@localhost/dbname)
        """
        if db_url:
            # Use provided database URL (PostgreSQL, MySQL, etc.)
            self.engine = create_engine(
                db_url,
                echo=False,
                pool_size=20,
                pool_recycle=3600
            )
        else:
            # Use SQLite
            db_file = Path(db_path)
            db_file.parent.mkdir(parents=True, exist_ok=True)
            self.engine = create_engine(
                f'sqlite:///{db_path}',
                connect_args={'check_same_thread': False},
                echo=False
            )
        
        # Create all tables
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def get_session(self) -> Session:
        """Get database session"""
        return self.SessionLocal()
    
    def apply_permission_filter(self, query, user_id: Optional[int] = None, 
                                org_id: Optional[int] = None, 
                                is_superuser: bool = False,
                                model_class=None):
        """
        Apply permission filter to document query
        
        Permission logic:
        - Superuser with org filter: can see all documents from that org
        - Superuser without org filter: can see all documents
        - Regular user: can see:
          1. Public documents
          2. Organization documents (if same org_id)
          3. Documents they own
          4. Documents explicitly shared with them
        """
        # Default to Document model if not provided
        if model_class is None:
            model_class = Document
        
        # Superuser logic
        if is_superuser:
            if org_id is not None:
                # Superuser with org filter: only show docs from that org
                return query.filter(model_class.org_id == org_id)
            else:
                # Superuser without org filter: sees everything
                return query
        
        if user_id is None:
            # Anonymous users only see public documents
            return query.filter(model_class.visibility == 'public')
        
        # Build permission conditions
        from sqlalchemy import or_
        
        conditions = [
            model_class.visibility == 'public',  # Public documents
            model_class.owner_id == user_id,  # Documents owned by user
        ]
        
        # Add organization filter if user belongs to an organization
        if org_id:
            from sqlalchemy import and_
            conditions.append(
                and_(model_class.visibility == 'organization', model_class.org_id == org_id)
            )
            # Also support legacy 'org' value
            conditions.append(
                and_(model_class.visibility == 'org', model_class.org_id == org_id)
            )
        
        # Note: For shared documents, we'll filter in Python after query
        # because SQLite JSON array matching is complex and error-prone
        # The SQL LIKE approach can cause false positives (e.g., user_id=2 matching "12")
        
        return query.filter(or_(*conditions))
    
    def check_document_permission(self, doc_id: int, user_id: Optional[int] = None,
                                 org_id: Optional[int] = None, 
                                 is_superuser: bool = False,
                                 required_action: str = 'read') -> bool:
        """
        Check if user has permission to access a specific document
        
        Args:
            doc_id: Document ID
            user_id: User ID
            org_id: Organization ID
            is_superuser: Is user a superuser
            required_action: 'read', 'write', or 'delete'
        
        Returns:
            True if user has permission, False otherwise
        """
        if is_superuser:
            return True
        
        session = self.get_session()
        try:
            doc = session.query(Document).filter(Document.id == doc_id).first()
            if not doc:
                return False
            
            # Check ownership for write/delete actions
            if required_action in ['write', 'delete']:
                return doc.owner_id == user_id or is_superuser
            
            # Check read permission
            if doc.visibility == 'public':
                return True
            
            if doc.owner_id == user_id:
                return True
            
            if doc.visibility == 'org' and doc.org_id == org_id:
                return True
            
            # Check if shared with user
            if doc.shared_with_users:
                shared_users = json.loads(doc.shared_with_users)
                if user_id in shared_users:
                    return True
            
            return False
            
        finally:
            session.close()
    
    def create_document(
        self,
        filename: str,
        file_path: str,
        file_type: str,
        file_size: int,
        checksum: str,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        author: Optional[str] = None,
        description: Optional[str] = None,
        ocr_engine: Optional[str] = None,
        owner_id: Optional[int] = None,
        org_id: Optional[int] = None,
        visibility: str = 'private'
    ) -> Document:
        """Create new document record"""
        with self._db_lock:
            session = self.get_session()
            try:
                doc = Document(
                    filename=filename,
                    file_path=file_path,
                    file_type=file_type,
                    file_size=file_size,
                    checksum=checksum,
                    category=category,
                    tags=','.join(tags) if tags else '',
                    author=author,
                    description=description,
                    ocr_engine=ocr_engine,
                    owner_id=owner_id,
                    org_id=org_id,
                    visibility=visibility,
                    status='pending'
                )
                session.add(doc)
                session.commit()
                session.refresh(doc)
                return doc
            finally:
                session.close()
    
    def update_document_status(
        self,
        doc_id: int,
        status: str,
        num_chunks: Optional[int] = None,
        es_document_ids: Optional[str] = None,
        error_message: Optional[str] = None,
        pages_data: Optional[str] = None
    ):
        """Update document processing status - auto-detects version control vs legacy"""
        with self._db_lock:
            session = self.get_session()
            try:
                # Try version control first
                doc_version = session.query(DocumentVersion).filter(DocumentVersion.id == doc_id).first()
                if doc_version:
                    # Update version
                    doc_version.status = status
                    if num_chunks is not None:
                        doc_version.num_chunks = num_chunks
                    if es_document_ids:
                        doc_version.es_document_ids = es_document_ids
                    if error_message:
                        doc_version.error_message = error_message
                    if pages_data:
                        doc_version.pages_data = pages_data
                    if status == 'completed':
                        doc_version.processed_at = datetime.utcnow()
                        doc_version.progress_percentage = 100
                    session.commit()
                else:
                    # Fallback to legacy Document table
                    doc = session.query(Document).filter(Document.id == doc_id).first()
                    if doc:
                        doc.status = status
                        if num_chunks is not None:
                            doc.num_chunks = num_chunks
                        if es_document_ids:
                            doc.es_document_ids = es_document_ids
                        if error_message:
                            doc.error_message = error_message
                        if pages_data:
                            doc.pages_data = pages_data
                        if status == 'completed':
                            doc.processed_at = datetime.utcnow()
                            doc.progress_percentage = 100
                        session.commit()
            finally:
                session.close()
    
    def update_document_progress(
        self,
        doc_id: int,
        progress_percentage: int,
        progress_message: str,
        processed_pages: Optional[int] = None,
        total_pages: Optional[int] = None
    ):
        """Update document processing progress - auto-detects version control vs legacy"""
        with self._db_lock:
            session = self.get_session()
            try:
                # Try version control first
                doc_version = session.query(DocumentVersion).filter(DocumentVersion.id == doc_id).first()
                if doc_version:
                    doc_version.progress_percentage = min(100, max(0, progress_percentage))
                    doc_version.progress_message = progress_message
                    if processed_pages is not None:
                        doc_version.processed_pages = processed_pages
                    if total_pages is not None:
                        doc_version.total_pages = total_pages
                    session.commit()
                else:
                    # Fallback to legacy Document table
                    doc = session.query(Document).filter(Document.id == doc_id).first()
                    if doc:
                        doc.progress_percentage = min(100, max(0, progress_percentage))
                        doc.progress_message = progress_message
                        if processed_pages is not None:
                            doc.processed_pages = processed_pages
                        if total_pages is not None:
                            doc.total_pages = total_pages
                        session.commit()
            finally:
                session.close()
    
    def update_document_pages_data(self, doc_id: int, pages_data: list):
        """Update document pages_data field and total_pages count - auto-detects version control vs legacy"""
        import json
        with self._db_lock:
            session = self.get_session()
            try:
                # Try version control first
                doc_version = session.query(DocumentVersion).filter(DocumentVersion.id == doc_id).first()
                if doc_version:
                    doc_version.pages_data = json.dumps(pages_data)
                    doc_version.total_pages = len(pages_data)
                    session.commit()
                else:
                    # Fallback to legacy Document table
                    doc = session.query(Document).filter(Document.id == doc_id).first()
                    if doc:
                        doc.pages_data = json.dumps(pages_data)
                        doc.total_pages = len(pages_data)
                        session.commit()
            finally:
                session.close()
    
    def get_document(self, doc_id: int, user_id: Optional[int] = None,
                    org_id: Optional[int] = None, is_superuser: bool = False) -> Optional[Document]:
        """
        Get document by ID with permission check
        
        Args:
            doc_id: Document ID
            user_id: Current user ID for permission check (None to skip check)
            org_id: Current user's organization ID
            is_superuser: Is user a superuser
        """
        session = self.get_session()
        try:
            query = session.query(Document).filter(Document.id == doc_id)
            
            # Apply permission filter if user_id is provided
            if user_id is not None or not is_superuser:
                query = self.apply_permission_filter(query, user_id, org_id, is_superuser)
            
            return query.first()
        finally:
            session.close()
    
    def get_document_by_checksum(self, checksum: str) -> Optional[Document]:
        """Get document by checksum"""
        session = self.get_session()
        try:
            return session.query(Document).filter(Document.checksum == checksum).first()
        finally:
            session.close()
    
    def get_documents_by_status(self, statuses: List[str]) -> List[Document]:
        """Get documents by a list of statuses"""
        session = self.get_session()
        try:
            return session.query(Document).filter(Document.status.in_(statuses)).all()
        finally:
            session.close()

    def list_documents(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
        exclude_file_types: Optional[List[str]] = None,
        user_id: Optional[int] = None,
        org_id: Optional[int] = None,
        is_superuser: bool = False
    ) -> Tuple[List[Document], int]:
        """
        List documents with permission filtering
        
        Args:
            limit: Maximum number of documents to return
            offset: Number of documents to skip
            status: Filter by document status
            exclude_file_types: File types to exclude
            user_id: Current user ID for permission filtering
            org_id: Current user's organization ID
            is_superuser: Is user a superuser
        """
        session = self.get_session()
        try:
            query = session.query(Document)
            
            # Apply permission filter
            query = self.apply_permission_filter(query, user_id, org_id, is_superuser)
            
            if status:
                query = query.filter(Document.status == status)
            if exclude_file_types:
                query = query.filter(Document.file_type.notin_(exclude_file_types))
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            docs = query.order_by(Document.uploaded_at.desc()).limit(limit).offset(offset).all()
            
            return docs, total
        finally:
            session.close()
    
    def delete_document(self, doc_id: int) -> bool:
        """Delete document by ID"""
        import structlog
        logger = structlog.get_logger(__name__)
        
        with self._db_lock:
            session = self.get_session()
            session_closed = False
            try:
                logger.info("delete_document_start", doc_id=doc_id)
                doc = session.query(Document).filter(Document.id == doc_id).first()
                if not doc:
                    logger.warning("document_not_found_for_deletion", doc_id=doc_id)
                    return False
                
                logger.info("document_found_deleting", doc_id=doc_id, filename=doc.filename)
                session.delete(doc)
                logger.info("before_commit", doc_id=doc_id)
                session.commit()
                logger.info("after_commit_success", doc_id=doc_id)
                
                # Close current session before verification
                session.close()
                session_closed = True
                
                # Verify deletion with a fresh session
                verify_session = self.get_session()
                try:
                    verify_doc = verify_session.query(Document).filter(Document.id == doc_id).first()
                    if verify_doc:
                        logger.error("deletion_verification_failed", doc_id=doc_id, message="Document still exists after commit!")
                        return False
                    else:
                        logger.info("deletion_verified", doc_id=doc_id)
                        return True
                finally:
                    verify_session.close()
                    
            except Exception as e:
                logger.error("delete_document_exception", doc_id=doc_id, error=str(e))
                try:
                    session.rollback()
                except:
                    pass
                return False
            finally:
                # Only close if not already closed
                if not session_closed:
                    try:
                        session.close()
                    except:
                        pass
    
    def delete_all_documents(self):
        """Delete all documents"""
        with self._db_lock:
            session = self.get_session()
            try:
                session.query(Document).delete()
                session.commit()
            finally:
                session.close()
    
    def get_stats(
        self,
        user_id: Optional[int] = None,
        org_id: Optional[int] = None,
        is_superuser: bool = False
    ):
        """
        Get database statistics (filtered by user permissions)
        
        Args:
            user_id: User ID for permission filtering
            org_id: Organization ID for permission filtering
            is_superuser: Whether the user is a superuser
            
        Returns:
            Statistics dictionary with counts
        """
        session = self.get_session()
        try:
            # Build base query with permission filter
            base_query = session.query(Document)
            base_query = self.apply_permission_filter(
                base_query, user_id, org_id, is_superuser
            )
            
            # Get counts
            total = base_query.count()
            completed = base_query.filter(Document.status == 'completed').count()
            failed = base_query.filter(Document.status == 'failed').count()
            
            # queued 和 processing 都算作"处理中"
            processing = base_query.filter(
                Document.status.in_(['queued', 'processing'])
            ).count()
            
            # Calculate total pages across filtered documents
            from sqlalchemy import func
            total_pages_result = base_query.with_entities(
                func.sum(Document.total_pages)
            ).scalar()
            total_pages = total_pages_result or 0
            
            return {
                'total': total,
                'completed': completed,
                'failed': failed,
                'processing': processing,  # queued + processing 合并
                'total_pages': total_pages
            }
        finally:
            session.close()
    
    # ===== Version Control Methods =====
    
    def create_document_master(
        self,
        filename_base: str,
        owner_id: int,
        org_id: int,
        visibility: str = 'private',
        category: str = None,
        tags: List[str] = None,
        author: str = None,
        description: str = None,
        shared_with_users: List[int] = None,
        shared_with_roles: List[str] = None
    ) -> DocumentMaster:
        """Create a new document master"""
        with self._db_lock:
            session = self.get_session()
            try:
                master = DocumentMaster(
                    document_group_id=str(uuid.uuid4()),
                    filename_base=filename_base,
                    owner_id=owner_id,
                    org_id=org_id,
                    visibility=visibility,
                    category=category,
                    tags=','.join(tags) if tags else None,
                    author=author,
                    description=description,
                    shared_with_users=json.dumps(shared_with_users or []),
                    shared_with_roles=json.dumps(shared_with_roles or [])
                )
                session.add(master)
                session.commit()
                session.refresh(master)
                
                # Expunge the object from session so it can be used after session closes
                session.expunge(master)
                
                return master
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def create_document_version(
        self,
        document_master_id: int,
        version: int,
        file_path: str,
        file_type: str,
        file_size: int,
        checksum: str,
        ocr_engine: str,
        uploaded_by_id: int,
        version_note: str = None
    ) -> DocumentVersion:
        """Create a new document version"""
        with self._db_lock:
            session = self.get_session()
            try:
                doc_version = DocumentVersion(
                    document_master_id=document_master_id,
                    version=version,
                    file_path=file_path,
                    file_type=file_type,
                    file_size=file_size,
                    checksum=checksum,
                    ocr_engine=ocr_engine,
                    uploaded_by_id=uploaded_by_id,
                    version_note=version_note,
                    status='pending'
                )
                session.add(doc_version)
                session.flush()  # Get the ID without committing
                
                # Update master's latest_version_id
                master = session.query(DocumentMaster).filter(
                    DocumentMaster.id == document_master_id
                ).first()
                if master:
                    master.latest_version_id = doc_version.id
                    master.updated_at = datetime.utcnow()
                
                # Now commit everything together
                session.commit()
                session.refresh(doc_version)
                
                # Expunge the object from session so it can be used after session closes
                session.expunge(doc_version)
                
                return doc_version
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def get_document_master_by_filename(
        self,
        filename: str,
        org_id: int
    ) -> Optional[DocumentMaster]:
        """Get document master by filename and organization"""
        session = self.get_session()
        try:
            return session.query(DocumentMaster).filter(
                DocumentMaster.filename_base == filename,
                DocumentMaster.org_id == org_id
            ).first()
        finally:
            session.close()
    
    def get_document_master_by_group_id(
        self,
        document_group_id: str
    ) -> Optional[DocumentMaster]:
        """Get document master by group ID"""
        session = self.get_session()
        try:
            return session.query(DocumentMaster).filter(
                DocumentMaster.document_group_id == document_group_id
            ).first()
        finally:
            session.close()
    
    def get_latest_version(
        self,
        document_master_id: int
    ) -> Optional[DocumentVersion]:
        """Get latest version of a document"""
        session = self.get_session()
        try:
            master = session.query(DocumentMaster).filter(
                DocumentMaster.id == document_master_id
            ).first()
            if master and master.latest_version_id:
                return session.query(DocumentVersion).filter(
                    DocumentVersion.id == master.latest_version_id
                ).first()
            return None
        finally:
            session.close()
    
    def get_version_history(
        self,
        document_master_id: int
    ) -> List[DocumentVersion]:
        """Get all versions of a document, ordered by version number descending"""
        session = self.get_session()
        try:
            return session.query(DocumentVersion).filter(
                DocumentVersion.document_master_id == document_master_id,
                DocumentVersion.is_active == True
            ).order_by(DocumentVersion.version.desc()).all()
        finally:
            session.close()
    
    def get_document_version_by_id(
        self,
        version_id: int
    ) -> Optional[DocumentVersion]:
        """Get specific document version by ID"""
        session = self.get_session()
        try:
            return session.query(DocumentVersion).filter(
                DocumentVersion.id == version_id
            ).first()
        finally:
            session.close()
    
    def get_document_version_by_number(
        self,
        document_master_id: int,
        version_number: int
    ) -> Optional[DocumentVersion]:
        """Get specific version by number"""
        session = self.get_session()
        try:
            return session.query(DocumentVersion).filter(
                DocumentVersion.document_master_id == document_master_id,
                DocumentVersion.version == version_number
            ).first()
        finally:
            session.close()
    
    def restore_version(
        self,
        document_master_id: int,
        version_number: int
    ) -> DocumentVersion:
        """
        Restore a specific version by making it the latest.
        Creates a new version that is a copy of the specified version.
        """
        with self._db_lock:
            session = self.get_session()
            try:
                # Get the version to restore
                old_version = session.query(DocumentVersion).filter(
                    DocumentVersion.document_master_id == document_master_id,
                    DocumentVersion.version == version_number
                ).first()
                
                if not old_version:
                    raise ValueError(f"Version {version_number} not found")
                
                # Get current latest version number
                latest = session.query(DocumentVersion).filter(
                    DocumentVersion.document_master_id == document_master_id
                ).order_by(DocumentVersion.version.desc()).first()
                
                new_version_number = (latest.version + 1) if latest else 1
                
                # Create new version as a copy
                new_version = DocumentVersion(
                    document_master_id=document_master_id,
                    version=new_version_number,
                    file_path=old_version.file_path,
                    file_type=old_version.file_type,
                    file_size=old_version.file_size,
                    checksum=old_version.checksum,
                    ocr_engine=old_version.ocr_engine,
                    uploaded_by_id=old_version.uploaded_by_id,
                    version_note=f"Restored from version {version_number}",
                    status=old_version.status,
                    num_chunks=old_version.num_chunks,
                    progress_percentage=old_version.progress_percentage,
                    total_pages=old_version.total_pages,
                    processed_pages=old_version.processed_pages,
                    es_document_ids=old_version.es_document_ids,
                    pages_data=old_version.pages_data,
                    processed_at=old_version.processed_at
                )
                session.add(new_version)
                session.commit()
                session.refresh(new_version)
                
                # Update master's latest_version_id
                master = session.query(DocumentMaster).filter(
                    DocumentMaster.id == document_master_id
                ).first()
                if master:
                    master.latest_version_id = new_version.id
                    master.updated_at = datetime.utcnow()
                    session.commit()
                
                # Expunge the object from session so it can be used after session closes
                session.expunge(new_version)
                
                return new_version
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def update_document_master_metadata(
        self,
        document_master_id: int,
        category: str = None,
        tags: List[str] = None,
        author: str = None,
        description: str = None,
        visibility: str = None,
        shared_with_users: List[int] = None,
        shared_with_roles: List[str] = None
    ) -> DocumentMaster:
        """Update document master metadata"""
        with self._db_lock:
            session = self.get_session()
            try:
                master = session.query(DocumentMaster).filter(
                    DocumentMaster.id == document_master_id
                ).first()
                
                if not master:
                    raise ValueError(f"Document master {document_master_id} not found")
                
                if category is not None:
                    master.category = category
                if tags is not None:
                    master.tags = ','.join(tags) if tags else None
                if author is not None:
                    master.author = author
                if description is not None:
                    master.description = description
                if visibility is not None:
                    master.visibility = visibility
                if shared_with_users is not None:
                    master.shared_with_users = json.dumps(shared_with_users)
                if shared_with_roles is not None:
                    master.shared_with_roles = json.dumps(shared_with_roles)
                
                master.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(master)
                
                # Expunge the object from session so it can be used after session closes
                session.expunge(master)
                
                return master
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def update_document_version_status(
        self,
        version_id: int,
        status: str,
        progress_percentage: int = None,
        progress_message: str = None,
        error_message: str = None
    ) -> DocumentVersion:
        """Update document version processing status"""
        with self._db_lock:
            session = self.get_session()
            try:
                version = session.query(DocumentVersion).filter(
                    DocumentVersion.id == version_id
                ).first()
                
                if not version:
                    raise ValueError(f"Document version {version_id} not found")
                
                version.status = status
                if progress_percentage is not None:
                    version.progress_percentage = progress_percentage
                if progress_message is not None:
                    version.progress_message = progress_message
                if error_message is not None:
                    version.error_message = error_message
                if status == 'completed':
                    version.processed_at = datetime.utcnow()
                
                session.commit()
                session.refresh(version)
                
                # Expunge the object from session so it can be used after session closes
                session.expunge(version)
                
                return version
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def delete_document_version(
        self,
        version_id: int,
        soft_delete: bool = True
    ) -> bool:
        """Delete a document version (soft or hard delete)"""
        with self._db_lock:
            session = self.get_session()
            try:
                version = session.query(DocumentVersion).filter(
                    DocumentVersion.id == version_id
                ).first()
                
                if not version:
                    return False
                
                if soft_delete:
                    version.is_active = False
                    session.commit()
                else:
                    session.delete(version)
                    session.commit()
                
                return True
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def delete_document_master(
        self,
        document_master_id: int
    ) -> bool:
        """Delete document master and all its versions"""
        with self._db_lock:
            session = self.get_session()
            try:
                master = session.query(DocumentMaster).filter(
                    DocumentMaster.id == document_master_id
                ).first()
                
                if not master:
                    return False
                
                # Delete all versions (cascade should handle this, but being explicit)
                session.query(DocumentVersion).filter(
                    DocumentVersion.document_master_id == document_master_id
                ).delete()
                
                # Delete master
                session.delete(master)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
    
    def list_document_masters(
        self,
        org_id: int = None,
        user_id: int = None,
        limit: int = 100,
        offset: int = 0,
        status: str = None,
        is_superuser: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List document masters with their latest versions.
        Returns combined view compatible with old Document API.
        
        Args:
            org_id: Filter by organization ID (for superusers, None means all orgs)
            user_id: Current user ID for permission check
            limit: Maximum number of results
            offset: Pagination offset
            status: Filter by status
            is_superuser: Whether user is superuser
        """
        session = self.get_session()
        try:
            query = session.query(DocumentMaster).options(
                joinedload(DocumentMaster.latest_version)
            )
            
            # Apply permission filters
            query = self.apply_permission_filter(
                query, 
                user_id=user_id, 
                org_id=org_id, 
                is_superuser=is_superuser,
                model_class=DocumentMaster
            )
            
            # Order by updated_at desc
            query = query.order_by(DocumentMaster.updated_at.desc())
            
            # Pagination
            query = query.limit(limit).offset(offset)
            
            masters = query.all()
            
            # Convert to combined dict format
            results = []
            for master in masters:
                if master.latest_version:
                    # Filter by status if specified
                    if status and master.latest_version.status != status:
                        continue
                    results.append(master.latest_version.to_combined_dict(master))
            
            return results
        finally:
            session.close()
    
    def check_version_exists_by_checksum(
        self,
        document_master_id: int,
        checksum: str
    ) -> Optional[DocumentVersion]:
        """Check if a version with this checksum already exists for this master"""
        session = self.get_session()
        try:
            return session.query(DocumentVersion).filter(
                DocumentVersion.document_master_id == document_master_id,
                DocumentVersion.checksum == checksum
            ).first()
        finally:
            session.close()
    
    def count_documents_by_org(self, org_id: int) -> int:
        """
        Count total documents for an organization.
        Counts Document records (current architecture).
        """
        session = self.get_session()
        try:
            # Count documents in current architecture
            count = session.query(Document).filter(
                Document.org_id == org_id
            ).count()
            
            return count
        finally:
            session.close()


class AuthManager:
    """Manager for authentication-related operations"""
    
    def __init__(self, engine):
        """Initialize auth manager with database engine"""
        self.engine = engine
        self.SessionLocal = sessionmaker(bind=self.engine)
        self._db_lock = threading.Lock()
    
    def get_session(self) -> Session:
        """Get database session"""
        return self.SessionLocal()
    
    # Organization methods
    def create_organization(self, name: str, description: Optional[str] = None) -> Organization:
        """Create new organization"""
        with self._db_lock:
            session = self.get_session()
            try:
                org = Organization(name=name, description=description)
                session.add(org)
                session.commit()
                session.refresh(org)
                return org
            finally:
                session.close()
    
    def get_organization(self, org_id: int) -> Optional[Organization]:
        """Get organization by ID"""
        session = self.get_session()
        try:
            return session.query(Organization).filter(Organization.id == org_id).first()
        finally:
            session.close()
    
    # User methods
    def create_user(
        self,
        username: str,
        email: str,
        password_hash: str,
        org_id: Optional[int] = None,
        is_superuser: bool = False
    ) -> User:
        """Create new user"""
        with self._db_lock:
            session = self.get_session()
            try:
                user = User(
                    username=username,
                    email=email,
                    password_hash=password_hash,
                    org_id=org_id,
                    is_superuser=is_superuser
                )
                session.add(user)
                session.commit()
                session.refresh(user)
                return user
            finally:
                session.close()
    
    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID with roles preloaded"""
        session = self.get_session()
        try:
            user = session.query(User).options(joinedload(User.roles)).filter(User.id == user_id).first()
            if user:
                # Force load roles to prevent DetachedInstanceError
                _ = user.roles
            return user
        finally:
            session.close()
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username with roles preloaded"""
        session = self.get_session()
        try:
            user = session.query(User).options(joinedload(User.roles)).filter(User.username == username).first()
            if user:
                # Force load roles to prevent DetachedInstanceError
                _ = user.roles
            return user
        finally:
            session.close()
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email with roles preloaded"""
        session = self.get_session()
        try:
            user = session.query(User).options(joinedload(User.roles)).filter(User.email == email).first()
            if user:
                # Force load roles to prevent DetachedInstanceError
                _ = user.roles
            return user
        finally:
            session.close()
    
    def update_user_last_login(self, user_id: int):
        """Update user's last login time"""
        with self._db_lock:
            session = self.get_session()
            try:
                user = session.query(User).filter(User.id == user_id).first()
                if user:
                    user.last_login = datetime.utcnow()
                    session.commit()
            finally:
                session.close()
    
    # Role methods
    def create_role(
        self,
        name: str,
        code: str,
        description: Optional[str] = None,
        org_id: Optional[int] = None,
        is_system: bool = False
    ) -> Role:
        """Create new role"""
        with self._db_lock:
            session = self.get_session()
            try:
                role = Role(
                    name=name,
                    code=code,
                    description=description,
                    org_id=org_id,
                    is_system=is_system
                )
                session.add(role)
                session.commit()
                session.refresh(role)
                return role
            finally:
                session.close()
    
    def get_role_by_code(self, code: str) -> Optional[Role]:
        """Get role by code"""
        session = self.get_session()
        try:
            return session.query(Role).filter(Role.code == code).first()
        finally:
            session.close()
    
    def assign_role_to_user(self, user_id: int, role_id: int):
        """Assign role to user"""
        with self._db_lock:
            session = self.get_session()
            try:
                user = session.query(User).filter(User.id == user_id).first()
                role = session.query(Role).filter(Role.id == role_id).first()
                if user and role and role not in user.roles:
                    user.roles.append(role)
                    session.commit()
            finally:
                session.close()
    
    # Permission methods
    def create_permission(
        self,
        code: str,
        resource: str,
        action: str,
        description: Optional[str] = None
    ) -> Permission:
        """Create new permission"""
        with self._db_lock:
            session = self.get_session()
            try:
                permission = Permission(
                    code=code,
                    resource=resource,
                    action=action,
                    description=description
                )
                session.add(permission)
                session.commit()
                session.refresh(permission)
                return permission
            finally:
                session.close()
    
    def get_permission_by_code(self, code: str) -> Optional[Permission]:
        """Get permission by code"""
        session = self.get_session()
        try:
            return session.query(Permission).filter(Permission.code == code).first()
        finally:
            session.close()
    
    def assign_permission_to_role(self, role_id: int, permission_id: int):
        """Assign permission to role"""
        with self._db_lock:
            session = self.get_session()
            try:
                role = session.query(Role).filter(Role.id == role_id).first()
                permission = session.query(Permission).filter(Permission.id == permission_id).first()
                if role and permission and permission not in role.permissions:
                    role.permissions.append(permission)
                    session.commit()
            finally:
                session.close()
    
    def get_user_permissions(self, user_id: int) -> List[str]:
        """Get all permission codes for a user"""
        session = self.get_session()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                return []
            
            permissions = set()
            for role in user.roles:
                for perm in role.permissions:
                    permissions.add(perm.code)
            
            return list(permissions)
        finally:
            session.close()

    def get_user_roles(self, user_id: int) -> List[str]:
        """Get all role codes for a user"""
        session = self.get_session()
        try:
            user = session.query(User).options(joinedload(User.roles)).filter(User.id == user_id).first()
            if not user:
                return []
            return [role.code for role in user.roles]
        finally:
            session.close()
    
    # Admin-level organization management methods
    def list_organizations(self) -> List[Organization]:
        """List all organizations"""
        session = self.get_session()
        try:
            return session.query(Organization).order_by(Organization.name).all()
        finally:
            session.close()
    
    def update_organization(self, org_id: int, name: str, description: Optional[str] = None) -> Optional[Organization]:
        """Update organization"""
        with self._db_lock:
            session = self.get_session()
            try:
                org = session.query(Organization).filter(Organization.id == org_id).first()
                if org:
                    org.name = name
                    if description is not None:
                        org.description = description
                    session.commit()
                    session.refresh(org)
                return org
            finally:
                session.close()
    
    def delete_organization(self, org_id: int) -> bool:
        """
        Delete organization (checks for users and documents first)
        Returns True if deleted, False if organization has users/documents
        """
        with self._db_lock:
            session = self.get_session()
            try:
                org = session.query(Organization).filter(Organization.id == org_id).first()
                if not org:
                    return False
                
                # Check for users
                user_count = session.query(User).filter(User.org_id == org_id).count()
                if user_count > 0:
                    return False
                
                # Check for documents
                doc_count = session.query(Document).filter(Document.org_id == org_id).count()
                if doc_count > 0:
                    return False
                
                # Safe to delete
                session.delete(org)
                session.commit()
                return True
            finally:
                session.close()
    
    def get_organization_members(self, org_id: int) -> List[User]:
        """Get all users in an organization"""
        session = self.get_session()
        try:
            return session.query(User).options(joinedload(User.roles)).filter(User.org_id == org_id).order_by(User.username).all()
        finally:
            session.close()
    
    # Admin-level user management methods
    def list_users_paginated(
        self,
        page: int = 1,
        per_page: int = 50,
        search: Optional[str] = None,
        org_id: Optional[int] = None,
        role_code: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> tuple[List[User], int]:
        """
        List users with pagination and filters
        Returns (users, total_count)
        """
        session = self.get_session()
        try:
            query = session.query(User).options(joinedload(User.roles))
            
            # Apply filters
            if search:
                query = query.filter(
                    (User.username.like(f'%{search}%')) | 
                    (User.email.like(f'%{search}%'))
                )
            
            if org_id is not None:
                query = query.filter(User.org_id == org_id)
            
            if is_active is not None:
                query = query.filter(User.is_active == is_active)
            
            if role_code:
                role = session.query(Role).filter(Role.code == role_code).first()
                if role:
                    query = query.filter(User.roles.contains(role))
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            offset = (page - 1) * per_page
            users = query.order_by(User.id.desc()).offset(offset).limit(per_page).all()
            
            # Force load roles
            for user in users:
                _ = user.roles
            
            return users, total
        finally:
            session.close()
    
    def create_user_by_admin(
        self,
        username: str,
        email: str,
        password_hash: str,
        org_id: int,
        role_codes: List[str],
        is_active: bool = True,
        is_superuser: bool = False
    ) -> User:
        """Create user by admin (with role assignment)"""
        with self._db_lock:
            session = self.get_session()
            try:
                # Create user
                user = User(
                    username=username,
                    email=email,
                    password_hash=password_hash,
                    org_id=org_id,
                    is_active=is_active,
                    is_superuser=is_superuser
                )
                session.add(user)
                session.flush()  # Get user ID
                
                # Assign roles
                for role_code in role_codes:
                    role = session.query(Role).filter(Role.code == role_code).first()
                    if role:
                        user.roles.append(role)
                
                session.commit()
                session.refresh(user)
                _ = user.roles  # Force load roles
                return user
            finally:
                session.close()
    
    def update_user_by_admin(
        self,
        user_id: int,
        email: Optional[str] = None,
        org_id: Optional[int] = None,
        role_codes: Optional[List[str]] = None,
        is_active: Optional[bool] = None,
        is_superuser: Optional[bool] = None,
        password_hash: Optional[str] = None
    ) -> Optional[User]:
        """Update user by admin"""
        with self._db_lock:
            session = self.get_session()
            try:
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    return None
                
                # Update fields
                if email is not None:
                    user.email = email
                if org_id is not None:
                    user.org_id = org_id
                if is_active is not None:
                    user.is_active = is_active
                if is_superuser is not None:
                    user.is_superuser = is_superuser
                if password_hash is not None:
                    user.password_hash = password_hash
                
                # Update roles if provided
                if role_codes is not None:
                    user.roles.clear()
                    for role_code in role_codes:
                        role = session.query(Role).filter(Role.code == role_code).first()
                        if role:
                            user.roles.append(role)
                
                session.commit()
                session.refresh(user)
                _ = user.roles  # Force load roles
                return user
            finally:
                session.close()
    
    def list_all_roles(self) -> List[Role]:
        """List all roles"""
        session = self.get_session()
        try:
            return session.query(Role).order_by(Role.name).all()
        finally:
            session.close()


class TokenManager:
    """Manager for token operations"""
    
    def __init__(self, engine):
        """Initialize token manager with database engine"""
        self.engine = engine
        self.SessionLocal = sessionmaker(bind=self.engine)
        self._db_lock = threading.Lock()
    
    def get_session(self) -> Session:
        """Get database session"""
        return self.SessionLocal()
    
    # MCP Token methods
    def create_mcp_token(
        self,
        token_id: str,
        user_id: int,
        name: str,
        expires_at: datetime,
        token: str = None  # New parameter
    ) -> McpToken:
        """Create new MCP token"""
        with self._db_lock:
            session = self.get_session()
            try:
                token_obj = McpToken(
                    token_id=token_id,
                    user_id=user_id,
                    name=name,
                    expires_at=expires_at,
                    token=token  # Store token
                )
                session.add(token_obj)
                session.commit()
                session.refresh(token_obj)
                return token_obj
            finally:
                session.close()
    
    def get_mcp_token_by_token_id(self, token_id: str) -> Optional[McpToken]:
        """Get MCP token by token_id (UUID)"""
        session = self.get_session()
        try:
            return session.query(McpToken).filter(McpToken.token_id == token_id).first()
        finally:
            session.close()
    
    def get_mcp_token_by_id(self, id: int) -> Optional[McpToken]:
        """Get MCP token by primary key ID"""
        session = self.get_session()
        try:
            return session.query(McpToken).filter(McpToken.id == id).first()
        finally:
            session.close()
    
    def list_user_mcp_tokens(self, user_id: int) -> List[McpToken]:
        """List all MCP tokens for a user"""
        session = self.get_session()
        try:
            return session.query(McpToken).filter(
                McpToken.user_id == user_id
            ).order_by(McpToken.created_at.desc()).all()
        finally:
            session.close()
    
    def update_mcp_token_last_used(self, token_id: str):
        """Update MCP token's last used timestamp"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(McpToken).filter(McpToken.token_id == token_id).first()
                if token:
                    token.last_used_at = datetime.utcnow()
                    session.commit()
            finally:
                session.close()
    
    def revoke_mcp_token(self, token_id: str) -> bool:
        """Revoke MCP token by token_id (UUID)"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(McpToken).filter(McpToken.token_id == token_id).first()
                if token:
                    token.is_active = False
                    session.commit()
                    return True
                return False
            finally:
                session.close()
    
    def revoke_mcp_token_by_id(self, id: int) -> bool:
        """Revoke MCP token by primary key ID"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(McpToken).filter(McpToken.id == id).first()
                if token:
                    token.is_active = False
                    session.commit()
                    return True
                return False
            finally:
                session.close()
    
    def delete_mcp_token(self, token_id: str) -> bool:
        """Delete MCP token"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(McpToken).filter(McpToken.token_id == token_id).first()
                if token:
                    session.delete(token)
                    session.commit()
                    return True
                return False
            finally:
                session.close()

    def delete_mcp_token_by_id(self, id: int) -> bool:
        """Delete MCP token by primary key ID"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(McpToken).filter(McpToken.id == id).first()
                if token:
                    session.delete(token)
                    session.commit()
                    return True
                return False
            finally:
                session.close()
    
    # Refresh Token methods
    def create_refresh_token(
        self,
        token_id: str,
        user_id: int,
        expires_at: datetime
    ) -> RefreshToken:
        """Create new refresh token"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = RefreshToken(
                    token_id=token_id,
                    user_id=user_id,
                    expires_at=expires_at
                )
                session.add(token)
                session.commit()
                session.refresh(token)
                return token
            finally:
                session.close()
    
    def get_refresh_token(self, token_id: str) -> Optional[RefreshToken]:
        """Get refresh token by token_id"""
        session = self.get_session()
        try:
            return session.query(RefreshToken).filter(
                RefreshToken.token_id == token_id
            ).first()
        finally:
            session.close()
    
    def revoke_refresh_token(self, token_id: str) -> bool:
        """Revoke refresh token"""
        with self._db_lock:
            session = self.get_session()
            try:
                token = session.query(RefreshToken).filter(RefreshToken.token_id == token_id).first()
                if token:
                    token.is_revoked = True
                    session.commit()
                    return True
                return False
            finally:
                session.close()
    
    def revoke_user_refresh_tokens(self, user_id: int):
        """Revoke all refresh tokens for a user"""
        with self._db_lock:
            session = self.get_session()
            try:
                tokens = session.query(RefreshToken).filter(
                    RefreshToken.user_id == user_id,
                    RefreshToken.is_revoked == False
                ).all()
                for token in tokens:
                    token.is_revoked = True
                session.commit()
            finally:
                session.close()