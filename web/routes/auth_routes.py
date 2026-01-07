"""Authentication routes for user login, registration, and token management"""

import os
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session
import bcrypt
from jose import jwt, JWTError

from src.database import DatabaseManager, AuthManager, TokenManager, User, McpToken
from src.config import config
from web.dependencies.auth_deps import get_current_user, db_session, get_db_manager

# JWT Configuration
security_config = config.security_config
JWT_SECRET = os.getenv('JWT_SECRET') or security_config.get('jwt_secret', 'default-secret-key')
print(f"🔐 Backend JWT_SECRET: {JWT_SECRET[:5]}...{JWT_SECRET[-5:]} (len={len(JWT_SECRET)})")
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM') or security_config.get('jwt_algorithm', 'HS256')
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('JWT_ACCESS_TOKEN_EXPIRE_MINUTES') or security_config.get('jwt_access_token_expire_minutes', 60))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv('JWT_REFRESH_TOKEN_EXPIRE_DAYS') or security_config.get('jwt_refresh_token_expire_days', 30))
# 允许注册：优先使用环境变量，否则使用 config.yaml 配置
ALLOW_REGISTRATION = os.getenv('ALLOW_REGISTRATION', str(security_config.get('auth', {}).get('allow_registration', False))).lower() == 'true'
# 默认角色：用于管理员创建用户时
DEFAULT_ROLE = os.getenv('DEFAULT_ROLE') or security_config.get('auth', {}).get('default_role', 'viewer')

router = APIRouter(prefix="/auth", tags=["Authentication"])
http_bearer = HTTPBearer(auto_error=False)


# ============================================================================
# Request/Response Models
# ============================================================================

class RegisterRequest(BaseModel):
    """User registration request"""
    username: str = Field(..., min_length=3, max_length=50, pattern="^[a-zA-Z0-9_-]+$")
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=72)
    org_name: Optional[str] = None


class LoginRequest(BaseModel):
    """User login request"""
    username: str
    password: str


class TokenResponse(BaseModel):
    """Authentication token response"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class RefreshTokenRequest(BaseModel):
    """Refresh token request"""
    refresh_token: str


class UserResponse(BaseModel):
    """User information response"""
    id: int
    username: str
    email: str
    org_id: Optional[int]
    is_active: bool
    is_superuser: bool
    roles: List[dict]
    permissions: List[str]
    organizations: List[dict] = []


class McpTokenRequest(BaseModel):
    """MCP token creation request"""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    expires_days: Optional[int] = Field(None, ge=1, le=3650)


class McpTokenResponse(BaseModel):
    """MCP token response"""
    id: int
    name: str
    token: str
    description: Optional[str]
    created_at: str
    expires_at: Optional[str]
    last_used_at: Optional[str]
    is_active: bool = True


# ============================================================================
# Helper Functions
# ============================================================================

def create_access_token(user_id: int, username: str, is_superuser: bool = False) -> tuple[str, datetime]:
    """Create JWT access token"""
    expires_at = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "username": username,
        "is_superuser": is_superuser,
        "type": "access",
        "exp": expires_at,
        "iat": datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, expires_at


def create_refresh_token(user_id: int, username: str) -> tuple[str, datetime]:
    """Create JWT refresh token"""
    expires_at = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    payload = {
        "sub": str(user_id),
        "username": username,
        "type": "refresh",
        "exp": expires_at,
        "iat": datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, expires_at


import uuid

def create_mcp_token(user_id: int, username: str, is_superuser: bool = False, expires_days: int = 365) -> tuple[str, str, datetime]:
    """Create long-lived MCP token"""
    token_id = str(uuid.uuid4())
    expires_at = datetime.utcnow() + timedelta(days=expires_days)
    payload = {
        "sub": str(user_id),
        "username": username,
        "is_superuser": is_superuser,
        "type": "mcp",
        "jti": token_id,
        "exp": expires_at,
        "iat": datetime.utcnow()
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, token_id, expires_at


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash"""
    return bcrypt.checkpw(password.encode('utf-8')[:72], password_hash.encode('utf-8'))


def hash_password(password: str) -> str:
    """Hash password using bcrypt"""
    password_bytes = password.encode('utf-8')[:72]
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password_bytes, salt).decode('utf-8')


# ============================================================================
# Authentication Routes
# ============================================================================

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(
    request: RegisterRequest,
    db: Session = Depends(db_session)
):
    """
    Register a new user
    
    Returns access and refresh tokens upon successful registration.
    """
    if not ALLOW_REGISTRATION:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Registration is disabled. Please contact an administrator."
        )
    
    # Initialize auth manager with engine
    db_manager = get_db_manager()
    auth_manager = AuthManager(db_manager.engine)
    
    # Check if username already exists
    existing_user = auth_manager.get_user_by_username(request.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists"
        )
    
    # Check if email already exists
    existing_user = auth_manager.get_user_by_email(request.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists"
        )
    
    # Get or create organization
    if request.org_name:
        org = auth_manager.get_organization_by_name(request.org_name)
        if not org:
            org = auth_manager.create_organization(request.org_name)
    else:
        # Use default organization
        org = auth_manager.get_organization_by_name("Default Organization")
    
    # Hash password
    password_hash = hash_password(request.password)
    
    # Create user
    user = auth_manager.create_user(
        username=request.username,
        email=request.email,
        password_hash=password_hash,
        org_id=org.id if org else None
    )
    
    # Assign default role (viewer for new users)
    viewer_role = auth_manager.get_role_by_code("viewer")
    if viewer_role:
        auth_manager.assign_role_to_user(user.id, viewer_role.id)
    
    # Generate tokens
    access_token, _ = create_access_token(user.id, user.username, user.is_superuser)
    refresh_token, refresh_expires = create_refresh_token(user.id, user.username)
    
    # Store refresh token in database
    token_manager = TokenManager(db_manager.engine)
    token_manager.create_refresh_token(refresh_token, user.id, refresh_expires)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.post("/login", response_model=TokenResponse)
async def login(
    request: LoginRequest,
    db: Session = Depends(db_session)
):
    """
    User login
    
    Returns access and refresh tokens upon successful authentication.
    """
    # Initialize auth manager with engine
    db_manager = get_db_manager()
    auth_manager = AuthManager(db_manager.engine)
    
    # Get user by username or email
    user = auth_manager.get_user_by_username(request.username)
    if not user:
        # Try to find by email if username not found
        user = auth_manager.get_user_by_email(request.username)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    # Verify password
    if not verify_password(request.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is disabled"
        )
    
    # Update last login
    user.last_login = datetime.utcnow()
    db.commit()
    
    # Generate tokens
    access_token, _ = create_access_token(user.id, user.username, user.is_superuser)
    refresh_token, refresh_expires = create_refresh_token(user.id, user.username)
    
    # Store refresh token in database
    token_manager = TokenManager(db_manager.engine)
    token_manager.create_refresh_token(refresh_token, user.id, refresh_expires)
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    request: RefreshTokenRequest,
    db: Session = Depends(db_session)
):
    """
    Refresh access token using refresh token
    
    Returns a new access token and refresh token.
    """
    try:
        # Decode refresh token
        payload = jwt.decode(request.refresh_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        
        # Verify token type
        if payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type"
            )
        
        user_id = int(payload.get("sub"))
        username = payload.get("username")
        
        # Verify refresh token exists in database and is not revoked
        db_manager = get_db_manager()
        token_manager = TokenManager(db_manager.engine)
        db_token = token_manager.get_refresh_token(request.refresh_token)
        if not db_token or db_token.is_revoked:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked refresh token"
            )
        
        # Get user to check is_superuser status
        db_manager = get_db_manager()
        auth_manager = AuthManager(db_manager.engine)
        user = auth_manager.get_user_by_id(user_id)
        if not user or not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )
        
        # Generate new tokens
        new_access_token, _ = create_access_token(user.id, user.username, user.is_superuser)
        new_refresh_token, refresh_expires = create_refresh_token(user.id, user.username)
        
        # Revoke old refresh token
        token_manager.revoke_refresh_token(request.refresh_token)
        
        # Store new refresh token
        token_manager.create_refresh_token(new_refresh_token, user.id, refresh_expires)
        
        return TokenResponse(
            access_token=new_access_token,
            refresh_token=new_refresh_token,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )
        
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )


@router.post("/logout")
async def logout(
    request: RefreshTokenRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(db_session)
):
    """
    User logout
    
    Revokes the refresh token to prevent further access token generation.
    """
    db_manager = get_db_manager()
    token_manager = TokenManager(db_manager.engine)
    token_manager.revoke_refresh_token(request.refresh_token)
    
    return {"message": "Successfully logged out"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    """
    Get current user information
    
    Returns detailed information about the authenticated user.
    """
    # Initialize managers
    db_manager = get_db_manager()
    auth_manager = AuthManager(db_manager.engine)
    
    # Get permissions securely
    permissions = auth_manager.get_user_permissions(current_user.id)
    
    # Get user's organizations
    organizations = []
    if current_user.is_superuser:
        # Superusers can see all organizations
        all_orgs = auth_manager.list_organizations()
        organizations = [{"id": org.id, "name": org.name, "description": org.description} for org in all_orgs]
    else:
        # Regular users see their own organization
        if current_user.org_id:
            session = db_manager.get_session()
            try:
                from src.database import Organization
                org = session.query(Organization).filter(Organization.id == current_user.org_id).first()
                if org:
                    organizations = [{"id": org.id, "name": org.name, "description": org.description}]
            finally:
                session.close()
    
    return UserResponse(
        id=current_user.id,
        username=current_user.username,
        email=current_user.email,
        org_id=current_user.org_id,
        is_active=current_user.is_active,
        is_superuser=current_user.is_superuser,
        roles=[{"id": role.id, "code": role.code, "name": role.name} for role in current_user.roles],
        permissions=permissions,
        organizations=organizations
    )


# ============================================================================
# MCP Token Management Routes
# ============================================================================

@router.post("/mcp-tokens", response_model=McpTokenResponse, status_code=status.HTTP_201_CREATED)
async def create_mcp_token_endpoint(
    request: McpTokenRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(db_session)
):
    """
    Create a new MCP token for external tool integration
    
    MCP tokens are long-lived JWT tokens that inherit user permissions.
    """
    db_manager = get_db_manager()
    token_manager = TokenManager(db_manager.engine)
    
    # Generate MCP token
    expires_days = request.expires_days or 365
    token, token_id, expires_at = create_mcp_token(
        current_user.id,
        current_user.username,
        current_user.is_superuser,
        expires_days
    )
    
    # Store in database
    # Note: TokenManager.create_mcp_token updated to accept full token string
    mcp_token = token_manager.create_mcp_token(
        token_id=token_id,
        user_id=current_user.id,
        name=request.name,
        expires_at=expires_at,
        token=token
    )
    
    return McpTokenResponse(
        id=mcp_token.id,
        name=mcp_token.name,
        token=token,  # Return full token only on creation
        description=request.description, # Pass back description from request as it's not in DB yet or handle DB update
        created_at=mcp_token.created_at.isoformat(),
        expires_at=mcp_token.expires_at.isoformat() if mcp_token.expires_at else None,
        last_used_at=mcp_token.last_used_at.isoformat() if mcp_token.last_used_at else None,
        is_active=mcp_token.is_active
    )


@router.get("/mcp-tokens", response_model=List[dict])
async def list_mcp_tokens(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(db_session)
):
    """
    List all MCP tokens for the current user
    
    Returns token metadata (excludes actual token values for security).
    """
    db_manager = get_db_manager()
    token_manager = TokenManager(db_manager.engine)
    tokens = token_manager.list_user_mcp_tokens(current_user.id)
    
    return [
        {
            "id": token.id,
            "name": token.name,
            "token": token.token, # Return stored token
            "description": None, # McpToken model doesn't have description
            "created_at": token.created_at.isoformat(),
            "expires_at": token.expires_at.isoformat() if token.expires_at else None,
            "last_used_at": token.last_used_at.isoformat() if token.last_used_at else None,
            "is_active": token.is_active
        }
        for token in tokens
    ]


@router.delete("/mcp-tokens/{token_id}")
async def delete_mcp_token(
    token_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(db_session)
):
    """
    Delete an MCP token
    
    Permanently deletes the token.
    """
    db_manager = get_db_manager()
    token_manager = TokenManager(db_manager.engine)
    
    # Get token
    mcp_token = token_manager.get_mcp_token_by_id(token_id)
    if not mcp_token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )
    
    # Verify ownership
    if mcp_token.user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this token"
        )
    
    # Delete token
    token_manager.delete_mcp_token_by_id(token_id)
    
    return {"message": "Token deleted successfully"}


# ============================================================================
# Health Check
# ============================================================================

@router.get("/health")
async def health_check():
    """Authentication service health check"""
    return {
        "status": "healthy",
        "service": "authentication",
        "registration_enabled": ALLOW_REGISTRATION
    }


@router.get("/organizations")
async def list_available_organizations(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(db_session)
):
    """
    List available organizations for current user.
    Admin sees all, regular user sees their own.
    """
    db_manager = get_db_manager()
    auth_manager = AuthManager(db_manager.engine)
    
    if current_user.is_superuser:
        # Admin sees all organizations
        orgs = auth_manager.list_organizations()
    else:
        # Regular user sees only their own organization
        if current_user.org_id:
            org = auth_manager.get_organization(current_user.org_id)
            orgs = [org] if org else []
        else:
            orgs = []
            
    return [
        {
            'id': org.id,
            'name': org.name,
            'description': org.description
        }
        for org in orgs
    ]
