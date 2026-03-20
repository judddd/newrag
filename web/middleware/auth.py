"""JWT Authentication Middleware"""

from typing import Optional
import structlog
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from jose import jwt, JWTError

from src.config import config
from src.database import DatabaseManager, AuthManager

logger = structlog.get_logger(__name__)


class AuthMiddleware(BaseHTTPMiddleware):
    """JWT Authentication Middleware"""
    
    # Public endpoints that don't require authentication
    PUBLIC_PATHS = {
        '/',
        '/health',
        '/docs',
        '/openapi.json',
        '/redoc',
        '/auth/login',
        '/auth/register',
        '/auth/refresh',
    }
    
    # Public path prefixes
    PUBLIC_PREFIXES = (
        '/static/',
    )
    
    def __init__(self, app):
        super().__init__(app)
        
        # Get JWT configuration
        security_config = config.security_config
        self.jwt_secret = security_config.get('jwt_secret', 'kJ9mP2vX8nQ4wR7tY3zL6hF5dS1aG0bN8cM4xV9pK2uE7iW3oA6qT5rH8jL1mN4pS9v')
        self.jwt_algorithm = security_config.get('jwt_algorithm', 'HS256')
        self.auth_enabled = security_config.get('auth', {}).get('enabled', False)
        
        # Initialize database managers
        db_config = config.database_config
        db_url = db_config.get('url', 'sqlite:///data/documents.db')
        db = DatabaseManager(db_url=db_url)
        self.auth_manager = AuthManager(db.engine)
        
        if not self.auth_enabled:
            logger.info("authentication_disabled", message="Auth middleware loaded but disabled in config")
        else:
            logger.info("authentication_enabled", message="Auth middleware active")
    
    async def dispatch(self, request: Request, call_next):
        """Process request and verify JWT token"""
        
        # Skip authentication if disabled
        if not self.auth_enabled:
            request.state.user = None
            return await call_next(request)
        
        # Check if path is public
        path = request.url.path
        if path in self.PUBLIC_PATHS or any(path.startswith(prefix) for prefix in self.PUBLIC_PREFIXES):
            request.state.user = None
            return await call_next(request)
        
        # Extract token from Authorization header
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            # No token provided - allow request but set user to None
            # Individual routes can enforce authentication using dependencies
            request.state.user = None
            return await call_next(request)
        
        if not auth_header.startswith('Bearer '):
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid authorization header format. Use 'Bearer <token>'"}
            )
        
        token = auth_header[7:]  # Remove 'Bearer ' prefix
        
        try:
            # Decode and verify JWT token
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm]
            )
            
            # Extract user information from token
            user_id = int(payload.get('sub'))  # 'sub' contains user_id as string
            token_type = payload.get('type', 'access')
            
            if not user_id:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid token: missing user_id"}
                )
            
            # Get user from database
            user = self.auth_manager.get_user_by_id(user_id)
            
            if not user:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "User not found"}
                )
            
            if not user.is_active:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "User account is disabled"}
                )
            
            # Get user permissions
            permissions = self.auth_manager.get_user_permissions(user_id)
            
            # Get user roles
            roles = self.auth_manager.get_user_roles(user_id)
            
            # Attach user info to request state
            request.state.user = {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'org_id': user.org_id,
                'is_superuser': user.is_superuser,
                'roles': roles,
                'permissions': permissions,
                'token_type': token_type
            }
            
            logger.debug(
                "user_authenticated",
                user_id=user.id,
                username=user.username,
                token_type=token_type
            )
            
        except JWTError as e:
            logger.warning("jwt_verification_failed", error=str(e))
            return JSONResponse(
                status_code=401,
                content={"detail": f"Invalid or expired token: {str(e)}"}
            )
        except Exception as e:
            logger.error("authentication_error", error=str(e))
            return JSONResponse(
                status_code=500,
                content={"detail": "Authentication error"}
            )
        
        # Continue with request
        return await call_next(request)

