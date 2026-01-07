"""Dependency injection modules"""

from .auth_deps import (
    get_current_user,
    get_optional_user,
    require_permission,
    require_role
)

__all__ = [
    'get_current_user',
    'get_optional_user',
    'require_permission',
    'require_role'
]











