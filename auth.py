import hashlib
import secrets
import logging
from typing import Dict, Set, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class Permission(Enum):
    READ = "read"
    WRITE = "write"
    CREATE = "create"
    DELETE = "delete"
    ADMIN = "admin"

class AuthManager:
    """Simple authentication and authorization manager"""
    
    def __init__(self):
        self.sessions: Dict[str, Dict] = {}
        self.users: Dict[str, Dict] = {
            "admin": {
                "password_hash": self._hash_password("admin123"),
                "permissions": {Permission.READ, Permission.WRITE, Permission.CREATE, Permission.DELETE, Permission.ADMIN}
            },
            "readonly": {
                "password_hash": self._hash_password("readonly123"),
                "permissions": {Permission.READ}
            }
        }
        self.current_user = None 
    
    def _hash_password(self, password: str) -> str:
        """Hash password with salt"""
        salt = secrets.token_hex(16)
        return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() + salt
    
    def _verify_password(self, password: str, stored_hash: str) -> bool:
        """Verify password against stored hash"""
        if len(stored_hash) < 32:
            return False
        hash_part = stored_hash[:-32]
        salt = stored_hash[-32:]
        return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex() == hash_part
    
    def authenticate(self, username: str, password: str) -> bool:
        """Authenticate user"""
        if username not in self.users:
            return False
            
        if not self._verify_password(password, self.users[username]["password_hash"]):
            return False
        
        self.current_user = username
        logger.info(f"User {username} authenticated successfully")
        return True
    
    def check_permission(self, required_permission: Permission) -> bool:
        """Check if current user has required permission"""
        if not self.current_user:
            return False
            
        user_permissions = self.users[self.current_user]["permissions"]
        return required_permission in user_permissions or Permission.ADMIN in user_permissions
    
    def logout(self):
        """Logout current user"""
        if self.current_user:
            logger.info(f"User {self.current_user} logged out")
            self.current_user = None

TOOL_PERMISSIONS = {
    "list_tables": Permission.READ,
    "describe_table": Permission.READ,
    "read_data": Permission.READ,
    "insert_data": Permission.WRITE,
    "update_data": Permission.WRITE,
    "delete_data": Permission.DELETE,
    "create_table": Permission.CREATE,
    "drop_table": Permission.DELETE
}

DESTRUCTIVE_TOOLS = {"insert_data", "update_data", "delete_data", "drop_table", "create_table"}
