import hashlib
import secrets
import logging
import time
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
    """Enhanced authentication and authorization manager with session management"""
    
    def __init__(self, session_timeout: int = 3600):
        self.sessions: Dict[str, Dict] = {}
        self.session_timeout = session_timeout
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
        self.login_time = None
        
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
    
    def is_session_valid(self) -> bool:
        """Check if current session is still valid"""
        if not self.current_user or not self.login_time:
            return False
        if time.time() - self.login_time > self.session_timeout:
            logger.info(f"Session expired for user {self.current_user}")
            self.logout()
            return False
        return True
    
    def authenticate(self, username: str, password: str) -> bool:
        """Authenticate user with session management"""
        if username not in self.users:
            logger.warning(f"Authentication failed: Unknown user {username}")
            return False
        if not self._verify_password(password, self.users[username]["password_hash"]):
            logger.warning(f"Authentication failed: Invalid password for user {username}")
            return False
        self.current_user = username
        self.login_time = time.time()
        logger.info(f"User {username} authenticated successfully")
        return True
    
    def check_permission(self, required_permission: Permission) -> bool:
        """Check if current user has required permission"""
        if not self.is_session_valid():
            return False
        user_permissions = self.users[self.current_user]["permissions"]
        has_permission = required_permission in user_permissions or Permission.ADMIN in user_permissions
        if not has_permission:
            logger.warning(f"Permission denied: User {self.current_user} lacks {required_permission}")
        return has_permission
    
    def refresh_session(self):
        """Refresh the current session timestamp"""
        if self.current_user:
            self.login_time = time.time()
    
    def logout(self):
        """Logout current user"""
        if self.current_user:
            logger.info(f"User {self.current_user} logged out")
            self.current_user = None
            self.login_time = None

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
