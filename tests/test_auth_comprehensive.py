import pytest
from ..auth import AuthManager, Permission, TOOL_PERMISSIONS, DESTRUCTIVE_TOOLS

class TestAuthManager:
    
    @pytest.fixture
    def auth_manager(self):
        """Create fresh AuthManager for each test"""
        return AuthManager()
    
    def test_password_hashing_and_verification(self, auth_manager):
        """Test password hashing security"""
        password = "test_password_123"
        
        hash1 = auth_manager._hash_password(password)
        hash2 = auth_manager._hash_password(password)
        assert hash1 != hash2
        
        assert auth_manager._verify_password(password, hash1)
        assert auth_manager._verify_password(password, hash2)
        
        assert not auth_manager._verify_password("wrong_password", hash1)
        
        assert not auth_manager._verify_password(password, "corrupted_hash")
    
    def test_user_authentication_flow(self, auth_manager):
        """Test complete authentication workflow"""
        
        assert auth_manager.current_user is None
        
        assert auth_manager.authenticate("admin", "admin123") is True
        assert auth_manager.current_user == "admin"
        
        auth_manager.logout()
        assert auth_manager.authenticate("nonexistent", "password") is False
        assert auth_manager.current_user is None
        
        assert auth_manager.authenticate("admin", "wrongpassword") is False
        assert auth_manager.current_user is None
        
        assert auth_manager.authenticate("readonly", "readonly123") is True
        assert auth_manager.current_user == "readonly"
    
    def test_permission_system(self, auth_manager):
        """Test role-based permission system"""
        
        auth_manager.authenticate("admin", "admin123")
        assert auth_manager.check_permission(Permission.READ) is True
        assert auth_manager.check_permission(Permission.WRITE) is True
        assert auth_manager.check_permission(Permission.CREATE) is True
        assert auth_manager.check_permission(Permission.DELETE) is True
        assert auth_manager.check_permission(Permission.ADMIN) is True
        
        auth_manager.authenticate("readonly", "readonly123")
        assert auth_manager.check_permission(Permission.READ) is True
        assert auth_manager.check_permission(Permission.WRITE) is False
        assert auth_manager.check_permission(Permission.CREATE) is False
        assert auth_manager.check_permission(Permission.DELETE) is False
        assert auth_manager.check_permission(Permission.ADMIN) is False
        
        auth_manager.logout()
        assert auth_manager.check_permission(Permission.READ) is False
    
    def test_tool_permission_mapping(self):
        """Test that all tools have proper permission mappings"""
        
        expected_tools = [
            "list_tables", "describe_table", "read_data",
            "insert_data", "update_data", "delete_data",
            "create_table", "drop_table"
        ]
        
        for tool in expected_tools:
            assert tool in TOOL_PERMISSIONS
            assert isinstance(TOOL_PERMISSIONS[tool], Permission)
        
        assert "insert_data" in DESTRUCTIVE_TOOLS
        assert "update_data" in DESTRUCTIVE_TOOLS
        assert "delete_data" in DESTRUCTIVE_TOOLS
        assert "drop_table" in DESTRUCTIVE_TOOLS
        assert "create_table" in DESTRUCTIVE_TOOLS
        
        assert "list_tables" not in DESTRUCTIVE_TOOLS
        assert "describe_table" not in DESTRUCTIVE_TOOLS
        assert "read_data" not in DESTRUCTIVE_TOOLS
    
    def test_logout_functionality(self, auth_manager):
        """Test logout clears current user"""
        auth_manager.authenticate("admin", "admin123")
        assert auth_manager.current_user == "admin"
        
        auth_manager.logout()
        assert auth_manager.current_user is None
        assert auth_manager.check_permission(Permission.READ) is False
    
    def test_admin_permission_override(self, auth_manager):
        """Test that ADMIN permission grants access to everything"""
        auth_manager.authenticate("admin", "admin123")
        
        for permission in Permission:
            assert auth_manager.check_permission(permission) is True
    
    def test_logout_functionality(self, auth_manager):
        """Test logout clears current user"""
        auth_manager.authenticate("admin", "admin123")
        assert auth_manager.current_user == "admin"
        
        auth_manager.logout()
        assert auth_manager.current_user is None
        assert auth_manager.check_permission(Permission.READ) is False
    
    def test_admin_permission_override(self, auth_manager):
        """Test that ADMIN permission grants access to everything"""
        auth_manager.authenticate("admin", "admin123")
        
        # Admin should have access to all permission types
        for permission in Permission:
            assert auth_manager.check_permission(permission) is True
