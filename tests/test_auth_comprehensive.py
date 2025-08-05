import time
import pytest

from ..auth import AuthManager, Permission, TOOL_PERMISSIONS, DESTRUCTIVE_TOOLS


class TestAuthManager:
    @pytest.fixture
    def auth_manager(self):
        """Return a fresh AuthManager for every test."""
        return AuthManager()

    def test_password_hashing_and_verification(self, auth_manager):
        password = "test_password_123"

        hash1 = auth_manager._hash_password(password)
        hash2 = auth_manager._hash_password(password)

        assert hash1 != hash2
        assert auth_manager._verify_password(password, hash1)
        assert auth_manager._verify_password(password, hash2)
        assert not auth_manager._verify_password("wrong_password", hash1)
        assert not auth_manager._verify_password(password, "deadbeef")

    def test_user_authentication_flow(self, auth_manager):
        assert auth_manager.current_user is None
        assert auth_manager.authenticate("admin", "admin123") is True
        assert auth_manager.current_user == "admin"
        auth_manager.logout()
        assert auth_manager.current_user is None
        assert auth_manager.authenticate("ghost", "nopass") is False
        assert auth_manager.current_user is None
        assert auth_manager.authenticate("admin", "badpass") is False
        assert auth_manager.current_user is None
        assert auth_manager.authenticate("readonly", "readonly123") is True
        assert auth_manager.current_user == "readonly"

    def test_session_timeout_enforcement(self, auth_manager):
        auth_manager.session_timeout = 1
        assert auth_manager.authenticate("admin", "admin123")
        assert auth_manager.is_session_valid() is True
        time.sleep(1.5)
        assert auth_manager.is_session_valid() is False
        assert auth_manager.current_user is None

    def test_permission_system(self, auth_manager):
        auth_manager.authenticate("admin", "admin123")
        for perm in Permission:
            assert auth_manager.check_permission(perm)
        auth_manager.authenticate("readonly", "readonly123")
        assert auth_manager.check_permission(Permission.READ)
        for perm in (Permission.WRITE, Permission.CREATE, Permission.DELETE, Permission.ADMIN):
            assert not auth_manager.check_permission(perm)
        auth_manager.logout()
        assert not auth_manager.check_permission(Permission.READ)

    def test_tool_permission_mapping(self):
        expected_tools = {
            "list_tables",
            "describe_table",
            "read_data",
            "insert_data",
            "update_data",
            "delete_data",
            "create_table",
            "drop_table",
        }

        assert expected_tools.issubset(TOOL_PERMISSIONS.keys())
        for p in TOOL_PERMISSIONS.values():
            assert isinstance(p, Permission)
        destructive_expected = {
            "insert_data",
            "update_data",
            "delete_data",
            "drop_table",
            "create_table",
        }
        assert destructive_expected == DESTRUCTIVE_TOOLS
        for safe_tool in {"list_tables", "describe_table", "read_data"}:
            assert safe_tool not in DESTRUCTIVE_TOOLS

    def test_logout_functionality(self, auth_manager):
        auth_manager.authenticate("admin", "admin123")
        assert auth_manager.current_user == "admin"
        auth_manager.logout()
        assert auth_manager.current_user is None
        assert not auth_manager.check_permission(Permission.READ)
