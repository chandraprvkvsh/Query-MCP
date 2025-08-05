import pytest
import asyncio
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastmcp import Client

class TestMCPServerIntegration:

    @pytest.fixture
    async def mcp_client(self):
        """Setup FastMCP Client with isolated server state"""
        from ..server import mcp, db_manager, auth_manager, user_consent_cache

        auth_manager.current_user = None
        auth_manager.login_time = None
        user_consent_cache.clear()

        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)

        await db_manager.disconnect()
        db_manager.db_path = db_path
        db_manager._connection = None
        db_manager._connection_healthy = False
        await db_manager.connect()

        client = Client(mcp)

        yield client

        await db_manager.disconnect()
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
            except:
                pass

    def _extract_result_text(self, result):
        """Extract text content from CallToolResult"""
        if hasattr(result, 'data') and result.data is not None:
            return str(result.data)
        if hasattr(result, 'content') and result.content:
            for content in result.content:
                if hasattr(content, 'text'):
                    return content.text
        return str(result)

    @pytest.mark.asyncio
    async def test_authentication_workflow(self, mcp_client):
        """Test complete authentication workflow through MCP tools"""
        async with mcp_client as client:
            result = await client.call_tool("authenticate", {
                "username": "admin",
                "password": "wrongpassword"
            })
            result_text = self._extract_result_text(result)
            assert "Authentication failed" in result_text

            result = await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            result_text = self._extract_result_text(result)
            assert "Authentication successful" in result_text

    @pytest.mark.asyncio
    async def test_health_check_tool(self, mcp_client):
        """Test the new health check functionality"""
        async with mcp_client as client:
            result = await client.call_tool("health_check", {})
            result_text = self._extract_result_text(result)
            health_data = json.loads(result_text)

            assert "server" in health_data
            assert "database" in health_data
            assert health_data["server"] == "healthy"

    @pytest.mark.asyncio
    async def test_logout_tool(self, mcp_client):
        """Test the new logout functionality"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })

            result = await client.call_tool("logout", {})
            result_text = self._extract_result_text(result)
            assert "Logout successful" in result_text

            result = await client.call_tool("list_tables", {})
            result_text = self._extract_result_text(result)
            assert "Authentication required" in result_text

    @pytest.mark.asyncio
    async def test_resource_access_via_mcp(self, mcp_client):
        """Test MCP resource access"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })

            resources = await client.list_resources()
            resource_uris = [str(r.uri) for r in resources]
            assert "db://schema" in resource_uris
            assert "db://health" in resource_uris

            health_resource = await client.read_resource("db://health")
            if isinstance(health_resource, list) and len(health_resource) > 0:
                health_data = json.loads(health_resource[0].text)
            else:
                health_data = json.loads(health_resource)
            assert "server" in health_data

    @pytest.mark.asyncio
    async def test_mcp_tool_listing(self, mcp_client):
        """Test that all expected MCP tools are available"""
        async with mcp_client as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]

            expected_tools = [
                "authenticate", "grant_consent", "logout", "list_tables", 
                "describe_table", "read_data", "insert_data", "update_data", 
                "delete_data", "create_table", "drop_table", "health_check"
            ]

            for expected_tool in expected_tools:
                assert expected_tool in tool_names, f"Missing tool: {expected_tool}"
