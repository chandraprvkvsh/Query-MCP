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
        
        from server import mcp, db_manager, auth_manager, user_consent_cache
        
        auth_manager.current_user = None
        user_consent_cache.clear()
        
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        
        await db_manager.disconnect()
        db_manager.db_path = db_path
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
    async def test_unauthorized_access_prevention(self, mcp_client):
        
        async with mcp_client as client:
            result = await client.call_tool("list_tables", {})
            result_text = self._extract_result_text(result)
            assert "Authentication required" in result_text
            
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            
            result = await client.call_tool("list_tables", {})
            result_text = self._extract_result_text(result)
            assert "Tables:" in result_text
    
    @pytest.mark.asyncio
    async def test_consent_mechanism_through_mcp(self, mcp_client):
        
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            
            await client.call_tool("grant_consent", {
                "tool_name": "create_table",
                "table": "test_consent"
            })
            
            create_result = await client.call_tool("create_table", {
                "table_name": "test_consent",
                "schema_def": {
                    "columns": {
                        "id": {"type": "INTEGER", "primary_key": True},
                        "data": {"type": "TEXT"}
                    }
                }
            })
            create_text = self._extract_result_text(create_result)
            assert "Created table: test_consent" in create_text
            
            result = await client.call_tool("insert_data", {
                "table": "test_consent",
                "row": {"data": "test"}
            })
            result_text = self._extract_result_text(result)
            assert "consent required" in result_text.lower()
            
            consent_result = await client.call_tool("grant_consent", {
                "tool_name": "insert_data",
                "table": "test_consent"
            })
            consent_text = self._extract_result_text(consent_result)
            assert "Consent granted" in consent_text
            
            result = await client.call_tool("insert_data", {
                "table": "test_consent",
                "row": {"data": "test"}
            })
            result_text = self._extract_result_text(result)
            assert "Inserted row with ID" in result_text
    
    @pytest.mark.asyncio
    async def test_complete_crud_workflow_via_mcp(self, mcp_client):
        
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            
            await client.call_tool("grant_consent", {
                "tool_name": "create_table",
                "table": "products"
            })
            
            create_result = await client.call_tool("create_table", {
                "table_name": "products",
                "schema_def": {
                    "columns": {
                        "id": {"type": "INTEGER", "primary_key": True},
                        "name": {"type": "TEXT", "not_null": True},
                        "price": {"type": "REAL", "not_null": True},
                        "category": {"type": "TEXT"}
                    }
                }
            })
            create_text = self._extract_result_text(create_result)
            assert "Created table: products" in create_text
            
            for operation in ["insert_data", "update_data", "delete_data"]:
                await client.call_tool("grant_consent", {
                    "tool_name": operation,
                    "table": "products"
                })
            
            insert_result = await client.call_tool("insert_data", {
                "table": "products",
                "row": {
                    "name": "Laptop",
                    "price": 999.99,
                    "category": "Electronics"
                }
            })
            insert_text = self._extract_result_text(insert_result)
            assert "Inserted row with ID" in insert_text
            
            read_result = await client.call_tool("read_data", {
                "table": "products"
            })
            read_text = self._extract_result_text(read_result)
            assert "Laptop" in read_text
            assert "999.99" in read_text
            
            update_result = await client.call_tool("update_data", {
                "table": "products",
                "updates": {"price": 899.99},
                "where": {"name": "Laptop"}
            })
            update_text = self._extract_result_text(update_result)
            assert "Updated 1 rows" in update_text
            
            updated_read = await client.call_tool("read_data", {
                "table": "products",
                "where": {"name": "Laptop"}
            })
            updated_text = self._extract_result_text(updated_read)
            assert "899.99" in updated_text
            
            delete_result = await client.call_tool("delete_data", {
                "table": "products",
                "where": {"name": "Laptop"}
            })
            delete_text = self._extract_result_text(delete_result)
            assert "Deleted 1 rows" in delete_text
            
            final_read = await client.call_tool("read_data", {
                "table": "products"
            })
            final_text = self._extract_result_text(final_read)
            assert "[]" in final_text
    
    @pytest.mark.asyncio
    async def test_schema_exploration_via_mcp(self, mcp_client):
        
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            
            await client.call_tool("grant_consent", {
                "tool_name": "create_table",
                "table": "schema_test"
            })
            
            await client.call_tool("create_table", {
                "table_name": "schema_test",
                "schema_def": {
                    "columns": {
                        "id": {"type": "INTEGER", "primary_key": True},
                        "name": {"type": "TEXT", "not_null": True, "unique": True},
                        "email": {"type": "TEXT", "unique": True},
                        "created_at": {"type": "DATETIME", "default": "CURRENT_TIMESTAMP"}
                    }
                }
            })
            
            tables_result = await client.call_tool("list_tables", {})
            tables_text = self._extract_result_text(tables_result)
            assert "schema_test" in tables_text
            
            describe_result = await client.call_tool("describe_table", {
                "table_name": "schema_test"
            })
            describe_text = self._extract_result_text(describe_result)
            
            assert "Schema:" in describe_text
            schema_json = describe_text.replace("Schema: ", "")
            schema_data = json.loads(schema_json)
            
            assert schema_data["table"] == "schema_test"
            assert len(schema_data["columns"]) == 4
            
            columns = {col["name"]: col for col in schema_data["columns"]}
            assert columns["id"]["primary_key"] is True
            assert columns["name"]["not_null"] is True
    
    @pytest.mark.asyncio
    async def test_resource_access_via_mcp(self, mcp_client):

        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })

            resources = await client.list_resources()
            resource_uris = [str(r.uri) for r in resources]
            assert "db://schema" in resource_uris

            schema_resource = await client.read_resource("db://schema")
            assert isinstance(schema_resource, list)
            assert len(schema_resource) > 0

            schema_data = json.loads(schema_resource[0].text)
            assert "tables" in schema_data
            assert isinstance(schema_data["tables"], dict)

    @pytest.mark.asyncio
    async def test_permission_enforcement_via_mcp(self, mcp_client):
        
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "readonly",
                "password": "readonly123"
            })
            
            tables_result = await client.call_tool("list_tables", {})
            tables_text = self._extract_result_text(tables_result)
            assert "Tables:" in tables_text
            
            write_result = await client.call_tool("create_table", {
                "table_name": "unauthorized",
                "schema_def": {"columns": {"id": {"type": "INTEGER"}}}
            })
            write_text = self._extract_result_text(write_result)
            assert "Insufficient permissions" in write_text
    
    @pytest.mark.asyncio
    async def test_mcp_tool_listing(self, mcp_client):
        
        async with mcp_client as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            
            expected_tools = [
                "authenticate", "grant_consent", "list_tables", "describe_table",
                "read_data", "insert_data", "update_data", "delete_data", 
                "create_table", "drop_table"
            ]
            
            for expected_tool in expected_tools:
                assert expected_tool in tool_names, f"Missing tool: {expected_tool}"
            