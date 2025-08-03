import pytest
import asyncio
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastmcp import Client

class TestPracticalMCPScenarios:
    """Practical scenario tests using MCP tools through FastMCP Client"""
    
    @pytest.fixture
    async def mcp_client(self):
        """Setup FastMCP Client for practical testing"""
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
    async def test_ecommerce_inventory_via_mcp(self, mcp_client):
        """Test e-commerce inventory management through MCP"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            await client.call_tool("grant_consent", {
                "tool_name": "create_table",
                "table": "inventory"
            })
            await client.call_tool("create_table", {
                "table_name": "inventory",
                "schema_def": {
                    "columns": {
                        "id": {"type": "INTEGER", "primary_key": True},
                        "sku": {"type": "TEXT", "unique": True, "not_null": True},
                        "name": {"type": "TEXT", "not_null": True},
                        "quantity": {"type": "INTEGER", "not_null": True},
                        "price": {"type": "REAL", "not_null": True},
                        "category": {"type": "TEXT"},
                        "supplier": {"type": "TEXT"}
                    }
                }
            })
            for operation in ["insert_data", "update_data", "delete_data"]:
                await client.call_tool("grant_consent", {
                    "tool_name": operation,
                    "table": "inventory"
                })
            products = [
                {"sku": "LAP001", "name": "MacBook Pro 16", "quantity": 15, "price": 2499.99, "category": "Laptops", "supplier": "Apple"},
                {"sku": "LAP002", "name": "Dell XPS 13", "quantity": 8, "price": 1299.99, "category": "Laptops", "supplier": "Dell"},
                {"sku": "PHN001", "name": "iPhone 15 Pro", "quantity": 25, "price": 999.99, "category": "Phones", "supplier": "Apple"}
            ]
            for product in products:
                result = await client.call_tool("insert_data", {
                    "table": "inventory",
                    "row": product
                })
                result_text = self._extract_result_text(result)
                assert "Inserted row with ID" in result_text
            all_items = await client.call_tool("read_data", {
                "table": "inventory"
            })
            all_items_text = self._extract_result_text(all_items)
            assert "MacBook Pro 16" in all_items_text
            assert "iPhone 15 Pro" in all_items_text
            items_data = json.loads(all_items_text.replace("Data: ", ""))
            assert len(items_data) == 3
            low_stock_update = await client.call_tool("update_data", {
                "table": "inventory",
                "updates": {"quantity": 2},
                "where": {"sku": "LAP002"}
            })
            update_text = self._extract_result_text(low_stock_update)
            assert "Updated 1 rows" in update_text
            updated_item = await client.call_tool("read_data", {
                "table": "inventory",
                "where": {"sku": "LAP002"}
            })
            updated_text = self._extract_result_text(updated_item)
            assert '"quantity": 2' in updated_text
            discontinued_result = await client.call_tool("delete_data", {
                "table": "inventory",
                "where": {"sku": "PHN001"}
            })
            delete_text = self._extract_result_text(discontinued_result)
            assert "Deleted 1 rows" in delete_text
            final_count = await client.call_tool("read_data", {"table": "inventory"})
            final_text = self._extract_result_text(final_count)
            remaining_data = json.loads(final_text.replace("Data: ", ""))
            assert len(remaining_data) == 2
            assert "iPhone 15 Pro" not in final_text
    
    @pytest.mark.asyncio
    async def test_user_management_via_mcp(self, mcp_client):
        """Test user management system through MCP"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {
                "username": "admin",
                "password": "admin123"
            })
            await client.call_tool("grant_consent", {
                "tool_name": "create_table",
                "table": "users"
            })
            await client.call_tool("create_table", {
                "table_name": "users",
                "schema_def": {
                    "columns": {
                        "id": {"type": "INTEGER", "primary_key": True},
                        "username": {"type": "TEXT", "unique": True, "not_null": True},
                        "email": {"type": "TEXT", "unique": True, "not_null": True},
                        "role": {"type": "TEXT", "not_null": True},
                        "active": {"type": "INTEGER", "default": "1"},
                        "created_at": {"type": "DATETIME", "default": "CURRENT_TIMESTAMP"}
                    }
                }
            })
            for operation in ["insert_data", "update_data", "delete_data"]:
                await client.call_tool("grant_consent", {
                    "tool_name": operation,
                    "table": "users"
                })
            users = [
                {"username": "admin_user", "email": "admin@company.com", "role": "admin", "active": 1},
                {"username": "manager_user", "email": "manager@company.com", "role": "manager", "active": 1},
                {"username": "employee1", "email": "emp1@company.com", "role": "employee", "active": 1},
                {"username": "employee2", "email": "emp2@company.com", "role": "employee", "active": 0}
            ]
            for user in users:
                result = await client.call_tool("insert_data", {
                    "table": "users",
                    "row": user
                })
                result_text = self._extract_result_text(result)
                assert "Inserted row with ID" in result_text
            admins = await client.call_tool("read_data", {
                "table": "users",
                "where": {"role": "admin"}
            })
            admins_text = self._extract_result_text(admins)
            assert "admin_user" in admins_text
            active_users = await client.call_tool("read_data", {
                "table": "users",
                "where": {"active": 1}
            })
            active_text = self._extract_result_text(active_users)
            active_data = json.loads(active_text.replace("Data: ", ""))
            assert len(active_data) == 3
            deactivate_result = await client.call_tool("update_data", {
                "table": "users",
                "updates": {"active": 0},
                "where": {"username": "employee1"}
            })
            deactivate_text = self._extract_result_text(deactivate_result)
            assert "Updated 1 rows" in deactivate_text
            final_active = await client.call_tool("read_data", {
                "table": "users",
                "where": {"active": 1}
            })
            final_text = self._extract_result_text(final_active)
            final_data = json.loads(final_text.replace("Data: ", ""))
            assert len(final_data) == 2
