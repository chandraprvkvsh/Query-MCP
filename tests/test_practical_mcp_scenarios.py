import json
import os
import tempfile
import pytest

from fastmcp import Client


class TestPracticalMCPScenarios:
    @staticmethod
    def _t(result):
        if hasattr(result, "data") and result.data is not None:
            return str(result.data)
        if hasattr(result, "content") and result.content:
            for block in result.content:
                if hasattr(block, "text"):
                    return block.text
        return str(result)

    @pytest.fixture
    async def mcp_client(self):
        from server import mcp, db_manager, auth_manager, user_consent_cache

        auth_manager.current_user = None
        auth_manager.login_time = None
        user_consent_cache.clear()

        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)

        await db_manager.disconnect()
        db_manager.db_path = db_path
        db_manager._connection = None
        db_manager._connection_healthy = False
        await db_manager.connect()

        client = Client(mcp)
        yield client

        await db_manager.disconnect()
        os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_ecommerce_inventory(self, mcp_client):
        async with mcp_client as client:
            await client.call_tool("authenticate", {"username": "admin", "password": "admin123"})
            await client.call_tool("grant_consent", {"tool_name": "create_table", "table": "inventory"})
            await client.call_tool(
                "create_table",
                {
                    "table_name": "inventory",
                    "schema_def": {
                        "columns": {
                            "id": {"type": "INTEGER", "primary_key": True},
                            "sku": {"type": "TEXT", "unique": True, "not_null": True},
                            "name": {"type": "TEXT", "not_null": True},
                            "quantity": {"type": "INTEGER", "not_null": True},
                            "price": {"type": "REAL", "not_null": True},
                        }
                    },
                },
            )

            for tool in ("insert_data", "update_data", "delete_data"):
                await client.call_tool("grant_consent", {"tool_name": tool, "table": "inventory"})

            products = [
                {"sku": "SKU1", "name": "MacBook 14", "quantity": 10, "price": 2499.0},
                {"sku": "SKU2", "name": "Surface Laptop", "quantity": 5, "price": 1899.0},
                {"sku": "SKU3", "name": "ThinkPad X1", "quantity": 8, "price": 2199.0},
            ]

            for p in products:
                res = await client.call_tool("insert_data", {"table": "inventory", "row": p})
                assert "Inserted" in self._t(res)

            low_stock = await client.call_tool(
                "update_data",
                {"table": "inventory", "updates": {"quantity": 2}, "where": {"sku": "SKU2"}},
            )
            assert "Updated 1 rows" in self._t(low_stock)

            res = await client.call_tool(
                "delete_data", {"table": "inventory", "where": {"sku": "SKU3"}}
            )
            assert "Deleted 1 rows" in self._t(res)

            res = await client.call_tool("read_data", {"table": "inventory"})
            assert len(json.loads(self._t(res).replace("Data: ", ""))) == 2

    @pytest.mark.asyncio
    async def test_user_management(self, mcp_client):
        async with mcp_client as client:
            await client.call_tool("authenticate", {"username": "admin", "password": "admin123"})
            
            table_name = "company_users"
            await client.call_tool("grant_consent", {"tool_name": "create_table", "table": table_name})

            await client.call_tool(
                "create_table",
                {
                    "table_name": table_name,
                    "schema_def": {
                        "columns": {
                            "id": {"type": "INTEGER", "primary_key": True},
                            "username": {"type": "TEXT", "unique": True, "not_null": True},
                            "email": {"type": "TEXT", "unique": True, "not_null": True},
                            "role": {"type": "TEXT", "not_null": True},
                            "active": {"type": "INTEGER", "default": "1"},
                        }
                    },
                },
            )

            for tool in ("insert_data", "update_data", "delete_data"):
                await client.call_tool("grant_consent", {"tool_name": tool, "table": table_name})

            users = [
                {"username": "admin_u", "email": "admin@corp.com", "role": "admin", "active": 1},
                {"username": "emp_1", "email": "e1@corp.com", "role": "employee", "active": 1},
                {"username": "emp_2", "email": "e2@corp.com", "role": "employee", "active": 0},
            ]

            for u in users:
                res = await client.call_tool("insert_data", {"table": table_name, "row": u})
                assert "Inserted" in self._t(res)

            res = await client.call_tool(
                "update_data",
                {"table": table_name, "updates": {"active": 0}, "where": {"username": "emp_1"}},
            )
            assert "Updated 1 rows" in self._t(res)

            res = await client.call_tool("read_data", {"table": table_name, "where": {"active": 1}})
            assert len(json.loads(self._t(res).replace("Data: ", ""))) == 1

    @pytest.mark.asyncio
    async def test_sample_data_management(self, mcp_client):
        """Test using the existing sample data tables (users/posts)"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {"username": "admin", "password": "admin123"})
            
            for tool in ("insert_data", "update_data", "delete_data"):
                await client.call_tool("grant_consent", {"tool_name": tool, "table": "users"})
            
            res = await client.call_tool("read_data", {"table": "users"})
            existing_data = json.loads(self._t(res).replace("Data: ", ""))
            original_count = len(existing_data)
            
            new_user = {
                "name": "Alice Johnson", 
                "email": "alice@example.com"
            }
            
            res = await client.call_tool("insert_data", {"table": "users", "row": new_user})
            assert "Inserted" in self._t(res)
            
            res = await client.call_tool(
                "update_data",
                {"table": "users", "updates": {"name": "Alice Smith"}, "where": {"email": "alice@example.com"}}
            )
            assert "Updated 1 rows" in self._t(res)
            
            res = await client.call_tool("read_data", {"table": "users"})
            final_data = json.loads(self._t(res).replace("Data: ", ""))
            assert len(final_data) == original_count + 1
            
            alice_users = [u for u in final_data if u.get("email") == "alice@example.com"]
            assert len(alice_users) == 1
            assert alice_users[0]["name"] == "Alice Smith"

    @pytest.mark.asyncio
    async def test_production_monitoring(self, mcp_client):
        async with mcp_client as client:
            res = await client.call_tool("health_check", {})
            health_data = json.loads(self._t(res))
            assert health_data["server"] == "healthy"

            await client.call_tool("authenticate", {"username": "admin", "password": "admin123"})

            uris = [str(r.uri) for r in await client.list_resources()]
            assert {"db://health", "db://schema"}.issubset(uris)

            health_res = await client.read_resource("db://health")
            health_data = json.loads(health_res[0].text if isinstance(health_res, list) else health_res)
            assert health_data["server"] == "healthy"

    @pytest.mark.asyncio
    async def test_schema_exploration_with_sample_data(self, mcp_client):
        """Test that we can explore the existing sample data schema"""
        async with mcp_client as client:
            await client.call_tool("authenticate", {"username": "admin", "password": "admin123"})
            
            res = await client.call_tool("list_tables", {})
            tables_text = self._t(res)
            assert "users" in tables_text
            assert "posts" in tables_text
            
            res = await client.call_tool("describe_table", {"table_name": "users"})
            schema_text = self._t(res)
            assert "Schema:" in schema_text
            
            schema_json = schema_text.replace("Schema: ", "")
            schema_data = json.loads(schema_json)
            
            assert schema_data["table"] == "users"
            assert len(schema_data["columns"]) >= 4
            
            column_names = [col["name"] for col in schema_data["columns"]]
            expected_columns = ["id", "name", "email", "created_at"]
            for col in expected_columns:
                assert col in column_names
