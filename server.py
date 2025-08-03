import asyncio
import logging
import json
from typing import Dict, List, Any, Optional, Union
from fastmcp import FastMCP

from database import DatabaseManager
from auth import AuthManager, TOOL_PERMISSIONS, DESTRUCTIVE_TOOLS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP(name="Database MCP Server")

db_manager = DatabaseManager()
auth_manager = AuthManager()
user_consent_cache: Dict[str, set] = {}

async def create_sample_data():
    """Create sample tables and data"""
    await db_manager.create_table("users", {
        "columns": {
            "id": {"type": "INTEGER", "primary_key": True},
            "name": {"type": "TEXT", "not_null": True},
            "email": {"type": "TEXT", "unique": True, "not_null": True},
            "created_at": {"type": "DATETIME", "default": "CURRENT_TIMESTAMP"}
        }
    })
    await db_manager.create_table("posts", {
        "columns": {
            "id": {"type": "INTEGER", "primary_key": True},
            "user_id": {"type": "INTEGER", "not_null": True},
            "title": {"type": "TEXT", "not_null": True},
            "content": {"type": "TEXT"},
            "created_at": {"type": "DATETIME", "default": "CURRENT_TIMESTAMP"}
        }
    })
    user_id = await db_manager.insert_data("users", {
        "name": "John Doe",
        "email": "john@example.com"
    })
    await db_manager.insert_data("posts", {
        "user_id": user_id,
        "title": "Welcome Post",
        "content": "This is a sample post"
    })

def check_auth_and_consent(tool_name: str, params: Dict = None) -> str:
    """Check authentication and consent for tools"""
    if not auth_manager.current_user:
        return "Authentication required. Use authenticate tool first."
    required_permission = TOOL_PERMISSIONS.get(tool_name)
    if required_permission and not auth_manager.check_permission(required_permission):
        return f"Insufficient permissions for {tool_name}"
    if tool_name in DESTRUCTIVE_TOOLS:
        consent_key = f"{tool_name}:{params.get('table', '') if params else ''}"
        user_consents = user_consent_cache.get(auth_manager.current_user, set())
        if consent_key not in user_consents:
            return f"User consent required for {tool_name}. Use grant_consent tool first."
    return None

@mcp.tool
def authenticate(username: str, password: str) -> str:
    """Authenticate with username and password"""
    if auth_manager.authenticate(username, password):
        return "Authentication successful"
    else:
        return "Authentication failed"

@mcp.tool
def grant_consent(tool_name: str, table: str = "") -> str:
    """Grant consent for destructive operations"""
    if not auth_manager.current_user:
        return "Authentication required"
    consent_key = f"{tool_name}:{table}"
    if auth_manager.current_user not in user_consent_cache:
        user_consent_cache[auth_manager.current_user] = set()
    user_consent_cache[auth_manager.current_user].add(consent_key)
    return f"Consent granted for {tool_name} on table {table}"

@mcp.tool
async def list_tables() -> str:
    """List all tables in the database"""
    auth_error = check_auth_and_consent("list_tables")
    if auth_error:
        return auth_error
    try:
        tables = await db_manager.list_tables()
        logger.info(f"Listed tables for user {auth_manager.current_user}")
        return f"Tables: {tables}"
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def describe_table(table_name: str) -> str:
    """Get detailed schema information for a table"""
    auth_error = check_auth_and_consent("describe_table")
    if auth_error:
        return auth_error
    try:
        schema = await db_manager.describe_table(table_name)
        logger.info(f"Described table {table_name} for user {auth_manager.current_user}")
        return f"Schema: {json.dumps(schema, indent=2)}"
    except Exception as e:
        logger.error(f"Error describing table {table_name}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def read_data(table: str, where: Optional[Dict] = None, 
                   limit: Optional[int] = None, order_by: Optional[str] = None) -> str:
    """Read data from a table with optional filtering"""
    auth_error = check_auth_and_consent("read_data")
    if auth_error:
        return auth_error
    try:
        data = await db_manager.read_data(table, where, limit, order_by)
        logger.info(f"Read data from {table} for user {auth_manager.current_user}")
        return f"Data: {json.dumps(data, indent=2)}"
    except Exception as e:
        logger.error(f"Error reading from {table}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def insert_data(table: str, row: Dict[str, Any]) -> str:
    """Insert a new row into a table"""
    auth_error = check_auth_and_consent("insert_data", {"table": table})
    if auth_error:
        return auth_error
    try:
        row_id = await db_manager.insert_data(table, row)
        logger.info(f"Inserted data into {table} for user {auth_manager.current_user}")
        return f"Inserted row with ID: {row_id}"
    except Exception as e:
        logger.error(f"Error inserting into {table}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def update_data(table: str, updates: Dict[str, Any], where: Dict[str, Any]) -> str:
    """Update existing rows in a table"""
    auth_error = check_auth_and_consent("update_data", {"table": table})
    if auth_error:
        return auth_error
    try:
        count = await db_manager.update_data(table, updates, where)
        logger.info(f"Updated {count} rows in {table} for user {auth_manager.current_user}")
        return f"Updated {count} rows"
    except Exception as e:
        logger.error(f"Error updating {table}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def delete_data(table: str, where: Dict[str, Any]) -> str:
    """Delete rows from a table"""
    auth_error = check_auth_and_consent("delete_data", {"table": table})
    if auth_error:
        return auth_error
    try:
        count = await db_manager.delete_data(table, where)
        logger.info(f"Deleted {count} rows from {table} for user {auth_manager.current_user}")
        return f"Deleted {count} rows"
    except Exception as e:
        logger.error(f"Error deleting from {table}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def create_table(table_name: str, schema_def: Dict[str, Any]) -> str:
    """Create a new table with specified schema"""
    auth_error = check_auth_and_consent("create_table", {"table": table_name})
    if auth_error:
        return auth_error
    try:
        await db_manager.create_table(table_name, schema_def)
        logger.info(f"Created table {table_name} for user {auth_manager.current_user}")
        return f"Created table: {table_name}"
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        return f"Error: {str(e)}"

@mcp.tool
async def drop_table(table_name: str) -> str:
    """Drop an existing table"""
    auth_error = check_auth_and_consent("drop_table", {"table": table_name})
    if auth_error:
        return auth_error
    try:
        await db_manager.drop_table(table_name)
        logger.info(f"Dropped table {table_name} for user {auth_manager.current_user}")
        return f"Dropped table: {table_name}"
    except Exception as e:
        logger.error(f"Error dropping table {table_name}: {e}")
        return f"Error: {str(e)}"

@mcp.resource("db://schema")
async def get_database_schema() -> str:
    """Get complete database schema as JSON"""
    if not auth_manager.current_user:
        return "Authentication required"
    try:
        schema = await db_manager.get_full_schema()
        return json.dumps(schema, indent=2)
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        return f"Error: {str(e)}"

@mcp.resource("db://table/{table_name}")
async def get_table_info(table_name: str) -> str:
    """Get information about a specific table"""
    if not auth_manager.current_user:
        return "Authentication required"
    try:
        schema = await db_manager.describe_table(table_name)
        return json.dumps(schema, indent=2)
    except Exception as e:
        logger.error(f"Error getting table info for {table_name}: {e}")
        return f"Error: {str(e)}"

async def initialize_server():
    """Initialize database connection and sample data"""
    await db_manager.connect()
    logger.info("Database connected")
    tables = await db_manager.list_tables()
    if not tables:
        await create_sample_data()
        logger.info("Sample data created")

if __name__ == "__main__":
    async def startup():
        await initialize_server()
    asyncio.run(startup())
    mcp.run()
