import sqlite3
import asyncio
from typing import Dict, List, Any, Optional, Union
import json
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Secure database manager with SQL injection protection"""
    
    def __init__(self, db_path: str = "database.db"):
        self.db_path = db_path
        self._connection = None
        self._lock = asyncio.Lock()
        
    async def connect(self):
        """Initialize database connection"""
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        
    async def disconnect(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            
    @asynccontextmanager
    async def get_cursor(self):
        """Thread-safe cursor context manager"""
        async with self._lock:
            cursor = self._connection.cursor()
            try:
                yield cursor
                self._connection.commit()
            except Exception as e:
                self._connection.rollback()
                raise e
            finally:
                cursor.close()
    
    def _sanitize_identifier(self, identifier: str) -> str:
        """Sanitize table/column names to prevent injection"""
        if not identifier.replace('_', '').replace('-', '').isalnum():
            raise ValueError(f"Invalid identifier: {identifier}")
        return identifier
    
    def _build_where_clause(self, where_dict: Dict[str, Any]) -> tuple:
        """Build parameterized WHERE clause"""
        if not where_dict:
            return "", []
            
        conditions = []
        params = []
        for key, value in where_dict.items():
            safe_key = self._sanitize_identifier(key)
            conditions.append(f"{safe_key} = ?")
            params.append(value)
            
        return f"WHERE {' AND '.join(conditions)}", params
    
    async def list_tables(self) -> List[str]:
        """List all tables in database"""
        async with self.get_cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall()]
    
    async def describe_table(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        safe_table = self._sanitize_identifier(table_name)
        
        async with self.get_cursor() as cursor:
            cursor.execute(f"PRAGMA table_info({safe_table})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[1],
                    "type": row[2],
                    "not_null": bool(row[3]),
                    "default": row[4],
                    "primary_key": bool(row[5])
                })
            
            cursor.execute(f"PRAGMA foreign_key_list({safe_table})")
            foreign_keys = [dict(row) for row in cursor.fetchall()]
            
            cursor.execute(f"PRAGMA index_list({safe_table})")
            indexes = [dict(row) for row in cursor.fetchall()]
            
            return {
                "table": table_name,
                "columns": columns,
                "foreign_keys": foreign_keys,
                "indexes": indexes
            }
    
    async def read_data(self, table: str, where: Optional[Dict] = None, 
                       limit: Optional[int] = None, order_by: Optional[str] = None) -> List[Dict]:
        """SELECT data with parameterized queries"""
        safe_table = self._sanitize_identifier(table)
        query = f"SELECT * FROM {safe_table}"
        params = []
        
        if where:
            where_clause, where_params = self._build_where_clause(where)
            query += f" {where_clause}"
            params.extend(where_params)
            
        if order_by:
            safe_order = self._sanitize_identifier(order_by)
            query += f" ORDER BY {safe_order}"
            
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        async with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    async def insert_data(self, table: str, row: Dict[str, Any]) -> int:
        """INSERT data with parameterized query"""
        safe_table = self._sanitize_identifier(table)
        
        columns = list(row.keys())
        safe_columns = [self._sanitize_identifier(col) for col in columns]
        placeholders = ",".join(["?" for _ in columns])
        
        query = f"INSERT INTO {safe_table} ({','.join(safe_columns)}) VALUES ({placeholders})"
        
        async with self.get_cursor() as cursor:
            cursor.execute(query, list(row.values()))
            return cursor.lastrowid
    
    async def update_data(self, table: str, updates: Dict[str, Any], 
                         where: Dict[str, Any]) -> int:
        """UPDATE data with parameterized query"""
        safe_table = self._sanitize_identifier(table)
        
        set_clause = []
        params = []
        for key, value in updates.items():
            safe_key = self._sanitize_identifier(key)
            set_clause.append(f"{safe_key} = ?")
            params.append(value)
            
        where_clause, where_params = self._build_where_clause(where)
        params.extend(where_params)
        
        query = f"UPDATE {safe_table} SET {','.join(set_clause)} {where_clause}"
        
        async with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
    
    async def delete_data(self, table: str, where: Dict[str, Any]) -> int:
        """DELETE data with parameterized query"""
        safe_table = self._sanitize_identifier(table)
        
        if not where:
            raise ValueError("DELETE requires WHERE clause for safety")
            
        where_clause, params = self._build_where_clause(where)
        query = f"DELETE FROM {safe_table} {where_clause}"
        
        async with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
    
    async def create_table(self, table_name: str, schema_def: Dict[str, Any]) -> None:
        """CREATE TABLE with schema definition"""
        safe_table = self._sanitize_identifier(table_name)
        
        columns = []
        for col_name, col_def in schema_def.get("columns", {}).items():
            safe_col = self._sanitize_identifier(col_name)
            col_type = col_def.get("type", "TEXT")
            constraints = []
            
            if col_def.get("primary_key"):
                constraints.append("PRIMARY KEY")
            if col_def.get("not_null"):
                constraints.append("NOT NULL")
            if col_def.get("unique"):
                constraints.append("UNIQUE")
            if "default" in col_def:
                constraints.append(f"DEFAULT {col_def['default']}")
                
            columns.append(f"{safe_col} {col_type} {' '.join(constraints)}")
        
        query = f"CREATE TABLE {safe_table} ({','.join(columns)})"
        
        async with self.get_cursor() as cursor:
            cursor.execute(query)
    
    async def drop_table(self, table_name: str) -> None:
        """DROP TABLE"""
        safe_table = self._sanitize_identifier(table_name)
        query = f"DROP TABLE {safe_table}"
        
        async with self.get_cursor() as cursor:
            cursor.execute(query)
    
    async def get_full_schema(self) -> Dict[str, Any]:
        """Get complete database schema"""
        tables = await self.list_tables()
        schema = {"tables": {}}
        
        for table in tables:
            schema["tables"][table] = await self.describe_table(table)
            
        return schema
