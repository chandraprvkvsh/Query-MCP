"""
Configuration settings for MCP Database Server
"""
import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "./production.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

SESSION_TIMEOUT = int(os.getenv("SESSION_TIMEOUT", "3600"))  # 1 hour
REQUIRE_HTTPS = os.getenv("REQUIRE_HTTPS", "true").lower() == "true"
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "100"))

SERVER_NAME = os.getenv("SERVER_NAME", "Database MCP Server")
ENABLE_SAMPLE_DATA = os.getenv("ENABLE_SAMPLE_DATA", "true").lower() == "true"
