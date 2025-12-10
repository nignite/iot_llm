#!/usr/bin/env python3
"""
Database abstraction layer for IoT Database Application
Supports both SQLite (development) and Oracle (production) databases
"""

import sqlite3
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from .config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection(ABC):
    """Abstract base class for database connections"""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """Execute a query and return results"""
        pass
    
    @abstractmethod
    def execute_non_query(self, sql: str, params: Optional[List] = None) -> int:
        """Execute a non-query statement (INSERT, UPDATE, DELETE)"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table"""
        pass
    
    @abstractmethod
    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        pass

class SQLiteConnection(DatabaseConnection):
    """SQLite database connection for development"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish SQLite connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Enable column access by name
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            return False
    
    def disconnect(self):
        """Close SQLite connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("Disconnected from SQLite database")
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """Execute a query and return results"""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            # Convert sqlite3.Row objects to dictionaries
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"SQL: {sql}")
            if params:
                logger.error(f"Params: {params}")
            raise
    
    def execute_non_query(self, sql: str, params: Optional[List] = None) -> int:
        """Execute a non-query statement"""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            self.conn.commit()
            return self.cursor.rowcount
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Non-query execution failed: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table"""
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            return [dict(col) for col in columns]
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return []
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = self.cursor.fetchall()
            return [table['name'] for table in tables if table['name'] != 'sqlite_sequence']
        except Exception as e:
            logger.error(f"Failed to get table list: {e}")
            return []

class OracleConnection(DatabaseConnection):
    """Oracle database connection for production"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.cursor = None
        self._oracle_available = False
        
        # Try to import Oracle driver
        try:
            import oracledb
            self.oracledb = oracledb
            self._oracle_available = True
            logger.info("Oracle driver (oracledb) available")
        except ImportError:
            try:
                import cx_Oracle
                self.cx_Oracle = cx_Oracle
                self._oracle_available = True
                logger.info("Oracle driver (cx_Oracle) available")
            except ImportError:
                logger.error("No Oracle driver available. Install: pip install oracledb")
                self._oracle_available = False
    
    def connect(self) -> bool:
        """Establish Oracle connection"""
        if not self._oracle_available:
            logger.error("Oracle driver not available")
            return False
        
        try:
            # Build connection string
            dsn = f"{self.config['host']}:{self.config['port']}/{self.config['service']}"
            
            if hasattr(self, 'oracledb'):
                # Using modern oracledb driver
                self.conn = self.oracledb.connect(
                    user=self.config['user'],
                    password=self.config['password'],
                    dsn=dsn
                )
            else:
                # Using legacy cx_Oracle driver
                self.conn = self.cx_Oracle.connect(
                    user=self.config['user'],
                    password=self.config['password'],
                    dsn=dsn
                )
            
            self.cursor = self.conn.cursor()
            
            # Set current schema if specified
            if 'schema' in self.config and self.config['schema']:
                self.cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.config['schema']}")
            
            logger.info(f"Connected to Oracle database: {self.config['host']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            return False
    
    def disconnect(self):
        """Close Oracle connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.conn = None
        self.cursor = None
        logger.info("Disconnected from Oracle database")
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """Execute a query and return results"""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            
            # Fetch results and convert to dictionaries
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Oracle query execution failed: {e}")
            logger.error(f"SQL: {sql}")
            if params:
                logger.error(f"Params: {params}")
            raise
    
    def execute_non_query(self, sql: str, params: Optional[List] = None) -> int:
        """Execute a non-query statement"""
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            self.conn.commit()
            return self.cursor.rowcount
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Oracle non-query execution failed: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table"""
        try:
            schema = self.config.get('schema', 'BMI_CIMS')
            sql = """
                SELECT 
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    column_id
                FROM all_tab_columns 
                WHERE owner = :schema AND table_name = :table_name
                ORDER BY column_id
            """
            self.cursor.execute(sql, [schema, table_name.upper()])
            columns = self.cursor.fetchall()
            
            column_names = [desc[0].lower() for desc in self.cursor.description]
            return [dict(zip(column_names, col)) for col in columns]
            
        except Exception as e:
            logger.error(f"Failed to get Oracle schema for table {table_name}: {e}")
            return []
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        try:
            schema = self.config.get('schema', 'BMI_CIMS')
            sql = """
                SELECT table_name 
                FROM all_tables 
                WHERE owner = :schema
                ORDER BY table_name
            """
            self.cursor.execute(sql, [schema])
            tables = self.cursor.fetchall()
            return [table[0] for table in tables]
            
        except Exception as e:
            logger.error(f"Failed to get Oracle table list: {e}")
            return []

class DatabaseManager:
    """Database manager that handles connection based on configuration"""
    
    def __init__(self):
        self.config = get_config()
        self.connection = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup database connection based on configuration"""
        db_config = self.config.get_database_config()
        
        if db_config['type'] == 'sqlite':
            self.connection = SQLiteConnection(db_config['path'])
        elif db_config['type'] == 'oracle':
            self.connection = OracleConnection(db_config)
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")
    
    def connect(self) -> bool:
        """Connect to database"""
        return self.connection.connect()
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.disconnect()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[Dict]:
        """Execute a query and return results"""
        return self.connection.execute_query(sql, params)
    
    def execute_non_query(self, sql: str, params: Optional[List] = None) -> int:
        """Execute a non-query statement"""
        return self.connection.execute_non_query(sql, params)
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table"""
        return self.connection.get_table_schema(table_name)
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        return self.connection.get_all_tables()
    
    def get_database_type(self) -> str:
        """Get the current database type"""
        return self.config.get_database_config()['type']
    
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self.connection and self.connection.conn is not None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

def get_database_manager() -> DatabaseManager:
    """Get a database manager instance"""
    return DatabaseManager()

if __name__ == "__main__":
    print("Testing Database Abstraction Layer")
    print("=" * 50)
    
    with get_database_manager() as db:
        print(f"Database type: {db.get_database_type()}")
        print(f"Connected: {db.is_connected()}")
        
        if db.is_connected():
            tables = db.get_all_tables()
            print(f"Available tables: {len(tables)}")
            for table in tables[:5]:  # Show first 5 tables
                print(f"  - {table}")
            
            if tables:
                schema = db.get_table_schema(tables[0])
                print(f"\nSchema for {tables[0]}:")
                for col in schema[:3]:  # Show first 3 columns
                    print(f"  - {col}")