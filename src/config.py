#!/usr/bin/env python3
"""
Configuration management for IoT Database Application
Supports both development (SQLite) and production (Oracle) environments
"""

import os
from pathlib import Path
from typing import Dict, Any

class Config:
    """Base configuration class"""
    
    # Environment detection
    ENVIRONMENT = os.getenv('IOT_ENV', 'development').lower()
    
    # Application settings
    APP_NAME = "IoT Database Query Interface"
    VERSION = "1.0.0"
    
    # Claude API settings
    CLAUDE_API_KEY = os.getenv('ANTHROPIC_API_KEY')
    CLAUDE_MODEL = "claude-3-haiku-20240307"
    CLAUDE_MAX_TOKENS = 1000
    CLAUDE_TEMPERATURE = 0.1
    
    # Query settings
    DEFAULT_QUERY_LIMIT = 100
    MAX_QUERY_TIMEOUT = 30  # seconds
    
    # Domain mapping settings
    DOMAIN_MAPPINGS_PATH = Path(__file__).parent.parent / "db"
    
    @classmethod
    def get_config(cls):
        """Get configuration based on environment"""
        env = cls.ENVIRONMENT
        
        if env in ('production', 'prod'):
            return ProductionConfig()
        elif env in ('staging', 'stage'):
            return StagingConfig()
        else:
            return DevelopmentConfig()

class DevelopmentConfig(Config):
    """Development environment configuration (SQLite)"""
    
    DEBUG = True
    TESTING = False
    
    # Database settings
    DATABASE_TYPE = "sqlite"
    SQLITE_DB_PATH = "iot_production.db"
    
    # Enable verbose logging
    LOG_LEVEL = "DEBUG"
    ENABLE_QUERY_LOGGING = True
    
    # Development-specific settings
    AUTO_CREATE_DATABASE = True
    POPULATE_SAMPLE_DATA = True
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration for development"""
        return {
            "type": self.DATABASE_TYPE,
            "path": self.SQLITE_DB_PATH,
            "auto_create": self.AUTO_CREATE_DATABASE,
            "sample_data": self.POPULATE_SAMPLE_DATA
        }

class StagingConfig(Config):
    """Staging environment configuration"""
    
    DEBUG = True
    TESTING = False
    
    # Database settings - can be either SQLite or Oracle
    DATABASE_TYPE = os.getenv('STAGING_DB_TYPE', 'sqlite')
    
    # Oracle settings for staging
    ORACLE_HOST = os.getenv('ORACLE_STAGING_HOST', 'staging-oracle.company.com')
    ORACLE_PORT = int(os.getenv('ORACLE_STAGING_PORT', '1521'))
    ORACLE_SERVICE = os.getenv('ORACLE_STAGING_SERVICE', 'STAGING')
    ORACLE_USER = os.getenv('ORACLE_STAGING_USER', 'iot_staging')
    ORACLE_PASSWORD = os.getenv('ORACLE_STAGING_PASSWORD')
    ORACLE_SCHEMA = os.getenv('ORACLE_STAGING_SCHEMA', 'IOT_STAGING')
    
    # SQLite fallback
    SQLITE_DB_PATH = "iot_staging.db"
    
    LOG_LEVEL = "INFO"
    ENABLE_QUERY_LOGGING = True
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration for staging"""
        if self.DATABASE_TYPE.lower() == 'oracle':
            return {
                "type": "oracle",
                "host": self.ORACLE_HOST,
                "port": self.ORACLE_PORT,
                "service": self.ORACLE_SERVICE,
                "user": self.ORACLE_USER,
                "password": self.ORACLE_PASSWORD,
                "schema": self.ORACLE_SCHEMA
            }
        else:
            return {
                "type": "sqlite",
                "path": self.SQLITE_DB_PATH,
                "auto_create": True,
                "sample_data": False
            }

class ProductionConfig(Config):
    """Production environment configuration (Oracle)"""
    
    DEBUG = False
    TESTING = False
    
    # Database settings - Oracle only in production
    DATABASE_TYPE = "oracle"
    
    # Oracle connection settings
    ORACLE_HOST = os.getenv('ORACLE_PROD_HOST')
    ORACLE_PORT = int(os.getenv('ORACLE_PROD_PORT', '1521'))
    ORACLE_SERVICE = os.getenv('ORACLE_PROD_SERVICE')
    ORACLE_USER = os.getenv('ORACLE_PROD_USER')
    ORACLE_PASSWORD = os.getenv('ORACLE_PROD_PASSWORD')
    ORACLE_SCHEMA = os.getenv('ORACLE_PROD_SCHEMA', 'BMI_CIMS')
    
    # Production-specific settings
    CONNECTION_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '10'))
    CONNECTION_TIMEOUT = int(os.getenv('DB_TIMEOUT', '30'))
    
    LOG_LEVEL = "WARNING"
    ENABLE_QUERY_LOGGING = False
    
    # Security settings
    REQUIRE_SSL = True
    MAX_QUERY_EXECUTION_TIME = 60  # seconds
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration for production"""
        if not all([self.ORACLE_HOST, self.ORACLE_SERVICE, self.ORACLE_USER, self.ORACLE_PASSWORD]):
            raise ValueError(
                "Missing required Oracle configuration. Set environment variables: "
                "ORACLE_PROD_HOST, ORACLE_PROD_SERVICE, ORACLE_PROD_USER, ORACLE_PROD_PASSWORD"
            )
        
        return {
            "type": "oracle",
            "host": self.ORACLE_HOST,
            "port": self.ORACLE_PORT,
            "service": self.ORACLE_SERVICE,
            "user": self.ORACLE_USER,
            "password": self.ORACLE_PASSWORD,
            "schema": self.ORACLE_SCHEMA,
            "pool_size": self.CONNECTION_POOL_SIZE,
            "timeout": self.CONNECTION_TIMEOUT,
            "ssl_required": self.REQUIRE_SSL
        }

# Global configuration instance
config = Config.get_config()

def get_config() -> Config:
    """Get the current configuration instance"""
    return config

def print_config_info():
    """Print current configuration information"""
    cfg = get_config()
    
    print(f"Environment: {cfg.ENVIRONMENT}")
    print(f"Database Type: {cfg.get_database_config()['type']}")
    print(f"Debug Mode: {cfg.DEBUG}")
    print(f"Claude API Available: {'Yes' if cfg.CLAUDE_API_KEY else 'No'}")
    
    if cfg.DATABASE_TYPE == 'oracle':
        db_config = cfg.get_database_config()
        print(f"Oracle Host: {db_config.get('host', 'Not configured')}")
        print(f"Oracle Schema: {db_config.get('schema', 'Not configured')}")
    else:
        print(f"SQLite Path: {cfg.get_database_config()['path']}")

if __name__ == "__main__":
    print("IoT Database Configuration")
    print("=" * 50)
    print_config_info()