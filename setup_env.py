#!/usr/bin/env python3
"""
Environment Setup Script for IoT Database Application
Helps configure development and production environments
"""

import os
import shutil
import sys
from pathlib import Path

def setup_development():
    """Setup development environment"""
    print("Setting up DEVELOPMENT environment...")
    
    # Copy development env file
    if Path('.env.development').exists():
        shutil.copy('.env.development', '.env')
        print("✅ Copied .env.development to .env")
    else:
        print("❌ .env.development file not found")
        return False
    
    # Set environment variable
    os.environ['IOT_ENV'] = 'development'
    
    # Check if database needs setup
    if not Path('iot_production.db').exists():
        print("📊 Database not found. Running setup...")
        try:
            from src.iot_database_setup import IoTDatabaseSetup
            setup = IoTDatabaseSetup()
            setup.create_tables()
            setup.populate_data()
            setup.close()
            print("✅ Development database created and populated")
        except Exception as e:
            print(f"❌ Failed to setup database: {e}")
            return False
    else:
        print("✅ Development database already exists")
    
    print("🎉 Development environment ready!")
    print("\nTo use:")
    print("  python3 main.py")
    print("  python3 -m streamlit run iot_streamlit_app.py")
    return True

def setup_production():
    """Setup production environment"""
    print("Setting up PRODUCTION environment...")
    
    # Copy production env file
    if Path('.env.production').exists():
        shutil.copy('.env.production', '.env')
        print("✅ Copied .env.production to .env")
    else:
        print("❌ .env.production file not found")
        return False
    
    # Set environment variable
    os.environ['IOT_ENV'] = 'production'
    
    print("⚠️  IMPORTANT: Update .env with your Oracle database credentials:")
    print("  - ORACLE_PROD_HOST")
    print("  - ORACLE_PROD_SERVICE") 
    print("  - ORACLE_PROD_USER")
    print("  - ORACLE_PROD_PASSWORD")
    print("  - ANTHROPIC_API_KEY")
    
    print("\n📦 Install Oracle driver:")
    print("  pip install oracledb")
    
    print("🎉 Production environment configured!")
    return True

def setup_staging():
    """Setup staging environment"""
    print("Setting up STAGING environment...")
    
    # Copy staging env file
    if Path('.env.staging').exists():
        shutil.copy('.env.staging', '.env')
        print("✅ Copied .env.staging to .env")
    else:
        print("❌ .env.staging file not found")
        return False
    
    # Set environment variable
    os.environ['IOT_ENV'] = 'staging'
    
    print("📝 Edit .env to configure staging database (SQLite or Oracle)")
    print("🎉 Staging environment configured!")
    return True

def check_environment():
    """Check current environment configuration"""
    print("Current Environment Configuration")
    print("=" * 50)
    
    env = os.getenv('IOT_ENV', 'development')
    print(f"Environment: {env}")
    
    if Path('.env').exists():
        print("✅ .env file exists")
        
        # Read and display key settings (without sensitive data)
        with open('.env', 'r') as f:
            lines = f.readlines()
            
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                if 'PASSWORD' in line or 'API_KEY' in line:
                    key = line.split('=')[0]
                    print(f"  {key}=***hidden***")
                else:
                    print(f"  {line}")
    else:
        print("❌ .env file not found")
    
    # Check database
    try:
        from src.config import get_config
        config = get_config()
        db_config = config.get_database_config()
        print(f"\nDatabase Type: {db_config['type']}")
        
        if db_config['type'] == 'sqlite':
            db_path = db_config['path']
            if Path(db_path).exists():
                print(f"✅ SQLite database exists: {db_path}")
            else:
                print(f"❌ SQLite database missing: {db_path}")
        else:
            print(f"Oracle Host: {db_config.get('host', 'Not configured')}")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")

def main():
    """Main setup interface"""
    if len(sys.argv) < 2:
        print("IoT Database Environment Setup")
        print("=" * 40)
        print("Usage:")
        print("  python3 setup_env.py dev        # Setup development environment")
        print("  python3 setup_env.py prod       # Setup production environment")  
        print("  python3 setup_env.py staging    # Setup staging environment")
        print("  python3 setup_env.py check      # Check current environment")
        print("\nEnvironments:")
        print("  dev      - SQLite database with sample data")
        print("  staging  - SQLite or Oracle (configurable)")
        print("  prod     - Oracle database connection")
        return
    
    command = sys.argv[1].lower()
    
    if command in ('dev', 'development'):
        setup_development()
    elif command in ('prod', 'production'):
        setup_production()
    elif command in ('staging', 'stage'):
        setup_staging()
    elif command == 'check':
        check_environment()
    else:
        print(f"Unknown command: {command}")
        print("Use: dev, prod, staging, or check")

if __name__ == "__main__":
    main()