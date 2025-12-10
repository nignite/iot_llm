# Environment Configuration Guide

This IoT Database application supports multiple environments with different database backends.

## Quick Start

### 🔧 Development (SQLite)
```bash
# Setup development environment
python3 setup_env.py dev

# Use the app
python3 main.py
python3 -m streamlit run iot_streamlit_app.py
```

### 🏭 Production (Oracle)
```bash
# Setup production environment  
python3 setup_env.py prod

# Install Oracle driver
pip install oracledb

# Edit .env with your Oracle credentials
# Then use the app
python3 main.py
```

## Environment Types

### Development
- **Database**: SQLite with sample data
- **Purpose**: Local development, testing, demos
- **Setup**: Automatic database creation
- **Data**: 50,000+ generated IoT sensor readings

### Staging  
- **Database**: SQLite or Oracle (configurable)
- **Purpose**: Testing before production
- **Setup**: Manual configuration in `.env`

### Production
- **Database**: Oracle database (network connection)
- **Purpose**: Real production IoT data
- **Setup**: Requires Oracle credentials

## Configuration

### Environment Variables
Set the environment using:
```bash
export IOT_ENV=development  # or staging, production
```

### Configuration Files
- `.env.development` - Development settings
- `.env.staging` - Staging settings  
- `.env.production` - Production settings

The setup script copies the appropriate file to `.env`.

### Oracle Connection
For production Oracle database, configure in `.env`:
```bash
ORACLE_PROD_HOST=your-oracle-server.company.com
ORACLE_PROD_PORT=1521
ORACLE_PROD_SERVICE=PROD
ORACLE_PROD_USER=iot_user
ORACLE_PROD_PASSWORD=your_password
ORACLE_PROD_SCHEMA=BMI_CIMS
```

## Commands

```bash
# Environment setup
python3 setup_env.py dev        # Development
python3 setup_env.py staging    # Staging
python3 setup_env.py prod       # Production
python3 setup_env.py check      # Check current config

# Application usage (same commands for all environments)
python3 main.py                 # CLI interface
python3 -m streamlit run iot_streamlit_app.py  # Web interface
```

## Database Abstraction

The application automatically detects the environment and connects to the appropriate database:

- **Development**: SQLite file (`iot_production.db`)
- **Production**: Oracle network connection
- **Staging**: Configurable (SQLite or Oracle)

All queries work identically regardless of database backend.

## Benefits

✅ **Seamless switching** between development and production  
✅ **Same codebase** works with SQLite and Oracle  
✅ **Environment isolation** - no accidental production queries during dev  
✅ **Easy deployment** - just change environment and credentials  
✅ **Consistent interface** - domain names work the same in all environments