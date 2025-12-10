# IoT Database with Claude LLM Natural Language Query Interface

A complete IoT database system with SQLite backend and Claude LLM integration for natural language queries. The system includes domain name mapping to translate business terminology into database table/column names.

## Features

- **Production-like IoT Database**: SQLite database with 50,000+ sensor readings, devices, alerts, and aggregated data
- **Domain Name Mapping**: Translates business terms (e.g., "signals", "alerts") to actual table names (e.g., "RepData", "AlertLog")
- **Natural Language Queries**: Ask questions in plain English and get SQL results
- **Time Expression Parsing**: Understands "last week", "yesterday", "past month" etc.
- **Smart Query Building**: Automatically joins tables and applies filters based on intent

## Database Schema

### Tables (with domain mappings):

| Table Name | Domain Names | Purpose |
|------------|--------------|---------|
| `RepData` | signal, signals, sensor_data, measurements | Raw sensor/signal data |
| `RepItem` | log, logs, calculated_data, analytics | Aggregated/calculated data |
| `DevMap` | device, devices, equipment, sensors | Device configuration |
| `ThreshSet` | thresholds, limits, boundaries, ranges | Value limits/constraints |
| `AlertLog` | alerts, alarms, notifications, incidents | Alert history |
| `LocRef` | locations, sites, facilities, areas, zones | Location reference |

## Quick Start

1. **Setup the database:**
```bash
python3 src/iot_database_setup.py
```

2. **Test natural language queries:**
```bash
python3 main.py
# or
python3 iot_cli.py  # backwards compatible
```

3. **Run comprehensive tests:**
```bash
python3 tests/test_queries.py
```

## Sample Natural Language Queries

- "Which signals crossed the value limits last week?"
- "Show me all alerts from yesterday"
- "What devices are currently offline?"
- "Find temperature readings above 30 degrees"
- "Count how many anomalies were detected this week"
- "What was the average humidity last month?"
- "Show me all critical alerts"
- "List all devices in Factory Floor A"

## Usage Examples

### Basic Query Interface
```python
from claude_query_interface import ClaudeQueryInterface

interface = ClaudeQueryInterface()
result = interface.execute_natural_language_query("Which signals crossed limits last week?")

if result['success']:
    print(f"Found {result['count']} results")
    print(f"SQL: {result['sql']}")
    for row in result['results'][:5]:
        print(row)
```

### Domain Mapping
```python
from domain_mapping import DomainMapper

mapper = DomainMapper()
table_name = mapper.get_table_name("signals")  # Returns "RepData"
business_terms = mapper.resolve_business_term("crossed")  # Returns ["exceeded", "violated", "breached"]
```

## Architecture

### Components:

1. **iot_database_setup.py**: Creates and populates SQLite database with realistic IoT data
2. **domain_mapping.py**: Handles translation between business/domain terms and database schema
3. **claude_query_interface.py**: Main interface for natural language query processing
4. **test_queries.py**: Comprehensive test suite demonstrating various query capabilities

### Query Processing Flow:

1. **Parse Time Expressions**: Extract temporal constraints ("last week" → date range)
2. **Extract Intent**: Identify action (SELECT, COUNT, etc.) and target entity
3. **Map Domain Terms**: Convert business terms to actual table/column names
4. **Build SQL**: Generate appropriate SQL with joins and conditions
5. **Execute & Format**: Run query and return structured results

## Sample Data

The database contains:
- **50,000 sensor readings** across 10 devices
- **10 IoT devices** (temperature, humidity, pressure, vibration, etc.)
- **200 alerts** with various severity levels
- **5 locations** with realistic facility mapping
- **Threshold configurations** for all sensor types
- **Calculated/aggregated data** logs

## Database Statistics

- Total signal records: 50,000
- Total devices: 10 (8 online, 1 offline, 1 maintenance)
- Total alerts: 200
- Sensor types: 10 different types
- Locations: 5 facilities
- Time range: Past 30 days of data

## Customization

### Adding New Domain Mappings:
```python
# In domain_mapping.py, extend the mappings:
self.table_mappings["your_term"] = "ActualTableName"
self.business_terms["your_business_term"] = ["synonym1", "synonym2"]
```

### Adding New Query Patterns:
```python
# In claude_query_interface.py, extend extract_query_intent():
if "your_pattern" in query_lower:
    intent['condition'] = 'your_condition'
```

## File Structure

```
├── src/                      # Core source code
│   ├── iot_database_setup.py     # Database creation and data generation
│   ├── domain_mapping.py         # Business term to technical term mapping
│   └── claude_query_interface.py # Main natural language query interface
├── tests/                    # Test suite
│   ├── test_queries.py          # Comprehensive test suite
│   └── test_frontends.py        # Frontend testing
├── sql/                      # SQL schema files
├── db/oracle/               # Oracle-related files (legacy)
├── docs/                    # Documentation
│   └── SETUP.md             # Detailed setup guide
├── main.py                 # Main application entry point
├── iot_cli.py              # Command-line interface module
├── iot_streamlit_app.py    # Web interface
├── iot_production.db       # Generated SQLite database
└── README.md              # This file
```

## Requirements

- Python 3.7+
- sqlite3 (included with Python)
- No external dependencies required

This system demonstrates how to create a production-ready IoT database with intelligent natural language query capabilities, making it easy for non-technical users to access and analyze IoT data using plain English questions.