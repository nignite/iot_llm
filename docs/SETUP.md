# IoT Database Query Interface Setup Guide

Complete setup and usage instructions for the CLI and web frontends.

## Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Setup Claude API (Optional but Recommended):**
```bash
# Get API key from: https://console.anthropic.com/
export ANTHROPIC_API_KEY=your_api_key_here

# Or create a .env file (copy from .env.example)
cp .env.example .env
# Edit .env file with your API key
```

3. **Setup the database:**
```bash
python3 iot_database_setup.py
```

4. **Test the system:**
```bash
python3 test_frontends.py
```

4. **Use the interfaces:**

**CLI Interface:**
```bash
python3 iot_cli.py
```

**Web Interface:**
```bash
streamlit run iot_streamlit_app.py
```

## Detailed Setup Instructions

### Prerequisites

- Python 3.7 or higher
- SQLite3 (included with Python)
- Terminal/Command prompt access

### 1. Database Setup

The database setup script creates a SQLite database with production-like IoT data:

```bash
python3 iot_database_setup.py
```

This creates:
- `iot_production.db` - SQLite database file
- 50,000+ sensor readings
- 10 IoT devices
- 200 alerts
- 5 facility locations
- Complete domain mappings

### 2. Install Dependencies

For basic functionality (CLI only):
```bash
# No additional dependencies needed - uses only Python standard library
```

For web interface:
```bash
pip install streamlit plotly pandas
```

Or install all at once:
```bash
pip install -r requirements.txt
```

### 3. Test the System

Run comprehensive tests:
```bash
python3 test_frontends.py
```

Expected output:
```
🧪 Starting IoT Database Frontend Test Suite
============================================================

📊 Testing Core Functionality:
✅ Database Connectivity: PASS
✅ Query Interface Basic: PASS
✅ Domain Mapping Table: PASS

💻 Testing CLI Frontend:
✅ CLI Help Command: PASS
✅ CLI JSON Output: PASS

🌐 Testing Web Frontend:
✅ Streamlit Import: PASS

Success Rate: 100.0%
```

## Usage Examples

### CLI Interface

**Interactive Mode:**
```bash
python3 iot_cli.py
```

```
IoT> Which signals crossed the value limits last week?
IoT> Show me all alerts from yesterday
IoT> What devices are currently offline?
```

**Single Query Mode:**
```bash
python3 iot_cli.py --query "count total signals recorded"
```

**JSON Output:**
```bash
python3 iot_cli.py --query "find temperature readings above 30 degrees" --json
```

**Batch Mode:**
```bash
echo "count total signals
show me offline devices
list critical alerts" > queries.txt

python3 iot_cli.py --batch queries.txt
```

**CLI Commands:**
- `help` - Show available commands
- `examples` - Show sample queries  
- `schema` - Show database schema
- `stats` - Show database statistics
- `history` - Show query history
- `clear` - Clear screen
- `quit/exit` - Exit

### Web Interface

**Start the web server:**
```bash
streamlit run iot_streamlit_app.py
```

**Features:**
- 🎯 Natural language query input
- 📊 Real-time visualizations
- 📈 Interactive charts and graphs
- 📚 Query history
- 💾 CSV download
- 💡 Sample queries
- 🔗 Domain mappings reference

**Web Interface URL:**
- Local: `http://localhost:8501`
- Network: `http://[your-ip]:8501`

## Sample Queries

### Basic Queries
- "Which signals crossed the value limits last week?"
- "Show me all alerts from yesterday"
- "What devices are currently offline?"
- "Find temperature readings above 30 degrees"

### Time-based Queries
- "Count how many anomalies were detected this week"
- "What was the average humidity last month?"
- "Show me alerts from the past 3 days"

### Device and Location Queries
- "List all devices in Factory Floor A"
- "Show me sensors in building LOC001"
- "What devices need maintenance?"

### Analysis Queries
- "Show me all critical alerts"
- "Find sensors with quality issues today"
- "What are the maximum pressure readings from last week?"

## Domain Mappings

### Table Mappings
| Business Term | Database Table | Purpose |
|---------------|----------------|---------|
| signals, sensor_data | RepData | Raw sensor readings |
| alerts, alarms | AlertLog | Alert history |
| devices, equipment | DevMap | Device configuration |
| thresholds, limits | ThreshSet | Value limits |
| logs, analytics | RepItem | Calculated data |
| locations, sites | LocRef | Location reference |

### Business Terms
| Term | Synonyms |
|------|----------|
| crossed | exceeded, violated, breached |
| limits | thresholds, boundaries, ranges |
| offline | disconnected, unavailable, down |
| anomaly | outlier, unusual, abnormal |

## Troubleshooting

### Common Issues

**Database not found:**
```bash
# Make sure you've run the setup script
python3 iot_database_setup.py
```

**Permission denied:**
```bash
# Make sure files are executable
chmod +x iot_cli.py
chmod +x iot_streamlit_app.py
```

**Import errors:**
```bash
# Install missing dependencies
pip install streamlit plotly pandas
```

**CLI colors not working:**
```bash
# Disable colors if terminal doesn't support them
python3 iot_cli.py --no-color
```

**Streamlit port already in use:**
```bash
# Use a different port
streamlit run iot_streamlit_app.py --server.port 8502
```

### Performance Tips

**For large result sets:**
- Use time constraints: "last week", "yesterday"
- Limit scope: "temperature sensors only"
- Use aggregate queries: "count", "average"

**For better query results:**
- Use specific terms from domain mappings
- Include time ranges when relevant
- Be specific about what you're looking for

### Advanced Configuration

**Database Location:**
- Default: `iot_production.db` in current directory
- Modify `db_path` in the Python files to change location

**Customizing Domain Mappings:**
- Edit `domain_mapping.py` to add new terms
- Restart applications after changes

**Web Interface Customization:**
- Modify `iot_streamlit_app.py`
- Change colors, layout, or add new visualizations

## File Structure

```
├── iot_database_setup.py     # Database creation and data generation
├── domain_mapping.py         # Business term mappings
├── claude_query_interface.py # Core query processing
├── iot_cli.py               # CLI frontend
├── iot_streamlit_app.py     # Web frontend  
├── test_frontends.py        # Test suite
├── requirements.txt         # Dependencies
├── SETUP.md                # This file
├── README.md               # Project overview
└── iot_production.db       # Generated database
```

## Support

For issues or questions:
1. Run the test suite: `python3 test_frontends.py`
2. Check the troubleshooting section above
3. Review sample queries and domain mappings
4. Ensure all dependencies are installed

## Next Steps

- Explore the sample queries in both interfaces
- Try creating your own natural language questions
- Examine the database schema to understand available data
- Experiment with different visualization types in the web interface