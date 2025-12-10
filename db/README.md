# Database Files and Schemas

This directory contains database-related files organized by database system.

## Directory Structure

### JSON Mapping Files
- **`table_domain_mappings.json`** - Comprehensive table and domain mappings with descriptions
- **`domain_to_table.json`** - Simple domain term to table name mappings

### `sqlite/`
SQLite schema files for the current IoT database implementation:

- **`schema.sql`** - Main IoT database schema (converted from Oracle)
- **`key_tables.sql`** - Essential IoT tables extracted from Oracle CIMS
- **`km_export_converted.sql`** - Converted Oracle export for SQLite compatibility

### `oracle/`
Legacy Oracle-related files and original exports:

- **`km_export_1.sql`** - Original Oracle DDL export
- **`oracle_*.py`** - Oracle-specific Python modules
- **`oracle_*.db`** - Oracle database files
- **`populate_oracle_schema.py`** - Oracle schema population script

## Usage

The SQLite schemas in `sqlite/` are the active schemas used by the current IoT application. The Oracle files in `oracle/` are maintained for reference and legacy compatibility.