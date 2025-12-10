#!/usr/bin/env python3
"""
Extract key IoT tables from Oracle schema and convert to SQLite

This utility script processes Oracle export files (km_export_1.sql) and extracts
specific IoT-related tables, converting them to SQLite format.

Usage:
    cd db/oracle/
    python3 extract_key_tables.py

Input: km_export_1.sql (Oracle export in current directory)
Output: ../sqlite/key_tables.sql (SQLite schema)
"""

import re

def extract_and_convert_key_tables():
    """Extract specific tables we need for IoT functionality"""
    
    # Key tables for IoT functionality
    key_tables = [
        'SIGNALITEM',
        'SIGNALCHANNEL', 
        'SIGNALVALUE',
        'REPDATA',
        'REPITEM',
        'ADDRESS',
        'CHANNELGROUP'
    ]
    
    # Read the Oracle file
    try:
        with open('km_export_1.sql', 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        with open('km_export_1.sql', 'r', encoding='latin-1') as f:
            content = f.read()
    
    sqlite_statements = []
    sqlite_statements.append('-- Key IoT Tables converted from Oracle to SQLite')
    sqlite_statements.append('-- Extracted from Oracle CIMS database schema')
    sqlite_statements.append('')
    
    for table_name in key_tables:
        print(f"Processing table: {table_name}")
        
        # Find the CREATE TABLE statement for this table
        pattern = rf'CREATE TABLE "BMI_CIMS"."{table_name}" \([^;]+\)'
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        
        if match:
            oracle_sql = match.group(0)
            sqlite_sql = convert_table_to_sqlite(table_name, oracle_sql)
            sqlite_statements.append(sqlite_sql)
            sqlite_statements.append('')
        else:
            print(f"  Table {table_name} not found")
    
    # Write the result to the sqlite directory
    output_file = '../sqlite/key_tables.sql'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sqlite_statements))
    
    print(f"Extracted {len(key_tables)} tables to {output_file}")

def convert_table_to_sqlite(table_name, oracle_sql):
    """Convert a single Oracle table to SQLite"""
    
    # Extract columns from between parentheses
    columns_match = re.search(r'\((.*)\)', oracle_sql, re.DOTALL)
    if not columns_match:
        return f'-- ERROR: Could not parse {table_name}'
    
    columns_text = columns_match.group(1).strip()
    
    # Split by comma but handle parentheses properly
    columns = []
    current = ""
    paren_depth = 0
    
    for char in columns_text:
        if char == '(' and paren_depth >= 0:
            paren_depth += 1
        elif char == ')' and paren_depth > 0:
            paren_depth -= 1
        elif char == ',' and paren_depth == 0:
            if current.strip():
                columns.append(current.strip())
            current = ""
            continue
        current += char
    
    if current.strip():
        columns.append(current.strip())
    
    # Convert columns
    sqlite_columns = []
    for col in columns:
        col = col.strip()
        if not col:
            continue
            
        converted = convert_column(col)
        if converted:
            sqlite_columns.append(converted)
    
    # Build SQLite CREATE TABLE
    result = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
    for i, col in enumerate(sqlite_columns):
        comma = ',' if i < len(sqlite_columns) - 1 else ''
        result += f'  {col}{comma}\n'
    result += ');'
    
    return result

def convert_column(oracle_column):
    """Convert a single column definition"""
    
    # Clean the column definition
    oracle_column = oracle_column.strip().strip('"')
    
    # Extract column name (first quoted or unquoted identifier)
    name_match = re.match(r'^"([^"]+)"|^(\w+)', oracle_column)
    if not name_match:
        return None
    
    col_name = name_match.group(1) or name_match.group(2)
    rest = oracle_column[name_match.end():].strip()
    
    # Convert data type
    sqlite_type = 'TEXT'  # default
    
    if re.match(r'NUMBER\(\d+,0\)', rest, re.IGNORECASE):
        sqlite_type = 'INTEGER'
    elif re.match(r'NUMBER\(\d+,\d+\)', rest, re.IGNORECASE):
        sqlite_type = 'REAL'
    elif re.match(r'NUMBER', rest, re.IGNORECASE):
        sqlite_type = 'REAL'
    elif re.match(r'FLOAT', rest, re.IGNORECASE):
        sqlite_type = 'REAL'
    elif re.match(r'VARCHAR2|CHAR|CLOB', rest, re.IGNORECASE):
        sqlite_type = 'TEXT'
    elif re.match(r'TIMESTAMP|DATE', rest, re.IGNORECASE):
        sqlite_type = 'DATETIME'
    elif re.match(r'RAW|BLOB', rest, re.IGNORECASE):
        sqlite_type = 'BLOB'
    
    # Handle constraints
    constraints = ''
    
    # Handle DEFAULT
    default_match = re.search(r'DEFAULT\s+(\S+)', rest, re.IGNORECASE)
    if default_match:
        default_val = default_match.group(1)
        if default_val.upper() == 'SYSDATE':
            constraints += ' DEFAULT CURRENT_TIMESTAMP'
        elif default_val.isdigit():
            constraints += f' DEFAULT {default_val}'
        elif default_val.upper() in ['NULL', 'CURRENT_TIMESTAMP']:
            constraints += f' DEFAULT {default_val}'
        else:
            constraints += f' DEFAULT {default_val}'
    
    return f'{col_name} {sqlite_type}{constraints}'

if __name__ == "__main__":
    extract_and_convert_key_tables()