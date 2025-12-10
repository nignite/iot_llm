#!/usr/bin/env python3
"""
Oracle to SQLite Schema Converter
Converts Oracle SQL DDL statements to SQLite-compatible format
"""

import re
import sys
from typing import List, Dict, Tuple

class OracleToSQLiteConverter:
    def __init__(self):
        # Oracle to SQLite data type mappings
        self.type_mappings = {
            # Number types
            r'NUMBER\(\d+,0\)': 'INTEGER',
            r'NUMBER\(\d+,\d+\)': 'REAL',
            r'NUMBER\(\d+\)': 'INTEGER', 
            r'NUMBER': 'REAL',
            r'FLOAT\(\d+\)': 'REAL',
            r'FLOAT': 'REAL',
            
            # String types
            r'VARCHAR2\(\d+\s+CHAR\)': 'TEXT',
            r'VARCHAR2\(\d+\)': 'TEXT',
            r'VARCHAR\(\d+\)': 'TEXT',
            r'CHAR\(\d+\)': 'TEXT',
            r'CLOB': 'TEXT',
            r'BLOB': 'BLOB',
            
            # Date/Time types
            r'TIMESTAMP\(\d+\)\s+WITH\s+TIME\s+ZONE': 'DATETIME',
            r'TIMESTAMP\(\d+\)': 'DATETIME',
            r'TIMESTAMP': 'DATETIME',
            r'DATE': 'DATETIME',
            
            # Raw/Binary
            r'RAW\(\d+\)': 'BLOB'
        }
        
        # Oracle functions to SQLite equivalents
        self.function_mappings = {
            'SYSDATE': 'CURRENT_TIMESTAMP',
            'SYSTIMESTAMP': 'CURRENT_TIMESTAMP',
            'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP'
        }
        
        # Constraints that need special handling
        self.constraint_patterns = {
            'primary_key': r'CONSTRAINT\s+\w+\s+PRIMARY\s+KEY\s*\([^)]+\)',
            'foreign_key': r'CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\([^)]+\)\s+REFERENCES\s+[^)]+\)',
            'check': r'CONSTRAINT\s+\w+\s+CHECK\s*\([^)]+\)',
            'unique': r'CONSTRAINT\s+\w+\s+UNIQUE\s*\([^)]+\)'
        }

    def convert_data_type(self, oracle_type: str) -> str:
        """Convert Oracle data type to SQLite equivalent"""
        oracle_type = oracle_type.upper().strip()
        
        for pattern, sqlite_type in self.type_mappings.items():
            if re.search(pattern, oracle_type):
                return sqlite_type
        
        # Default fallback
        return 'TEXT'

    def convert_default_value(self, default_value: str) -> str:
        """Convert Oracle default values to SQLite format"""
        if not default_value:
            return default_value
            
        default_value = default_value.strip()
        
        # Handle Oracle functions
        for oracle_func, sqlite_func in self.function_mappings.items():
            default_value = re.sub(rf'\b{oracle_func}\b', sqlite_func, default_value, flags=re.IGNORECASE)
        
        return default_value

    def clean_identifier(self, identifier: str) -> str:
        """Remove Oracle schema qualifiers and quotes"""
        # Remove schema prefix like "BMI_CIMS"."TABLE_NAME"
        identifier = re.sub(r'^"[^"]+"\."([^"]+)"$', r'\1', identifier)
        # Remove just quotes
        identifier = identifier.strip('"')
        return identifier

    def parse_column_definition(self, column_def: str) -> Tuple[str, str, str]:
        """Parse a column definition line"""
        # Remove leading/trailing whitespace and quotes
        column_def = column_def.strip().strip(',')
        
        # Extract column name (first quoted string or first word)
        name_match = re.match(r'^"([^"]+)"|^(\w+)', column_def)
        if not name_match:
            return None, None, None
            
        column_name = name_match.group(1) or name_match.group(2)
        
        # Extract the rest of the definition
        rest = column_def[name_match.end():].strip()
        
        # Find data type (everything up to constraints or DEFAULT)
        type_match = re.match(r'^([^,\s]+(?:\([^)]+\))?(?:\s+CHAR)?)', rest)
        if not type_match:
            return column_name, 'TEXT', ''
            
        oracle_type = type_match.group(1)
        sqlite_type = self.convert_data_type(oracle_type)
        
        # Extract constraints and defaults
        constraints = rest[type_match.end():].strip()
        
        # Handle DEFAULT values
        default_match = re.search(r'DEFAULT\s+([^,\s]+(?:\([^)]*\))?)', constraints, re.IGNORECASE)
        if default_match:
            default_value = self.convert_default_value(default_match.group(1))
            constraints = re.sub(r'DEFAULT\s+[^,\s]+(?:\([^)]*\))?', f'DEFAULT {default_value}', constraints, flags=re.IGNORECASE)
        
        # Handle NOT NULL
        if re.search(r'\bNOT\s+NULL\b', constraints, re.IGNORECASE):
            if 'NOT NULL' not in constraints:
                constraints = re.sub(r'\bNOT\s+NULL\b', 'NOT NULL', constraints, flags=re.IGNORECASE)
        
        return column_name, sqlite_type, constraints

    def convert_create_table(self, table_sql: str) -> str:
        """Convert a single CREATE TABLE statement"""
        # Remove extra whitespace and normalize
        table_sql = re.sub(r'\s+', ' ', table_sql.strip())
        
        # Extract table name
        table_name_match = re.search(r'CREATE\s+TABLE\s+"?([^".\s]+)"?\."?([^".\s]+)"?', table_sql, re.IGNORECASE)
        if not table_name_match:
            table_name_match = re.search(r'CREATE\s+TABLE\s+"?([^".\s]+)"?', table_sql, re.IGNORECASE)
            table_name = self.clean_identifier(table_name_match.group(1)) if table_name_match else 'unknown_table'
        else:
            table_name = self.clean_identifier(table_name_match.group(2))
        
        # Extract column definitions (everything between parentheses)
        columns_match = re.search(r'\((.*)\)', table_sql, re.DOTALL)
        if not columns_match:
            return f'-- ERROR: Could not parse table {table_name}'
            
        columns_text = columns_match.group(1).strip()
        
        # Split columns by comma, but be careful with commas inside parentheses
        columns = []
        current_column = ""
        paren_depth = 0
        
        for char in columns_text:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                if current_column.strip():
                    columns.append(current_column.strip())
                current_column = ""
                continue
            current_column += char
            
        # Don't forget the last column
        if current_column.strip():
            columns.append(current_column.strip())
        
        # Convert each column
        result_lines = [f'CREATE TABLE IF NOT EXISTS {table_name} (']
        
        converted_columns = []
        for column in columns:
            column = column.strip()
            if not column or column.upper().startswith('CONSTRAINT'):
                continue  # Skip constraints for now
                
            column_name, sqlite_type, constraints = self.parse_column_definition(column)
            if column_name:
                column_def = f'  {column_name} {sqlite_type}'
                if constraints:
                    constraints = re.sub(r'\s+', ' ', constraints).strip()
                    if constraints and not constraints.startswith(','):
                        column_def += f' {constraints}'
                converted_columns.append(column_def)
        
        # Add columns to result
        for i, col_def in enumerate(converted_columns):
            if i < len(converted_columns) - 1:
                result_lines.append(col_def + ',')
            else:
                result_lines.append(col_def)
        
        result_lines.append(');')
        
        return '\n'.join(result_lines)

    def extract_table_statements(self, sql_content: str) -> List[str]:
        """Extract individual CREATE TABLE statements from the SQL file"""
        # Find all CREATE TABLE statements
        pattern = r'CREATE\s+TABLE\s+[^;]+;'
        matches = re.finditer(pattern, sql_content, re.IGNORECASE | re.DOTALL)
        
        statements = []
        for match in matches:
            statement = match.group(0).strip()
            # Clean up the statement
            statement = re.sub(r'\s+', ' ', statement)
            statements.append(statement)
        
        return statements

    def convert_file(self, input_file: str, output_file: str = None):
        """Convert an entire Oracle SQL file to SQLite format"""
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(input_file, 'r', encoding='latin-1') as f:
                content = f.read()
        
        # Extract CREATE TABLE statements
        table_statements = self.extract_table_statements(content)
        
        converted_statements = []
        converted_statements.append('-- Converted from Oracle to SQLite')
        converted_statements.append('-- Original file: ' + input_file)
        converted_statements.append('-- Conversion date: ' + str(__import__('datetime').datetime.now()))
        converted_statements.append('')
        
        for statement in table_statements:
            try:
                converted = self.convert_create_table(statement)
                converted_statements.append(converted)
                converted_statements.append('')
            except Exception as e:
                converted_statements.append(f'-- ERROR converting table: {e}')
                converted_statements.append(f'-- Original: {statement[:100]}...')
                converted_statements.append('')
        
        result = '\n'.join(converted_statements)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(result)
            print(f"Conversion complete. Output written to: {output_file}")
        else:
            return result

def main():
    if len(sys.argv) < 2:
        print("Usage: python oracle_to_sqlite_converter.py <oracle_file> [output_file]")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.sql', '_sqlite.sql')
    
    converter = OracleToSQLiteConverter()
    converter.convert_file(input_file, output_file)

if __name__ == "__main__":
    main()