#!/usr/bin/env python3
"""
CLI Frontend for Oracle CIMS IoT Database Natural Language Queries
Interactive command-line interface for querying Oracle-based IoT data
"""

import argparse
import cmd
import sys
import json
from datetime import datetime
from typing import List, Dict
import sqlite3
from oracle_query_interface import OracleQueryInterface
from oracle_domain_mapping import OracleDomainMapper

try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False
    print("Warning: readline not available. Command history and editing disabled.")

class OracleIoTCLI(cmd.Cmd):
    """Interactive CLI for Oracle CIMS IoT Database"""
    
    intro = '''
╔══════════════════════════════════════════════════════════════════════════════╗
║                   Oracle CIMS IoT Database Query Interface                  ║
║                    Natural Language Query Command Line                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

Welcome to the Oracle CIMS IoT Database CLI!
Ask questions in natural language about your IoT data.

Examples:
  > show me all temperature signals
  > what are the current signal values?
  > list channels in group 1
  > count signals with values above 50

Type 'help' for commands or 'exit' to quit.
'''
    
    prompt = 'Oracle IoT> '
    
    def __init__(self):
        super().__init__()
        self.interface = OracleQueryInterface()
        self.mapper = OracleDomainMapper()
        self.query_history = []
        self.show_sql = False
        self.max_results = 10
        
    def default(self, line):
        """Handle natural language queries"""
        if not line.strip():
            return
            
        # Execute the natural language query
        self.execute_query(line.strip())
    
    def execute_query(self, query: str):
        """Execute a natural language query"""
        print(f"\n🔍 Processing: {query}")
        
        try:
            start_time = datetime.now()
            result = self.interface.execute_natural_language_query(query)
            duration = (datetime.now() - start_time).total_seconds()
            
            # Store in history
            self.query_history.append({
                'query': query,
                'result': result,
                'timestamp': start_time,
                'duration': duration
            })
            
            if result['success']:
                self.display_success_result(result, duration)
            else:
                self.display_error_result(result)
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
    
    def display_success_result(self, result: Dict, duration: float):
        """Display successful query results"""
        count = result['count']
        print(f"✅ Found {count:,} records in {duration:.3f}s")
        
        # Show SQL if enabled
        if self.show_sql:
            print(f"📝 SQL: {result['sql']}")
            if result.get('params'):
                print(f"📋 Parameters: {result['params']}")
        
        # Show time range if available
        if result.get('time_range', {}).get('start'):
            tr = result['time_range']
            print(f"📅 Time range: {tr['start']} to {tr['end']}")
        
        # Display results
        if result['results']:
            print(f"\n📊 Results (showing first {min(self.max_results, count)}):")
            print("─" * 80)
            
            for i, row in enumerate(result['results'][:self.max_results], 1):
                print(f"{i:2d}. {self.format_row(row)}")
            
            if count > self.max_results:
                print(f"    ... and {count - self.max_results:,} more records")
                print(f"    (use 'set max_results {count}' to see all)")
        else:
            print("📭 No data found.")
        
        print()
    
    def display_error_result(self, result: Dict):
        """Display error results"""
        print(f"❌ Query failed: {result['error']}")
        if result.get('sql'):
            print(f"📝 SQL attempted: {result['sql']}")
        print()
    
    def format_row(self, row: Dict) -> str:
        """Format a single result row for display"""
        # Create a condensed string representation
        items = []
        for key, value in row.items():
            if value is not None:
                if isinstance(value, float):
                    items.append(f"{key}={value:.2f}")
                elif isinstance(value, str) and len(value) > 30:
                    items.append(f"{key}={value[:27]}...")
                else:
                    items.append(f"{key}={value}")
        
        return " | ".join(items[:5])  # Limit to 5 fields for readability
    
    # CLI Commands
    def do_help(self, arg):
        """Show help information"""
        if arg:
            super().do_help(arg)
        else:
            print("""
Available commands:
  help [command]     - Show this help or help for specific command
  show sql          - Toggle SQL display on/off
  set max_results N - Set maximum number of results to display
  stats             - Show database statistics
  tables            - List all available tables
  schema [table]    - Show schema for table
  history           - Show query history
  samples           - Show sample queries
  clear             - Clear the screen
  exit/quit         - Exit the CLI

Natural Language Queries:
  Just type your question in plain English, for example:
  - show me all temperature signals
  - what are the current values for pressure sensors?
  - count signals in channel group 1
  - list historical data from yesterday
            """)
    
    def do_show(self, arg):
        """Show various information"""
        if arg.strip().lower() == 'sql':
            self.show_sql = not self.show_sql
            print(f"SQL display {'enabled' if self.show_sql else 'disabled'}")
        else:
            print("Usage: show sql")
    
    def do_set(self, arg):
        """Set configuration options"""
        parts = arg.strip().split()
        if len(parts) == 2 and parts[0] == 'max_results':
            try:
                self.max_results = int(parts[1])
                print(f"Max results set to {self.max_results}")
            except ValueError:
                print("Error: max_results must be a number")
        else:
            print("Usage: set max_results <number>")
    
    def do_stats(self, arg):
        """Show database statistics"""
        print("\n📊 Database Statistics:")
        print("─" * 50)
        
        stats_queries = {
            "Total Signals": "SELECT COUNT(*) FROM SIGNALITEM",
            "Signal Channels": "SELECT COUNT(*) FROM SIGNALCHANNEL", 
            "Channel Groups": "SELECT COUNT(*) FROM CHANNELGROUP",
            "Current Values": "SELECT COUNT(*) FROM SIGNALVALUE",
            "Historical Records": "SELECT COUNT(*) FROM REPDATA",
            "Report Items": "SELECT COUNT(*) FROM REPITEM",
            "Process Periods": "SELECT COUNT(*) FROM PROCINSTANCE",
            "External Systems": "SELECT COUNT(*) FROM ADDRESS"
        }
        
        for name, query in stats_queries.items():
            try:
                self.interface.cursor.execute(query)
                count = self.interface.cursor.fetchone()[0]
                print(f"{name:20s}: {count:,}")
            except Exception as e:
                print(f"{name:20s}: Error - {e}")
        print()
    
    def do_tables(self, arg):
        """List all available tables"""
        print("\n📋 Available Tables:")
        print("─" * 50)
        
        schema_info = self.mapper.get_schema_info()
        for table, info in schema_info.items():
            print(f"{table:15s} - {info['description']}")
        print()
    
    def do_schema(self, arg):
        """Show schema for a specific table"""
        if not arg.strip():
            print("Usage: schema <table_name>")
            return
            
        table_name = arg.strip().upper()
        schema_info = self.mapper.get_schema_info()
        
        if table_name in schema_info:
            info = schema_info[table_name]
            print(f"\n📋 Schema for {table_name}:")
            print("─" * 50)
            print(f"Description: {info['description']}")
            print(f"Key Columns: {', '.join(info['key_columns'])}")
            print(f"Common Filters: {', '.join(info['common_filters'])}")
            
            # Get actual column info
            try:
                self.interface.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = self.interface.cursor.fetchall()
                print(f"\nColumns:")
                for col in columns:
                    pk = " (PK)" if col[5] else ""
                    nn = " NOT NULL" if col[3] else ""
                    print(f"  {col[1]:20s} {col[2]}{pk}{nn}")
            except Exception as e:
                print(f"Error getting column info: {e}")
        else:
            print(f"Table '{table_name}' not found.")
            print(f"Available tables: {', '.join(schema_info.keys())}")
        print()
    
    def do_history(self, arg):
        """Show query history"""
        if not self.query_history:
            print("No query history available.")
            return
            
        print("\n📚 Query History:")
        print("─" * 80)
        
        for i, entry in enumerate(self.query_history[-10:], 1):
            timestamp = entry['timestamp'].strftime("%H:%M:%S")
            duration = entry['duration']
            success = "✅" if entry['result']['success'] else "❌"
            count = entry['result'].get('count', 0) if entry['result']['success'] else 0
            
            print(f"{i:2d}. [{timestamp}] {success} {entry['query']}")
            if entry['result']['success']:
                print(f"     Found {count} records in {duration:.3f}s")
            else:
                print(f"     Error: {entry['result']['error']}")
        print()
    
    def do_samples(self, arg):
        """Show sample queries"""
        samples = [
            "show me all temperature signals",
            "what are the current signal values?",
            "list channels in group 1", 
            "show historical data from last week",
            "what signals have values above 50?",
            "count how many signals are online",
            "show me all pressure sensors",
            "list report items for calculations",
            "what channel groups exist?",
            "show signals with good quality data",
            "find signals in channel 101",
            "what are the analog signals?",
            "show process instances from yesterday",
            "list external system addresses"
        ]
        
        print("\n💡 Sample Queries:")
        print("─" * 50)
        for i, sample in enumerate(samples, 1):
            print(f"{i:2d}. {sample}")
        print("\nJust copy and paste any of these, or ask your own questions!")
        print()
    
    def do_clear(self, arg):
        """Clear the screen"""
        import os
        os.system('clear' if os.name == 'posix' else 'cls')
        print(self.intro)
    
    def do_exit(self, arg):
        """Exit the CLI"""
        print("\n👋 Goodbye! Thanks for using Oracle CIMS IoT Database CLI.")
        return True
    
    def do_quit(self, arg):
        """Exit the CLI"""
        return self.do_exit(arg)
    
    def do_EOF(self, arg):
        """Handle Ctrl+D"""
        print("\n")
        return self.do_exit(arg)

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='Oracle CIMS IoT Database Natural Language Query CLI')
    parser.add_argument('--db', default='oracle_iot_db.db', help='Database file path')
    parser.add_argument('--query', help='Execute single query and exit')
    parser.add_argument('--sql', action='store_true', help='Show SQL queries')
    parser.add_argument('--max-results', type=int, default=10, help='Maximum results to display')
    
    args = parser.parse_args()
    
    try:
        # Test database connection
        interface = OracleQueryInterface(args.db)
        interface.cursor.execute("SELECT COUNT(*) FROM SIGNALITEM")
        
        cli = OracleIoTCLI()
        cli.show_sql = args.sql
        cli.max_results = args.max_results
        
        if args.query:
            # Single query mode
            cli.execute_query(args.query)
        else:
            # Interactive mode
            if HAS_READLINE:
                # Set up command history
                import os
                history_file = os.path.expanduser('~/.oracle_iot_cli_history')
                try:
                    readline.read_history_file(history_file)
                except (FileNotFoundError, PermissionError):
                    # Ignore if we can't read history file
                    pass
                
                def save_history():
                    try:
                        readline.write_history_file(history_file)
                    except PermissionError:
                        # Ignore if we can't save history
                        pass
                
                import atexit
                atexit.register(save_history)
            
            cli.cmdloop()
            
    except FileNotFoundError:
        print(f"Error: Database file '{args.db}' not found.")
        print("Run the setup script first to create the database.")
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)

if __name__ == "__main__":
    main()