#!/usr/bin/env python3
"""
CLI Frontend for IoT Database Natural Language Queries
Interactive command-line interface with rich formatting and auto-completion
"""

import argparse
import cmd
import sys
import json
from datetime import datetime
from typing import List, Dict
import sqlite3
from src.claude_query_interface import ClaudeQueryInterface
from src.domain_mapping import DomainMapper

try:
    import readline
    HAS_READLINE = True
except ImportError:
    HAS_READLINE = False

class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class IoTCLI(cmd.Cmd):
    """Interactive CLI for IoT Database Queries"""
    
    intro = f"""{Colors.HEADER}
╔══════════════════════════════════════════════════════════════════════════════╗
║                       IoT Database Query Interface                          ║
║                     Natural Language Query System                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
{Colors.ENDC}

Welcome to the IoT Database CLI! Ask questions in natural language.

{Colors.OKGREEN}Examples:{Colors.ENDC}
  • Which signals crossed the value limits last week?
  • Show me all alerts from yesterday
  • What devices are currently offline?
  • Find temperature readings above 30 degrees

{Colors.OKCYAN}Commands:{Colors.ENDC}
  • help - Show available commands
  • examples - Show sample queries
  • schema - Show database schema
  • stats - Show database statistics
  • history - Show query history
  • clear - Clear screen
  • quit/exit - Exit the CLI

Type your question or use 'help' for more information.
"""
    
    prompt = f"{Colors.OKBLUE}IoT> {Colors.ENDC}"
    
    def __init__(self):
        super().__init__()
        self.interface = ClaudeQueryInterface()
        self.mapper = DomainMapper()
        self.query_history = []
        
        # Setup autocomplete if readline is available
        if HAS_READLINE:
            self.setup_autocomplete()
    
    def setup_autocomplete(self):
        """Setup autocomplete for common terms"""
        common_terms = [
            "which", "what", "show", "find", "count", "list",
            "signals", "alerts", "devices", "sensors", "readings",
            "temperature", "humidity", "pressure", "vibration",
            "last week", "yesterday", "today", "this month",
            "above", "below", "over", "under", "crossed", "exceeded",
            "offline", "online", "critical", "warning", "anomaly"
        ]
        
        def completer(text, state):
            options = [term for term in common_terms if term.startswith(text)]
            try:
                return options[state]
            except IndexError:
                return None
        
        readline.set_completer(completer)
        readline.parse_and_bind("tab: complete")
    
    def default(self, line):
        """Handle natural language queries"""
        if not line.strip():
            return
        
        query = line.strip()
        self.execute_query(query)
    
    def execute_query(self, query: str):
        """Execute a natural language query and display results"""
        print(f"\n{Colors.OKCYAN}Processing query: {Colors.ENDC}{query}")
        print("─" * 80)
        
        start_time = datetime.now()
        result = self.interface.execute_natural_language_query(query)
        end_time = datetime.now()
        
        # Add to history
        self.query_history.append({
            'timestamp': start_time,
            'query': query,
            'result': result,
            'duration': (end_time - start_time).total_seconds()
        })
        
        if result['success']:
            self.display_success_result(result, end_time - start_time)
        else:
            self.display_error_result(result)
    
    def display_success_result(self, result: Dict, duration):
        """Display successful query results with formatting"""
        print(f"{Colors.OKGREEN}✓ Query executed successfully{Colors.ENDC}")
        print(f"{Colors.BOLD}SQL:{Colors.ENDC} {result['sql']}")
        
        if result.get('params'):
            print(f"{Colors.BOLD}Parameters:{Colors.ENDC} {result['params']}")
        
        if result.get('time_range') and result['time_range'] and result['time_range'].get('start'):
            print(f"{Colors.BOLD}Time Range:{Colors.ENDC} {result['time_range']['start']} to {result['time_range']['end']}")
        
        print(f"{Colors.BOLD}Results:{Colors.ENDC} {result['count']:,} records found")
        print(f"{Colors.BOLD}Duration:{Colors.ENDC} {duration.total_seconds():.3f}s")
        
        if result['results']:
            print(f"\n{Colors.HEADER}Sample Results:{Colors.ENDC}")
            self.display_results_table(result['results'][:10])  # Show first 10
            
            if result['count'] > 10:
                print(f"\n{Colors.WARNING}... and {result['count'] - 10:,} more records{Colors.ENDC}")
        else:
            print(f"{Colors.WARNING}No results found{Colors.ENDC}")
    
    def display_error_result(self, result: Dict):
        """Display error results"""
        print(f"{Colors.FAIL}✗ Query failed{Colors.ENDC}")
        print(f"{Colors.BOLD}Error:{Colors.ENDC} {result['error']}")
        
        if result.get('sql'):
            print(f"{Colors.BOLD}Attempted SQL:{Colors.ENDC} {result['sql']}")
        
        print(f"\n{Colors.OKCYAN}Suggestions:{Colors.ENDC}")
        print("• Try rephrasing your question")
        print("• Use 'examples' to see sample queries")
        print("• Use 'schema' to see available data")
    
    def display_results_table(self, results: List[Dict]):
        """Display results in a formatted table"""
        if not results:
            return
        
        # Get column names
        columns = list(results[0].keys())
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = max(len(str(col)), max(len(str(row.get(col, ''))) for row in results))
            widths[col] = min(widths[col], 30)  # Max width of 30
        
        # Print header
        header = "│ " + " │ ".join(f"{col:>{widths[col]}}" for col in columns) + " │"
        separator = "├" + "┼".join("─" * (widths[col] + 2) for col in columns) + "┤"
        top_border = "┌" + "┬".join("─" * (widths[col] + 2) for col in columns) + "┐"
        bottom_border = "└" + "┴".join("─" * (widths[col] + 2) for col in columns) + "┘"
        
        print(f"{Colors.OKBLUE}{top_border}{Colors.ENDC}")
        print(f"{Colors.BOLD}{header}{Colors.ENDC}")
        print(f"{Colors.OKBLUE}{separator}{Colors.ENDC}")
        
        # Print rows
        for row in results:
            formatted_row = []
            for col in columns:
                value = str(row.get(col, ''))
                if len(value) > 30:
                    value = value[:27] + "..."
                formatted_row.append(f"{value:>{widths[col]}}")
            
            row_str = "│ " + " │ ".join(formatted_row) + " │"
            print(row_str)
        
        print(f"{Colors.OKBLUE}{bottom_border}{Colors.ENDC}")
    
    def do_examples(self, arg):
        """Show sample queries"""
        samples = self.interface.get_sample_queries()
        
        print(f"\n{Colors.HEADER}Sample Natural Language Queries:{Colors.ENDC}")
        print("=" * 50)
        
        for i, sample in enumerate(samples, 1):
            print(f"{Colors.OKGREEN}{i:2}.{Colors.ENDC} {sample}")
        
        print(f"\n{Colors.OKCYAN}Tip:{Colors.ENDC} You can copy and paste any of these queries to try them!")
    
    def do_schema(self, arg):
        """Show database schema with domain mappings"""
        schema = self.interface.get_database_schema()
        
        print(f"\n{Colors.HEADER}Database Schema & Domain Mappings:{Colors.ENDC}")
        print("=" * 60)
        
        for table_name, table_info in schema.items():
            if table_name == 'sqlite_sequence':
                continue
                
            print(f"\n{Colors.BOLD}{table_name}{Colors.ENDC}")
            
            # Show domain names
            domain_names = table_info.get('domain_names', [])
            if domain_names:
                print(f"{Colors.OKCYAN}  Domain names:{Colors.ENDC} {', '.join(domain_names)}")
            
            # Show columns
            print(f"{Colors.OKCYAN}  Columns:{Colors.ENDC}")
            for col in table_info['columns']:
                pk_marker = " (PK)" if col['pk'] else ""
                print(f"    • {col['name']} ({col['type']}){pk_marker}")
    
    def do_stats(self, arg):
        """Show database statistics"""
        print(f"\n{Colors.HEADER}Database Statistics:{Colors.ENDC}")
        print("=" * 30)
        
        stats_queries = [
            ("Total signal records", "SELECT COUNT(*) FROM RepData"),
            ("Total devices", "SELECT COUNT(*) FROM DevMap"),
            ("Total alerts", "SELECT COUNT(*) FROM AlertLog"),
            ("Online devices", "SELECT COUNT(*) FROM DevMap WHERE status = 'online'"),
            ("Offline devices", "SELECT COUNT(*) FROM DevMap WHERE status = 'offline'"),
            ("Recent alerts (24h)", "SELECT COUNT(*) FROM AlertLog WHERE timestamp > datetime('now', '-1 day')"),
            ("Unique sensor types", "SELECT COUNT(DISTINCT sensor_type) FROM RepData"),
            ("Total locations", "SELECT COUNT(*) FROM LocRef")
        ]
        
        for description, sql in stats_queries:
            try:
                self.interface.cursor.execute(sql)
                result = self.interface.cursor.fetchone()[0]
                print(f"{Colors.OKGREEN}  {description:25}:{Colors.ENDC} {result:,}")
            except Exception as e:
                print(f"{Colors.FAIL}  {description:25}:{Colors.ENDC} Error: {e}")
    
    def do_history(self, arg):
        """Show query history"""
        if not self.query_history:
            print(f"{Colors.WARNING}No queries in history{Colors.ENDC}")
            return
        
        print(f"\n{Colors.HEADER}Query History:{Colors.ENDC}")
        print("=" * 60)
        
        for i, entry in enumerate(reversed(self.query_history[-10:]), 1):
            timestamp = entry['timestamp'].strftime("%H:%M:%S")
            status = "✓" if entry['result']['success'] else "✗"
            duration = f"{entry['duration']:.3f}s"
            
            print(f"{Colors.OKGREEN}{i:2}. [{timestamp}] {status}{Colors.ENDC} {entry['query']}")
            if entry['result']['success']:
                count = entry['result']['count']
                print(f"     → {count:,} results in {duration}")
            else:
                print(f"     → Error: {entry['result']['error']}")
    
    def do_clear(self, arg):
        """Clear the screen"""
        import os
        os.system('clear' if os.name == 'posix' else 'cls')
        print(self.intro)
    
    def do_quit(self, arg):
        """Exit the CLI"""
        print(f"\n{Colors.OKGREEN}Thank you for using IoT Database CLI! Goodbye!{Colors.ENDC}")
        self.interface.close()
        return True
    
    def do_exit(self, arg):
        """Exit the CLI"""
        return self.do_quit(arg)
    
    def do_EOF(self, arg):
        """Handle Ctrl+D"""
        print()
        return self.do_quit(arg)
    
    def emptyline(self):
        """Handle empty line input"""
        pass
    
    def cmdloop(self, intro=None):
        """Enhanced command loop with error handling"""
        try:
            super().cmdloop(intro)
        except KeyboardInterrupt:
            print(f"\n\n{Colors.WARNING}Interrupted by user{Colors.ENDC}")
        except Exception as e:
            print(f"\n{Colors.FAIL}Unexpected error: {e}{Colors.ENDC}")
        finally:
            self.interface.close()

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='IoT Database Natural Language Query CLI')
    parser.add_argument('--query', '-q', help='Execute a single query and exit')
    parser.add_argument('--batch', '-b', help='Execute queries from a file')
    parser.add_argument('--json', action='store_true', help='Output results in JSON format')
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    
    args = parser.parse_args()
    
    # Disable colors if requested or not in a terminal
    if args.no_color or not sys.stdout.isatty():
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')
    
    cli = IoTCLI()
    
    if args.query:
        # Single query mode
        if args.json:
            result = cli.interface.execute_natural_language_query(args.query)
            print(json.dumps(result, indent=2, default=str))
        else:
            cli.execute_query(args.query)
        cli.interface.close()
    
    elif args.batch:
        # Batch mode
        try:
            with open(args.batch, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    query = line.strip()
                    if query and not query.startswith('#'):
                        print(f"\n--- Query {line_num}: {query} ---")
                        cli.execute_query(query)
        except FileNotFoundError:
            print(f"{Colors.FAIL}Error: File '{args.batch}' not found{Colors.ENDC}")
        except Exception as e:
            print(f"{Colors.FAIL}Error processing batch file: {e}{Colors.ENDC}")
        finally:
            cli.interface.close()
    
    else:
        # Interactive mode
        cli.cmdloop()

if __name__ == "__main__":
    main()