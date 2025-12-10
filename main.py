#!/usr/bin/env python3
"""
Main entry point for IoT Database CLI
Handles command-line arguments and application startup
"""

import argparse
import sys
import json
from iot_cli import IoTCLI, Colors

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