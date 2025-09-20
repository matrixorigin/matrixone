#!/usr/bin/env python3
"""
MatrixOne Python SDK CLI

Command-line interface for MatrixOne operations.
"""

import argparse
import sys
import os
from typing import Optional

from . import Client, __version__
from .exceptions import ConnectionError, QueryError


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="MatrixOne Python SDK CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Connect and execute a query
  matrixone-client -H localhost -P 6001 -u root -p 111 -d test -q "SELECT 1"
  
  # Check version
  matrixone-client -H localhost -P 6001 -u root -p 111 --version
  
  # Interactive mode
  matrixone-client -H localhost -P 6001 -u root -p 111 -d test -i
        """
    )
    
    # Connection arguments
    parser.add_argument('-H', '--host', default='localhost',
                       help='MatrixOne host (default: localhost)')
    parser.add_argument('-P', '--port', type=int, default=6001,
                       help='MatrixOne port (default: 6001)')
    parser.add_argument('-u', '--user', default='root',
                       help='Username (default: root)')
    parser.add_argument('-p', '--password', 
                       help='Password')
    parser.add_argument('-d', '--database',
                       help='Database name')
    
    # SSL arguments
    parser.add_argument('--ssl-mode', choices=['disabled', 'preferred', 'required'],
                       default='preferred', help='SSL mode (default: preferred)')
    parser.add_argument('--ssl-ca',
                       help='SSL CA certificate file')
    parser.add_argument('--ssl-cert',
                       help='SSL client certificate file')
    parser.add_argument('--ssl-key',
                       help='SSL client key file')
    
    # Operation arguments
    parser.add_argument('-q', '--query',
                       help='SQL query to execute')
    parser.add_argument('-i', '--interactive', action='store_true',
                       help='Interactive mode')
    parser.add_argument('--version', action='store_true',
                       help='Show MatrixOne backend version')
    parser.add_argument('--sdk-version', action='store_true',
                       help='Show SDK version')
    
    # Output arguments
    parser.add_argument('-f', '--format', choices=['table', 'csv', 'json'],
                       default='table', help='Output format (default: table)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Handle SDK version (doesn't require connection)
    if args.sdk_version:
        print(f"MatrixOne Python SDK version: {__version__}")
        return 0
    
    # Create client
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            ssl_mode=args.ssl_mode,
            ssl_ca=args.ssl_ca,
            ssl_cert=args.ssl_cert,
            ssl_key=args.ssl_key
        )
        
        if args.verbose:
            print(f"Connected to MatrixOne at {args.host}:{args.port}")
        
        # Handle version check
        if args.version:
            version = client.get_backend_version()
            is_dev = client.is_development_version()
            version_type = "Development" if is_dev else "Release"
            print(f"MatrixOne backend version: {version} ({version_type})")
            return 0
        
        # Handle single query
        if args.query:
            execute_query(client, args.query, args.format, args.verbose)
            return 0
        
        # Handle interactive mode
        if args.interactive:
            interactive_mode(client, args.format, args.verbose)
            return 0
        
        # No operation specified
        parser.print_help()
        return 1
        
    except ConnectionError as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return 1
    except QueryError as e:
        print(f"Query failed: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            client.disconnect()
        except:
            pass


def execute_query(client: Client, query: str, format_type: str, verbose: bool):
    """Execute a single query and display results"""
    if verbose:
        print(f"Executing query: {query}")
    
    result = client.execute(query)
    
    if result.rows:
        display_results(result, format_type)
    else:
        print(f"Query executed successfully. Affected rows: {result.affected_rows}")


def interactive_mode(client: Client, format_type: str, verbose: bool):
    """Interactive mode for executing multiple queries"""
    print("MatrixOne Python SDK Interactive Mode")
    print("Type 'exit' or 'quit' to exit, 'help' for help")
    print("-" * 50)
    
    while True:
        try:
            query = input("MatrixOne> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['exit', 'quit']:
                print("Goodbye!")
                break
            
            if query.lower() == 'help':
                print("Available commands:")
                print("  SELECT ...  - Execute SELECT query")
                print("  INSERT ...  - Execute INSERT query")
                print("  UPDATE ...  - Execute UPDATE query")
                print("  DELETE ...  - Execute DELETE query")
                print("  SHOW ...    - Execute SHOW query")
                print("  exit/quit   - Exit interactive mode")
                print("  help        - Show this help")
                continue
            
            if verbose:
                print(f"Executing: {query}")
            
            result = client.execute(query)
            
            if result.rows:
                display_results(result, format_type)
            else:
                print(f"✓ Query executed. Affected rows: {result.affected_rows}")
                
        except KeyboardInterrupt:
            print("\nUse 'exit' or 'quit' to exit")
        except Exception as e:
            print(f"Error: {e}")


def display_results(result, format_type: str):
    """Display query results in specified format"""
    if not result.rows:
        return
    
    if format_type == 'table':
        display_table(result)
    elif format_type == 'csv':
        display_csv(result)
    elif format_type == 'json':
        display_json(result)


def display_table(result):
    """Display results in table format"""
    if not result.rows:
        return
    
    # Calculate column widths
    col_widths = []
    for i, col in enumerate(result.columns):
        max_width = len(col)
        for row in result.rows:
            max_width = max(max_width, len(str(row[i])))
        col_widths.append(max_width)
    
    # Print header
    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(result.columns))
    print(header)
    print("-" * len(header))
    
    # Print rows
    for row in result.rows:
        row_str = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
        print(row_str)


def display_csv(result):
    """Display results in CSV format"""
    import csv
    import io
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(result.columns)
    
    # Write rows
    writer.writerows(result.rows)
    
    print(output.getvalue().strip())


def display_json(result):
    """Display results in JSON format"""
    import json
    
    data = {
        "columns": result.columns,
        "rows": [dict(zip(result.columns, row)) for row in result.rows],
        "affected_rows": result.affected_rows
    }
    
    print(json.dumps(data, indent=2, default=str))


if __name__ == "__main__":
    sys.exit(main())
