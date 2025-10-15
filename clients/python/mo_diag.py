#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MatrixOne Diagnostic Tool - Entry Point

Quick start:
    # Interactive mode
    python mo_diag.py
    python mo_diag.py --host localhost --port 6001 --database test
    
    # Non-interactive mode
    python mo_diag.py -d test -c "show_ivf_status"
    python mo_diag.py -d test -c "show_table_stats my_table -a"
"""

from matrixone.cli_tools import start_interactive_tool, MatrixOneCLI
from matrixone import Client
import argparse
import logging
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='MatrixOne Interactive Diagnostic Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Interactive mode
    python mo_diag.py
    python mo_diag.py --database test
    python mo_diag.py --host 192.168.1.100 --port 6001 --user admin --password secret
    
    # Non-interactive mode - execute single command
    python mo_diag.py -d test -c "show_ivf_status"
    python mo_diag.py -d test -c "show_table_stats my_table -a"
    python mo_diag.py -d test -c "show_table_stats my_table -a -d"
    python mo_diag.py -d test -c "sql SELECT COUNT(*) FROM my_table"
        """
    )

    parser.add_argument('--host', default='localhost',
                        help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=6001,
                        help='Database port (default: 6001)')
    parser.add_argument('--user', default='root',
                        help='Database user (default: root)')
    parser.add_argument('--password', default='111',
                        help='Database password (default: 111)')
    parser.add_argument('--database', '-d',
                        help='Database name (optional)')
    parser.add_argument('--log-level', default='ERROR',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level (default: ERROR)')
    parser.add_argument('--command', '-c',
                        help='Execute a single command and exit (non-interactive mode)')

    args = parser.parse_args()

    # Non-interactive mode: execute single command
    if args.command:
        # Set logging level
        level = getattr(logging, args.log_level.upper(), logging.ERROR)
        mo_logger = logging.getLogger('matrixone')
        mo_logger.setLevel(level)
        for handler in mo_logger.handlers:
            handler.setLevel(level)
        
        # Create client
        sql_log_mode = 'off' if level >= logging.ERROR else 'auto'
        client = Client(sql_log_mode=sql_log_mode)
        
        # Set level again after client creation
        mo_logger.setLevel(level)
        for handler in mo_logger.handlers:
            handler.setLevel(level)
        
        # Connect
        try:
            client.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.database)
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            sys.exit(1)
        
        # Create CLI instance and execute command
        cli = MatrixOneCLI(client)
        try:
            cli.onecmd(args.command)
        except Exception as e:
            print(f"❌ Command execution failed: {e}")
            sys.exit(1)
        finally:
            if client:
                try:
                    client.disconnect()
                except:
                    pass
    else:
        # Interactive mode
        start_interactive_tool(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            log_level=args.log_level
        )
