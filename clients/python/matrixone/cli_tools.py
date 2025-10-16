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
MatrixOne Interactive Diagnostic Tool

A command-line tool for diagnosing and inspecting MatrixOne database objects,
especially secondary indexes and vector indexes.
"""

import cmd
import logging
import sys
from pathlib import Path
from typing import Optional

from .client import Client

# Set default logging level to ERROR to keep output clean
# Users can override this with --log-level parameter
logging.getLogger('matrixone').setLevel(logging.ERROR)

# Try to import prompt_toolkit for better input experience
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.formatted_text import HTML
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.styles import Style

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False


# Custom completer for mo-diag commands
if PROMPT_TOOLKIT_AVAILABLE:

    class MODiagCompleter(Completer):
        """Smart completer for mo-diag commands that provides table and database name completion"""

        def __init__(self, cli_instance):
            self.cli = cli_instance

        def get_completions(self, document, complete_event):
            """Generate completions based on current input"""
            text = document.text_before_cursor
            words = text.split()

            # Available commands
            all_commands = [
                'show_indexes',
                'show_all_indexes',
                'verify_counts',
                'show_ivf_status',
                'show_table_stats',
                'flush_table',
                'tables',
                'databases',
                'sql',
                'use',
                'connect',
                'history',
                'help',
                'exit',
            ]

            # If empty or only whitespace, suggest commands
            if not words:
                for command in all_commands:
                    yield Completion(command, start_position=0)
                return

            # If we're typing the first word (command), complete it
            if len(words) == 1 and not text.endswith(' '):
                partial_command = words[0]
                for command in all_commands:
                    if command.startswith(partial_command):
                        yield Completion(command, start_position=-len(partial_command))
                return

            command = words[0]

            # Commands that expect table name as first argument
            table_commands = ['show_indexes', 'verify_counts', 'show_table_stats', 'flush_table']
            # Commands that expect database name
            database_commands = ['use']

            # Determine what to complete
            if command in table_commands:
                # If we're on the first argument after the command
                if len(words) == 1 or (len(words) == 2 and not text.endswith(' ')):
                    # Complete table names
                    partial = words[1] if len(words) == 2 else ''
                    for table in self._get_tables():
                        if table.startswith(partial):
                            yield Completion(table, start_position=-len(partial))
                # Second argument might be database name
                elif len(words) == 2 or (len(words) == 3 and not text.endswith(' ')):
                    partial = words[2] if len(words) == 3 else ''
                    for db in self._get_databases():
                        if db.startswith(partial):
                            yield Completion(db, start_position=-len(partial))

            elif command in database_commands:
                # Complete database names
                if len(words) == 1 or (len(words) == 2 and not text.endswith(' ')):
                    partial = words[1] if len(words) == 2 else ''
                    for db in self._get_databases():
                        if db.startswith(partial):
                            yield Completion(db, start_position=-len(partial))

            elif command == 'show_ivf_status':
                # Can take -t <table> or database name
                if '-t' in words:
                    # After -t, complete table names
                    t_index = words.index('-t')
                    if len(words) == t_index + 1 or (len(words) == t_index + 2 and not text.endswith(' ')):
                        partial = words[t_index + 1] if len(words) == t_index + 2 else ''
                        for table in self._get_tables():
                            if table.startswith(partial):
                                yield Completion(table, start_position=-len(partial))
                else:
                    # First argument is database name
                    if len(words) == 1 or (len(words) == 2 and not text.endswith(' ')):
                        partial = words[1] if len(words) == 2 else ''
                        for db in self._get_databases():
                            if db.startswith(partial):
                                yield Completion(db, start_position=-len(partial))

        def _get_tables(self):
            """Get list of tables in current database"""
            if not self.cli.client or not self.cli.current_database:
                return []
            try:
                result = self.cli.client.execute("SHOW TABLES")
                return [row[0] for row in result.fetchall()]
            except Exception:
                return []

        def _get_databases(self):
            """Get list of all databases"""
            if not self.cli.client:
                return []
            try:
                result = self.cli.client.execute("SHOW DATABASES")
                return [row[0] for row in result.fetchall()]
            except Exception:
                return []


# ANSI Color codes for terminal output
class Colors:
    """ANSI color codes"""

    RESET = '\033[0m'
    BOLD = '\033[1m'

    # Foreground colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

    # Background colors
    BG_RED = '\033[101m'
    BG_GREEN = '\033[102m'
    BG_YELLOW = '\033[103m'

    @staticmethod
    def disable():
        """Disable colors (for non-terminal output)"""
        Colors.RESET = ''
        Colors.BOLD = ''
        Colors.RED = ''
        Colors.GREEN = ''
        Colors.YELLOW = ''
        Colors.BLUE = ''
        Colors.MAGENTA = ''
        Colors.CYAN = ''
        Colors.WHITE = ''
        Colors.BG_RED = ''
        Colors.BG_GREEN = ''
        Colors.BG_YELLOW = ''


# Check if output is to a terminal
if not sys.stdout.isatty():
    Colors.disable()


def success(msg):
    """Print success message in green"""
    return f"{Colors.GREEN}{msg}{Colors.RESET}"


def error(msg):
    """Print error message in red"""
    return f"{Colors.RED}{msg}{Colors.RESET}"


def warning(msg):
    """Print warning message in yellow"""
    return f"{Colors.YELLOW}{msg}{Colors.RESET}"


def info(msg):
    """Print info message in cyan"""
    return f"{Colors.CYAN}{msg}{Colors.RESET}"


def bold(msg):
    """Print message in bold"""
    return f"{Colors.BOLD}{msg}{Colors.RESET}"


def header(msg):
    """Print header in bold cyan"""
    return f"{Colors.BOLD}{Colors.CYAN}{msg}{Colors.RESET}"


class MatrixOneCLI(cmd.Cmd):
    """Interactive CLI for MatrixOne diagnostics"""

    intro = """
╔══════════════════════════════════════════════════════════════╗
║         MatrixOne Interactive Diagnostic Tool                ║
║                                                              ║
║  Type help or ? to list commands.                            ║
║  Type help <command> for detailed help on a command.         ║
║                                                              ║
║  Tips:                                                       ║
║    • Press Tab for auto-completion (tables/databases)        ║
║    • Use ↑/↓ arrows to browse command history                ║
║    • Press Ctrl+R for history search                         ║
╚══════════════════════════════════════════════════════════════╝
"""

    prompt = f'{Colors.BOLD}{Colors.GREEN}MO-DIAG>{Colors.RESET} '

    def __init__(self, client: Optional[Client] = None):
        """
        Initialize the CLI tool.

        Args:
            client: Optional MatrixOne client. If not provided, you'll need to connect manually.
        """
        super().__init__()
        self.client = client
        self.current_database = None

        # Setup prompt_toolkit session if available
        if PROMPT_TOOLKIT_AVAILABLE:
            # Setup command history file
            history_file = Path.home() / '.mo_diag_history'

            # Create completer
            completer = MODiagCompleter(self)

            self.session = PromptSession(
                history=FileHistory(str(history_file)),
                completer=completer,
                complete_while_typing=False,  # Only complete on Tab
                style=Style.from_dict(
                    {
                        'prompt': 'bold ansigreen',
                        'database': 'bold ansiyellow',
                    }
                ),
            )
        else:
            self.session = None

        if self.client and hasattr(self.client, '_connection_params'):
            self.current_database = self.client._connection_params.get('database')
            if self.current_database:
                self.prompt = (
                    f'{Colors.BOLD}{Colors.GREEN}MO-DIAG{Colors.RESET}{Colors.BOLD}'
                    f'[{Colors.YELLOW}{self.current_database}{Colors.RESET}{Colors.BOLD}]'
                    f'{Colors.GREEN}>{Colors.RESET} '
                )

    def cmdloop(self, intro=None):
        """Override cmdloop to use prompt_toolkit for better input experience"""
        if not PROMPT_TOOLKIT_AVAILABLE or not self.session:
            # Fall back to standard cmdloop
            return super().cmdloop(intro)

        # Print intro
        self.preloop()
        if intro is not None:
            self.intro = intro
        if self.intro:
            self.stdout.write(str(self.intro) + "\n")

        stop = None
        while not stop:
            try:
                # Create colored prompt
                if self.current_database:
                    prompt_text = HTML(
                        f'<prompt>MO-DIAG</prompt>[<database>{self.current_database}</database>]<prompt>&gt;</prompt> '
                    )
                else:
                    prompt_text = HTML('<prompt>MO-DIAG&gt;</prompt> ')

                line = self.session.prompt(prompt_text)
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            except KeyboardInterrupt:
                print("^C")
            except EOFError:
                print()
                break
        self.postloop()

    def do_connect(self, arg):
        """
        Connect to MatrixOne database.

        Usage: connect <host> <port> <user> <password> [database]
        Example: connect localhost 6001 root 111 test
        """
        args = arg.split()
        if len(args) < 4:
            print("❌ Usage: connect <host> <port> <user> <password> [database]")
            return

        host = args[0]
        port = int(args[1])
        user = args[2]
        password = args[3]
        database = args[4] if len(args) > 4 else None

        try:
            if not self.client:
                self.client = Client()

            self.client.connect(host=host, port=port, user=user, password=password, database=database)
            self.current_database = database
            if database:
                self.prompt = f'{Colors.BOLD}{Colors.GREEN}MO-DIAG{Colors.RESET}{Colors.BOLD}[{Colors.YELLOW}{database}{Colors.RESET}{Colors.BOLD}]{Colors.GREEN}>{Colors.RESET} '
            else:
                self.prompt = f'{Colors.BOLD}{Colors.GREEN}MO-DIAG>{Colors.RESET} '
            print(
                f"{Colors.GREEN}✓ Connected to {host}:{port}"
                + (f" (database: {database})" if database else "")
                + Colors.RESET
            )
        except Exception as e:
            print(f"{Colors.RED}❌ Connection failed: {e}{Colors.RESET}")

    def do_use(self, arg):
        """
        Switch to a different database.

        Usage: use <database>
        Example: use test
        """
        if not arg:
            print("❌ Usage: use <database>")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        # Remove trailing semicolon if present
        database = arg.strip().rstrip(';')

        try:
            self.client.execute(f"USE {database}")
            self.current_database = database
            self.prompt = f'{Colors.BOLD}{Colors.GREEN}MO-DIAG{Colors.RESET}{Colors.BOLD}[{Colors.YELLOW}{database}{Colors.RESET}{Colors.BOLD}]{Colors.GREEN}>{Colors.RESET} '
            print(f"{Colors.GREEN}✓ Switched to database: {database}{Colors.RESET}")
        except Exception as e:
            print(f"❌ Failed to switch database: {e}")

    def do_show_indexes(self, arg):
        """
        Show all secondary indexes for a table, including IVF, HNSW, Fulltext, and regular indexes.

        Uses vertical output format (like MySQL \\G) for easy reading of long table names.

        Usage: show_indexes <table_name> [database]

        Example:
            show_indexes cms_all_content_chunk_info
            show_indexes cms_all_content_chunk_info repro3
        """
        # Remove trailing semicolon
        arg = arg.strip().rstrip(';')
        args = arg.split()
        if not args:
            print("❌ Usage: show_indexes <table_name> [database]")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        table_name = args[0]
        database = args[1] if len(args) > 1 else self.current_database

        try:
            # Use the new comprehensive index detail API
            indexes = self.client.get_table_indexes_detail(table_name, database)

            if not indexes:
                print(f"⚠️  No secondary indexes found for table '{table_name}' in database '{database}'")
                return

            table_info = f"'{database}.{table_name}'"
            print(f"\n{header('📊 Secondary Indexes for')} {bold(table_info)}\n")

            # Vertical display format (like MySQL \G)
            for i, idx in enumerate(indexes, 1):
                index_name = idx['index_name']
                algo = idx['algo'] if idx['algo'] else 'regular'
                table_type = idx['algo_table_type'] if idx['algo_table_type'] else '-'
                physical_table = idx['physical_table_name']

                # Filter out internal columns like __mo_alias___mo_cpkey_col
                user_columns = [col for col in idx['columns'] if not col.startswith('__mo_alias_')]
                columns = ', '.join(user_columns) if user_columns else 'N/A'

                # Color code by algorithm type
                if algo == 'ivfflat':
                    algo_display = f"{Colors.CYAN}{algo}{Colors.RESET}"
                elif algo == 'hnsw':
                    algo_display = f"{Colors.GREEN}{algo}{Colors.RESET}"
                elif algo == 'fulltext':
                    algo_display = f"{Colors.YELLOW}{algo}{Colors.RESET}"
                else:
                    algo_display = algo

                # Print in vertical format
                print(info(f"{'*' * 27} {i}. row {'*' * 27}"))
                print(f"      {bold('Index Name')}: {Colors.CYAN}{index_name}{Colors.RESET}")
                print(f"       {bold('Algorithm')}: {algo_display}")
                print(f"      {bold('Table Type')}: {table_type}")
                print(f"  {bold('Physical Table')}: {physical_table}")
                print(f"         {bold('Columns')}: {columns}")

                # For vector/fulltext indexes, show table statistics using metadata.scan interface
                if algo in ['ivfflat', 'hnsw', 'fulltext']:
                    try:
                        # Use SDK's metadata.scan interface with columns="*" to get structured results
                        stats = self.client.metadata.scan(database, physical_table, columns="*")

                        if stats:
                            # Aggregate statistics from all objects
                            total_rows = 0
                            total_compress_size = 0
                            total_origin_size = 0

                            # Deduplicate by object_name (same logic as show_table_stats)
                            seen_objects = set()
                            object_count = 0

                            for obj in stats:
                                # MetadataRow has attributes, not dictionary keys
                                object_name = getattr(obj, 'object_name', None)
                                if object_name and object_name not in seen_objects:
                                    seen_objects.add(object_name)
                                    object_count += 1

                                    # Sum up statistics using attributes
                                    row_cnt = getattr(obj, 'row_cnt', None) or getattr(obj, 'rows_cnt', 0) or 0
                                    total_rows += int(row_cnt) if row_cnt else 0

                                    compress_size = getattr(obj, 'compress_size', 0) or 0
                                    total_compress_size += int(compress_size) if compress_size else 0

                                    origin_size = getattr(obj, 'origin_size', 0) or 0
                                    total_origin_size += int(origin_size) if origin_size else 0

                            # Format sizes
                            def format_size(size_bytes):
                                if size_bytes >= 1024 * 1024:
                                    return f"{size_bytes / (1024 * 1024):.2f} MB"
                                elif size_bytes >= 1024:
                                    return f"{size_bytes / 1024:.2f} KB"
                                else:
                                    return f"{size_bytes} B"

                            compress_size_str = format_size(total_compress_size)
                            origin_size_str = format_size(total_origin_size)

                            print(f"      {bold('Statistics')}:")
                            print(f"                   - Objects: {object_count}")
                            print(f"                   - Rows: {total_rows:,}")
                            print(f"                   - Compressed Size: {compress_size_str}")
                            print(f"                   - Original Size: {origin_size_str}")
                    except Exception:
                        # If stats not available, just skip (no error message needed)
                        pass

            # Summary
            print(info("=" * 60))

            algo_counts = {}
            for idx in indexes:
                algo = idx['algo'] if idx['algo'] else 'regular'
                algo_counts[algo] = algo_counts.get(algo, 0) + 1

            summary_parts = []
            for algo, count in sorted(algo_counts.items()):
                summary_parts.append(f"{count} {algo}")

            print(bold(f"Total: {len(indexes)} index tables") + f" ({', '.join(summary_parts)})" + "\n")

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_show_all_indexes(self, arg):
        """
        Show index health report for all tables with secondary indexes.

        This command performs diagnostic checks including:
        - Row count consistency between main table and index tables
        - Vector index building status (IVF/HNSW)
        - Index type distribution
        - Problem detection

        Usage: show_all_indexes [database]
        Example:
            show_all_indexes
            show_all_indexes repro3
        """
        # Remove trailing semicolon
        arg = arg.strip().rstrip(';')
        database = arg if arg else self.current_database

        if not database:
            print("❌ No database specified and no current database set. Use 'use <database>' or provide database name.")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        try:
            # Get all tables with indexes
            sql = """
                SELECT DISTINCT mo_tables.relname AS table_name
                FROM mo_catalog.mo_indexes
                JOIN mo_catalog.mo_tables ON mo_indexes.table_id = mo_tables.rel_id
                WHERE mo_indexes.type IN ('MULTIPLE', 'UNIQUE') AND mo_tables.reldatabase = ?
                ORDER BY mo_tables.relname
            """
            result = self.client.execute(sql, (database,))
            tables = [row[0] for row in result.fetchall()]

            if not tables:
                print(f"⚠️  No tables with secondary indexes found in database '{database}'")
                return

            print(f"\n{header('📊 Index Health Report')} for Database '{database}':")
            print("=" * 120)

            healthy_tables = []
            attention_tables = []

            # Check each table
            for table_name in tables:
                try:
                    # Get index details
                    indexes = self.client.get_table_indexes_detail(table_name, database)
                    index_count = len(set(idx['index_name'] for idx in indexes))

                    # Determine if table has special indexes (IVF/HNSW/Fulltext)
                    has_vector_or_fulltext = any(idx['algo'] in ['ivfflat', 'hnsw', 'fulltext'] for idx in indexes)

                    # Check row consistency (only for regular/UNIQUE indexes)
                    consistency_status = None
                    has_issue = False
                    issue_detail = None

                    if not has_vector_or_fulltext:
                        # Only check row consistency for tables with regular/UNIQUE indexes
                        try:
                            row_count = self.client.verify_table_index_counts(table_name)
                            consistency_status = f"✓ {row_count:,} rows"
                        except ValueError as e:
                            consistency_status = "❌ Mismatch"
                            has_issue = True
                            issue_detail = str(e).split('\n')[0]  # First line of error
                        except Exception:
                            consistency_status = "⚠️  Unknown"
                    else:
                        # For vector/fulltext indexes, just show row count without verification
                        try:
                            result = self.client.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                            row_count = result.fetchone()[0]
                            consistency_status = f"{row_count:,} rows"
                        except Exception:
                            consistency_status = "Unknown"

                    # Check IVF/HNSW/Fulltext status
                    special_index_status = None
                    for idx in indexes:
                        if idx['algo'] == 'ivfflat':
                            try:
                                stats = self.client.vector_ops.get_ivf_stats(table_name, idx['columns'][0])
                                if stats and 'distribution' in stats:
                                    centroid_ids = stats['distribution'].get('centroid_id', [])
                                    centroid_counts = stats['distribution'].get('centroid_count', [])
                                    centroid_count = len(centroid_ids)
                                    total_vectors = sum(centroid_counts) if centroid_counts else 0

                                    if centroid_count > 0 and total_vectors > 0:
                                        special_index_status = f"IVF: {centroid_count} centroids, {total_vectors} vectors"
                                    elif centroid_count > 0:
                                        special_index_status = f"IVF: {centroid_count} centroids"
                                    else:
                                        special_index_status = "IVF: building"
                                        has_issue = True
                                        issue_detail = "IVF index not built yet"
                                else:
                                    special_index_status = "IVF: no stats available"
                            except Exception as e:
                                error_msg = str(e)
                                # Truncate long error messages for display
                                if len(error_msg) > 30:
                                    error_short = error_msg[:27] + "..."
                                else:
                                    error_short = error_msg
                                special_index_status = f"IVF: error ({error_short})"
                                has_issue = True
                                if not issue_detail:
                                    issue_detail = f"Failed to get IVF stats: {error_msg}"
                        elif idx['algo'] == 'hnsw':
                            special_index_status = "HNSW index"
                        elif idx['algo'] == 'fulltext':
                            special_index_status = "Fulltext index"

                    # Categorize table
                    table_info = {
                        'name': table_name,
                        'index_count': index_count,
                        'consistency': consistency_status,
                        'special_index_status': special_index_status,
                        'has_issue': has_issue,
                        'issue_detail': issue_detail,
                    }

                    if has_issue:
                        attention_tables.append(table_info)
                    else:
                        healthy_tables.append(table_info)

                except Exception as e:
                    # If we can't check this table, mark it as needing attention
                    attention_tables.append(
                        {
                            'name': table_name,
                            'index_count': '?',
                            'consistency': '❌ Error',
                            'special_index_status': None,
                            'has_issue': True,
                            'issue_detail': str(e)[:50],
                        }
                    )

            # Display healthy tables
            if healthy_tables:
                print(f"\n{success('✓ HEALTHY')} ({len(healthy_tables)} tables)")
                print("-" * 120)
                print(f"{'Table Name':<35} | {'Indexes':<8} | {'Row Count':<20} | {'Notes'}")
                print("-" * 120)

                for table in healthy_tables:
                    notes = table['special_index_status'] if table['special_index_status'] else '-'
                    print(f"{table['name']:<35} | {table['index_count']:<8} | {table['consistency']:<20} | {notes}")

            # Display tables needing attention
            if attention_tables:
                print(f"\n{warning('⚠️  ATTENTION NEEDED')} ({len(attention_tables)} tables)")
                print("-" * 120)
                print(f"{'Table Name':<35} | {'Issue':<40} | {'Details'}")
                print("-" * 120)

                for table in attention_tables:
                    if 'Mismatch' in (table['consistency'] or ''):
                        issue = "Row count mismatch between indexes"
                        details = table['issue_detail'] if table['issue_detail'] else "Check with verify_counts"
                    elif 'building' in (table.get('special_index_status') or ''):
                        issue = "Vector index building incomplete"
                        details = table['issue_detail'] if table['issue_detail'] else "Check with show_ivf_status"
                    elif 'error' in (table.get('special_index_status') or '').lower():
                        issue = "Vector index error"
                        details = table['issue_detail'] if table['issue_detail'] else "Check index status"
                    elif 'Error' in (table['consistency'] or ''):
                        issue = "Unable to verify table"
                        details = table['issue_detail'] if table['issue_detail'] else "Unknown error"
                    else:
                        issue = "Unknown issue"
                        details = str(table.get('issue_detail', '-'))

                    print(f"{table['name']:<35} | {issue:<40} | {details[:38]}")

            # Summary
            print("\n" + "=" * 120)
            print(f"{bold('Summary:')}")
            print(f"  {success('✓')} {len(healthy_tables)} healthy tables")
            if attention_tables:
                print(f"  {warning('⚠️ ')} {len(attention_tables)} tables need attention")
            else:
                print(f"  {success('✓')} All indexes healthy!")
            print(f"  Total: {len(tables)} tables with indexes\n")

            if attention_tables:
                print(f"{info('💡 Tip:')} Use 'verify_counts <table>' or 'show_ivf_status' for detailed diagnostics")

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_verify_counts(self, arg):
        """
        Verify row counts between main table and all its index tables.

        Usage: verify_counts <table_name> [database]
        Example:
            verify_counts cms_all_content_chunk_info
            verify_counts cms_all_content_chunk_info repro3
        """
        # Remove trailing semicolon
        arg = arg.strip().rstrip(';')
        args = arg.split()
        if not args:
            print("❌ Usage: verify_counts <table_name> [database]")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        table_name = args[0]
        database = args[1] if len(args) > 1 else self.current_database

        if not database:
            print("❌ No database specified and no current database set.")
            return

        try:
            # Switch to the database temporarily if needed
            current_db = self.client._connection_params.get('database')
            if current_db != database:
                self.client.execute(f"USE {database}")

            # Get index tables
            index_tables = self.client.get_secondary_index_tables(table_name, database)

            if not index_tables:
                print(f"⚠️  No secondary indexes found for table '{table_name}'")
                # Still show main table count
                result = self.client.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                count = result.fetchone()[0]
                print(f"Main table '{table_name}': {count:,} rows")
                return

            # Build verification SQL
            from .index_utils import build_verify_counts_sql

            sql = build_verify_counts_sql(table_name, index_tables)

            result = self.client.execute(sql)
            row = result.fetchone()

            main_count = row[0]
            mismatches = []

            table_info = f"'{database}.{table_name}'"
            print(f"\n{header('📊 Row Count Verification for')} {bold(table_info)}")
            print(info("=" * 80))
            print(bold(f"Main table: {main_count:,} rows"))
            print(info("-" * 80))

            for idx, index_table in enumerate(index_tables):
                index_count = row[idx + 1]
                if index_count == main_count:
                    print(f"{success('✓')} {index_table}: {bold(f'{index_count:,}')} rows")
                else:
                    print(f"{error('✗ MISMATCH')} {index_table}: {bold(f'{index_count:,}')} rows")
                    mismatches.append((index_table, index_count))

            print(info("=" * 80))

            if mismatches:
                print(f"❌ FAILED: {len(mismatches)} index table(s) have mismatched counts!")
            else:
                print(f"✅ PASSED: All index tables match ({main_count:,} rows)")
            print()

            # Switch back if needed
            if current_db and current_db != database:
                self.client.execute(f"USE {current_db}")

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_show_ivf_status(self, arg):
        """
        Show IVF index centroids building status.

        Usage:
            show_ivf_status [database]             - Show compact summary
            show_ivf_status [database] -v          - Show detailed view
            show_ivf_status [database] -t table    - Filter by table name

        Example:
            show_ivf_status
            show_ivf_status test -v
            show_ivf_status test -t my_table
        """
        # Parse arguments
        args = arg.strip().rstrip(';').split() if arg.strip() else []
        database = None
        verbose = False
        filter_table = None

        i = 0
        while i < len(args):
            if args[i] == '-v':
                verbose = True
            elif args[i] == '-t' and i + 1 < len(args):
                filter_table = args[i + 1]
                i += 1
            elif not database:
                database = args[i]
            i += 1

        if not database:
            database = self.current_database

        if not database:
            print("❌ No database specified and no current database set.")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        try:
            # Get all tables with IVF indexes and their column information
            # Group by table and index name to avoid duplicates from multiple index table types
            sql = """
                SELECT
                    mo_tables.relname,
                    mo_indexes.name,
                    mo_indexes.column_name,
                    MAX(mo_indexes.algo_table_type) as algo_table_type
                FROM mo_catalog.mo_indexes
                JOIN mo_catalog.mo_tables ON mo_indexes.table_id = mo_tables.rel_id
                WHERE mo_tables.reldatabase = ?
                  AND mo_indexes.algo LIKE '%ivf%'
                GROUP BY mo_tables.relname, mo_indexes.name, mo_indexes.column_name
                ORDER BY mo_tables.relname, mo_indexes.name
            """
            result = self.client.execute(sql, (database,))
            index_info = result.fetchall()

            if not index_info:
                print(f"⚠️  No IVF indexes found in database '{database}'")
                return

            # Filter by table if specified
            if filter_table:
                index_info = [row for row in index_info if row[0] == filter_table]
                if not index_info:
                    print(f"⚠️  No IVF indexes found for table '{filter_table}'")
                    return

            print(f"\n📊 IVF Index Status in '{database}':")
            print("=" * 150)

            if not verbose:
                # Compact table view
                print(
                    f"{'Table':<30} | {'Index':<25} | {'Column':<20} | {'Centroids':<10} | {'Vectors':<12} | {'Balance':<10} | {'Status':<15}"
                )
                print("-" * 150)

            total_ivf_indexes = 0

            # For each index, get its stats using SDK API
            for row in index_info:
                table_name = row[0]
                index_name = row[1]
                column_name = row[2]
                algo_table_type = row[3] if len(row) > 3 else None

                try:
                    # Use SDK's get_ivf_stats method with column name
                    stats = self.client.vector_ops.get_ivf_stats(table_name, column_name)

                    if stats and 'index_tables' in stats:
                        # Show centroid distribution
                        if 'distribution' in stats:
                            dist = stats['distribution']
                            centroid_counts = dist.get('centroid_count', [])

                            if centroid_counts:
                                total_centroids = len(centroid_counts)
                                total_vectors = sum(centroid_counts)
                                min_count = min(centroid_counts)
                                max_count = max(centroid_counts)
                                avg_count = total_vectors / total_centroids if total_centroids > 0 else 0
                                balance = max_count / min_count if min_count > 0 else 0

                                if verbose:
                                    print(f"\nTable: {table_name} | Index: {index_name} | Column: {column_name}")
                                    print("-" * 150)

                                    # Show physical tables
                                    index_tables = stats['index_tables']
                                    print("Physical Tables:")
                                    for table_type, physical_table in index_tables.items():
                                        print(f"  - {table_type:<15}: {physical_table}")

                                    print("\nCentroid Distribution:")
                                    print(f"  Total Centroids: {total_centroids}")
                                    print(f"  Total Vectors:   {total_vectors:,}")
                                    print(f"  Min/Avg/Max:     {min_count} / {avg_count:.1f} / {max_count}")
                                    print(
                                        f"  Load Balance:    {balance:.2f}x" if min_count > 0 else "  Load Balance:    N/A"
                                    )

                                    # Show top 10 centroids by size
                                    if len(centroid_counts) > 0:
                                        centroid_ids = dist.get('centroid_id', [])
                                        centroid_versions = dist.get('centroid_version', [])
                                        print("\n  Top Centroids (by vector count):")
                                        centroid_data = list(zip(centroid_ids, centroid_counts, centroid_versions))
                                        centroid_data.sort(key=lambda x: x[1], reverse=True)

                                        for i, (cid, count, version) in enumerate(centroid_data[:10], 1):
                                            print(f"    {i:2}. Centroid {cid}: {count:,} vectors (version {version})")
                                else:
                                    # Compact view
                                    status = "✓ active"
                                    print(
                                        f"{table_name:<30} | {index_name:<25} | {column_name:<20} | {total_centroids:<10} | {total_vectors:<12,} | {balance:<10.2f} | {status:<15}"
                                    )

                                total_ivf_indexes += 1
                            else:
                                if verbose:
                                    print("\nCentroid Distribution: ⚠️  No centroids found (empty index)")
                                else:
                                    print(
                                        f"{table_name:<30} | {index_name:<25} | {column_name:<20} | {'0':<10} | {'0':<12} | {'N/A':<10} | {'⚠️ empty':<15}"
                                    )
                        else:
                            if verbose:
                                print("\nCentroid Distribution: ⚠️  No distribution data available")
                            else:
                                print(
                                    f"{table_name:<30} | {index_name:<25} | {column_name:<20} | {'?':<10} | {'?':<12} | {'?':<10} | {'⚠️ no data':<15}"
                                )
                    else:
                        # Index exists but no stats available
                        if verbose:
                            print(f"\nTable: {table_name} | Index: {index_name}")
                            print("-" * 150)
                            print(f"Status: ⚠️  {algo_table_type if algo_table_type else 'No stats available'}")
                        else:
                            print(
                                f"{table_name:<30} | {index_name:<25} | {column_name:<20} | {'?':<10} | {'?':<12} | {'?':<10} | {'⚠️ no stats':<15}"
                            )

                except Exception as e:
                    # If we can't get IVF stats, show the error
                    if verbose:
                        print(f"\nTable: {table_name} | Index: {index_name}")
                        print("-" * 150)
                        print(f"Status: ❌ Error - {str(e)}")
                    else:
                        error_msg = str(e)[:10]
                        print(
                            f"{table_name:<30} | {index_name:<25} | {column_name:<20} | {'?':<10} | {'?':<12} | {'?':<10} | {'❌ ' + error_msg:<15}"
                        )
                    continue

            print("=" * 150)
            if total_ivf_indexes == 0:
                print("⚠️  No accessible IVF indexes found")
            else:
                print(f"Total: {total_ivf_indexes} IVF indexes")
                if not verbose:
                    print(f"\nTip: Use 'show_ivf_status {database} -v' for detailed view with top centroids")
                    print(f"     Use 'show_ivf_status {database} -t <table>' to filter by table")
            print()

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_show_table_stats(self, arg):
        """
        Show table statistics using metadata_scan.

        Usage:
            show_table_stats <table> [database]           - Show table stats summary
            show_table_stats <table> [database] -t        - Include tombstone stats
            show_table_stats <table> [database] -i idx1,idx2  - Include specific index stats
            show_table_stats <table> [database] -a        - Include all (tombstone + all indexes)
            show_table_stats <table> [database] -d        - Show detailed object list

        Example:
            show_table_stats my_table
            show_table_stats my_table test -t
            show_table_stats my_table test -i idx_vec,idx_name
            show_table_stats my_table test -a
            show_table_stats my_table test -a -d
        """
        args = arg.strip().rstrip(';').split() if arg.strip() else []

        if not args:
            print("❌ Usage: show_table_stats <table> [database] [-t] [-i index_names] [-a] [-d]")
            return

        table_name = args[0]
        database = None
        include_tombstone = False
        include_indexes = None
        include_all = False
        show_detail = False

        # Parse remaining arguments
        i = 1
        while i < len(args):
            if args[i] == '-t':
                include_tombstone = True
            elif args[i] == '-a':
                include_all = True
            elif args[i] == '-d':
                show_detail = True
            elif args[i] == '-i' and i + 1 < len(args):
                include_indexes = [idx.strip() for idx in args[i + 1].split(',')]
                i += 1
            elif not database:
                database = args[i]
            i += 1

        if not database:
            database = self.current_database

        if not database:
            print("❌ No database specified and no current database set.")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        try:
            # If -a flag is set, get all secondary indexes for the table
            if include_all:
                include_tombstone = True
                # Get all secondary indexes for the table (exclude PRIMARY and UNIQUE)
                try:
                    index_list = self.client.get_secondary_index_tables(table_name, database)
                    if index_list:
                        # Extract index names from physical table names
                        # Physical table names are like: __mo_index_secondary_xxxxx
                        # We need to get the actual index names from mo_indexes
                        # Filter out PRIMARY, UNIQUE and other system indexes
                        sql = """
                            SELECT DISTINCT mo_indexes.name
                            FROM mo_catalog.mo_indexes
                            JOIN mo_catalog.mo_tables ON mo_indexes.table_id = mo_tables.rel_id
                            WHERE mo_tables.relname = ?
                              AND mo_tables.reldatabase = ?
                              AND mo_indexes.name NOT IN ('PRIMARY', '__mo_rowid_idx')
                              AND mo_indexes.type != 'PRIMARY KEY'
                              AND mo_indexes.type != 'UNIQUE'
                        """
                        result = self.client.execute(sql, (table_name, database))
                        include_indexes = [row[0] for row in result.fetchall()]
                        if not include_indexes:
                            include_indexes = None  # No secondary indexes
                except Exception as e:
                    print(f"⚠️  Warning: Could not fetch index list: {e}")
                    include_indexes = None

            if show_detail:
                # Check if showing all indexes (-a -d together = hierarchical view)
                if include_all:
                    # Show hierarchical view: Table -> Index Name -> Physical Table (with type) -> Object List
                    print(f"\n📊 Detailed Table Statistics for '{database}.{table_name}':")
                    print("=" * 150)

                    # Helper functions
                    def format_size(size_bytes):
                        if isinstance(size_bytes, str):
                            return size_bytes
                        if size_bytes >= 1024 * 1024:
                            return f"{size_bytes / (1024 * 1024):.2f} MB"
                        elif size_bytes >= 1024:
                            return f"{size_bytes / 1024:.2f} KB"
                        else:
                            return f"{size_bytes} B"

                    def deduplicate_objects(objects):
                        seen = set()
                        unique_objects = []
                        for obj in objects:
                            # Handle both dict and MetadataRow objects
                            if hasattr(obj, 'object_name'):
                                obj_name = getattr(obj, 'object_name', None)
                            elif isinstance(obj, dict):
                                obj_name = obj.get('object_name')
                            else:
                                obj_name = None
                            if obj_name and obj_name not in seen:
                                seen.add(obj_name)
                                unique_objects.append(obj)
                        return unique_objects

                    # 1. Show main table with aggregated stats
                    try:
                        main_table_objects = self.client.metadata.scan(database, table_name, columns="*")
                        if main_table_objects:
                            unique_objs = deduplicate_objects(list(main_table_objects))
                            total_rows = sum(getattr(obj, 'row_cnt', 0) or 0 for obj in unique_objs)
                            total_null = sum(getattr(obj, 'null_cnt', 0) or 0 for obj in unique_objs)
                            total_origin = sum(getattr(obj, 'origin_size', 0) or 0 for obj in unique_objs)
                            total_compress = sum(getattr(obj, 'compress_size', 0) or 0 for obj in unique_objs)

                            print(f"\n{bold('Table:')} {Colors.CYAN}{table_name}{Colors.RESET}")
                            print(
                                f"  Objects: {len(unique_objs)} | Rows: {total_rows:,} | Null: {total_null:,} | Original: {format_size(total_origin)} | Compressed: {format_size(total_compress)}"
                            )

                            # Show object details
                            if unique_objs:
                                print(f"\n  {bold('Objects:')}")
                                print(
                                    f"  {'Object Name':<50} | {'Rows':<12} | {'Null Cnt':<10} | {'Original Size':<15} | {'Compressed Size':<15}"
                                )
                                print("  " + "-" * 148)
                                for obj in unique_objs:
                                    obj_name = getattr(obj, 'object_name', 'N/A')
                                    rows = getattr(obj, 'row_cnt', 0) or 0
                                    nulls = getattr(obj, 'null_cnt', 0) or 0
                                    orig_size = getattr(obj, 'origin_size', 0) or 0
                                    comp_size = getattr(obj, 'compress_size', 0) or 0
                                    print(
                                        f"  {obj_name:<50} | {rows:<12,} | {nulls:<10,} | {format_size(orig_size):<15} | {format_size(comp_size):<15}"
                                    )
                    except Exception:
                        pass

                    # 2. Get all indexes and their physical tables
                    try:
                        indexes = self.client.get_table_indexes_detail(table_name, database)

                        if indexes:
                            # Group indexes by index name
                            indexes_by_name = {}
                            for idx in indexes:
                                idx_name = idx['index_name']
                                if idx_name not in indexes_by_name:
                                    indexes_by_name[idx_name] = []
                                indexes_by_name[idx_name].append(idx)

                            # Display each index with its physical tables
                            for idx_name, idx_tables in indexes_by_name.items():
                                print(f"\n{bold('Index:')} {Colors.CYAN}{idx_name}{Colors.RESET}")

                                # Show each physical table for this index
                                for idx_table in idx_tables:
                                    physical_table = idx_table['physical_table_name']
                                    table_type = idx_table['algo_table_type'] if idx_table['algo_table_type'] else 'index'

                                    # Get objects for this physical table
                                    try:
                                        phys_objects = self.client.metadata.scan(database, physical_table, columns="*")
                                        if phys_objects:
                                            unique_objs = deduplicate_objects(list(phys_objects))
                                            total_rows = sum(getattr(obj, 'row_cnt', 0) or 0 for obj in unique_objs)
                                            total_null = sum(getattr(obj, 'null_cnt', 0) or 0 for obj in unique_objs)
                                            total_origin = sum(getattr(obj, 'origin_size', 0) or 0 for obj in unique_objs)
                                            total_compress = sum(
                                                getattr(obj, 'compress_size', 0) or 0 for obj in unique_objs
                                            )

                                            # Color code by table type
                                            if table_type == 'metadata':
                                                type_display = f"{Colors.YELLOW}{table_type}{Colors.RESET}"
                                            elif table_type == 'centroids':
                                                type_display = f"{Colors.GREEN}{table_type}{Colors.RESET}"
                                            elif table_type == 'entries':
                                                type_display = f"{Colors.CYAN}{table_type}{Colors.RESET}"
                                            else:
                                                type_display = table_type

                                            print(f"  └─ Physical Table ({type_display}): {physical_table}")
                                            print(
                                                f"     Objects: {len(unique_objs)} | Rows: {total_rows:,} | Null: {total_null:,} | Original: {format_size(total_origin)} | Compressed: {format_size(total_compress)}"
                                            )

                                            # Show object details
                                            if unique_objs:
                                                print(f"\n     {bold('Objects:')}")
                                                print(
                                                    f"     {'Object Name':<50} | {'Rows':<12} | {'Null Cnt':<10} | {'Original Size':<15} | {'Compressed Size':<15}"
                                                )
                                                print("     " + "-" * 148)
                                                for obj in unique_objs:
                                                    obj_name = getattr(obj, 'object_name', 'N/A')
                                                    rows = getattr(obj, 'row_cnt', 0) or 0
                                                    nulls = getattr(obj, 'null_cnt', 0) or 0
                                                    orig_size = getattr(obj, 'origin_size', 0) or 0
                                                    comp_size = getattr(obj, 'compress_size', 0) or 0
                                                    print(
                                                        f"     {obj_name:<50} | {rows:<12,} | {nulls:<10,} | {format_size(orig_size):<15} | {format_size(comp_size):<15}"
                                                    )
                                    except Exception:
                                        # If can't get stats for this physical table, skip
                                        pass
                    except Exception:
                        pass

                    print("\n" + "=" * 150)
                    print()
                    return

                # Regular detailed view (not hierarchical)
                # Get detailed statistics with object list
                stats = self.client.metadata.get_table_detail_stats(
                    dbname=database,
                    tablename=table_name,
                    include_tombstone=include_tombstone,
                    include_indexes=include_indexes,
                )

                if not stats:
                    print(f"⚠️  No statistics available for table '{database}.{table_name}'")
                    return

                print(f"\n📊 Detailed Table Statistics for '{database}.{table_name}':")
                print("=" * 150)

                # Helper function to format object row
                def format_obj_row(obj):
                    # Try different possible key names
                    object_name = obj.get('object_name', 'N/A')
                    create_ts = obj.get('create_ts', 'N/A')

                    # Safely convert to int or string for formatting
                    def safe_value(value, default=0):
                        # If value is already a string (formatted), return as is
                        if isinstance(value, str):
                            return value
                        try:
                            return int(value) if value is not None else default
                        except (ValueError, TypeError):
                            return default

                    rows_cnt = safe_value(obj.get('rows_cnt', obj.get('row_cnt', 0)))
                    null_cnt = safe_value(obj.get('null_cnt', 0))

                    # Size fields might already be formatted strings (e.g., "1.5 MB")
                    origin_size = obj.get('origin_size', obj.get('original_size', 0))
                    compress_size = obj.get('compress_size', obj.get('compressed_size', 0))

                    # Format row based on whether sizes are numbers or strings
                    if isinstance(rows_cnt, str):
                        rows_str = f"{rows_cnt:<12}"
                    else:
                        rows_str = f"{rows_cnt:<12,}"

                    if isinstance(null_cnt, str):
                        null_str = f"{null_cnt:<10}"
                    else:
                        null_str = f"{null_cnt:<10,}"

                    if isinstance(origin_size, str):
                        orig_str = f"{origin_size:<15}"
                    else:
                        orig_str = f"{origin_size:<15,}"

                    if isinstance(compress_size, str):
                        comp_str = f"{compress_size:<15}"
                    else:
                        comp_str = f"{compress_size:<15,}"

                    return f"{object_name:<50} | {create_ts:<20} | {rows_str} | {null_str} | {orig_str} | {comp_str}"

                # Helper function to deduplicate objects by object_name
                def deduplicate_objects(objects):
                    """Deduplicate objects by object_name, keeping first occurrence"""
                    seen = set()
                    unique_objects = []
                    for obj in objects:
                        obj_name = obj.get('object_name', 'N/A')
                        if obj_name not in seen:
                            seen.add(obj_name)
                            unique_objects.append(obj)
                    return unique_objects

                # Show main table details
                if table_name in stats:
                    unique_objs = deduplicate_objects(stats[table_name])
                    print(f"\n{Colors.BOLD}Table: {table_name}{Colors.RESET} ({len(unique_objs)} objects)")
                    print("-" * 150)
                    print(
                        f"{'Object Name':<50} | {'Create Time':<20} | {'Rows':<12} | {'Null Cnt':<10} | {'Original Size':<15} | {'Compressed Size':<15}"
                    )
                    print("-" * 150)
                    for obj in unique_objs:
                        print(format_obj_row(obj))

                # Show tombstone details
                if 'tombstone' in stats:
                    unique_objs = deduplicate_objects(stats['tombstone'])
                    print(f"\n{Colors.BOLD}Tombstone{Colors.RESET} ({len(unique_objs)} objects)")
                    print("-" * 150)
                    print(
                        f"{'Object Name':<50} | {'Create Time':<20} | {'Rows':<12} | {'Null Cnt':<10} | {'Original Size':<15} | {'Compressed Size':<15}"
                    )
                    print("-" * 150)
                    for obj in unique_objs:
                        print(format_obj_row(obj))

                # Show index details
                for key, objects in stats.items():
                    if key not in [table_name, 'tombstone']:
                        unique_objs = deduplicate_objects(objects)
                        index_name = key.replace(f'{table_name}_', '')
                        print(f"\n{Colors.BOLD}Index: {index_name}{Colors.RESET} ({len(unique_objs)} objects)")
                        print("-" * 150)
                        print(
                            f"{'Object Name':<50} | {'Create Time':<20} | {'Rows':<12} | {'Null Cnt':<10} | {'Original Size':<15} | {'Compressed Size':<15}"
                        )
                        print("-" * 150)
                        for obj in unique_objs:
                            print(format_obj_row(obj))

                print("=" * 150)
            else:
                # Get brief statistics (summary)
                stats = self.client.metadata.get_table_brief_stats(
                    dbname=database,
                    tablename=table_name,
                    include_tombstone=include_tombstone,
                    include_indexes=include_indexes,
                )

                if not stats:
                    print(f"⚠️  No statistics available for table '{database}.{table_name}'")
                    return

                print(f"\n📊 Table Statistics for '{database}.{table_name}':")
                print("=" * 120)
                print(
                    f"{'Component':<30} | {'Objects':<10} | {'Rows':<15} | {'Null Count':<12} | {'Original Size':<15} | {'Compressed Size':<15}"
                )
                print("-" * 120)

                # Show main table stats
                if table_name in stats:
                    table_stats = stats[table_name]
                    print(
                        f"{table_name:<30} | {table_stats['total_objects']:<10} | {table_stats['row_cnt']:<15,} | {table_stats['null_cnt']:<12,} | {table_stats['original_size']:<15} | {table_stats['compress_size']:<15}"  # type: ignore[call-overload]
                    )

                # Show tombstone stats
                if 'tombstone' in stats:
                    tomb_stats = stats['tombstone']
                    print(
                        f"{'  └─ tombstone':<30} | {tomb_stats['total_objects']:<10} | {tomb_stats['row_cnt']:<15,} | {tomb_stats['null_cnt']:<12,} | {tomb_stats['original_size']:<15} | {tomb_stats['compress_size']:<15}"  # type: ignore[call-overload]
                    )

                # Show index stats
                for key, value in stats.items():
                    if key not in [table_name, 'tombstone']:
                        index_name = key.replace(f'{table_name}_', '')  # Clean up index name if prefixed
                        print(
                            f"{'  └─ index: ' + index_name:<30} | {value['total_objects']:<10} | {value['row_cnt']:<15,} | {value['null_cnt']:<12,} | {value['original_size']:<15} | {value['compress_size']:<15}"  # type: ignore[call-overload]
                        )

                print("=" * 120)
                print(
                    "\nTip: Use '-t' for tombstone, '-i idx1,idx2' for indexes, '-a' for all, '-d' for detailed object list"
                )
            print()

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_flush_table(self, arg):
        """Flush table and all its secondary index tables

        Usage:
            flush_table <table> [database]    - Flush main table and all its index tables

        Example:
            flush_table my_table
            flush_table my_table test

        Note: Requires sys user privileges
        """
        if not arg.strip():
            print(f"{error('Error:')} Table name is required")
            print(f"{info('Usage:')} flush_table <table> [database]")
            return

        # Parse arguments
        parts = arg.strip().split()
        if len(parts) < 1:
            print(f"{error('Error:')} Table name is required")
            return

        table_name = parts[0].strip().rstrip(';')
        database_name = self.current_database

        # Check for database parameter
        if len(parts) > 1:
            database_name = parts[1].strip().rstrip(';')

        try:
            print(f"{info('🔄 Flushing table:')} {database_name}.{table_name}")

            # Flush main table
            try:
                self.client.moctl.flush_table(database_name, table_name)
                print(f"{success('✓')} Main table flushed successfully")
            except Exception as e:
                print(f"{error('❌')} Failed to flush main table: {e}")
                return

            # Get all index tables (including IVF/HNSW/Fulltext physical tables)
            try:
                indexes = self.client.get_table_indexes_detail(table_name, database_name)
                if indexes:
                    # Extract all unique physical table names
                    physical_tables = list(set(idx['physical_table_name'] for idx in indexes if idx['physical_table_name']))

                    if physical_tables:
                        print(f"{info('📋 Found')} {len(physical_tables)} index physical tables")

                        # Group by index name for better display
                        indexes_by_name = {}
                        for idx in indexes:
                            idx_name = idx['index_name']
                            if idx_name not in indexes_by_name:
                                indexes_by_name[idx_name] = []
                            indexes_by_name[idx_name].append(idx)

                        # Flush each physical table
                        success_count = 0
                        for idx_name, idx_tables in indexes_by_name.items():
                            print(f"\n{info('📑 Index:')} {idx_name}")
                            for idx_table in idx_tables:
                                physical_table = idx_table['physical_table_name']
                                table_type = idx_table['algo_table_type'] if idx_table['algo_table_type'] else 'index'
                                try:
                                    self.client.moctl.flush_table(database_name, physical_table)
                                    print(f"  {success('✓')} {table_type}: {physical_table}")
                                    success_count += 1
                                except Exception as e:
                                    print(f"  {error('❌')} {table_type}: {physical_table} - {e}")

                        print(f"\n{info('📊 Summary:')}")
                        print(f"  Main table: {success('✓')} flushed")
                        print(f"  Index physical tables: {success_count}/{len(physical_tables)} flushed successfully")
                    else:
                        print(f"{info('ℹ️')} No index physical tables found")
                        print(f"{info('📊 Summary:')} Main table: {success('✓')} flushed")
                else:
                    print(f"{info('ℹ️')} No indexes found")
                    print(f"{info('📊 Summary:')} Main table: {success('✓')} flushed")

            except Exception as e:
                print(f"{warning('⚠️')} Failed to get index tables: {e}")
                print(f"{info('📊 Summary:')} Main table: {success('✓')} flushed (index tables not processed)")

        except Exception as e:
            print(f"{error('❌ Error:')} {e}")

    def do_tables(self, arg):
        """Show all tables in current database or specified database

        Usage:
            tables                - Show all tables in current database
            tables <database>     - Show all tables in specified database

        Example:
            tables
            tables test
        """
        if not self.client:
            print(f"{error('❌ Error:')} Not connected. Use 'connect' first.")
            return

        # Parse arguments
        database_name = self.current_database
        if arg.strip():
            database_name = arg.strip().rstrip(';')

        if not database_name:
            print(f"{error('❌ Error:')} No database specified and no current database set.")
            print(f"{info('Usage:')} tables [database]")
            return

        try:
            # Execute SHOW TABLES
            sql = f"SHOW TABLES FROM `{database_name}`"
            result = self.client.execute(sql)
            rows = result.fetchall()

            if rows:
                print(f"\n{header('📋 Tables in database')} '{database_name}':")
                print("=" * 80)

                # Print table names
                for i, row in enumerate(rows, 1):
                    table_name = row[0]
                    print(f"{i:4}. {table_name}")

                print("=" * 80)
                print(f"{info('Total:')} {len(rows)} tables")
            else:
                print(f"{warning('⚠️')} No tables found in database '{database_name}'")
            print()

        except Exception as e:
            print(f"{error('❌ Error:')} {e}")

    def do_databases(self, arg):
        """Show all databases

        Usage:
            databases    - Show all databases

        Example:
            databases
        """
        if not self.client:
            print(f"{error('❌ Error:')} Not connected. Use 'connect' first.")
            return

        try:
            # Execute SHOW DATABASES
            result = self.client.execute("SHOW DATABASES")
            rows = result.fetchall()

            if rows:
                print(f"\n{header('🗄️  Databases:')}")
                print("=" * 80)

                # Print database names with current database highlighted
                for i, row in enumerate(rows, 1):
                    db_name = row[0]
                    if db_name == self.current_database:
                        print(f"{i:4}. {success(db_name)} {info('← current')}")
                    else:
                        print(f"{i:4}. {db_name}")

                print("=" * 80)
                print(f"{info('Total:')} {len(rows)} databases")
            else:
                print(f"{warning('⚠️')} No databases found")
            print()

        except Exception as e:
            print(f"{error('❌ Error:')} {e}")

    def do_history(self, arg):
        """Show command history

        Usage:
            history           - Show last 20 commands
            history <n>       - Show last n commands
            history -c        - Clear history

        Example:
            history
            history 50
            history -c
        """
        if not PROMPT_TOOLKIT_AVAILABLE or not self.session:
            print(f"{warning('⚠️')} History is only available when prompt_toolkit is installed.")
            print(f"{info('Tip:')} Install with: pip install prompt_toolkit")
            return

        # Parse arguments
        arg = arg.strip()

        # Clear history
        if arg == '-c':
            try:
                history_file = Path.home() / '.mo_diag_history'
                if history_file.exists():
                    history_file.unlink()
                    print(f"{success('✓')} History cleared")
                else:
                    print(f"{info('ℹ️')} No history file found")
                # Recreate the history
                self.session.history = FileHistory(str(history_file))
            except Exception as e:
                print(f"{error('❌ Error:')} Failed to clear history: {e}")
            return

        # Determine how many commands to show
        try:
            count = int(arg) if arg else 20
        except ValueError:
            print(f"{error('❌ Error:')} Invalid number: {arg}")
            return

        # Get history
        try:
            history_file = Path.home() / '.mo_diag_history'
            if not history_file.exists():
                print(f"{info('ℹ️')} No history yet")
                return

            # Read history file
            with open(history_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Parse prompt_toolkit FileHistory format:
            # Empty line
            # # timestamp (comment)
            # +command
            # Empty line
            commands = []
            seen = set()
            for line in lines:
                line = line.strip()
                # Skip empty lines and timestamp comments
                if not line or line.startswith('#'):
                    continue
                # Commands start with '+'
                if line.startswith('+'):
                    cmd = line[1:]  # Remove the '+' prefix
                    if cmd and cmd not in seen:
                        commands.append(cmd)
                        seen.add(cmd)

            # Show last N commands
            if commands:
                start_idx = max(0, len(commands) - count)
                print(f"\n{header('📜 Command History')} (last {min(count, len(commands))} commands):")
                print("=" * 80)

                for i, cmd in enumerate(commands[start_idx:], start=start_idx + 1):
                    print(f"{i:4}. {cmd}")

                print("=" * 80)
                print(f"{info('Total:')} {len(commands)} commands in history\n")
            else:
                print(f"{info('ℹ️')} No history yet")

        except Exception as e:
            print(f"{error('❌ Error:')} Failed to read history: {e}")

    def do_sql(self, arg):
        """
        Execute a SQL query directly.

        Usage: sql <SQL statement>
        Example: sql SELECT COUNT(*) FROM cms_all_content_chunk_info
        """
        if not arg:
            print("❌ Usage: sql <SQL statement>")
            return

        if not self.client:
            print("❌ Not connected. Use 'connect' first.")
            return

        try:
            result = self.client.execute(arg)
            rows = result.fetchall()

            if rows:
                # Print column headers if available
                try:
                    # Try to get column names from result
                    if hasattr(result, 'keys') and callable(result.keys):
                        headers = result.keys()
                    elif hasattr(result, 'columns'):
                        headers = result.columns
                    elif hasattr(result, '_metadata') and hasattr(result._metadata, 'keys'):
                        headers = result._metadata.keys
                    else:
                        # Fallback: use column numbers
                        headers = [f"col{i}" for i in range(len(rows[0]))]

                    print("\n" + " | ".join(str(h) for h in headers))
                    print("-" * (sum(len(str(h)) for h in headers) + len(headers) * 3))
                except Exception:
                    # If we can't get headers, just skip them
                    pass

                # Print rows
                for row in rows:
                    print(" | ".join(str(v) if v is not None else "NULL" for v in row))

                print(f"\n{len(rows)} row(s) returned\n")
            else:
                print("✓ Query executed successfully (0 rows returned)\n")

        except Exception as e:
            print(f"❌ Error: {e}")

    def do_exit(self, arg):
        """Exit the interactive tool."""
        print("\n👋 Goodbye!\n")
        if self.client:
            try:
                self.client.disconnect()
            except Exception:
                pass
        return True

    def do_quit(self, arg):
        """Exit the interactive tool."""
        return self.do_exit(arg)

    def do_EOF(self, arg):
        """Handle Ctrl+D to exit."""
        print()
        return self.do_exit(arg)

    def emptyline(self):
        """Do nothing on empty line."""
        pass


def start_interactive_tool(host='localhost', port=6001, user='root', password='111', database=None, log_level='ERROR'):
    """
    Start the interactive diagnostic tool.

    Args:
        host: Database host
        port: Database port
        user: Database user
        password: Database password
        database: Database name (optional)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: ERROR
    """
    import logging

    # Set logging level BEFORE creating Client
    level = getattr(logging, log_level.upper(), logging.ERROR)

    # Set for matrixone logger and all its handlers
    mo_logger = logging.getLogger('matrixone')
    mo_logger.setLevel(level)

    # Set level for all existing handlers
    for handler in mo_logger.handlers:
        handler.setLevel(level)

    # Create client with minimal SQL logging
    sql_log_mode = 'off' if level >= logging.ERROR else 'auto'
    client = Client(sql_log_mode=sql_log_mode)

    # Set level again after client creation (in case client adds handlers)
    mo_logger.setLevel(level)
    for handler in mo_logger.handlers:
        handler.setLevel(level)

    try:
        client.connect(host=host, port=port, user=user, password=password, database=database)
        print(f"✓ Connected to {host}:{port}" + (f" (database: {database})" if database else ""))
    except Exception as e:
        print(f"⚠️  Failed to connect: {e}")
        print("You can connect manually using the 'connect' command.\n")
        client = None

    cli = MatrixOneCLI(client)
    cli.cmdloop()


def main_cli():
    """
    Main entry point for the CLI tool when installed via pip.
    This function is called when running 'mo-diag' command.

    Supports both interactive and non-interactive modes:
    - Interactive: mo-diag (enters interactive shell)
    - Non-interactive: mo-diag -c "show_ivf_status test" (executes single command and exits)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='MatrixOne Interactive Diagnostic Tool',
        epilog='''
Examples:
  # Interactive mode
  mo-diag --host localhost --port 6001 --user root --password 111

  # Non-interactive mode - execute single command
  mo-diag -d test -c "show_ivf_status"
  mo-diag -d test -c "show_table_stats my_table -a"
  mo-diag -d test -c "sql SELECT COUNT(*) FROM my_table"
        ''',
    )
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=6001, help='Database port (default: 6001)')
    parser.add_argument('--user', default='root', help='Database user (default: root)')
    parser.add_argument('--password', default='111', help='Database password (default: 111)')
    parser.add_argument('--database', '-d', help='Database name (optional)')
    parser.add_argument(
        '--log-level',
        default='ERROR',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: ERROR)',
    )
    parser.add_argument('--command', '-c', help='Execute a single command and exit (non-interactive mode)')

    args = parser.parse_args()

    # Non-interactive mode: execute single command
    if args.command:
        import logging

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
            import sys

            sys.exit(1)

        # Create CLI instance and execute command
        cli = MatrixOneCLI(client)
        try:
            cli.onecmd(args.command)
        except Exception as e:
            print(f"❌ Command execution failed: {e}")
            import sys

            sys.exit(1)
        finally:
            if client:
                try:
                    client.disconnect()
                except Exception:
                    pass
    else:
        # Interactive mode
        start_interactive_tool(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            log_level=args.log_level,
        )


if __name__ == '__main__':
    main_cli()
