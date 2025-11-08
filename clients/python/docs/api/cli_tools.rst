CLI Tools
=========

This module provides interactive diagnostic tools for MatrixOne database administration and maintenance.

.. module:: matrixone.cli_tools

MatrixOneCLI Class
------------------

.. autoclass:: matrixone.cli_tools.MatrixOneCLI
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

   Interactive command-line interface for MatrixOne diagnostics.

   The MatrixOneCLI class provides a comprehensive set of commands for:
   
   * Index inspection and verification
   * Vector index (IVF/HNSW) status monitoring
   * Table statistics and metadata analysis
   * Database operations (flush, connect, etc.)
   * SQL query execution

   **Available Commands:**

   Connection Management:
      * ``connect <host> <port> <user> <password> [database]`` - Connect to database
      * ``use <database>`` - Switch database
      * ``databases`` - List all databases
      * ``tables [database]`` - List tables

   Index Inspection:
      * ``show_indexes <table> [database]`` - Show all indexes for a table
      * ``show_all_indexes [database]`` - Health report for all tables with indexes
      * ``verify_counts <table> [database]`` - Verify row count consistency
      * ``show_ivf_status [database] [-v] [-t table]`` - Show IVF index status

   Table Statistics:
      * ``show_table_stats <table> [database] [-t] [-a] [-d]`` - Show table statistics

   Operations:
      * ``flush_table <table> [database]`` - Flush table and indexes (requires sys)
      * ``sql <query>`` - Execute SQL query

   Utilities:
      * ``history [n]`` - Show command history
      * ``help [command]`` - Show help
      * ``exit / quit`` - Exit tool

   **Example Usage:**

   .. code-block:: python

      from matrixone import Client
      from matrixone.cli_tools import MatrixOneCLI

      # Create client and CLI
      client = Client()
      client.connect(host='localhost', port=6001, user='root', password='111', database='test')
      
      cli = MatrixOneCLI(client)
      
      # Execute commands programmatically
      cli.onecmd("show_all_indexes")
      cli.onecmd("show_ivf_status -v")
      cli.onecmd("show_table_stats documents -a")
      
      # Or start interactive mode
      cli.cmdloop()

Helper Functions
----------------

.. autofunction:: matrixone.cli_tools.start_interactive_tool

   Start the interactive diagnostic tool with connection parameters.

   **Parameters:**
      * ``host`` (str): Database host (default: 'localhost')
      * ``port`` (int): Database port (default: 6001)
      * ``user`` (str): Database user (default: 'root')
      * ``password`` (str): Database password (default: '111')
      * ``database`` (str): Database name (optional)
      * ``log_level`` (str): Logging level (default: 'ERROR')

   **Example:**

   .. code-block:: python

      from matrixone.cli_tools import start_interactive_tool

      # Start interactive tool
      start_interactive_tool(
          host='localhost',
          port=6001,
          user='root',
          password='111',
          database='test',
          log_level='ERROR'
      )

.. autofunction:: matrixone.cli_tools.main_cli

   Main entry point for the mo-diag command-line tool.

   This function is called when running ``mo-diag`` from the command line.
   It supports both interactive and non-interactive modes.

   **Command-Line Options:**

   .. code-block:: bash

      mo-diag [options]

      Options:
        --host HOST           Database host (default: localhost)
        --port PORT           Database port (default: 6001)
        --user USER           Database user (default: root)
        --password PASSWORD   Database password (default: 111)
        --database DATABASE   Database name (optional)
        -d DATABASE           Short form of --database
        --log-level LEVEL     Logging level (default: ERROR)
        --command COMMAND     Execute single command and exit
        -c COMMAND            Short form of --command

      Sub-commands:
        cdc                   Manage CDC tasks (show/create/drop helpers)

   **Examples:**

   .. code-block:: bash

      # Interactive mode
      mo-diag --database test

      # Non-interactive mode - execute single command
      mo-diag -d test -c "show_ivf_status"
      mo-diag -d test -c "show_table_stats my_table -a"

      # CDC management helpers
      mo-diag cdc show
      mo-diag cdc show nightly_sync --details --threshold=5m
      mo-diag cdc create --table-level
      mo-diag cdc drop nightly_sync --force

Utility Functions
-----------------

.. autofunction:: matrixone.cli_tools.success

   Format success message in green color.

.. autofunction:: matrixone.cli_tools.error

   Format error message in red color.

.. autofunction:: matrixone.cli_tools.warning

   Format warning message in yellow color.

.. autofunction:: matrixone.cli_tools.info

   Format info message in cyan color.

.. autofunction:: matrixone.cli_tools.bold

   Format message in bold text.

.. autofunction:: matrixone.cli_tools.header

   Format header in bold cyan.

Colors Class
------------

.. autoclass:: matrixone.cli_tools.Colors
   :members:
   :undoc-members:

   ANSI color codes for terminal output formatting.

   **Attributes:**
      * ``RESET`` - Reset all formatting
      * ``BOLD`` - Bold text
      * ``RED`` - Red foreground
      * ``GREEN`` - Green foreground
      * ``YELLOW`` - Yellow foreground
      * ``BLUE`` - Blue foreground
      * ``MAGENTA`` - Magenta foreground
      * ``CYAN`` - Cyan foreground
      * ``WHITE`` - White foreground

   **Methods:**

   .. automethod:: disable
      :noindex:

      Disable all colors (for non-terminal output).

MODiagCompleter Class
---------------------

.. autoclass:: matrixone.cli_tools.MODiagCompleter
   :members:
   :undoc-members:
   :show-inheritance:

   Smart completer for mo-diag commands that provides table and database name completion.

   This completer integrates with prompt_toolkit to provide:
   
   * Command name completion
   * Table name completion for relevant commands
   * Database name completion
   * Context-aware suggestions

   **Features:**
      * Auto-completes command names
      * Auto-completes table names from current database
      * Auto-completes database names
      * Context-aware based on command type

See Also
--------

* :doc:`../mo_diag_guide` - Complete guide to using the mo-diag tool
* :doc:`client` - Client class documentation
* :doc:`moctl_manager` - MoCTL manager documentation

