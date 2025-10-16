MO-DIAG Interactive Diagnostic Tool Guide
==========================================

This guide covers the MO-DIAG interactive diagnostic tool, a powerful command-line interface for diagnosing and inspecting MatrixOne database objects, especially secondary indexes and vector indexes.

Overview
--------

MO-DIAG (MatrixOne Diagnostic Tool) provides:

* **Index Inspection**: View and analyze all types of indexes (IVF, HNSW, Fulltext, and regular)
* **Health Monitoring**: Check index health across all tables in a database
* **Row Count Verification**: Verify consistency between main tables and index tables
* **IVF Status Monitoring**: Monitor IVF vector index centroid distribution and balance
* **Table Statistics**: Detailed table and index statistics using metadata scan
* **Table Flushing**: Flush tables and their indexes to disk (requires sys privileges)
* **Interactive Shell**: Tab completion, command history, and intuitive interface

Features
~~~~~~~~

* 🎯 **Interactive Mode**: Full-featured shell with tab completion and history
* 🚀 **Non-Interactive Mode**: Execute single commands for scripting
* 📊 **Comprehensive Diagnostics**: Inspect all aspects of tables and indexes
* 🔍 **Smart Completion**: Auto-complete table names, database names, and commands
* 📝 **Command History**: Browse and search command history with arrows and Ctrl+R
* 🎨 **Colored Output**: Easy-to-read colored output for better visibility

Getting Started
---------------

Installation
~~~~~~~~~~~~

The MO-DIAG tool is included in the MatrixOne Python SDK:

.. code-block:: bash

   pip install matrixone-python-sdk

Basic Usage
~~~~~~~~~~~

**Interactive Mode:**

.. code-block:: bash

   # Start with default connection
   python mo_diag.py
   
   # Start with specific connection parameters
   python mo_diag.py --host localhost --port 6001 --database test
   
   # Start with custom logging level
   python mo_diag.py --host localhost --port 6001 --log-level DEBUG

**Non-Interactive Mode:**

.. code-block:: bash

   # Execute a single command and exit
   python mo_diag.py -d test -c "show_ivf_status"
   python mo_diag.py -d test -c "show_table_stats my_table -a"
   python mo_diag.py -d test -c "sql SELECT COUNT(*) FROM my_table"

**If installed via pip:**

.. code-block:: bash

   # Interactive mode
   mo-diag --host localhost --port 6001 --database test
   
   # Non-interactive mode
   mo-diag -d test -c "show_ivf_status"

Connection Management
---------------------

connect
~~~~~~~

Connect to a MatrixOne database.

.. code-block:: text

   Usage: connect <host> <port> <user> <password> [database]
   
   Example:
       connect localhost 6001 root 111 test
       connect 192.168.1.100 6001 admin secret production_db

use
~~~

Switch to a different database.

.. code-block:: text

   Usage: use <database>
   
   Example:
       use test
       use production_db

databases
~~~~~~~~~

List all available databases.

.. code-block:: text

   Usage: databases
   
   Example:
       databases

tables
~~~~~~

List all tables in the current database.

.. code-block:: text

   Usage: tables [database]
   
   Example:
       tables
       tables test

Index Inspection Commands
--------------------------

show_indexes
~~~~~~~~~~~~

Show all secondary indexes for a table, including IVF, HNSW, Fulltext, and regular indexes.

Uses vertical output format (like MySQL \\G) for easy reading.

.. code-block:: text

   Usage: show_indexes <table_name> [database]
   
   Example:
       show_indexes my_vector_table
       show_indexes my_vector_table test

**Output includes:**

* Index name and algorithm (IVF, HNSW, Fulltext, or regular)
* Table type (metadata, centroids, entries)
* Physical table name
* Indexed columns
* Statistics (objects, rows, compressed size, original size)

**Example output:**

.. code-block:: text

   📊 Secondary Indexes for 'test.documents'
   
   *************************** 1. row ***************************
         Index Name: idx_embedding
          Algorithm: ivfflat
         Table Type: metadata
     Physical Table: __mo_index_secondary_018e1234...
            Columns: embedding
         Statistics:
                      - Objects: 5
                      - Rows: 10,000
                      - Compressed Size: 1.5 MB
                      - Original Size: 2.1 MB
   
   Total: 3 index tables (1 ivfflat, 2 regular)

show_all_indexes
~~~~~~~~~~~~~~~~

Show index health report for all tables with secondary indexes in a database.

This command performs diagnostic checks including:

* Row count consistency between main table and index tables
* Vector index building status (IVF/HNSW)
* Index type distribution
* Problem detection

.. code-block:: text

   Usage: show_all_indexes [database]
   
   Example:
       show_all_indexes
       show_all_indexes test

**Output includes:**

* **Healthy tables**: Tables with consistent indexes
* **Attention needed**: Tables with issues (mismatches, incomplete builds)
* Summary statistics

**Example output:**

.. code-block:: text

   📊 Index Health Report for Database 'test':
   
   ✓ HEALTHY (5 tables)
   Table Name                          | Indexes  | Row Count            | Notes
   documents                           | 2        | ✓ 10,000 rows        | IVF: 100 centroids, 10,000 vectors
   users                              | 1        | ✓ 5,000 rows         | -
   
   ⚠️  ATTENTION NEEDED (1 tables)
   Table Name                          | Issue                                    | Details
   temp_vectors                        | Vector index building incomplete         | IVF index not built yet
   
   Summary:
     ✓ 5 healthy tables
     ⚠️  1 tables need attention
     Total: 6 tables with indexes

verify_counts
~~~~~~~~~~~~~

Verify row counts between main table and all its index tables.

This is critical for ensuring index consistency, especially after data modifications.

.. code-block:: text

   Usage: verify_counts <table_name> [database]
   
   Example:
       verify_counts my_table
       verify_counts my_table test

**Output shows:**

* Main table row count
* Each index table row count
* Consistency status (✓ or ❌)

**Example output:**

.. code-block:: text

   📊 Row Count Verification for 'test.users'
   Main table: 10,000 rows
   
   Index: idx_email
     __mo_index_secondary_018e1234...: 10,000 rows ✓
   
   Index: idx_username
     __mo_index_secondary_018e5678...: 10,000 rows ✓
   
   ✅ All index tables have consistent row counts!

Vector Index Commands
---------------------

show_ivf_status
~~~~~~~~~~~~~~~

Show IVF index centroids building status and distribution.

This is essential for monitoring IVF vector index health and balance.

.. code-block:: text

   Usage:
       show_ivf_status [database]             - Show compact summary
       show_ivf_status [database] -v          - Show detailed view
       show_ivf_status [database] -t table    - Filter by table name
   
   Example:
       show_ivf_status
       show_ivf_status test -v
       show_ivf_status test -t documents

**Compact view shows:**

* Table and index names
* Number of centroids and vectors
* Balance ratio (max/min centroid size)
* Status (active, empty, or error)

**Detailed view (-v) shows:**

* Physical table names for each index
* Centroid distribution statistics
* Top 10 centroids by vector count
* Load balance metrics

**Example compact output:**

.. code-block:: text

   📊 IVF Index Status in 'test':
   Table                          | Index                     | Column               | Centroids  | Vectors      | Balance    | Status
   documents                      | idx_embedding             | embedding            | 100        | 10,000       | 2.15       | ✓ active
   images                         | idx_features              | features             | 50         | 5,000        | 1.85       | ✓ active
   
   Total: 2 IVF indexes
   
   Tip: Use 'show_ivf_status test -v' for detailed view with top centroids

**Example detailed output:**

.. code-block:: text

   Table: documents | Index: idx_embedding | Column: embedding
   Physical Tables:
     - metadata      : __mo_index_secondary_018e1234_meta
     - centroids     : __mo_index_secondary_018e1234_centroids
     - entries       : __mo_index_secondary_018e1234_entries
   
   Centroid Distribution:
     Total Centroids: 100
     Total Vectors:   10,000
     Min/Avg/Max:     80 / 100.0 / 172
     Load Balance:    2.15x
   
     Top Centroids (by vector count):
      1. Centroid 42: 172 vectors (version 1)
      2. Centroid 18: 165 vectors (version 1)
      3. Centroid 67: 158 vectors (version 1)
      ...

Table Statistics Commands
-------------------------

show_table_stats
~~~~~~~~~~~~~~~~

Show table statistics using metadata scan interface.

This provides comprehensive statistics for tables, including tombstone and index data.

.. code-block:: text

   Usage:
       show_table_stats <table> [database]           - Show table stats summary
       show_table_stats <table> [database] -t        - Include tombstone stats
       show_table_stats <table> [database] -i idx1,idx2  - Include specific index stats
       show_table_stats <table> [database] -a        - Include all (tombstone + all indexes)
       show_table_stats <table> [database] -d        - Show detailed object list
   
   Example:
       show_table_stats documents
       show_table_stats documents test -t
       show_table_stats documents test -i idx_embedding,idx_title
       show_table_stats documents test -a
       show_table_stats documents test -a -d

**Brief view (default):**

Shows aggregated statistics per component (table, tombstone, indexes).

.. code-block:: text

   📊 Table Statistics for 'test.documents':
   Component                      | Objects    | Rows            | Null Count   | Original Size   | Compressed Size
   documents                      | 5          | 10,000          | 0            | 2.5 MB          | 1.8 MB
     └─ tombstone                 | 2          | 500             | 0            | 128 KB          | 85 KB
     └─ index: idx_embedding      | 8          | 10,000          | 0            | 3.2 MB          | 2.1 MB

**Detailed view (-d):**

Shows individual object statistics.

.. code-block:: text

   📊 Detailed Table Statistics for 'test.documents':
   
   Table: documents (5 objects)
   Object Name                                        | Create Time          | Rows         | Null Cnt   | Original Size   | Compressed Size
   01234567-89ab-cdef-0123-456789abcdef              | 2024-01-15 10:30:00 | 2,000        | 0          | 512 KB          | 384 KB
   ...

**Hierarchical view (-a -d):**

Shows table → indexes → physical tables → objects in a tree structure.

.. code-block:: text

   📊 Detailed Table Statistics for 'test.documents':
   
   Table: documents
     Objects: 5 | Rows: 10,000 | Null: 0 | Original: 2.5 MB | Compressed: 1.8 MB
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ...
   
   Index: idx_embedding
     └─ Physical Table (metadata): __mo_index_secondary_018e1234_meta
        Objects: 1 | Rows: 100 | Null: 0 | Original: 24 KB | Compressed: 18 KB
        
        Objects:
        Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
        ...
     
     └─ Physical Table (centroids): __mo_index_secondary_018e1234_centroids
        ...

Database Operations
-------------------

flush_table
~~~~~~~~~~~

Flush table and all its secondary index tables to disk.

**⚠️ Requires sys user privileges.**

.. code-block:: text

   Usage: flush_table <table> [database]
   
   Example:
       flush_table documents
       flush_table documents test

**Note:** This command will:

1. Flush the main table
2. Automatically discover all index tables (IVF, HNSW, Fulltext, regular)
3. Flush each index table
4. Report success/failure for each operation

**Example output:**

.. code-block:: text

   🔄 Flushing table: test.documents
   ✓ Main table flushed successfully
   📋 Found 3 index physical tables
   
   Index: idx_embedding (ivfflat)
     ✓ Flushed metadata table
     ✓ Flushed centroids table
     ✓ Flushed entries table
   
   Index: idx_title (regular)
     ✓ Flushed index table
   
   ✅ All tables flushed successfully (4 total)

sql
~~~

Execute arbitrary SQL queries.

.. code-block:: text

   Usage: sql <query>
   
   Example:
       sql SELECT COUNT(*) FROM documents
       sql SHOW CREATE TABLE documents
       sql SELECT * FROM documents LIMIT 10

Utility Commands
----------------

history
~~~~~~~

Show command history.

.. code-block:: text

   Usage: history [n]
   
   Example:
       history        # Show all history
       history 10     # Show last 10 commands

help
~~~~

Show help information for commands.

.. code-block:: text

   Usage: help [command]
   
   Example:
       help                    # Show all commands
       help show_indexes       # Show help for show_indexes
       help show_ivf_status    # Show help for show_ivf_status

exit / quit
~~~~~~~~~~~

Exit the interactive tool.

.. code-block:: text

   Usage: exit
          quit
          Ctrl+D (EOF)

Real-World Examples
-------------------

Monitoring Vector Index Health
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Start mo_diag
   python mo_diag.py --database test
   
   # 1. Check overall index health
   MO-DIAG[test]> show_all_indexes
   
   # 2. Inspect specific table indexes
   MO-DIAG[test]> show_indexes documents
   
   # 3. Check IVF index status and balance
   MO-DIAG[test]> show_ivf_status -v
   
   # 4. Get detailed statistics
   MO-DIAG[test]> show_table_stats documents -a

Diagnosing Index Issues
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 1. List all tables
   MO-DIAG[test]> tables
   
   # 2. Check for row count mismatches
   MO-DIAG[test]> verify_counts my_table
   
   # 3. If mismatch found, check index details
   MO-DIAG[test]> show_indexes my_table
   
   # 4. Check table statistics to identify issues
   MO-DIAG[test]> show_table_stats my_table -a -d

Monitoring IVF Index Balance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 1. Check IVF status across all tables
   MO-DIAG[test]> show_ivf_status
   
   # 2. Get detailed view for specific table
   MO-DIAG[test]> show_ivf_status -t documents -v
   
   # 3. If balance ratio is high (>2.5), consider rebuilding
   # Check current statistics before rebuild
   MO-DIAG[test]> show_table_stats documents -i idx_embedding -d

Flushing Tables Before Backup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Connect with sys user (required for flush operations)
   python mo_diag.py --user sys#root --password 111 --database test
   
   # 1. List all tables
   MO-DIAG[test]> tables
   
   # 2. Flush critical tables
   MO-DIAG[test]> flush_table documents
   MO-DIAG[test]> flush_table users
   MO-DIAG[test]> flush_table orders
   
   # 3. Verify flush succeeded by checking statistics
   MO-DIAG[test]> show_table_stats documents -a

Scripting with Non-Interactive Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   #!/bin/bash
   # Daily health check script
   
   # Check overall health
   python mo_diag.py -d production -c "show_all_indexes" > health_report.txt
   
   # Check IVF index status
   python mo_diag.py -d production -c "show_ivf_status" >> health_report.txt
   
   # Get statistics for critical tables
   for table in documents users orders; do
       echo "=== $table ===" >> health_report.txt
       python mo_diag.py -d production -c "show_table_stats $table -a" >> health_report.txt
   done
   
   # Email report
   mail -s "Daily DB Health Report" admin@example.com < health_report.txt

Best Practices
--------------

Index Health Monitoring
~~~~~~~~~~~~~~~~~~~~~~~

1. **Regular Checks**: Run ``show_all_indexes`` daily to catch issues early
2. **IVF Balance**: Monitor IVF balance ratio; rebuild if > 2.5x
3. **Row Count Verification**: Verify counts after large data operations
4. **Statistics Review**: Check table statistics weekly for growth trends

Vector Index Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Monitor Centroids**: Use ``show_ivf_status -v`` to check centroid distribution
2. **Balance Ratio**: Keep balance ratio below 2.5x for optimal performance
3. **Regular Rebuilds**: Rebuild IVF indexes periodically if data distribution changes
4. **Size Tracking**: Monitor index sizes with ``show_table_stats -a``

Table Flushing
~~~~~~~~~~~~~~

1. **Sys Privileges**: Always connect with sys account for flush operations
2. **Before Backups**: Flush critical tables before taking backups
3. **After Bulk Loads**: Flush after large data inserts/updates
4. **Verify Success**: Check table statistics after flushing

Troubleshooting
---------------

Connection Issues
~~~~~~~~~~~~~~~~~

**Problem:** Cannot connect to database

**Solution:**

.. code-block:: text

   # Check connection parameters
   python mo_diag.py --host localhost --port 6001 --user root --password 111 --log-level DEBUG
   
   # Or connect manually within the tool
   MO-DIAG> connect localhost 6001 root 111 test

Permission Errors
~~~~~~~~~~~~~~~~~

**Problem:** "Permission denied" when flushing tables

**Solution:** Connect with sys account:

.. code-block:: bash

   # Method 1: Use sys#root format
   python mo_diag.py --user sys#root --password 111 --database test
   
   # Method 2: Within the tool
   MO-DIAG> connect localhost 6001 sys#root 111 test

No IVF Statistics
~~~~~~~~~~~~~~~~~

**Problem:** IVF index shows "no stats available"

**Solution:**

1. Check if index is built: ``show_indexes <table>``
2. Verify there's data in the table: ``sql SELECT COUNT(*) FROM <table>``
3. Wait for index building to complete (for new indexes)
4. Check index health: ``show_all_indexes``

Row Count Mismatches
~~~~~~~~~~~~~~~~~~~~

**Problem:** ``verify_counts`` shows mismatches

**Solution:**

1. Check if there are ongoing transactions
2. Flush the table: ``flush_table <table>``
3. Re-verify: ``verify_counts <table>``
4. If persists, inspect index details: ``show_indexes <table>``

Command Reference Summary
-------------------------

**Connection:**

* ``connect <host> <port> <user> <password> [database]`` - Connect to database
* ``use <database>`` - Switch database
* ``databases`` - List databases
* ``tables [database]`` - List tables

**Index Inspection:**

* ``show_indexes <table> [database]`` - Show all indexes for a table
* ``show_all_indexes [database]`` - Show health report for all tables
* ``verify_counts <table> [database]`` - Verify row count consistency
* ``show_ivf_status [database] [-v] [-t table]`` - Show IVF index status

**Statistics:**

* ``show_table_stats <table> [database] [-t] [-i idx] [-a] [-d]`` - Show table statistics

**Operations:**

* ``flush_table <table> [database]`` - Flush table and indexes (requires sys)
* ``sql <query>`` - Execute SQL query

**Utility:**

* ``history [n]`` - Show command history
* ``help [command]`` - Show help
* ``exit / quit`` - Exit tool

Command-Line Options
--------------------

.. code-block:: bash

   python mo_diag.py [options]
   
   Options:
     --host HOST           Database host (default: localhost)
     --port PORT           Database port (default: 6001)
     --user USER           Database user (default: root)
     --password PASSWORD   Database password (default: 111)
     --database DATABASE, -d DATABASE
                           Database name (optional)
     --log-level LEVEL     Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: ERROR)
     --command COMMAND, -c COMMAND
                           Execute a single command and exit (non-interactive mode)

Programmatic Usage
------------------

You can also use the MO-DIAG tool programmatically in your Python scripts:

.. code-block:: python

   from matrixone import Client
   from matrixone.cli_tools import MatrixOneCLI, start_interactive_tool
   
   # Method 1: Start interactive tool
   start_interactive_tool(
       host='localhost',
       port=6001,
       user='root',
       password='111',
       database='test',
       log_level='ERROR'
   )
   
   # Method 2: Use CLI programmatically
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')
   
   cli = MatrixOneCLI(client)
   
   # Execute commands
   cli.onecmd("show_all_indexes")
   cli.onecmd("show_ivf_status -v")
   cli.onecmd("show_table_stats documents -a")
   
   # Clean up
   client.disconnect()

For more information, see the :doc:`api/client` and :doc:`vector_guide`.

