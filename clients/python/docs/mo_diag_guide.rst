MO-DIAG Interactive Diagnostic Tool Guide
==========================================

This guide covers the MO-DIAG interactive diagnostic tool, a powerful command-line interface for diagnosing and inspecting MatrixOne database objects, especially secondary indexes and vector indexes.

Overview
--------

MO-DIAG (MatrixOne Diagnostic Tool) provides:

* **Index Inspection**: View and analyze all types of indexes (IVF, HNSW, Fulltext, and regular)
* **Health Monitoring**: Check index health across all tables in a database
* **CDC Health Checks**: Evaluate CDC task status, errors, and watermark lag
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
* 🩺 **CDC Monitoring**: Run CDC health checks to surface lagging or errored tasks
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

   📊 Secondary Indexes for 'test.ivf_health_demo_docs'
   
   *************************** 1. row ***************************
         Index Name: idx_embedding_ivf_v2
          Algorithm: ivfflat
         Table Type: metadata
     Physical Table: __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581
            Columns: embedding
         Statistics:
                      - Objects: 1
                      - Rows: 7
                      - Compressed Size: 940 B
                      - Original Size: 1.98 KB
   
   *************************** 2. row ***************************
         Index Name: idx_embedding_ivf_v2
          Algorithm: ivfflat
         Table Type: centroids
     Physical Table: __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1
            Columns: embedding
         Statistics:
                      - Objects: 1
                      - Rows: 17
                      - Compressed Size: 3.09 KB
                      - Original Size: 6.83 KB
   
   *************************** 3. row ***************************
         Index Name: idx_embedding_ivf_v2
          Algorithm: ivfflat
         Table Type: entries
     Physical Table: __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a
            Columns: embedding
         Statistics:
                      - Objects: 1
                      - Rows: 1,000
                      - Compressed Size: 156.34 KB
                      - Original Size: 176.07 KB
   
   Total: 3 index tables (1 ivfflat with 3 physical tables)

**Output解析:**

IVF索引有3个物理表：

* **metadata**: 存储索引元数据（7行）
* **centroids**: 存储质心向量（17个质心）
* **entries**: 存储向量条目（1,000个向量）

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
   ========================================================================================================================
   
   ✓ HEALTHY (3 tables)
   ------------------------------------------------------------------------------------------------------------------------
   Table Name                          | Indexes  | Row Count            | Notes
   ------------------------------------------------------------------------------------------------------------------------
   cms_all_content_chunk_info          | 3        | ✓ 32,712 rows        | -
   demo_mixed_indexes                  | 4        | ✓ 20 rows            | -
   ivf_health_demo_docs                | 1        | 300 rows             | IVF: 17 centroids, 300 vectors
   
   ========================================================================================================================
   Summary:
     ✓ 3 healthy tables
     Total: 3 tables with indexes
   
   💡 Tip: Use 'verify_counts <table>' or 'show_ivf_status' for detailed diagnostics

**Output解析:**

* **HEALTHY**: 所有索引表行数与主表一致
* **Row Count**: 显示主表行数，带 ✓ 表示已验证
* **Notes**: 对于IVF索引显示质心数和向量数
* 如果有索引不一致会显示在 "ATTENTION NEEDED" 区域

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

CDC Monitoring Commands
-----------------------

cdc_health
~~~~~~~~~~

Run CDC task health diagnostics to spot errors, stuck tasks, and delayed watermarks.

.. code-block:: text

   Usage: cdc_health [threshold_minutes]
   
   Example:
       cdc_health
       cdc_health 5

**What it checks:**

* Tasks whose `err_msg` field is populated
* Running tasks whose tables report per-table errors
* Tables whose watermarks lag beyond the configured threshold (default 10 minutes)

**Sample output:**

.. code-block:: text

   📈 CDC Health Overview
   Threshold for watermark lag: 10.0 minute(s)
   Evaluated CDC tasks: 3

   No CDC tasks currently report errors.

   Running tasks with per-table errors (1):
     • nightly_sync (state: running)

   Tables lagging beyond threshold (2):
     • nightly_sync.analytics.orders (watermark: 2025-11-07 15:02:10)
     • nightly_sync.analytics.payments (watermark: 2025-11-07 14:55:42)

   Healthy tasks (no reported issues): 2

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

   📊 Table Statistics for 'test.ivf_health_demo_docs':
   ========================================================================================================================
   Component                      | Objects    | Rows            | Null Count   | Original Size   | Compressed Size
   ------------------------------------------------------------------------------------------------------------------------
   ivf_health_demo_docs           | 1          | 1,200           | 0            | 704.12 KB       | 624.94 KB
   ========================================================================================================================

**With -a flag (all indexes):**

.. code-block:: text

   📊 Table Statistics for 'test.ivf_health_demo_docs':
   ========================================================================================================================
   Component                      | Objects    | Rows            | Null Count   | Original Size   | Compressed Size
   ------------------------------------------------------------------------------------------------------------------------
   ivf_health_demo_docs           | 1          | 1,200           | 0            | 704.12 KB       | 624.94 KB
     └─ index: idx_embedding_ivf_v2
        └─ (metadata)              | 1          | 7               | 0            | 1.98 KB         | 940 B
        └─ (centroids)             | 1          | 17              | 0            | 6.83 KB         | 3.09 KB
        └─ (entries)               | 1          | 1,200           | 0            | 696.11 KB       | 626.37 KB
   ========================================================================================================================

**Output解析:**

* 主表显示总体统计信息
* IVF索引的3个物理表分别显示统计信息
* Objects: 对象数（segment数）
* Rows: 行数
* Original Size: 原始大小
* Compressed Size: 压缩后大小

**Detailed view (-d):**

Shows individual object statistics.

.. code-block:: text

   📊 Detailed Table Statistics for 'test.ivf_health_demo_docs':
   ======================================================================================================================================================
   
   Table: ivf_health_demo_docs
     Objects: 1 | Rows: 1,000 | Null: 0 | Original: 176.03 KB | Compressed: 156.24 KB
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ----------------------------------------------------------------------------------------------------------------------------------------------------
     0199e729-642e-71e0-b338-67c4980ee294_00000         | 1000         | 0          | 176.03 KB       | 156.24 KB

**Output解析:**

* 显示表的总体统计
* 列出每个对象（segment）的详细信息
* Object Name: 对象的唯一标识符
* Rows/Null Cnt/Sizes: 该对象的统计信息

**Hierarchical view (-a -d):**

Shows table → indexes → physical tables → objects in a tree structure.

.. code-block:: text

   📊 Detailed Table Statistics for 'test.ivf_health_demo_docs':
   ======================================================================================================================================================
   
   Table: ivf_health_demo_docs
     Objects: 1 | Rows: 1,000 | Null: 0 | Original: 176.03 KB | Compressed: 156.24 KB
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ----------------------------------------------------------------------------------------------------------------------------------------------------
     0199e729-642e-71e0-b338-67c4980ee294_00000         | 1000         | 0          | 176.03 KB       | 156.24 KB
   
   Index: idx_embedding_ivf_v2
     └─ (metadata): __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581:272851
        Objects: 1 | Rows: 7 | Null: 0 | Original: 1.98 KB | Compressed: 940 B
        
        Objects:
        Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
        ----------------------------------------------------------------------------------------------------------------------------------------------------
        0199e729-642a-7d36-ac37-0ae17325f7ec_00000         | 7            | 0          | 1.98 KB         | 940 B
     
     └─ (centroids): __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1:272852
        Objects: 1 | Rows: 17 | Null: 0 | Original: 6.83 KB | Compressed: 3.09 KB
        
        Objects:
        Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
        ----------------------------------------------------------------------------------------------------------------------------------------------------
        0199e729-6431-794d-9c13-d10f2e4a59e7_00000         | 17           | 0          | 6.83 KB         | 3.09 KB
     
     └─ (entries): __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a:272853
        Objects: 1 | Rows: 1,000 | Null: 0 | Original: 696.11 KB | Compressed: 626.37 KB
        
        Objects:
        Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
        ----------------------------------------------------------------------------------------------------------------------------------------------------
        0199e729-6438-7db1-bcb6-7b74c59b0aee_00000         | 1000         | 0          | 696.11 KB       | 626.37 KB

**Output解析:**

* **层次结构**: 主表 → 索引 → 物理表 → 对象
* **IVF索引的3个物理表**: metadata, centroids, entries
* **物理表名格式**: ``table_name:table_id``
* **Data vs Tombstone**: 如果使用 ``-t`` 参数，会显示 Data 和 Tombstone 两部分
* **用途**: 用于深度分析表和索引的存储结构，定位具体对象

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

   🔄 Flushing table: test.ivf_health_demo_docs
   ✓ Main table flushed: ivf_health_demo_docs
   📋 Found 3 index physical tables
   
   Index: idx_embedding_ivf_v2
     ✓ metadata: __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581
     ✓ centroids: __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1
     ✓ entries: __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a
   
   📊 Summary:
     Main table: ✓ flushed
     Index tables: 3/3 flushed successfully

**Output解析:**

* **主表 flush**: 首先 flush 主表
* **索引表发现**: 自动发现所有索引物理表
* **IVF 索引**: 对于 IVF 索引，会 flush 3个物理表（metadata, centroids, entries）
* **普通索引**: 对于普通索引，flush 单个物理表
* **成功率**: 显示成功 flush 的表数量

**注意事项:**

* 需要 sys 用户权限
* 对于包含多个索引的表，flush 可能需要较长时间
* 建议在低峰期执行 flush 操作

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

