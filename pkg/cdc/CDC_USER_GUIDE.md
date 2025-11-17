# MatrixOne CDC User Guide

**Version**: 1.0  
**Last Updated**: November 2025  
**Target Audience**: Database Users, Testers, QA Engineers, Technical Writers

---

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
   - [Core Components](#core-components)
   - [Component Collaboration](#component-collaboration)
   - [Task Lifecycle](#task-lifecycle)
   - [Design Intentions](#design-intentions)
3. [Quick Start](#quick-start)
4. [CREATE CDC - Creating a CDC Task](#create-cdc---creating-a-cdc-task)
5. [SHOW CDC - Viewing CDC Tasks](#show-cdc---viewing-cdc-tasks)
6. [PAUSE CDC - Pausing a CDC Task](#pause-cdc---pausing-a-cdc-task)
7. [RESUME CDC - Resuming a CDC Task](#resume-cdc---resuming-a-cdc-task)
8. [DROP CDC - Dropping a CDC Task](#drop-cdc---dropping-a-cdc-task)
9. [RESTART CDC - Restarting a CDC Task](#restart-cdc---restarting-a-cdc-task)
10. [Monitoring and Error Handling](#monitoring-and-error-handling)
11. [Metrics and Observability](#metrics-and-observability)
12. [Configuration Options Reference](#configuration-options-reference)
13. [Best Practices](#best-practices)
14. [Troubleshooting](#troubleshooting)
15. [Test Scenarios](#test-scenarios)
16. [FAQ](#faq)

---

## Overview

### What is CDC?

Change Data Capture (CDC) is a data replication feature that captures and synchronizes data changes from a source MatrixOne database to a target MySQL-compatible database in real-time.

### Key Features

- **Real-time Synchronization**: Captures INSERT, UPDATE, and DELETE operations
- **Multi-Level Support**: Account, Database, or Table-level replication
- **Initial Snapshot**: Optional full data snapshot before incremental sync
- **Time Range Control**: Start and end timestamps for bounded replication
- **Flexible Filtering**: Include/exclude specific databases or tables
- **Automatic Recovery**: Built-in retry mechanism for transient errors
- **State Management**: Pause, resume, and restart tasks without data loss

### Supported Targets

- MySQL 5.7+
- MySQL 8.0+
- MatrixOne (inter-cluster replication)

---

## System Architecture

### Overview

The CDC system is built on a **pipeline architecture** that processes data changes through multiple stages: reading from source, processing changes, and writing to target. The system is designed for **high reliability**, **automatic recovery**, and **precise state management**.

### Core Components

The CDC system consists of five main components that work together to achieve reliable data replication:

#### 1. TableChangeStream (Stream)

**Purpose**: The main orchestrator that coordinates the entire data replication pipeline for a single table.

**Responsibilities**:
- Orchestrates the data replication lifecycle for one table
- Manages tick-based polling intervals (frequency control)
- Coordinates between Reader, Processor, and Sinker components
- Handles error classification and retry decisions
- Manages watermark progression and state persistence
- Implements StaleRead recovery mechanism
- Provides pause/cancel control signal handling

**Key Behaviors**:
- Runs in a continuous loop, processing one round at a time
- Each round: reads changes → processes → writes → updates watermark
- Automatically retries on retryable errors
- Stops processing on non-retryable errors (requires manual intervention)
- Persists error state to database for recovery after restarts

**Lifecycle**:
- Created per table when task starts
- Runs until task is paused, dropped, or encounters fatal error
- Cleans up resources on shutdown

#### 2. Reader (ChangeReader)

**Purpose**: Reads change data from MatrixOne source database.

**Responsibilities**:
- Creates transactions to read changes from source tables
- Collects changes within a timestamp range (fromTs to toTs)
- Handles table schema changes and truncation
- Provides change batches (snapshot + incremental) to processor
- Manages transaction lifecycle (begin, read, commit/rollback)

**Key Behaviors**:
- Uses MatrixOne transaction API to read changes
- Returns changes in batches (snapshot data + incremental changes)
- Handles StaleRead errors (data not yet available at requested timestamp)
- Supports table relation lookups and schema validation

**Components**:
- **TransactionManager**: Manages transaction lifecycle, coordinates with Sinker for BEGIN/COMMIT/ROLLBACK
- **ChangeCollector**: Collects changes from source tables within timestamp ranges
- **DataProcessor**: Processes change batches and coordinates with Sinker

#### 3. Sinker (mysqlSinker2)

**Purpose**: Writes data changes to downstream MySQL-compatible database.

**Responsibilities**:
- Executes SQL commands (INSERT, UPDATE, DELETE) on target database
- Manages transaction lifecycle on target (BEGIN, COMMIT, ROLLBACK)
- Implements retry mechanism with exponential backoff
- Provides circuit breaker to prevent retry storms
- Handles connection management and reconnection
- Tracks transaction state (IDLE, ACTIVE, COMMITTED, ROLLED_BACK)

**Key Behaviors**:
- Uses command channel pattern: commands are sent asynchronously, executed by consumer goroutine
- Once error occurs, all subsequent commands are skipped until `ClearError()`
- Producer (Reader) polls `Error()` to detect async failures
- Supports SQL batching and reuse query buffer optimization
- Automatic retry for transient errors (network, timeout, circuit breaker)

**Transaction State Machine**:
```
IDLE --SendBegin--> ACTIVE --SendCommit--> COMMITTED --cleanup--> IDLE
                    ACTIVE --SendRollback--> ROLLED_BACK --cleanup--> IDLE
```

#### 4. WatermarkUpdater

**Purpose**: Manages watermark state (checkpoint) and error persistence.

**Responsibilities**:
- Maintains per-table watermark (last synchronized timestamp)
- Persists error messages with metadata (retryable flag, retry count, timestamps)
- Provides in-memory cache for fast watermark lookups
- Implements asynchronous job queue for watermark updates (preserves batch processing design)
- Manages circuit breaker state for commit failures
- Clears errors on successful processing

**Key Behaviors**:
- Watermark represents the last successfully synchronized timestamp
- Errors are persisted with structured format: `R:<count>:<first>:<last>:<message>` or `N:<first>:<message>`
- Uses lazy batch processing: updates are queued and processed asynchronously
- Cache provides fast reads without synchronous SQL queries
- Circuit breaker tracks commit failure patterns

**Storage**:
- In-memory cache: fast lookups during processing
- Database (`mo_catalog.mo_cdc_watermark`): persistent state, survives restarts

**Watermark Persistence Design - At-Least-Once Semantics**:

The watermark persistence is **asynchronous by design** to preserve batch processing performance. This design choice has important implications:

1. **Asynchronous Persistence**:
   - Watermark updates are queued and processed asynchronously via job queue
   - This avoids blocking data processing with synchronous database writes
   - Improves overall throughput and reduces latency

2. **Watermark May Lag Behind Actual Progress**:
   - The persisted watermark in database may be **lower than the actual synchronized position**
   - This is **intentional and safe** due to idempotency guarantees

3. **Idempotency Guarantee**:
   - CDC operations (INSERT, UPDATE, DELETE) are **idempotent** on the target database
   - If system restarts before watermark is persisted, it will re-sync from the last persisted watermark
   - Re-syncing the same data multiple times is safe and does not cause data corruption
   - This ensures **at-least-once delivery** semantics

4. **Recovery Behavior**:
   - On restart, system resumes from the last persisted watermark (which may be lower than actual)
   - System will re-process some data that was already synchronized
   - This is acceptable because operations are idempotent
   - Better to have slight duplication than to lose data

5. **Error Handling for Watermark Stalls**:
   - If watermark cannot advance (e.g., due to persistent errors), system detects this condition
   - Watermark stall errors are logged and tracked via metrics
   - System will retry and eventually recover when underlying issues are resolved
   - See "Watermark Stall Errors" in Error Handling section for details

**Why This Design**:
- **Performance**: Asynchronous updates avoid blocking the critical data path
- **Reliability**: At-least-once semantics ensure no data loss
- **Simplicity**: Idempotent operations simplify error recovery
- **Trade-off**: Accepts slight data duplication in exchange for better performance and simpler recovery

#### 5. Task Scheduler (Frontend)

**Purpose**: Manages task lifecycle and coordinates multiple table streams.

**Responsibilities**:
- Creates and manages CDC tasks
- Starts/stops table streams based on task state
- Checks error state before starting streams (uses `ShouldRetry` logic)
- Handles task-level operations (pause, resume, restart, drop)
- Monitors task health and state transitions
- Clears errors on resume/restart

**Key Behaviors**:
- Scans for new tables and creates streams dynamically
- Before starting stream, checks persisted error in watermark table
- Skips tables with non-retryable errors (requires manual intervention)
- Allows retryable errors to continue processing
- Clears all table errors when task is resumed

### Component Collaboration

The components work together in a **producer-consumer pipeline**:

```
┌─────────────────────────────────────────────────────────────┐
│                    Task Scheduler                            │
│  - Creates/manages tasks                                     │
│  - Starts/stops table streams                                │
│  - Checks error state before starting                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              TableChangeStream (per table)                  │
│  - Orchestrates pipeline                                    │
│  - Manages tick-based polling                               │
│  - Coordinates components                                   │
└──────┬───────────────────────────────┬──────────────────────┘
       │                               │
       ▼                               ▼
┌──────────────────┐          ┌──────────────────┐
│     Reader       │          │   WatermarkUpdater│
│  - Read changes  │          │  - Manage state   │
│  - Collect data  │          │  - Persist errors │
└──────┬───────────┘          └──────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│              TransactionManager + DataProcessor             │
│  - Coordinate transaction lifecycle                        │
│  - Process change batches                                   │
│  - Send commands to Sinker                                  │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Sinker (mysqlSinker2)                    │
│  - Execute SQL on target                                    │
│  - Manage target transactions                               │
│  - Retry on transient errors                                │
└─────────────────────────────────────────────────────────────┘
```

**Data Flow**:

1. **TableChangeStream** starts a processing round (tick-based)
2. **Reader** creates transaction and collects changes from source (fromTs to toTs)
3. **DataProcessor** processes change batches:
   - Sends BEGIN to Sinker
   - Sends INSERT/UPDATE/DELETE commands for each change
   - Sends COMMIT to Sinker
4. **Sinker** executes commands on target database:
   - Receives commands asynchronously via channel
   - Executes SQL with retry mechanism
   - Reports errors back to producer
5. **WatermarkUpdater** updates watermark on success:
   - Updates in-memory cache
   - Queues database update (asynchronous)
6. **TableChangeStream** waits for next tick interval and repeats

**Error Handling Flow**:

1. Error occurs in any component (Reader, Sinker, etc.)
2. Error propagates to **TableChangeStream**
3. **TableChangeStream** classifies error (retryable/non-retryable)
4. Error state updated atomically (lastError + retryable flag)
5. Error persisted to **WatermarkUpdater** (with metadata)
6. If retryable: Stream waits for next tick and retries
7. If non-retryable: Stream stops, requires manual intervention
8. On restart: **Task Scheduler** checks persisted error, decides whether to start stream

### Task Lifecycle

A CDC task goes through the following states:

```
CREATED → STARTING → RUNNING → [PAUSED] → [RUNNING] → CANCELLED
                              ↓
                           [FAILED] → [RUNNING] (after resume)
```

**State Transitions**:

1. **CREATED**: Task is created via `CREATE CDC` command
   - Metadata stored in `mo_catalog.mo_cdc_task`
   - Initial watermark entries created in `mo_catalog.mo_cdc_watermark`

2. **STARTING**: Task is being initialized
   - Task scheduler scans for tables
   - Creates TableChangeStream for each table
   - Checks error state before starting streams

3. **RUNNING**: Task is actively replicating data
   - TableChangeStream processes changes in rounds
   - Watermarks are updated on successful rounds
   - Errors are handled automatically (retryable) or block processing (non-retryable)

4. **PAUSED**: Task is temporarily stopped
   - All table streams are stopped gracefully
   - State is preserved (watermarks, errors)
   - Can be resumed via `RESUME CDC` command

5. **FAILED**: Task encountered non-retryable error
   - Task remains in RUNNING state but sync is blocked
   - Error persists until manually cleared
   - Can be recovered via `RESUME CDC` (clears errors)

6. **CANCELLED**: Task is dropped
   - All resources are cleaned up
   - Metadata and watermarks are deleted
   - Cannot be recovered

**Control Operations**:

- **PAUSE**: Gracefully stops all table streams, preserves state
- **RESUME**: Clears all errors, restarts streams from last watermark
- **RESTART**: Stops and immediately restarts, clears errors
- **DROP**: Permanently deletes task and all metadata

### Design Intentions

The CDC system is designed with the following principles:

1. **Reliability First**:
   - All state is persisted (watermarks, errors)
   - Errors are classified and handled appropriately
   - System can recover from any error state via manual intervention

2. **Automatic Recovery**:
   - Transient errors (network, system unavailability) are automatically retried
   - Retry mechanism with exponential backoff prevents retry storms
   - Circuit breaker protects against persistent failures

3. **Precise State Management**:
   - Watermark represents exact synchronization point
   - Errors are tracked with metadata (retryable, count, timestamps)
   - State survives system restarts

4. **Performance Optimization**:
   - Asynchronous processing (command channel pattern)
   - Batch processing for watermark updates
   - SQL reuse query buffer optimization
   - In-memory cache for fast watermark lookups

5. **Flexibility**:
   - Supports multiple replication levels (account, database, table)
   - Configurable frequency, time ranges, filters
   - Optional initial snapshot

6. **Observability**:
   - Structured logging with consistent keys
   - Metrics for monitoring and alerting
   - Progress tracking per table

7. **User Control**:
   - Manual intervention always possible (resume clears all errors)
   - Clear error messages guide troubleshooting
   - Task operations (pause/resume/restart) provide full control

### Component Isolation

Each component is designed to be **loosely coupled**:

- **TableChangeStream** doesn't know about downstream MySQL details
- **Sinker** doesn't know about source MatrixOne details
- **WatermarkUpdater** is a shared service, used by all streams
- **Reader** components are independent per table

This design allows:
- Easy testing (components can be mocked)
- Independent scaling (each table stream is independent)
- Clear error boundaries (errors are contained and handled appropriately)

---

## Quick Start

### Basic Example: Replicate a Single Table

```sql
-- Create a CDC task to replicate one table
create cdc task1 
  'mysql://myaccount#root:password@127.0.0.1:6001' 
  'mysql' 
  'mysql://root:password@192.168.1.100:3306' 
  'mydb.orders:targetdb.orders'
  {'Level'='table'};
```

### Check Task Status

```sql
-- View all CDC tasks
show cdc all;

-- View specific task details
show cdc task task1;
```

### Pause and Resume

```sql
-- Pause the task (stops data sync)
pause cdc task task1;

-- Resume the task (continues from last checkpoint)
resume cdc task task1;
```

### Drop Task

```sql
-- Permanently remove the CDC task
drop cdc task task1;
```

---

## CREATE CDC - Creating a CDC Task

### Syntax

```sql
create cdc <task_name>
  '<source_connection_string>'
  '<sink_type>'
  '<sink_connection_string>'
  '<table_mapping>'
  {<option_key>='<option_value>', ...};
```

### Parameters

#### Required Parameters

| Parameter | Description | Format |
|-----------|-------------|--------|
| `task_name` | Unique name for the CDC task | Alphanumeric string (no quotes) |
| `source_connection_string` | Source MatrixOne connection string | `mysql://account#user:password@host:port` |
| `sink_type` | Target database type | `mysql` or `matrixone` |
| `sink_connection_string` | Target connection string | `mysql://user:password@host:port` (or `mysql://account#user:password@host:port` for MatrixOne) |
| `table_mapping` | Table mapping specification | See [Table Mapping](#table-mapping) |

#### Connection String Format

**For Source (MatrixOne)**:
```
mysql://<account>#<user>:<password>@<host>:<port>
```

**For Sink (MySQL)**:
```
mysql://<user>:<password>@<host>:<port>
```

**For Sink (MatrixOne)**:
```
mysql://<account>#<user>:<password>@<host>:<port>
```

**Example**:
- Source: `mysql://my_account#admin:password123@127.0.0.1:6001`
- Sink (MySQL): `mysql://root:password@192.168.1.100:3306`
- Sink (MatrixOne): `mysql://target_account#admin:password@192.168.1.200:6001`

#### Table Mapping

Table mapping specifies which tables to replicate and how to map them to the target.

**Format**: `source_db.source_table:sink_db.sink_table[,source_db.source_table:sink_db.sink_table,...]`

**Examples**:

```sql
-- Single table with same name
'db1.users:db1.users'

-- Multiple tables
'db1.users:db1.users,db1.orders:db1.orders'

-- Different target database
'source_db.customers:target_db.customers'

-- Mixed mappings
'db1.t1:db2.t1,db1.t2:db2.new_t2'

-- Database-level (all tables in database)
'db1:db2'

-- Account-level (all databases and tables)
'*:*'
```

#### Optional Parameters

Use curly braces `{}` to specify options. Options are comma-separated key-value pairs with keys in single quotes:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Level` | string | **Required** | Replication granularity: `account`, `database`, or `table`. **Must be explicitly specified** |
| `Exclude` | string | (none) | Comma-separated list of tables to exclude |
| `StartTs` | timestamp | (current time) | Start timestamp for replication |
| `EndTs` | timestamp | (none) | End timestamp for bounded replication |
| `NoFull` | boolean | `false` | Skip initial snapshot, sync incremental only |
| `MaxSqlLength` | integer | 4194304 (4MB) | Maximum SQL statement size in bytes |
| `SendSqlTimeout` | duration | `10m` | Timeout for sending SQL to target |
| `InitSnapshotSplitTxn` | boolean | `true` | Split large snapshot into multiple transactions |
| `Frequency` | duration | `200ms` | Polling frequency for change detection |

**Option Syntax**:
```sql
{'option1'='value1', 'option2'='value2', 'option3'='value3'}
```

### Replication Levels

#### 1. Table Level (Default)

Replicate specific tables listed in the table mapping.

```sql
create cdc task_table_level
  'mysql://myaccount#root:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'mydb.users:mydb.users,mydb.orders:mydb.orders'
  {'Level'='table'};
```

#### 2. Database Level

Replicate all tables in specified databases.

```sql
create cdc task_db_level
  'mysql://myaccount#root:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'db1:db1'
  {'Level'='database'};
```

#### 3. Account Level

Replicate all databases and tables in the account.

```sql
create cdc task_account_level
  'mysql://myaccount#root:password@127.0.0.1:6001'
  'matrixone'
  'mysql://target_account#admin:password@192.168.1.200:6001'
  '*:*'
  {'Level'='account', 'Account'='myaccount'};
```

### Examples

#### Example 1: Basic Table Replication

```sql
create cdc replicate_orders
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'sales.orders:sales.orders'
  {'Level'='table'};
```

#### Example 2: Multiple Tables with Exclusion

```sql
create cdc replicate_sales_db
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'sales:sales'
  {'Level'='database', 'Exclude'='sales.temp_table,sales.staging_table'};
```

#### Example 3: Time-Bounded Replication

```sql
create cdc historical_sync
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'analytics.events:analytics.events'
  {'StartTs'='2025-01-01 00:00:00', 'EndTs'='2025-01-31 23:59:59'};
```

#### Example 4: Incremental Only (No Snapshot)

```sql
create cdc incremental_only
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'logs.access_log:logs.access_log'
  {'NoFull'='true'};
```

#### Example 5: High-Frequency Sync

```sql
create cdc realtime_prices
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'trading.prices:trading.prices'
  {'Frequency'='100ms'};
```

#### Example 6: MatrixOne to MatrixOne Replication

```sql
create cdc mo_to_mo_replication
  'mysql://source_account#admin:password@127.0.0.1:6001'
  'matrixone'
  'mysql://target_account#admin:password@192.168.1.200:6001'
  'mydb:mydb'
  {'Level'='database'};
```

---

## SHOW CDC - Viewing CDC Tasks

### Syntax

```sql
-- Show all CDC tasks for the current account
show cdc all;

-- Show details of a specific task
show cdc task <task_name>;
```

### Output Columns

#### show cdc all

| Column | Description |
|--------|-------------|
| `task_id` | Unique UUID for the task |
| `task_name` | User-defined task name |
| `source_uri` | Source database connection (password masked) |
| `sink_uri` | Target database connection (password masked) |
| `state` | Current state: `running`, `paused`, `cancelled`, `failed` |
| `checkpoint` | Last synchronized timestamp |
| `err_msg` | Error message if task is in error state |
| `timestamp` | Last update timestamp |

#### show cdc task <task_name>

Displays detailed information including:
- Full task configuration
- Per-table watermarks
- Per-table error messages
- Retry counts
- State information

### Examples

```sql
-- List all tasks
show cdc all;

-- Get detailed info for task 'replicate_orders'
show cdc task replicate_orders;
```

### Sample Output

```
mysql> show cdc all;
+--------------------------------------+-------------------+-------------------------------------+-------------------------------------+---------+---------+-------------------+---------------------+
| task_id                              | task_name         | source_uri                          | sink_uri                            | state   | err_msg | checkpoint        | timestamp           |
+--------------------------------------+-------------------+-------------------------------------+-------------------------------------+---------+---------+-------------------+---------------------+
| 018d1234-5678-7abc-def0-123456789abc | replicate_orders  | mysql://prod_account#***@127...     | mysql://root:***@192.168.1.100:3306 | running | null    | 2025-11-06 10:30  | 2025-11-06 10:30:05 |
| 018d2345-6789-8bcd-ef01-234567890bcd | replicate_logs    | mysql://prod_account#***@127...     | mysql://root:***@192.168.1.100:3306 | paused  | null    | 2025-11-06 09:15  | 2025-11-06 09:15:30 |
+--------------------------------------+-------------------+-------------------------------------+-------------------------------------+---------+---------+-------------------+---------------------+
```

---

## PAUSE CDC - Pausing a CDC Task

### Purpose

Temporarily stop data synchronization without losing state. The task can be resumed later from the last checkpoint.

### Syntax

```sql
-- Pause a specific task
pause cdc task <task_name>;

-- Pause all tasks (system-level operation)
pause cdc all;
```

### Behavior

1. **Graceful Shutdown**: Waits for in-flight transactions to complete
2. **Checkpoint Saved**: Current watermark is persisted
3. **Resources Released**: Connections and goroutines are cleaned up
4. **Resumable**: Task can be resumed with `resume cdc task`

### Use Cases

- **Maintenance Window**: Pause during target database maintenance
- **Rate Limiting**: Temporarily stop sync to reduce load
- **Investigation**: Pause to investigate data issues
- **Planned Downtime**: Pause before scheduled system maintenance

### Examples

```sql
-- Pause a specific task
pause cdc task replicate_orders;

-- Pause all tasks (requires admin privileges)
pause cdc all;
```

### Verification

```sql
-- Check task state
show cdc task replicate_orders;

-- Expected output: state = 'paused'
```

---

## RESUME CDC - Resuming a CDC Task

### Purpose

Restart a paused CDC task from its last checkpoint.

### Syntax

```sql
-- Resume a specific paused task
resume cdc task <task_name>;
```

**Note**: Unlike PAUSE and DROP, there is no `resume cdc all` command. Resume tasks individually.

### Behavior

1. **State Check**: Verifies task is in `paused` state
2. **Clears Errors**: Resets error messages from previous failures
3. **Continues from Checkpoint**: Resumes from last watermark
4. **Reacquires Resources**: Reconnects to source/target databases

### Use Cases

- **After Maintenance**: Resume after target database is back online
- **Error Recovery**: Resume after resolving error conditions
- **Manual Control**: Resume after intentional pause

### Examples

```sql
-- Resume a specific task
resume cdc task replicate_orders;

-- Resume multiple tasks individually
resume cdc task task1;
resume cdc task task2;
resume cdc task task3;
```

### Error Clearing

**`resume cdc task` automatically clears ALL error messages for all tables in the task**, regardless of error type (retryable or non-retryable). This allows fresh retry attempts after fixing underlying issues.

**Behavior:**
- Clears all errors in `mo_catalog.mo_cdc_watermark.err_msg` for the task
- Resets error state for all tables
- Allows system to retry from last watermark
- Works for both retryable and non-retryable errors

**Use Cases:**
- After fixing network issues
- After restoring downstream MySQL service
- After fixing schema mismatches
- After resolving authentication problems
- After any manual intervention to fix root causes

**Example:**
```sql
-- Before RESUME: check for errors
show cdc task replicate_orders;
-- Shows: table1 has error "N:1763362169:rollback failure"

-- Fix the underlying issue (e.g., restore service, fix schema)

-- Resume clears errors and restarts
resume cdc task replicate_orders;

-- After RESUME: errors are cleared, processing resumes
show cdc task replicate_orders;
-- Shows: table1 has no error, processing continues
```

**Important Notes:**
- Resume clears **all** errors, not just non-retryable ones
- Always fix underlying issues before resuming
- System will retry from last watermark after resume
- If issue persists, error will occur again (can be cleared again)

---

## DROP CDC - Dropping a CDC Task

### Purpose

Permanently delete a CDC task and all its metadata.

### Syntax

```sql
-- Drop a specific task
drop cdc task <task_name>;

-- Drop all tasks (requires admin privileges)
drop cdc all;
```

### Behavior

1. **Stops Task**: If running, gracefully stops synchronization
2. **Deletes Metadata**: Removes task configuration from `mo_catalog.mo_cdc_task`
3. **Deletes Watermarks**: Removes all watermarks from `mo_catalog.mo_cdc_watermark`
4. **Irreversible**: Cannot be undone

### ⚠️ Warning

Dropping a CDC task is permanent. To temporarily stop a task, use `pause cdc task` instead.

### Examples

```sql
-- Drop a specific task
drop cdc task replicate_orders;

-- Drop all tasks (admin only)
drop cdc all;
```

---

## RESTART CDC - Restarting a CDC Task

### Purpose

Stop and immediately restart a CDC task, useful for applying configuration changes or recovering from persistent errors.

### Syntax

```sql
-- Restart a specific task
resume cdc task <task_name> 'restart';
```

**Note**: The RESTART operation uses the `resume` command with the `'restart'` parameter.

### Behavior

1. **Stops Task**: Gracefully stops the running task
2. **Clears State**: Resets error state
3. **Restarts**: Immediately starts the task again
4. **Continues from Checkpoint**: Resumes from last watermark

### Use Cases

- **Configuration Reload**: After modifying task parameters
- **Connection Issues**: Reset connections to source/target
- **State Reset**: Clear transient error states

### Examples

```sql
-- Restart a specific task
resume cdc task replicate_orders 'restart';

-- Restart multiple tasks
resume cdc task task1 'restart';
resume cdc task task2 'restart';
```

---

## Monitoring and Error Handling

### Task States

| State | Description | Actions Available |
|-------|-------------|-------------------|
| `running` | Task is actively syncing data | pause, drop, restart |
| `paused` | Task is temporarily stopped | resume, drop |
| `cancelled` | Task was stopped via drop | (none - task deleted) |
| `failed` | Task encountered a non-recoverable error | resume (clears error), drop |

### Error Types

The CDC system automatically classifies errors as **retryable** or **non-retryable** based on error type and context. This classification determines whether the system will automatically retry or require manual intervention.

#### 1. Retryable Errors

**Automatically retried up to 3 times** before requiring manual intervention.

**MatrixOne System/Network Errors (Highest Priority):**
These errors indicate temporary system unavailability and are always retryable:
- **RPC Timeout**: MatrixOne RPC calls timed out
- **No Available Backend**: No backend services available
- **Backend Cannot Connect**: Cannot connect to remote backend
- **TN Shard Not Found**: Transaction node shard not found (temporary)
- **RPC Error**: Generic RPC communication errors
- **Client/Backend Closed**: Connection closed unexpectedly
- **Context Timeout**: Operation timeout due to system delays

**Network/Connection Issues:**
- Connection timeouts, connection resets, network errors
- Service unavailable errors
- Network-related begin transaction failures

**Transaction Errors:**
- **Commit Failures**: Transient network issues during transaction commit
- **Begin Failures (Network-related)**: Network issues when starting transactions

**Data Source Errors:**
- **Table Relation Errors**: Table truncated or temporarily unavailable (table may be recreated)
- **Watermark Stall Errors**: Snapshot timestamp stuck (temporary condition)
  - Occurs when watermark cannot advance for extended period (default threshold: 1 minute)
  - System detects this condition and logs warning: `cdc.table_stream.snapshot_not_advanced`
  - Error is retryable - system will continue attempting to advance watermark
  - Tracked via metrics for monitoring
  - Usually resolves automatically when source data becomes available
- **StaleRead Errors**: If recovery is possible (watermark reset succeeds)

**Sinker Errors (Transient):**
- Circuit breaker errors (indicates temporary overload)
- Timeout errors from downstream MySQL

**Behavior**: 
- Task continues running, error message shows retry count
- Automatic retry up to 3 times (configurable via `WithMaxRetryCount`)
- After max retries, automatically converted to non-retryable
- Error persists in watermark table with format: `R:<count>:<first>:<last>:<message>`
- Uses exponential backoff between retries (configurable via `WithRetryBackoff`)

**Example Error Message**:
```
R:2:1763362169:1763362200:internal error: commit failure
```
- `R`: Retryable
- `2`: Retry count (current attempt)
- `1763362169`: First seen timestamp
- `1763362200`: Last seen timestamp
- `internal error: commit failure`: Error message

#### Retry Mechanism Details

**Exponential Backoff Strategy**:

The CDC system implements exponential backoff for retryable errors to avoid overwhelming the system during transient failures. The backoff delay is calculated based on:
- **Error Type**: Different error types may have different base delays
- **Retry Count**: Delay increases exponentially with each retry attempt
- **Configurable Parameters**: Base delay, maximum delay, and backoff factor

**Default Configuration**:
- **Max Retry Count**: 3 attempts
- **Base Delay**: 200ms (for most error types)
- **Max Delay**: 30s (caps the exponential growth)
- **Backoff Factor**: 2.0 (doubles delay each retry)

**Retry Delay Calculation**:
- Retry 1: Base delay (e.g., 200ms)
- Retry 2: Base delay × factor (e.g., 400ms)
- Retry 3: Base delay × factor² (e.g., 800ms)
- Capped at maximum delay (e.g., 30s)

**Original Error Preservation**:

To ensure **deterministic error reporting**, the CDC system preserves the **original error** that triggered the retry sequence. This design ensures that:

1. **Consistent Error Reporting**: The error returned to users and persisted in the database is the original error that caused the retry, not auxiliary errors encountered during retry attempts (e.g., cache lookup failures during cleanup).

2. **Clear Root Cause**: Users can identify the actual problem (e.g., "commit failure") rather than secondary issues that occurred during retry (e.g., "cache error").

3. **Predictable Behavior**: The system behavior is deterministic - the same error condition will always report the same error, regardless of what happens during retry attempts.

**How It Works**:
- When a retryable error first occurs, it is stored as the **original error**
- During retry attempts, if auxiliary errors occur (e.g., during cleanup operations), they are logged but do not replace the original error
- If a new primary error occurs (different error type), it replaces the original error
- If a fatal (non-retryable) error occurs, it replaces the original error
- When retries are exhausted or a fatal error occurs, the **original error** is returned/persisted

**Example Scenario**:
```
1. Commit failure occurs → Original error: "commit failure"
2. Retry attempt 1 → Cleanup encounters cache error → Original error preserved: "commit failure"
3. Retry attempt 2 → Commit failure again → Original error preserved: "commit failure"
4. Retry attempt 3 → Commit failure again → Original error preserved: "commit failure"
5. Max retries exceeded → System stops, reports: "commit failure" (not "cache error")
```

This ensures that users always see the root cause of the problem, not transient issues that occurred during recovery attempts.

#### 2. Non-Retryable Errors

**Require manual intervention** - no automatic retry.

**System State Errors:**
- **Rollback Failures**: System state may be inconsistent after rollback failure
- **Begin Failures (State-related)**: Cannot start transaction due to state issues (e.g., "transaction already active")

**Fatal Errors:**
- **Simple Sinker Errors**: Fatal errors before transaction starts (no context indicating transient issue)
- **StaleRead with startTs**: Cannot recover (startTs is set, cannot reset watermark)
- **Target Table Not Found**: Schema mismatch or table deleted permanently
- **Authentication Failures**: Invalid credentials (requires configuration fix)
- **Syntax Errors**: SQL construction failures (requires code fix)

**Control Signals:**
- Pause/cancel signals (intentional stops)

**Unknown Errors:**
- Errors that don't match any known pattern (default to non-retryable for safety)

**Behavior**: 
- Task remains in `running` state but sync is blocked for affected tables
- Error persists until manually cleared via `resume cdc task`
- No automatic retry
- Error persists in watermark table with format: `N:<first>:<message>`

**Example Error Message**:
```
N:1763362169:internal error: rollback failure
```
- `N`: Non-retryable
- `1763362169`: First seen timestamp
- `internal error: rollback failure`: Error message

**Max Retry Exceeded Format**:
```
N:1763362169:max retry exceeded (4): connection timeout
```
- Auto-converted from retryable after exceeding 3 retries
- Requires manual intervention via `resume cdc task`

### Error Classification Logic

The system uses a **priority-based classification** approach to determine if errors are retryable:

**Classification Priority (Checked in Order):**

1. **Control Signals** (Non-retryable)
   - Pause/cancel signals are always non-retryable (intentional stops)

2. **MatrixOne System Errors** (Retryable)
   - System checks for specific MatrixOne error codes first
   - All system/network errors (RPC timeout, backend unavailable, shard not found, etc.) are automatically retryable
   - This ensures resilience during temporary MatrixOne system unavailability or network fluctuations

3. **Context Timeout Errors** (Retryable)
   - Operation timeouts due to system delays are retryable

4. **Error Message Pattern Matching** (Retryable/Non-retryable)
   - **Retryable Patterns**: `commit`, `connection`, `timeout`, `network`, `unavailable`, `rpc`, `backend`, `shard`, `relation`, `truncated`, `stuck`, `stall`
   - **Non-Retryable Patterns**: `rollback`, `begin` (without network context), simple `sinker error` (without context)

5. **Default** (Non-retryable)
   - Unknown errors default to non-retryable for safety

**Special Handling:**

- **Begin Failures**: 
  - Network-related (contains `connection`, `timeout`, `network`, `unavailable`, `rpc`, `backend`) → **Retryable**
  - State-related (e.g., "transaction already active") → **Non-retryable**

- **Sinker Errors**:
  - With transient context (`circuit`, `timeout`) → **Retryable**
  - Simple "sinker error" without context → **Non-retryable**

- **StaleRead Errors**:
  - Recovery attempted automatically
  - If recovery succeeds → Error is swallowed, processing continues
  - If recovery fails → **Non-retryable**

**Why This Matters:**

This classification ensures that:
- **Temporary system issues** (network抖动, service unavailability, MatrixOne node failures) are automatically retried
- **Permanent errors** (state inconsistencies, fatal configuration issues) require manual intervention
- **Users can always recover** by using `resume cdc task` to clear any error and retry

### Error Storage

Errors are stored in two places for different purposes:

1. **In-Memory (Runtime)**:
   - Tracks the last error that occurred during processing
   - Maintains retryable flag for current processing round
   - Updated atomically to ensure consistency
   - Used for immediate retry decisions

2. **Persistent (Database)**:
   - Stored in `mo_catalog.mo_cdc_watermark.err_msg` column
   - Format: `R:<count>:<first>:<last>:<message>` (retryable) or `N:<first>:<message>` (non-retryable)
   - Persisted asynchronously to avoid blocking data processing
   - Used by task scheduler to decide whether to start table processing
   - Survives system restarts

### Error Consumption

Errors are consumed at two levels:

1. **Runtime (During Processing)**:
   - When an error occurs, processing round stops immediately
   - Error state is persisted to database
   - If retryable: Stream waits for next tick interval and retries automatically
   - If non-retryable: Stream stops processing, waits for manual intervention

2. **Startup (Task Scheduler)**:
   - Before starting table stream, scheduler checks persisted error in watermark table
   - Uses retry decision logic:
     - **No error** → Start processing
     - **Retryable error with count ≤ 3** → Start processing (continue retrying)
     - **Retryable error with count > 3** → Skip table (converted to non-retryable, requires manual clear)
     - **Non-retryable error** → Skip table (requires manual clear)

**Retry Decision Logic:**
- System automatically allows retry for retryable errors up to 3 attempts
- After 3 retries, retryable errors are converted to non-retryable
- Non-retryable errors always require manual intervention via `resume cdc task`

### Error Lifecycle

**Stage 1: Error Occurs**
- Error is detected during processing (network issue, commit failure, etc.)
- System immediately classifies error as retryable or non-retryable

**Stage 2: Error State Update**
- Error state updated atomically (ensures consistency)
- Error persisted to watermark table with metadata (retry count, timestamps)

**Stage 3: Retry Decision**
- **Retryable errors**: 
  - Retry count incremented
  - If count ≤ 3: Continue processing, retry on next tick
  - If count > 3: Convert to non-retryable, stop processing
- **Non-retryable errors**: 
  - Stop processing immediately
  - Require manual intervention

**Stage 4: Manual Recovery (if needed)**
- User fixes underlying issue (network restored, service back online, etc.)
- User runs `resume cdc task <task_name>` to clear error
- System clears all errors for the task
- Processing resumes from last watermark

**Key Points:**
- All errors (retryable and non-retryable) can be cleared via `resume cdc task`
- System always allows recovery through manual intervention
- Errors persist across system restarts until manually cleared

### Multi-Level Retry

The CDC system implements retry at multiple levels for maximum resilience:

1. **Executor Level (Sinker - Downstream MySQL)**:
   - Automatic retry with exponential backoff for SQL execution
   - Circuit breaker prevents retry storms
   - Retries network errors, MySQL connection issues, and transient SQL errors
   - Handles downstream service unavailability

2. **TableChangeStream Level (Data Processing)**:
   - Tick-based retry for retryable errors
   - Automatic StaleRead recovery (resets watermark if possible)
   - Error state tracking and persistence
   - Handles MatrixOne system errors and data source issues

3. **Task Scheduler Level (Task Management)**:
   - Startup check before starting table processing
   - Skips tables with non-retryable errors (requires manual intervention)
   - Allows retryable errors to continue processing
   - Manual recovery via `resume cdc task`

**Benefits:**
- Network fluctuations are handled automatically at executor level
- System unavailability is handled at table stream level
- Permanent errors are caught early and require manual fix
- Users can always recover via `resume cdc task`

### Error Resolution

**For Retryable Errors:**
- Usually resolve automatically within 3 retries
- Monitor retry count in error message (format: `R:<count>:...`)
- If error persists after 3 retries:
  - Check underlying cause (network, service availability)
  - Fix the issue if possible
  - Use `resume cdc task` to clear error and retry

**For Non-Retryable Errors:**
1. **Identify the root cause**:
   - Check error message in `show cdc task` output
   - Review logs for detailed error context
   - Common causes: missing tables, schema mismatches, authentication failures, state inconsistencies

2. **Fix the underlying issue**:
   - Create missing tables
   - Fix schema mismatches
   - Update credentials
   - Resolve state inconsistencies

3. **Clear error and retry**:
   - Run `resume cdc task <task_name>` to clear all errors
   - System will automatically retry on next scan
   - Processing resumes from last watermark

**Important: All Errors Can Be Cleared**

- `resume cdc task` clears **all errors** (both retryable and non-retryable) for all tables in the task
- This ensures users can always recover from any error state
- After clearing errors, system will retry from last watermark
- If underlying issue is fixed, processing will succeed
- If underlying issue persists, error will occur again and can be cleared again

**Best Practice:**
- Always fix the underlying issue before clearing errors
- Monitor error messages to understand root causes
- Use `resume cdc task` as a recovery mechanism after fixing issues

### Debugging with Logs

CDC components emit structured logs with consistent keys using the pattern `cdc.<component>.<event>`. Combine logs with metrics to triage issues quickly.

**Where to find logs**:

```bash
tail -f log/system.log | grep "cdc."
```

**Common prefixes**:

- `cdc.frontend.task.*`: Task lifecycle (create/start/pause/resume/restart failures)
- `cdc.table_stream.*`: Per-table polling rounds, pause/cancel signals, cleanup
- `cdc.progress_tracker.*`: Round summaries, initial sync start/complete/fail events
- `cdc.data_processor.*`: Snapshot/tail batch handling and sinker coordination
- `cdc.watermark_updater.*`: Cache updates, commit errors, circuit breaker events
- `cdc.mysql_sinker2.*`: SQL execution, retries, transaction begin/commit/rollback
- `cdc.executor.*`: Detailed retry decisions and circuit breaker state for sinker SQL

**Notable events**:

- `cdc.progress_tracker.initial_sync_start` / `initial_sync_complete` / `initial_sync_failed`: initial snapshot progress, with rows/SQL counts and time window
- `cdc.mysql_sinker2.exec_*_sql_failed` and `cdc.executor.retry_*`: downstream error diagnosis
- `cdc.table_stream.force_next_interval` / `round_end`: identifying polling cadence issues

**Enable verbose logging**: Production defaults to `info`. For deep dives, temporarily raise the log level in `etc/mo-service.toml` and restart the CN node:

```toml
[log]
level = "debug"
```

Remember to revert to `info` afterwards to avoid excessive log volume.

**Useful greps**:

- `grep 'CDC-Task-Start' log/system.log` – verify tasks transition to `running`
- `grep 'CDC-DataProcessor-NoMoreData-HeartbeatUpdate'` – confirm heartbeat ticks without data
- `grep 'CDC-WatermarkUpdater-BufferUpdate'` – inspect old/new watermarks and cache sizes
- `grep 'CDC-Sinker'` – pinpoint SQL failures or retry reasons

Cross-referencing log timestamps with Prometheus alerts (e.g., watermark lag spikes) narrows down the failing component quickly.

### Monitoring Queries

#### Check Overall Task Health

```sql
-- Show all tasks with their states
select task_name, state, checkpoint, err_msg
from mo_catalog.mo_cdc_task
where account_id = current_account_id();
```

#### Check Per-Table Errors

```sql
-- Show tables with errors for a specific task
select 
    t.task_name,
    w.db_name,
    w.table_name,
    w.watermark,
    w.err_msg
from mo_catalog.mo_cdc_watermark w
join mo_catalog.mo_cdc_task t 
    on w.task_id = t.task_id
where t.task_name = 'replicate_orders'
  and w.err_msg is not null 
  and w.err_msg != '';
```

#### Check Synchronization Lag

```sql
-- Compare watermark to current time
select 
    t.task_name,
    w.db_name,
    w.table_name,
    w.watermark,
    timestampdiff(minute, w.watermark, now()) as lag_minutes
from mo_catalog.mo_cdc_watermark w
join mo_catalog.mo_cdc_task t 
    on w.task_id = t.task_id
where t.state = 'running'
order by lag_minutes desc;
```

#### Detect Snapshot Stalls

Each table stream tracks whether recent polling rounds failed to advance the snapshot watermark. When this happens, CDC:

- Increments `mo_cdc_table_snapshot_no_progress_total` for the affected table on every stalled round.
- Sets `mo_cdc_table_stuck` to `1` and records the time in `mo_cdc_table_last_activity_timestamp`.
- Emits throttled warning logs; if the stall exceeds the internal threshold (default 1 minute) the table stream raises a retryable error so the scheduler can retry later.
- Resets counters and gauges automatically once progress resumes.

```promql
# Tables currently flagged as stalled
mo_cdc_table_stuck == 1

# Minutes since last successful progress
(time() - mo_cdc_table_last_activity_timestamp) / 60

# Rounds without progress in the last 10 minutes
increase(mo_cdc_table_snapshot_no_progress_total[10m]) > 0
```

> **Note**: The stall threshold and warning interval are configurable (defaults: 1 minute stall threshold, 10 second warning throttle). Adjust your alerting thresholds if you override these values.

### Common Error Messages

| Error Message | Cause | Resolution |
|---------------|-------|------------|
| `retryable error: stale read` | Source data not yet available at requested timestamp | Wait for automatic retry (up to 3 times) |
| `target table not found: <table>` | Target table doesn't exist | Create the table in the target database |
| `schema mismatch: <details>` | Source and target schemas differ | Align schemas or adjust table mapping |
| `authentication failed` | Invalid credentials in connection string | Update credentials and restart task |
| `connection timeout` | Network issue or target unavailable | Check network, wait for automatic retry |

---

## Metrics and Observability

### Overview

MatrixOne CDC provides comprehensive Prometheus metrics for monitoring task health, performance, and data flow. These metrics enable real-time monitoring, alerting, and capacity planning.

### Accessing Metrics

Metrics are exposed via the Prometheus endpoint:

```bash
# Access metrics endpoint
curl http://<cn-host>:<port>/metrics | grep mo_cdc

# Example
curl http://localhost:7001/metrics | grep mo_cdc
```

### Task Grouping for Mixed-Frequency Environments

When you have multiple CDC tasks with different frequencies (e.g., real-time, hourly, daily), use the **task_group label** strategy to manage alerts:

#### Labeling Strategy

Add labels to your tasks using naming conventions or external configuration:

**Option 1: Use Task Naming Convention**
- Create tasks with prefixes: `realtime_*`, `hourly_*`, `daily_*`
- Use Prometheus relabeling to extract task_group from task name

**Option 2: Use External Label Configuration**
- Maintain a config file mapping task_id → task_group
- Apply labels when scraping metrics

**Example Prometheus Relabel Config**:
```yaml
scrape_configs:
  - job_name: 'matrixone-cdc'
    static_configs:
      - targets: ['<cn-host>:<port>']
    metric_relabel_configs:
      # Extract task_group from table label
      # Format: account.task-id.db.table
      - source_labels: [table]
        regex: '.*\\.realtime_.*'
        target_label: task_group
        replacement: 'realtime'
      - source_labels: [table]
        regex: '.*\\.hourly_.*'
        target_label: task_group
        replacement: 'hourly'
      - source_labels: [table]
        regex: '.*\\.daily_.*'
        target_label: task_group
        replacement: 'daily'
```

**Benefits**:
- Single unified alert rule set
- Easy to add new task groups
- Clear monitoring dashboards per frequency

### Available Metrics

#### Task Lifecycle Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_task_total` | Gauge | `state` | Total number of CDC tasks by state |
| `mo_cdc_task_state_change_total` | Counter | `from_state`, `to_state` | Count of task state transitions |
| `mo_cdc_task_error_total` | Counter | `error_type`, `retryable` | Count of task errors by type |

**Example Queries**:
```promql
# Total running tasks
mo_cdc_task_total{state="running"}

# Failed tasks
mo_cdc_task_total{state="failed"}

# Task errors in last 5 minutes
rate(mo_cdc_task_error_total[5m])

# State transition rate
rate(mo_cdc_task_state_change_total[5m])
```

#### Watermark Health Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_watermark_lag_seconds` | Gauge | `table` | Time lag between current time and watermark (absolute value) |
| `mo_cdc_watermark_lag_ratio` | Gauge | `table` | Ratio of actual lag to expected lag (frequency-agnostic, <2 normal, >5 critical) ⚠️ *Planned feature* |
| `mo_cdc_watermark_cache_size` | Gauge | `tier` | Number of watermarks in each cache tier |
| `mo_cdc_watermark_update_total` | Counter | `table`, `update_type` | Count of watermark updates |
| `mo_cdc_watermark_commit_duration_seconds` | Histogram | - | Duration of watermark commits to database |
| `mo_cdc_table_snapshot_no_progress_total` | Counter | `table` | Count of processing rounds where snapshot timestamps failed to advance (stall detection) |

**Example Queries**:
```promql
# Maximum watermark lag across all tables (absolute)
max(mo_cdc_watermark_lag_seconds)

# Tables with >1 minute absolute lag (for real-time tasks)
mo_cdc_watermark_lag_seconds{task_group="realtime"} > 60

# Tables with >5 hour absolute lag (for hourly tasks)
mo_cdc_watermark_lag_seconds{task_group="hourly"} > 18000

# Tables with >5 day absolute lag (for daily tasks)
mo_cdc_watermark_lag_seconds{task_group="daily"} > 432000

# Watermark cache distribution
mo_cdc_watermark_cache_size{tier="uncommitted"}
mo_cdc_watermark_cache_size{tier="committing"}
mo_cdc_watermark_cache_size{tier="committed"}

# Watermark commit latency (P99)
histogram_quantile(0.99, mo_cdc_watermark_commit_duration_seconds_bucket)

# Snapshot stalls detected in the last 10 minutes
increase(mo_cdc_table_snapshot_no_progress_total[10m]) > 0

# ⚠️ Planned feature: Watermark Lag Ratio (frequency-agnostic)
# mo_cdc_watermark_lag_ratio  # <2: normal, 2-5: warning, >5: critical
```

#### Heartbeat Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_heartbeat_total` | Counter | `table` | Count of heartbeat updates (watermark advances without data changes) |

**Purpose**: Indicates CDC task is alive even when no data changes occur.

**Example Queries**:
```promql
# Heartbeat rate (updates per second)
rate(mo_cdc_heartbeat_total[1m])

# Tables without heartbeat (last 5 minutes)
absent_over_time(mo_cdc_heartbeat_total[5m])
```

#### Data Processing Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_rows_processed_total` | Counter | `operation`, `table` | Total rows processed |
| `mo_cdc_bytes_processed_total` | Counter | `operation`, `table` | Total bytes processed |
| `mo_cdc_batch_size_rows` | Histogram | `type` | Distribution of batch sizes |

**Labels**:
- `operation`: `read`, `insert`, `delete`
- `type`: `snapshot`, `tail`

**Example Queries**:
```promql
# Total rows synced in last hour
sum(increase(mo_cdc_rows_processed_total{operation="insert"}[1h]))

# Throughput (rows per second)
sum(rate(mo_cdc_rows_processed_total[1m]))

# Average batch size
avg(mo_cdc_batch_size_rows)
```

#### Table Stream Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_table_stream_total` | Gauge | `state` | Number of active table streams |
| `mo_cdc_table_stream_round_total` | Counter | `table`, `status` | Count of processing rounds |
| `mo_cdc_table_stream_round_duration_seconds` | Histogram | `table` | Duration of processing rounds |
| `mo_cdc_table_stuck` | Gauge | `table` | Whether a table stream is currently flagged as stalled (1 = stuck, 0 = healthy) |
| `mo_cdc_table_last_activity_timestamp` | Gauge | `table` | Unix timestamp when the table stream last made forward progress |

**Example Queries**:
```promql
# Active table streams
mo_cdc_table_stream_total{state="running"}

# Success rate
rate(mo_cdc_table_stream_round_total{status="success"}[5m])
/
rate(mo_cdc_table_stream_round_total[5m])

# Processing duration P99
histogram_quantile(0.99, mo_cdc_table_stream_round_duration_seconds_bucket)

# Tables currently marked as stuck
mo_cdc_table_stuck == 1

# Minutes since last successful progress
(time() - mo_cdc_table_last_activity_timestamp) / 60
```

#### Sinker Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_sinker_transaction_total` | Counter | `operation`, `status` | Count of transaction operations |
| `mo_cdc_sinker_sql_total` | Counter | `sql_type`, `status` | Count of SQL executions |
| `mo_cdc_sinker_sql_duration_seconds` | Histogram | `sql_type` | SQL execution duration |
| `mo_cdc_sinker_retry_total` | Counter | `sink`, `reason`, `result` | Count of retry attempts grouped by sink label, classification, and retry outcome (`failed`, `success`, `circuit_open`, `blocked`) |

**Labels**:
- `operation`: `begin`, `commit`, `rollback`
- `sql_type`: `insert`, `delete`, `ddl`
- `status`: `success`, `error`

**Example Queries**:
```promql
# Transaction commit rate
rate(mo_cdc_sinker_transaction_total{operation="commit",status="success"}[1m])

# SQL error rate
rate(mo_cdc_sinker_sql_total{status="error"}[5m])

# INSERT duration P99
histogram_quantile(0.99, mo_cdc_sinker_sql_duration_seconds_bucket{sql_type="insert"})

# Total retries
sum(mo_cdc_sinker_retry_total)

# Retries that escalated to circuit-open
sum(rate(mo_cdc_sinker_retry_total{result="circuit_open"}[5m]))
```

#### Initial Synchronization Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `mo_cdc_initial_sync_status` | Gauge | `table` | Initial sync status per table (0=not started, 1=running, 2=success, 3=failed) |
| `mo_cdc_initial_sync_start_timestamp` | Gauge | `table` | Unix timestamp when initial sync began |
| `mo_cdc_initial_sync_end_timestamp` | Gauge | `table` | Unix timestamp when initial sync finished (success or failure) |
| `mo_cdc_initial_sync_duration_seconds` | Histogram | `table` | Duration of initial sync rounds (records once per completion) |
| `mo_cdc_initial_sync_rows` | Gauge | `table` | Rows processed during initial sync window |
| `mo_cdc_initial_sync_bytes` | Gauge | `table` | Estimated bytes processed during initial sync |
| `mo_cdc_initial_sync_sql_total` | Gauge | `table` | Number of SQL statements executed during initial sync |

**Example Queries**:
```promql
# Initial syncs still running
mo_cdc_initial_sync_status == 1

# Initial syncs that failed and need attention
mo_cdc_initial_sync_status == 3

# Duration distribution (P95) for completed initial syncs
histogram_quantile(0.95, mo_cdc_initial_sync_duration_seconds_bucket)

# Top tables by initial sync row volume
topk(5, mo_cdc_initial_sync_rows)
```

### Monitoring Dashboard Queries

#### Health Overview

```promql
# Tasks Summary
sum by (state) (mo_cdc_task_total)

# Active Tables
count(mo_cdc_watermark_lag_seconds)

# Overall Health (green if <5s lag)
avg(mo_cdc_watermark_lag_seconds) < 5
```

#### Performance Metrics

```promql
# Throughput (rows/second)
sum(rate(mo_cdc_rows_processed_total{operation="insert"}[1m]))

# Total Data Synced (last 24h)
sum(increase(mo_cdc_bytes_processed_total[24h]))

# Heartbeat Frequency
sum(rate(mo_cdc_heartbeat_total[1m]))
```

#### Error Monitoring

```promql
# Error Rate
sum(rate(mo_cdc_task_error_total[5m]))

# Failed Transactions
sum(mo_cdc_sinker_transaction_total{status="error"})

# SQL Failures by Type
sum by (sql_type) (rate(mo_cdc_sinker_sql_total{status="error"}[5m]))
```

### Alert Rules

#### Critical Alerts

These alerts indicate immediate action required:

```yaml
# 1. No Running Tasks
- alert: CDCNoRunningTasks
  expr: mo_cdc_task_total{state="running"} == 0
  for: 5m
  severity: critical
  description: "All CDC tasks have stopped"

# 2. Task Failed
- alert: CDCTaskFailed
  expr: mo_cdc_task_total{state="failed"} > 0
  for: 1m
  severity: critical
  description: "One or more CDC tasks in failed state"

# 3. Watermark Lag Ratio (Unified Alert - Planned Feature)
# ⚠️ Note: mo_cdc_watermark_lag_ratio is a planned metric
# When available, it will provide frequency-agnostic alerting
# - alert: CDCWatermarkLagHigh
#   expr: mo_cdc_watermark_lag_ratio > 5
#   for: 3m
#   severity: critical
#   description: "Watermark lag is 5x higher than expected"

# 3. Watermark Stuck (Current Recommended Approach: Group-based)
# Use separate rules for different task frequencies if you have task groups
# Example for real-time tasks (Frequency < 1 minute):
- alert: CDCWatermarkStuck_Realtime
  expr: mo_cdc_watermark_lag_seconds{task_group="realtime"} > 300
  for: 2m
  severity: critical
  description: "Real-time task watermark stuck >5 minutes (table: {{ $labels.table }})"

# Example for hourly tasks:
- alert: CDCWatermarkStuck_Hourly
  expr: mo_cdc_watermark_lag_seconds{task_group="hourly"} > 18000
  for: 30m
  severity: critical
  description: "Hourly task watermark stuck >5 hours (table: {{ $labels.table }})"

# Example for daily tasks:
- alert: CDCWatermarkStuck_Daily
  expr: mo_cdc_watermark_lag_seconds{task_group="daily"} > 432000
  for: 6h
  severity: critical
  description: "Daily task watermark stuck >5 days (table: {{ $labels.table }})"

# 4. No Heartbeat (Use Different Windows for Different Task Groups)
# For real-time tasks (default):
- alert: CDCNoHeartbeat_Realtime
  expr: rate(mo_cdc_heartbeat_total{task_group="realtime"}[5m]) == 0
  for: 5m
  severity: critical
  description: "No heartbeat detected for real-time task (table: {{ $labels.table }})"

# For hourly tasks:
- alert: CDCNoHeartbeat_Hourly
  expr: increase(mo_cdc_heartbeat_total{task_group="hourly"}[3h]) == 0
  for: 3h
  severity: critical
  description: "No heartbeat detected for hourly task (table: {{ $labels.table }})"

# For daily tasks:
- alert: CDCNoHeartbeat_Daily
  expr: increase(mo_cdc_heartbeat_total{task_group="daily"}[3d]) == 0
  for: 3d
  severity: critical
  description: "No heartbeat detected for daily task (table: {{ $labels.table }})"
```

#### Warning Alerts

These alerts indicate potential issues:

```yaml
# 1. High Watermark Lag
- alert: CDCHighWatermarkLag
  expr: mo_cdc_watermark_lag_seconds > 60
  for: 3m
  severity: warning
  description: "Watermark lag >1 minute (table: {{ $labels.table }})"

# 2. High Error Rate
- alert: CDCHighErrorRate
  expr: rate(mo_cdc_task_error_total[5m]) > 0.01
  for: 5m
  severity: warning
  description: "High error rate detected (>0.01/s)"

# 3. Slow SQL Execution
- alert: CDCSQLSlow
  expr: histogram_quantile(0.99, mo_cdc_sinker_sql_duration_seconds_bucket) > 1
  for: 5m
  severity: warning
  description: "SQL P99 latency >1s (type: {{ $labels.sql_type }})"

# 4. Frequent Retries
- alert: CDCFrequentRetries
  expr: rate(mo_cdc_sinker_retry_total[5m]) > 0.1
  for: 5m
  severity: warning
  description: "High retry rate (>0.1/s, reason: {{ $labels.reason }})"
```

### Grafana Dashboard Setup

#### Panel 1: Task Overview

```json
{
  "title": "CDC Tasks Overview",
  "targets": [
    {
      "expr": "sum by (state) (mo_cdc_task_total)",
      "legendFormat": "{{ state }}"
    }
  ],
  "type": "stat"
}
```

#### Panel 2: Watermark Lag

```json
{
  "title": "Watermark Lag (seconds)",
  "targets": [
    {
      "expr": "mo_cdc_watermark_lag_seconds",
      "legendFormat": "{{ table }}"
    }
  ],
  "type": "graph",
  "alert": {
    "conditions": [
      {
        "evaluator": { "params": [60], "type": "gt" },
        "query": { "datasourceId": 1, "model": { "expr": "mo_cdc_watermark_lag_seconds" } }
      }
    ]
  }
}
```

#### Panel 3: Heartbeat Rate

```json
{
  "title": "Heartbeat Rate (per second)",
  "targets": [
    {
      "expr": "sum(rate(mo_cdc_heartbeat_total[1m]))",
      "legendFormat": "Heartbeat Rate"
    }
  ],
  "type": "graph"
}
```

#### Panel 4: Throughput

```json
{
  "title": "Throughput (rows/second)",
  "targets": [
    {
      "expr": "sum by (operation) (rate(mo_cdc_rows_processed_total[1m]))",
      "legendFormat": "{{ operation }}"
    }
  ],
  "type": "graph"
}
```

### Metric Usage Examples

#### Example 1: Check if CDC is Running

```bash
# Query Prometheus
curl -s 'http://localhost:7001/metrics' | grep 'mo_cdc_task_total{state="running"}'

# Expected output:
# mo_cdc_task_total{state="running"} 1.0
```

#### Example 2: Monitor Watermark Lag

```bash
# Get current watermark lag for all tables
curl -s 'http://localhost:7001/metrics' | grep 'mo_cdc_watermark_lag_seconds'

# Expected output:
# mo_cdc_watermark_lag_seconds{table="0.task-id.db1.table1"} 0.524
# mo_cdc_watermark_lag_seconds{table="0.task-id.db1.table2"} 1.203
```

**Interpretation** (depends on task `Frequency` setting):

For default frequency (200ms):
- < 1 second: Excellent (real-time sync)
- 1-10 seconds: Good (normal operation)
- 10-60 seconds: Fair (possible backlog)
- \> 60 seconds: Poor (investigate)

For custom frequency:
- Lag < 2x frequency: Normal (e.g., 1h frequency → <2h lag is normal)
- Lag > 5x frequency: Investigate
- Lag > 10x frequency: Stuck

**Example**: If `Frequency='1h'`, watermark lag of 30 minutes is normal.

#### Example 3: Verify Heartbeat

```bash
# Check heartbeat counter
curl -s 'http://localhost:7001/metrics' | grep 'mo_cdc_heartbeat_total'

# Expected output:
# mo_cdc_heartbeat_total{table="0.task-id.db1.table1"} 150.0
```

**Interpretation**:
- Value increasing: CDC is alive (even without data changes)
- Value stuck: CDC may be stuck or paused

#### Example 4: Check Data Throughput

```bash
# Check rows processed
curl -s 'http://localhost:7001/metrics' | grep 'mo_cdc_rows_processed_total'

# Expected output:
# mo_cdc_rows_processed_total{operation="insert",table="db1.table1"} 10523.0
# mo_cdc_rows_processed_total{operation="delete",table="db1.table1"} 42.0
```

### Monitoring Best Practices

#### 1. Set Up Alerts

Configure alerts for critical conditions:
- ✅ Task failures (`mo_cdc_task_total{state="failed"} > 0`)
- ✅ Watermark stuck (`mo_cdc_watermark_lag_seconds > 300`)
- ✅ No heartbeat (`rate(mo_cdc_heartbeat_total[5m]) == 0`)
- ✅ High error rate (`rate(mo_cdc_task_error_total[5m]) > 0.01`)
- ✅ Snapshot stall detection (`increase(mo_cdc_table_snapshot_no_progress_total[10m]) > 0` or `mo_cdc_table_stuck == 1`)

#### 2. Monitor Watermark Lag

Check watermark lag regularly (adjust thresholds based on task `Frequency`):

```promql
# For default Frequency (200ms):
# Green: < 10s
# Yellow: 10-60s
# Red: > 60s
mo_cdc_watermark_lag_seconds

# For custom Frequency tasks, adjust thresholds:
# Frequency=1h: Green < 2h, Yellow 2-5h, Red > 5h
# Frequency=1d: Green < 2d, Yellow 2-5d, Red > 5d
```

**Rule of thumb**: Normal lag ≈ task frequency. Alert if lag > 5x frequency.

#### 3. Track Heartbeats

Ensure heartbeats are regular (rate depends on task frequency):

```promql
# Expected rate varies by Frequency setting:
# - Frequency=200ms: ~5 heartbeats/second
# - Frequency=1s: ~1 heartbeat/second  
# - Frequency=1h: ~0.0003 heartbeats/second (1 per hour)
# - Frequency=1d: ~0.000012 heartbeats/second (1 per day)
rate(mo_cdc_heartbeat_total[1m])

# For hourly/daily tasks, use longer time windows:
increase(mo_cdc_heartbeat_total[1h])  # Count in last hour
increase(mo_cdc_heartbeat_total[1d])  # Count in last day
```

#### 4. Monitor Cache Health

Watermark cache should flow smoothly:

```promql
# Ideal state:
# - uncommitted: 1-10 (recent updates)
# - committing: 0 (processed quickly)
# - committed: matches number of tables
mo_cdc_watermark_cache_size{tier="uncommitted"}  # Should be low (<10)
mo_cdc_watermark_cache_size{tier="committing"}   # Should be 0 most of the time
mo_cdc_watermark_cache_size{tier="committed"}    # Should equal number of active tables
```

**Red Flags**:
- `committing` always > 0: Commits stuck or slow
- `uncommitted` growing: Updates faster than commits (acceptable if temporary)

#### 5. Performance Tracking

Monitor throughput and latency:

```promql
# Current throughput
sum(rate(mo_cdc_rows_processed_total{operation="insert"}[1m]))

# SQL execution time P99
histogram_quantile(0.99, mo_cdc_sinker_sql_duration_seconds_bucket{sql_type="insert"})
```

#### 6. Adjust Monitoring for Custom Frequency Tasks

When tasks use custom `Frequency` (e.g., hourly or daily sync), adjust monitoring accordingly:

**Watermark Lag Thresholds**:
- `Frequency='200ms'` (default): Alert if lag > 5 minutes
- `Frequency='1h'`: Alert if lag > 5 hours (5x frequency)
- `Frequency='1d'`: Alert if lag > 5 days (5x frequency)

**Heartbeat Detection Windows**:
- `Frequency='200ms'`: Check `rate(...[5m])` (5-minute window)
- `Frequency='1h'`: Check `increase(...[3h])` (3-hour window)
- `Frequency='1d'`: Check `increase(...[3d])` (3-day window)

**Example Alert Rule for Hourly Tasks**:
```yaml
- alert: CDCWatermarkStuck_Hourly
  expr: mo_cdc_watermark_lag_seconds > 18000  # 5 hours
  for: 30m
  severity: critical
  description: "Hourly task watermark stuck (table: {{ $labels.table }})"
```

### Integration with Monitoring Systems

#### Prometheus Integration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'matrixone-cdc'
    static_configs:
      - targets: ['<cn-host>:<port>']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

#### Grafana Integration

1. Add Prometheus as data source
2. Import dashboard JSON (see Appendix C)
3. Configure alerts in Alert Rules

#### Alert Manager Integration

Example `alertmanager.yml`:

```yaml
route:
  group_by: ['alertname', 'severity']
  receiver: 'cdc-team'
  routes:
    - match:
        severity: critical
      receiver: 'cdc-oncall'
      continue: true

receivers:
  - name: 'cdc-team'
    email_configs:
      - to: 'cdc-team@example.com'
  
  - name: 'cdc-oncall'
    pagerduty_configs:
      - service_key: '<pagerduty-key>'
```

### Metric Retention

Recommended Prometheus retention settings:

```yaml
# prometheus.yml
storage:
  tsdb:
    retention.time: 30d  # Keep 30 days of metrics
    retention.size: 50GB # Max 50GB storage
```

### Troubleshooting with Metrics

#### Problem: Task Not Syncing

**Check**:
```promql
# 1. Is task running?
mo_cdc_task_total{state="running"}

# 2. Any errors?
mo_cdc_task_error_total

# 3. Watermark advancing?
mo_cdc_watermark_lag_seconds
```

#### Problem: High Latency

**Check**:
```promql
# 1. SQL execution slow?
histogram_quantile(0.99, mo_cdc_sinker_sql_duration_seconds_bucket)

# 2. Watermark commit slow?
histogram_quantile(0.99, mo_cdc_watermark_commit_duration_seconds_bucket)

# 3. Many retries?
sum(rate(mo_cdc_sinker_retry_total[5m]))
```

#### Problem: Data Missing

**Check**:
```promql
# 1. Processing rounds succeeding?
mo_cdc_table_stream_round_total{status="error"}

# 2. Rows being read but not inserted?
mo_cdc_rows_processed_total{operation="read"}
vs
mo_cdc_rows_processed_total{operation="insert"}

# 3. SQL failures?
mo_cdc_sinker_sql_total{status="error"}
```

---

## Configuration Options Reference

### Level Option

Controls the scope of replication.

```sql
{'Level'='<level>'}
```

| Value | Description | Table Mapping Format |
|-------|-------------|----------------------|
| `table` | Replicate specific tables | `db1.t1:db1.t1,db1.t2:db1.t2` |
| `database` | Replicate all tables in database(s) | `db1:db1` |
| `account` | Replicate all databases in account | `*:*` |

### Exclude Option

Exclude specific databases or tables from replication.

```sql
{'Exclude'='<exclusion_list>'}
```

**Format**: Comma-separated list of `database.table` patterns

**Examples**:

```sql
-- Exclude specific tables
{'Exclude'='mydb.temp_table,mydb.staging'}

-- Exclude entire databases
{'Exclude'='test_db.*,staging_db.*'}

-- Mixed exclusions
{'Exclude'='mydb.temp,logs.*'}
```

### Time Range Options

#### StartTs

Start replication from a specific timestamp.

```sql
{'StartTs'='<timestamp>'}
```

**Format**: `YYYY-MM-DD HH:MM:SS`

**Default**: Current time when task is created

**Example**:
```sql
{'StartTs'='2025-01-01 00:00:00'}
```

#### EndTs

Stop replication at a specific timestamp (bounded replication).

```sql
{'EndTs'='<timestamp>'}
```

**Format**: `YYYY-MM-DD HH:MM:SS`

**Default**: (none - continuous replication)

**Example**:
```sql
{'EndTs'='2025-12-31 23:59:59'}
```

**Use Case**: Historical data migration, backfill

### Performance Options

#### MaxSqlLength

Maximum size of SQL statements sent to target.

```sql
{'MaxSqlLength'='<bytes>'}
```

**Default**: `4194304` (4MB)  
**Range**: 1024 to 16777216 (1KB to 16MB)

**Impact**: Larger values improve throughput but may cause issues if target has lower limits.

#### SendSqlTimeout

Timeout for sending SQL to target database.

```sql
{'SendSqlTimeout'='<duration>'}
```

**Default**: `10m` (10 minutes)  
**Format**: `<number><unit>` where unit is `s` (seconds), `m` (minutes), `h` (hours)

**Examples**:
```sql
{'SendSqlTimeout'='30s'}  -- 30 seconds
{'SendSqlTimeout'='5m'}   -- 5 minutes
{'SendSqlTimeout'='1h'}   -- 1 hour
```

#### Frequency

Polling frequency for detecting changes.

```sql
{'Frequency'='<duration>'}
```

**Default**: `200ms` (200 milliseconds)  
**Range**: 10ms to 60s

**Trade-off**: 
- Lower values → lower latency, higher CPU usage
- Higher values → higher latency, lower CPU usage

**Examples**:
```sql
-- Low latency (real-time trading)
{'Frequency'='50ms'}

-- Balanced (default)
{'Frequency'='200ms'}

-- Batch-oriented (reporting)
{'Frequency'='5s'}
```

### Snapshot Options

#### NoFull

Skip initial full snapshot, sync incremental changes only.

```sql
{'NoFull'='true'}
```

**Default**: `false` (includes initial snapshot)

**Use Cases**:
- Target already has full data
- Only interested in changes after task creation
- Reducing initial load time

#### InitSnapshotSplitTxn

Split large initial snapshots into multiple transactions.

```sql
{'InitSnapshotSplitTxn'='true'}
```

**Default**: `true`

**Impact**: 
- `true`: Better memory usage, longer initial sync
- `false`: Faster initial sync, higher memory usage

---

## Best Practices

### 1. Naming Conventions

Use descriptive, consistent task names:

```sql
-- Good
create cdc prod_orders_to_analytics ...
create cdc replicate_user_events ...
create cdc backup_financial_data ...

-- Avoid
create cdc task1 ...
create cdc temp ...
create cdc test ...
```

### 2. Start Small

Begin with table-level replication before expanding to database or account level.

```sql
-- Start with one table
create cdc test_replication
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'mydb.orders:mydb.orders'
  {'Level'='table'};

-- After verification, expand
create cdc full_db_replication
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'mydb:mydb'
  {'Level'='database'};
```

### 3. Use Exclusions Wisely

Exclude temporary, staging, or high-churn tables:

```sql
create cdc prod_replication
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'production:production'
  {'Level'='database', 'Exclude'='production.temp_*,production.staging_*'};
```

### 4. Set Appropriate Time Ranges

For historical migrations, use bounded time ranges:

```sql
-- Migrate 2024 data only
create cdc historical_2024
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'analytics.events:analytics.events'
  {'StartTs'='2024-01-01 00:00:00', 'EndTs'='2024-12-31 23:59:59'};
```

### 5. Monitor Regularly

Set up monitoring queries as scheduled jobs:

```sql
-- Daily check: tasks with errors
select task_name, state, err_msg
from mo_catalog.mo_cdc_task
where err_msg is not null and err_msg != '';

-- Daily check: sync lag > 1 hour
select 
    task_name,
    timestampdiff(minute, checkpoint, now()) as lag_minutes
from mo_catalog.mo_cdc_task
where state = 'running'
  and timestampdiff(minute, checkpoint, now()) > 60;
```

### 6. Plan for Maintenance

Before target database maintenance:

```sql
-- 1. Pause CDC
pause cdc task prod_replication;

-- 2. Perform maintenance
-- (external operations)

-- 3. Resume CDC
resume cdc task prod_replication;
```

### 7. Test in Non-Production First

Always test CDC configurations in development/staging before production:

1. Create test task with same configuration
2. Verify data accuracy
3. Monitor performance impact
4. Validate error handling

### 8. Secure Connection Strings

- Use strong passwords
- Consider network security
- Rotate credentials regularly
- Limit user privileges to minimum required

---

## Troubleshooting

### Problem: Task Stuck in "Paused" State

**Symptoms**: Cannot resume task, remains paused after resume command

**Diagnosis**:
```sql
show cdc task <task_name>;
-- Check for error messages
```

**Resolution**:
1. Check target database connectivity
2. Verify credentials are still valid
3. Try restart instead of resume:
   ```sql
   resume cdc task <task_name> 'restart';
   ```

### Problem: High Synchronization Lag

**Symptoms**: Watermark is significantly behind current time

**Diagnosis**:
```sql
select 
    t.task_name,
    w.db_name,
    w.table_name,
    w.watermark,
    timestampdiff(minute, w.watermark, now()) as lag_minutes
from mo_catalog.mo_cdc_watermark w
join mo_catalog.mo_cdc_task t on w.task_id = t.task_id
where t.task_name = '<task_name>';
```

**Resolution**:
1. Check target database load (slow INSERTs/UPDATEs)
2. Increase `MaxSqlLength` for larger batches
3. Check for table-level errors blocking sync
4. Consider adding indexes on target tables

### Problem: "Target Table Not Found" Error

**Symptoms**: Error message indicates target table doesn't exist

**Resolution**:
1. Verify target table exists:
   ```sql
   -- On target database
   show tables like '<table_name>';
   ```
2. Create table if missing (use same schema as source)
3. Resume CDC:
   ```sql
   resume cdc task <task_name>;
   ```

### Problem: Schema Mismatch

**Symptoms**: Error about column count or type mismatch

**Resolution**:
1. Compare source and target schemas
2. Align schemas (ADD/MODIFY columns as needed)
3. For DDL changes on source:
   - Drop and recreate CDC task, OR
   - Manually apply DDL to target, then restart CDC

### Problem: Authentication Failures

**Symptoms**: "Authentication failed" or "Access denied" errors

**Resolution**:
1. Verify credentials in connection string
2. Check user privileges on target database:
   ```sql
   -- On target database
   show grants for 'cdc_user'@'%';
   ```
3. Required privileges: INSERT, UPDATE, DELETE, SELECT
4. After fixing credentials, restart CDC:
   ```sql
   resume cdc task <task_name> 'restart';
   ```

### Problem: Retryable Errors Exhausted

**Symptoms**: Error message shows "retry 3/3", sync blocked

**Resolution**:
1. Identify root cause (check target database logs)
2. Fix underlying issue (network, target capacity, etc.)
3. Resume to reset retry counter:
   ```sql
   resume cdc task <task_name>;
   ```

### Problem: Task Not Starting After CREATE

**Symptoms**: Task created but state remains in unexpected state

**Diagnosis**:
```sql
show cdc task <task_name>;
-- Check state and err_msg columns
```

**Resolution**:
1. Verify source and target connectivity
2. Check task logs (system logs)
3. Drop and recreate task with correct configuration:
   ```sql
   drop cdc task <task_name>;
   create cdc <task_name> ...
   ```

---

## Test Scenarios

### For QA Engineers and Testers

This section provides comprehensive test scenarios for validating CDC functionality.

#### Test Suite 1: Basic Operations

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-001 | Create table-level CDC | 1. `create cdc` with single table<br>2. `show cdc task` | Task created, state=running |
| TC-002 | Create database-level CDC | 1. `create cdc` with Level='database'<br>2. `show cdc task` | Task created, multiple tables detected |
| TC-003 | Create duplicate task | 1. `create cdc task1`<br>2. `create cdc task1` | Second CREATE fails with error |
| TC-004 | Show all tasks | 1. Create 3 CDC tasks<br>2. `show cdc all` | Returns 3 tasks |
| TC-005 | Show specific task | 1. `create cdc task1`<br>2. `show cdc task task1` | Returns detailed info for task1 |
| TC-006 | Show non-existent task | `show cdc task non_existent` | Error or empty result |

#### Test Suite 2: Pause and Resume

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-101 | Pause running task | 1. `create cdc task1`<br>2. `pause cdc task task1`<br>3. `show cdc task task1` | State changes to 'paused' |
| TC-102 | Resume paused task | 1. `pause cdc task task1`<br>2. `resume cdc task task1`<br>3. `show cdc task task1` | State changes to 'running' |
| TC-103 | Pause already paused task | 1. `pause cdc task task1`<br>2. `pause cdc task task1` | No error (idempotent) |
| TC-104 | Data sync stops on pause | 1. `pause cdc task task1`<br>2. Insert data on source<br>3. Check target | Target not updated |
| TC-105 | Data sync resumes from checkpoint | 1. Note checkpoint<br>2. Pause<br>3. Insert data<br>4. Resume<br>5. Check target | All data synced from checkpoint |
| TC-106 | Pause all tasks | 1. Create 3 tasks<br>2. `pause cdc all`<br>3. `show cdc all` | All tasks paused |

#### Test Suite 3: Drop and Restart

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-201 | Drop running task | 1. `create cdc task1`<br>2. `drop cdc task task1`<br>3. `show cdc task task1` | Task not found |
| TC-202 | Drop paused task | 1. `pause cdc task task1`<br>2. `drop cdc task task1`<br>3. `show cdc task task1` | Task not found |
| TC-203 | Drop non-existent task | `drop cdc task non_existent` | Error |
| TC-204 | Drop all tasks | 1. Create 3 tasks<br>2. `drop cdc all`<br>3. `show cdc all` | No tasks remain |
| TC-205 | Restart running task | 1. `resume cdc task task1 'restart'`<br>2. `show cdc task task1` | State=running, no errors |
| TC-206 | Restart paused task | 1. `pause cdc task task1`<br>2. `resume cdc task task1 'restart'`<br>3. `show cdc task task1` | State=running |
| TC-207 | Restart clears errors | 1. Cause error condition<br>2. `resume cdc task task1 'restart'`<br>3. Check err_msg | Error cleared |

#### Test Suite 4: Data Synchronization

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-301 | Sync INSERT | 1. CREATE CDC<br>2. INSERT on source<br>3. Check target | Row appears on target |
| TC-302 | Sync UPDATE | 1. CREATE CDC<br>2. UPDATE on source<br>3. Check target | Row updated on target |
| TC-303 | Sync DELETE | 1. CREATE CDC<br>2. DELETE on source<br>3. Check target | Row deleted on target |
| TC-304 | Sync bulk INSERT | 1. CREATE CDC<br>2. INSERT 10000 rows<br>3. Check target | All 10000 rows synced |
| TC-305 | Sync transaction | 1. CREATE CDC<br>2. BEGIN; INSERT; UPDATE; COMMIT;<br>3. Check target | Transaction applied atomically |
| TC-306 | Initial snapshot | 1. Insert 1000 rows<br>2. CREATE CDC (NoFull=false)<br>3. Check target | All 1000 rows synced |
| TC-307 | Skip initial snapshot | 1. Insert 1000 rows<br>2. CREATE CDC (NoFull=true)<br>3. Check target | No initial rows synced |
| TC-308 | Multiple tables | 1. CREATE CDC with 3 tables<br>2. INSERT in all tables<br>3. Check target | All tables synced |

#### Test Suite 5: Time Range and Filtering

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-401 | StartTs respected | 1. CREATE CDC with StartTs=tomorrow<br>2. INSERT now<br>3. Check target | No sync until tomorrow |
| TC-402 | EndTs respected | 1. CREATE CDC with EndTs=yesterday<br>2. Wait for task completion | Task stops at EndTs |
| TC-403 | Exclude tables | 1. CREATE CDC with Exclude='db1.t2'<br>2. INSERT in t1 and t2<br>3. Check target | Only t1 synced |
| TC-404 | Database-level with exclusions | 1. Level='database', Exclude='db1.temp'<br>2. INSERT in multiple tables | All except excluded synced |

#### Test Suite 6: Error Handling

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-501 | Target table missing | 1. CREATE CDC<br>2. Drop target table<br>3. INSERT on source | Error in err_msg, task still running |
| TC-502 | Resume clears error | 1. Cause error<br>2. Fix issue<br>3. `resume cdc task` | Error cleared, sync resumes |
| TC-503 | Network timeout retry | 1. Block network temporarily<br>2. INSERT on source<br>3. Unblock network | Auto-retry succeeds |
| TC-504 | Max retry exhausted | 1. Cause persistent error<br>2. Wait for 3 retries<br>3. Check err_msg | Shows "retry 3/3" |
| TC-505 | Per-table error isolation | 1. CDC with 2 tables<br>2. Drop target table1<br>3. INSERT in both tables | table2 syncs, table1 has error |

#### Test Suite 7: Concurrency

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-601 | Concurrent inserts | 1. CREATE CDC<br>2. INSERT from 10 concurrent sessions<br>3. Check target | All rows synced |
| TC-602 | Pause during sync | 1. Start bulk INSERT<br>2. `pause cdc task` mid-sync<br>3. Resume<br>4. Check target | All rows eventually synced |
| TC-603 | Drop during sync | 1. Start bulk INSERT<br>2. `drop cdc task` mid-sync | Task stopped cleanly |
| TC-604 | Multiple tasks on same table | 1. CREATE CDC task1 for table t1<br>2. CREATE CDC task2 for table t1 | Both tasks sync independently |

#### Test Suite 8: Edge Cases

| Test ID | Test Case | Steps | Expected Result |
|---------|-----------|-------|-----------------|
| TC-701 | Empty table sync | 1. CREATE CDC on empty table<br>2. `show cdc task` | Task created, no data synced |
| TC-702 | Very large row | 1. INSERT row with 10MB blob<br>2. Check target | Row synced (if < MaxSqlLength) |
| TC-703 | Special characters | 1. INSERT row with quotes, backslashes<br>2. Check target | Correctly escaped and synced |
| TC-704 | NULL values | 1. INSERT row with multiple NULLs<br>2. Check target | NULLs preserved |
| TC-705 | Max tasks per account | 1. Create 100 CDC tasks<br>2. Check status | All tasks running |
| TC-706 | Long-running task | 1. CREATE CDC<br>2. Run for 24 hours<br>3. Check status | Task still running, no errors |

---

## FAQ

### General Questions

**Q: What is the maximum number of CDC tasks per account?**  
A: There is no hard limit, but monitor system resources. Recommend < 100 concurrent tasks per CN node.

**Q: Does CDC support DDL synchronization?**  
A: Currently, CDC synchronizes DML (INSERT/UPDATE/DELETE) only. DDL changes require manual application to the target or task recreation.

**Q: Can I replicate between different MatrixOne accounts?**  
A: Yes, use the appropriate account names in the source and sink connection strings.

**Q: What happens if the target database is down?**  
A: CDC will retry transient errors up to 3 times. For persistent errors, the task remains running but sync is blocked. Resume after target is back online.

**Q: Is data encrypted during replication?**  
A: SSL/TLS encryption depends on the MySQL connection settings. Consult your database configuration for encryption options.

### Configuration Questions

**Q: What is the default sync frequency?**  
A: 200 milliseconds (configurable via `Frequency` option).

**Q: Can I change task configuration after creation?**  
A: No, CDC tasks are immutable. To change configuration, drop and create a new task.

**Q: How do I replicate only recent data?**  
A: Use the `NoFull` option to skip initial snapshot, or set `StartTs` to a specific timestamp.

**Q: What is the recommended MaxSqlLength?**  
A: Default 4MB is suitable for most cases. Increase for better throughput if target supports it.

### Operational Questions

**Q: How do I know if my task is healthy?**  
A: Check `state` is 'running', `err_msg` is empty, and sync lag is acceptable:
```sql
show cdc task <task_name>;
```

**Q: Can I pause a task temporarily without losing progress?**  
A: Yes, use `pause cdc task <task_name>`. Resume later with `resume cdc task <task_name>`.

**Q: How do I migrate a large database efficiently?**  
A: Use database-level CDC with appropriate exclusions:
```sql
create cdc migrate_prod
  'mysql://prod_account#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'prod_db:prod_db'
  {'Level'='database', 'Exclude'='prod_db.temp_*,prod_db.staging_*'};
```

**Q: What privileges are required on the target database?**  
A: INSERT, UPDATE, DELETE, SELECT (for duplicate key detection).

### Troubleshooting Questions

**Q: My task shows errors, what should I do?**  
A: 
1. Check error message with `show cdc task <task_name>`
2. Fix underlying issue (e.g., create missing table)
3. Resume task: `resume cdc task <task_name>`

**Q: Why is my sync lagging behind?**  
A: Possible causes:
- Target database overloaded
- Network latency
- Large transactions
- Table-level errors blocking sync

Check with the [lag monitoring query](#check-synchronization-lag).

**Q: Can I recover a dropped CDC task?**  
A: No, drop is permanent. You must create a new task. Consider pause instead for temporary stops.

**Q: How do I handle schema changes?**  
A: 
1. Pause CDC task
2. Apply DDL to both source and target
3. Resume CDC task

Or drop and create new task.

### Metrics Questions

**Q: How do I check if CDC is running properly?**  
A: Use Prometheus metrics:
```bash
# Check task status
curl http://localhost:7001/metrics | grep 'mo_cdc_task_total{state="running"}'

# Check watermark lag (should be <10 seconds)
curl http://localhost:7001/metrics | grep 'mo_cdc_watermark_lag_seconds'

# Check heartbeat (should be increasing)
curl http://localhost:7001/metrics | grep 'mo_cdc_heartbeat_total'
```

**Q: What does watermark lag mean?**  
A: Watermark lag is the time difference between the current time and the last synchronized data timestamp.

**For default frequency (200ms)**:
- < 1s: Excellent (real-time)
- 1-10s: Good (normal)
- 10-60s: Fair (possible backlog)
- \> 60s: Poor (investigate)

**For custom frequency tasks**:
- Normal lag ≈ task frequency (e.g., Frequency='1h' → lag ≈ 1 hour is normal)
- Investigate if lag > 5x frequency
- Alert if lag > 10x frequency (likely stuck)

**Q: What is a heartbeat and why is it important?**  
A: Heartbeat is a watermark update that occurs even when there's no data change. It indicates:
- ✅ CDC task is alive and monitoring
- ✅ Connection to source database is healthy
- ✅ System is ready to process new changes

A stopped heartbeat indicates CDC may be stuck or paused. **Note**: For tasks with `Frequency` >= 1 hour, heartbeats are naturally infrequent. Adjust monitoring windows:
- Frequency=200ms: Check 5-minute window
- Frequency=1h: Check 3-hour window  
- Frequency=1d: Check 3-day window

**Q: How do I set up alerts for CDC?**  
A: Use the alert rules provided in the [Metrics and Observability](#metrics-and-observability) section with Prometheus Alertmanager.

**Q: I have multiple tasks with different frequencies (1 day, 1 hour, 1 minute). How do I set unified alert rules?**  
A: **Current best practice**: Use task grouping with naming conventions:

1. **Name your tasks with frequency prefix**: `realtime_sync`, `hourly_backup`, `daily_archive`
2. **Configure Prometheus relabeling** to extract `task_group` label from task names
3. **Create separate alert rules per group**:

```yaml
# Real-time tasks (Frequency < 1 minute)
- alert: CDCWatermarkStuck_Realtime
  expr: mo_cdc_watermark_lag_seconds{task_group="realtime"} > 300  # 5 minutes
  
# Hourly tasks
- alert: CDCWatermarkStuck_Hourly
  expr: mo_cdc_watermark_lag_seconds{task_group="hourly"} > 18000  # 5 hours
  
# Daily tasks  
- alert: CDCWatermarkStuck_Daily
  expr: mo_cdc_watermark_lag_seconds{task_group="daily"} > 432000  # 5 days
```

See [Task Grouping](#task-grouping-for-mixed-frequency-environments) for detailed configuration.

**Future enhancement**: A `mo_cdc_watermark_lag_ratio` metric is planned to provide frequency-agnostic unified alerting.

**Q: Can I monitor CDC without Prometheus?**  
A: Yes, you can use SQL queries on system tables (`mo_cdc_task`, `mo_cdc_watermark`), but metrics provide more real-time and detailed monitoring.

---

## Appendix A: System Tables

### mo_catalog.mo_cdc_task

Stores CDC task metadata.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | bigint unsigned | Account ID |
| `task_id` | uuid | Unique task identifier |
| `task_name` | varchar(1000) | User-defined task name |
| `source_uri` | text | Source connection string |
| `sink_uri` | text | Target connection string |
| `sink_type` | varchar(20) | Target type: mysql/matrixone |
| `tables` | text | JSON-encoded table mappings |
| `filters` | text | Excluded tables |
| `start_ts` | varchar(1000) | Start timestamp |
| `end_ts` | varchar(1000) | End timestamp |
| `state` | varchar(20) | Current state |
| `checkpoint` | bigint unsigned | Global checkpoint (deprecated) |
| `no_full` | bool | Skip initial snapshot |
| `err_msg` | varchar(256) | Task-level error message |
| `task_create_time` | datetime | Task creation time |
| `additional_config` | text | JSON-encoded extra options |

### mo_catalog.mo_cdc_watermark

Stores per-table watermarks and errors.

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | bigint unsigned | Account ID |
| `task_id` | uuid | Task ID (FK to mo_cdc_task) |
| `db_name` | varchar(256) | Database name |
| `table_name` | varchar(256) | Table name |
| `watermark` | varchar(128) | Last synchronized timestamp |
| `err_msg` | varchar(256) | Per-table error message |

---

## Appendix B: Quick Reference

### Command Summary

| Command | Syntax |
|---------|--------|
| CREATE CDC | `create cdc <name> '<source>' '<sink_type>' '<sink>' '<tables>' {options};` |
| SHOW ALL | `show cdc all;` |
| SHOW TASK | `show cdc task <name>;` |
| PAUSE TASK | `pause cdc task <name>;` |
| PAUSE ALL | `pause cdc all;` |
| RESUME TASK | `resume cdc task <name>;` |
| RESTART TASK | `resume cdc task <name> 'restart';` |
| DROP TASK | `drop cdc task <name>;` |
| DROP ALL | `drop cdc all;` |

### State Transitions

```
             create cdc
                 ↓
             ┌──────────┐
             │ running  │←──────┐
             └──────────┘       │
               ↓      ↑         │
          pause│      │resume   │restart
               ↓      ↑         │
             ┌──────────┐       │
             │  paused  │───────┘
             └──────────┘
               ↓
           drop │
               ↓
             ┌──────────┐
             │cancelled │
             └──────────┘
```

### Connection String Examples

```
Source (MatrixOne):
  mysql://my_account#admin:password@127.0.0.1:6001

Sink (MySQL):
  mysql://root:password@192.168.1.100:3306

Sink (MatrixOne):
  mysql://target_account#admin:password@192.168.1.200:6001
```

---

## Document Information

**Version**: 1.0  
**Last Updated**: November 6, 2025  
**Maintainer**: MatrixOne CDC Team  
**Feedback**: Please report documentation issues to the development team

---

**End of User Guide**
