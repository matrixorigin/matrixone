# MatrixOne CDC User Guide

**Version**: 1.0  
**Last Updated**: November 2025  
**Target Audience**: Database Users, Testers, QA Engineers, Technical Writers

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [CREATE CDC - Creating a CDC Task](#create-cdc---creating-a-cdc-task)
4. [SHOW CDC - Viewing CDC Tasks](#show-cdc---viewing-cdc-tasks)
5. [PAUSE CDC - Pausing a CDC Task](#pause-cdc---pausing-a-cdc-task)
6. [RESUME CDC - Resuming a CDC Task](#resume-cdc---resuming-a-cdc-task)
7. [DROP CDC - Dropping a CDC Task](#drop-cdc---dropping-a-cdc-task)
8. [RESTART CDC - Restarting a CDC Task](#restart-cdc---restarting-a-cdc-task)
9. [Monitoring and Error Handling](#monitoring-and-error-handling)
10. [Configuration Options Reference](#configuration-options-reference)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)
13. [Test Scenarios](#test-scenarios)
14. [FAQ](#faq)

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
'db1.*:db1.*'

-- Account-level (all databases and tables)
'*.*:*.*'
```

#### Optional Parameters

Use curly braces `{}` to specify options. Options are comma-separated key-value pairs with keys in single quotes:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Level` | string | `table` | Replication granularity: `account`, `database`, or `table` |
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
  'db1.*:db1.*'
  {'Level'='database'};
```

#### 3. Account Level

Replicate all databases and tables in the account.

```sql
create cdc task_account_level
  'mysql://myaccount#root:password@127.0.0.1:6001'
  'matrixone'
  'mysql://target_account#admin:password@192.168.1.200:6001'
  '*.*:*.*'
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
  {};
```

#### Example 2: Multiple Tables with Exclusion

```sql
create cdc replicate_sales_db
  'mysql://prod_account#admin:SecurePass123@127.0.0.1:6001'
  'mysql'
  'mysql://root:MySQLPass456@192.168.1.100:3306'
  'sales.*:sales.*'
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
  'mydb.*:mydb.*'
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

`resume cdc task` automatically clears error messages for all tables, allowing fresh retry attempts.

```sql
-- Before RESUME: check for errors
show cdc task replicate_orders;

-- Resume clears errors and restarts
resume cdc task replicate_orders;

-- After RESUME: errors are cleared
show cdc task replicate_orders;
```

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

#### 1. Retryable Errors

Automatically retried up to 3 times:
- Network timeouts
- Transient connection failures
- Stale read errors

**Behavior**: Task continues running, error message shows retry count

**Example Error Message**:
```
retryable error: timeout connecting to target (retry 2/3)
```

#### 2. Non-Retryable Errors

Require manual intervention:
- Target table not found
- Schema mismatch
- Authentication failures
- Syntax errors

**Behavior**: Task remains in `running` state but sync is blocked for affected tables

**Resolution**: Fix the issue, then `resume cdc task <task_name>` to clear errors

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

### Common Error Messages

| Error Message | Cause | Resolution |
|---------------|-------|------------|
| `retryable error: stale read` | Source data not yet available at requested timestamp | Wait for automatic retry (up to 3 times) |
| `target table not found: <table>` | Target table doesn't exist | Create the table in the target database |
| `schema mismatch: <details>` | Source and target schemas differ | Align schemas or adjust table mapping |
| `authentication failed` | Invalid credentials in connection string | Update credentials and restart task |
| `connection timeout` | Network issue or target unavailable | Check network, wait for automatic retry |

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
| `database` | Replicate all tables in database(s) | `db1.*:db1.*` |
| `account` | Replicate all databases in account | `*.*:*.*` |

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
  {};

-- After verification, expand
create cdc full_db_replication
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'mydb.*:mydb.*'
  {'Level'='database'};
```

### 3. Use Exclusions Wisely

Exclude temporary, staging, or high-churn tables:

```sql
create cdc prod_replication
  'mysql://myaccount#admin:password@127.0.0.1:6001'
  'mysql'
  'mysql://root:password@192.168.1.100:3306'
  'production.*:production.*'
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
  'prod_db.*:prod_db.*'
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
