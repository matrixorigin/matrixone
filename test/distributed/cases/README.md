# BVT Test Case Tags Reference

This document describes the usage of mo-tester test tags for writing BVT test cases.

## File-Level Tags

### `-- @skip:issue#{IssueNo.}`
Skip the entire test file, used for known issues that cannot be fixed temporarily.

```sql
-- @skip:issue#16438

drop database if exists db1;
create database db1;
```

## SQL-Level Tags

### `-- @bvt:issue#{IssueNo.}` / `-- @bvt:issue`
Mark SQL statement blocks to be skipped due to known issues. Use `-g` parameter at runtime to skip these statements.

```sql
-- @bvt:issue#5790
drop table if exists t1;
create table t1(a int, b varchar(20), unique key(a));
insert into t1 values(null, '2');
-- @bvt:issue
```

### `-- @ignore:{col_index},...`
Ignore comparison of specified columns in the result set, column index starts from 0. Suitable for queries containing unstable data like timestamps or random values.

```sql
-- @ignore:5,6
show publications;

-- @ignore:0,4
show columns from `procs_priv`;
```

### `-- @sortkey:{col_index},...`
Specify sort key columns for the result set, used for queries with uncertain result order.

```sql
-- @sortkey:0,1
SELECT col1, col2 FROM t1;
```

### `-- @regex("<pattern>", <include:boolean>)`
Regular expression matching check. `include=true` means the result must contain a match, `false` means it must not contain.

```sql
-- @regex("acc_save",true)
-- @regex("root",false)
show accounts;
```

## Session Control Tags

### `-- @session:id={N}&user={user}&password={pwd}` / `-- @session}`
Create a new connection to execute SQL statement blocks, used for testing concurrent transaction scenarios.

```sql
begin;
select * from t1;
-- @session:id=1{
insert into t1 values (100);
select * from t1;
-- @session}
commit;
```

Parameters:
- `id`: Session ID, default 1
- `user`: Username, format `account:user`, defaults to mo.yml configuration
- `password`: Password, defaults to mo.yml configuration

### `-- @wait:{session_id}:{commit|rollback}`
Wait for the specified session to commit or rollback before continuing execution, used for testing transaction isolation.

```sql
begin;
update t1 set a = 1;
-- @session:id=1{
-- @wait:0:commit
update t1 set a = 2;  -- Execute after session 0 commits
-- @session}
commit;
```

## Metadata Comparison Tags

### `--- @metacmp(boolean)` (Document-level)
Control whether to compare result set metadata (column names, types, etc.) for the entire file.

```sql
--- @metacmp(false)
-- All SQL in the file will not compare metadata
```

### `-- @metacmp(boolean)` (SQL-level)
Control whether to compare metadata for a single SQL statement, takes precedence over document-level and global settings.

```sql
-- @metacmp(true)
SELECT * FROM t1;  -- Compare metadata
```

## Test Case Writing Guidelines

1. **Self-contained**: Test files should run independently without depending on other test states
2. **Resource cleanup**: Clean up created databases, tables, and other resources at the end of tests
3. **Reuse databases**: Reuse existing databases when possible to avoid creating too many temporary databases

## Running Tests

```bash
# Run a single test file
cd /root/mo-tester && ./run.sh -n -g -p /root/matrixone/test/distributed/cases/your_test.test

# Generate result file (for new test cases)
cd /root/mo-tester && ./run.sh -m genrs -n -g -p /root/matrixone/test/distributed/cases/your_test.test
```

## Result File Format

### Column Metadata Format

In generated `.result` files, each column's metadata format is: `column_name[type,precision,scale]`

Example:
```
➤ id[4,32,0]  ¦  name[12,255,0]  ¦  price[3,10,2]  𝄀
```

### Column Type Code Reference

mo-tester uses JDBC `java.sql.Types` integer codes to represent column types:

| Type Code | Type Name | Description |
|-----------|-----------|-------------|
| -7 | BIT | Bit type |
| -6 | TINYINT | Tiny integer |
| -5 | BIGINT | Big integer |
| -4 | LONGVARBINARY | Long variable binary |
| -3 | VARBINARY | Variable binary |
| -2 | BINARY | Binary |
| -1 | LONGVARCHAR | Long variable character |
| 0 | NULL | Null type |
| 1 | CHAR | Fixed-length character |
| 2 | NUMERIC | Numeric type |
| 3 | DECIMAL | Decimal number |
| 4 | INTEGER | Integer |
| 5 | SMALLINT | Small integer |
| 6 | FLOAT | Float |
| 7 | REAL | Real number |
| 8 | DOUBLE | Double precision float |
| 12 | VARCHAR | Variable character |
| 16 | BOOLEAN | Boolean |
| 91 | DATE | Date |
| 92 | TIME | Time |
| 93 | TIMESTAMP | Timestamp |
| 2003 | ARRAY | Array |
| 2004 | BLOB | Binary large object |
| 2005 | CLOB | Character large object |

**Common Type Examples:**
- `[4,32,0]` - INTEGER, precision 32, scale 0
- `[12,255,0]` - VARCHAR(255)
- `[3,10,2]` - DECIMAL(10,2)
- `[-5,64,0]` - BIGINT
- `[93,64,0]` - TIMESTAMP

For complete type list, refer to JDBC `java.sql.Types` specification.

## Deprecated Tags (Do Not Use in New Cases)

### `-- @separator:table` ⚠️ Deprecated
This tag is deprecated and has no actual effect. It is only kept for compatibility with existing cases. Do not use in new test cases.

### `-- @pattern` ⚠️ Deprecated
This tag is deprecated. Use `-- @regex` instead.

Migration example:
```sql
-- Old way (deprecated)
-- @pattern
insert into t1 values(1,'bell'),(2,'app'),(1,'com');

-- New way (recommended)
-- @regex("Duplicate entry",true)
insert into t1 values(1,'bell'),(2,'app'),(1,'com');
```
