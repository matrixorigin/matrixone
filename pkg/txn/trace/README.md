## Txn trace framework
During MO iterations, you may encounter various transaction correctness issues, such as Duplicate and W-W Conflict issues.

These problems are often difficult to analyze by relying on logs and error messages, and often take a lot of developer time because debugging transactional correctness problems is quite difficult. Often, these problems can be very difficult to reproduce.

## Design thinking
## Trace what data
Combined with previous experience analyzing transaction correctness issues, we typically need some of the following information:

* Transaction metadata

  The metadata of the transaction, and information about each metadata change. There is no doubt that this information is very important. At least, it includes, the transaction's snapshot ts, commit ts, and so on.

* Transaction Read data
  
  The content and version of all the data read during the execution of the transaction.

* Transaction Commit data

  The content of the data that the transaction commits to TN.

* Logtail data
 
  What is the Logtail received by CN, including the data content, and the order.

* Transaction correctness errors
  
  Known transaction correctness errors, including information about the table and row where the problem was found.

* Conflict Handling
  
  Information about lock conflicts that occur in pessimistic mode and how they are handled. This information determines whether or not the transaction will lost update.

When we have this information, and in chronological order of occurrence, then we have all the information we need to analyze the problem, and theoretically find the problem whenever it occurs.

### Storage and Access
The amount of data in Trace is huge and how it is stored and used determines how difficult it is to analyze the problem.

When analyzing the problem, we need to retrieve the data as well as filter the data based on some conditions and the best way to access it is SQL.

Once we decide to use SQL to access this trace data, it is natural that this data should be stored in MO's own internal tables.

## Design program

### Trace data internal table

#### Features
```sql
create table trace_features (
    name varchar(50) not null primary key, ``sql create table trace_features (
    state varchar(20) not null
);
```

* txn
  
  Tracks transaction metadata change events over the lifetime of the transaction.

  Enable : `select mo_ctl('cn', 'txn-trace', 'enable txn') `
  
  Disable: `select mo_ctl('cn', 'txn-trace', 'disable txn')`


* data

  Trace data change events such as commit, apply logtail, flush, etc.
  
  Enable : `select mo_ctl('cn', 'txn-trace', 'enable data')`
  
  Disable: `select mo_ctl('cn', 'txn-trace', 'disable data')`

* txn-workspace

  Tracks all data changes to the workspace during the lifecycle of the transaction.
  
  Enable:  `select mo_ctl('cn', 'txn-trace', 'enable txn-workspace')`
  
  Disable: `select mo_ctl('cn', 'txn-trace', 'disable txn-workspace')`

* txn-action

  Trace the overhead of all actions in the transaction's lifecycle (time consumed, number of data blocks read, etc.).
  
  Enable:   `select mo_ctl('cn', 'txn-trace', 'enable txn-action') `
  
  Disabled: `select mo_ctl('cn', 'txn-trace', 'disable txn-action')`

* statement
 
  Trace the elapsed time of a statement that satisfies a condition. Used in conjunction with the txn-action feature to track the overhead of each stage of statement execution for performance tuning.
  
  Enable:  `select mo_ctl('cn', 'txn-trace', 'enable statement')`
  
  Disable: `select mo_ctl('cn', 'txn-trace', 'disable statement')`

#### Trace data
```sql
create table trace_event_txn (
    ts 			          bigint       not null,
    txn_id            varchar(50)  not null,
    cn                varchar(100) not null,
    event_type        varchar(50)  not null,
    txn_status			  varchar(10),
    snapshot_ts       varchar(50),
    commit_ts         varchar(50),
    info              varchar(1000)
)

create table trace_event_data (
    ts 			          bigint          not null,
    cn                varchar(100)    not null,
    event_type        varchar(50)     not null,
    entry_type			  varchar(50)     not null,
    table_id 	        bigint UNSIGNED not null,
    txn_id            varchar(50),
    row_data          varchar(500)    not null, 
    committed_ts      varchar(50),
    snapshot_ts       varchar(50)
)

create table trace_event_txn_action (
    ts 			          bigint          not null,
    txn_id            varchar(50)     not null,
    cn                varchar(50)     not null,
    table_id          bigint UNSIGNED,
    action            varchar(100)    not null,
    action_sequence   bigint UNSIGNED not null,
    value             bigint,
    unit              varchar(10),
    err               varchar(100) 
)

create table trace_event_error (
    ts 			          bigint          not null,
    txn_id            varchar(50)     not null,
    error_info        varchar(1000)   not null
)

create table trace_statement (
    ts 			   bigint          not null,
    txn_id     varchar(50)     not null,
    sql        varchar(1000)   not null,
    cost_us    bigint          not null 
)
```

* trace_event_txn

  The transaction events table, which records data about changes to the transaction metadata, as well as information about lock conflict handling for the transaction. The `txn` feature takes effect when it is turned on.

* trace_event_data
 
  The trace_event_data table contains all transaction read data, commit data, and logtail event data. The `data` feature takes effect when it is turned on.

* trace_event_txn_action

  The transaction action table, which records information about all actions during the transaction lifecycle, including the overhead of those actions (execution time, number of data blocks, etc.). The `txn-action` feature takes effect when it is turned on.

* ` trace_event_error

  Event data table for transaction correctness errors. This table is the starting point for querying transaction correctness issues. Find the transactions that went wrong and analyze the data. The `txn` feature takes effect when it is opened.

  Use `select mo_ctl('cn', 'txn-trace', 'decode-complex complex_pk_value')` to decode those with a complex primary key.

* trace_statement

  statement table, the statement that meets the conditions will be recorded, including the content of the sql, time consumed, and transaction

#### filter
```sql
create table trace_table_filters (
    id              bigint UNSIGNED primary key auto_increment,
    table_id			  bigint UNSIGNED not null,
    table_name      varchar(50)     not null,
    columns         varchar(200)
);

create table trace_txn_filters (
    id             bigint UNSIGNED primary key auto_increment,
    method         varchar(50)     not null,
    value          varchar(500)    not null
);

create table trace_statement_filters (
    id             bigint UNSIGNED primary key auto_increment,
    method         varchar(50)     not null,
    value          varchar(500)    not null
);
```

Filter condition table, supports filtering by table, transaction and statement conditions, used to trace the transaction and data to be concerned.


* trace_table_filters

  Table data filtering, one row corresponds to one table, can create multiple records at the same time, used to trace multiple tables. columns field is not set, trace all the fields of this table. `There are multiple rows, data that satisfies any row will be traced; 0 rows, no data will be traced`.

  Add all columns of the table: `select mo_ctl('cn', 'txn-trace', 'add-table table_name')`

  Add table specified columns: `select mo_ctl('cn', 'txn-trace', 'add-table table_name col1,col2')`, multiple cols split by `,`

  Clear: `select mo_ctl('cn', 'txn-trace', 'clear data')`

  Refresh: `select mo_ctl('cn', 'txn-trace', 'refresh data')`


* trace_txn_filters
 
  Transaction filtering, one row represents a constraint on a transaction, multiple constraints can be created at the same time, `when there are multiple rows, all constraints must be satisfied for the transaction to be traced; 0 rows, no trace will be performed on the transaction`.

  Filter transactions that specify a user: `select mo_ctl('cn', 'txn-trace', 'add-txn user user_name')`

  Filter transactions for a specified tenant: `select mo_ctl('cn', 'txn-trace', 'add-txn tenant account_id user_name')`

  Filter transactions that specify a session: `select mo_ctl('cn', 'txn-trace', 'add-txn session session_id')`

  Filter transactions for the specified connection: `select mo_ctl('cn', 'txn-trace', 'add-txn connection session_id connection_id')`

  clear: `select mo_ctl('cn', 'txn-trace', 'clear txn')`

  Refresh: `select mo_ctl('cn', 'txn-trace', 'refresh txn')`

* trace_statement_filters

  Statement filtering, one row of records represents the constraints of a statement, multiple constraints can be created at the same time, `when there are multiple rows of records, all constraints must be satisfied for the statement to be traced; 0 rows of records, no trace will be performed on the statement`.

  Filter statements whose execution time exceeds a threshold: `select mo_ctl('cn', 'txn-trace', 'add-statement cost value[us|ms|s|m|h]')`.

  Filter statements containing keywords: `select mo_ctl('cn', 'txn-trace', 'add-statement contains value)`

  Clear: `select mo_ctl('cn', 'txn-trace', 'clear statement')`

  Refresh: `select mo_ctl('cn', 'txn-trace', 'refresh statement')`

### How trace data is written
Trace data if written in real time then this performance is terrible and almost unusable. So we take an asynchronous write approach.

Our CN nodes come with a local disk, so we use the local disk as a cache and use load csv file to write the trace data.

Using a single threaded approach, write trace data to CSV file using sequential write, write a certain size (default 16M), flush to disk, and load to MO.
