## Txn trace framework
During MO iterations, you may encounter various transaction correctness issues, such as Duplicate and W-W Conflict issues.

These problems are often difficult to analyze by relying on logs and error messages, and often take a lot of developer time because debugging transactional correctness problems is quite difficult. Often, these problems can be very difficult to reproduce.

The transactional trace framework is designed to quickly pinpoint transactional correctness problems.


## Design thinking
## What data to trace
Based on our previous experience in analyzing transaction correctness problems, we usually need the following information:

* Transaction metadata

  Transaction metadata, and information about each change to the metadata. There is no doubt that this information is very important. At least, it includes, the transaction's snapshot ts, commit ts, and so on.

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

When we have this information, and it is in chronological order of occurrence, then we have all the information we need to analyze the problem, and theoretically find the problem whenever it occurs.

### Storage and access
The amount of data in Trace is large, and how it is stored and used determines how difficult it is to analyze the problem.

When analyzing a problem, we need to retrieve the data as well as filter it based on some conditions, and the best way to access it is SQL.

Once we decide to use SQL to access this trace data, it is natural that this data should be stored in MO's own internal tables.

## Design Options

### Trace data internal tables

1. trace_features table
```sql
create table trace_features (
    name varchar(50) not null primary key, ``sql create table trace_features (
    state varchar(20) not null
);

insert into trace_features (name, state) values ('txn', 'disable');
```

trace_features table, this table stores the state of the trace features, the default is to disable trace.

2. trace_event_txn table
```sql
create table trace_event_txn (
    ts bigint not null,
    txn_id varchar(50) not null,
    cn varchar(100) not null, event_type varchar(50) not null, trace_event_txn
    event_type varchar(50) not null, txn_status varchar(100) not null, trace_event_txn (
    txn_status varchar(10), snapshot_ts varchar(10), txn_status
    snapshot_ts varchar(50), commit_ts varchar(50), not null
    
    check_changed varchar(100)
)
```

The transaction events table, which records data about changes to the transaction metadata, as well as information about the transaction's lock conflict handling.

3. trace_event_data
```sql
create table trace_event_data (
    ts bigint not null, cn varchar(100) not null, trace_event_data
    event_type varchar(50) not null, ``sql create table trace_event_data (
    event_type varchar(50) not null, entry_type varchar(50) not null, trace_event_data
    event_type varchar(50) not null, entry_type varchar(50) not null, table_id bigint UNSIGN
    table_id bigint UNSIGNED not null, txn_id varchar(50) not null, entry_type
    txn_id varchar(50), row_data varchar(50), not null, table_id
    row_data varchar(500) not null, committed_ts varchar(50) 
    committed_ts varchar(50), snapshot_ts varchar(50), not null, table_id bigint UNSIGNED
    snapshot_ts varchar(50)
)
```

Data read/write/apply event table, data read by all transactions, data committed and event data from Logtail.

4. trace_tables
```sql
create table trace_tables (
    id bigint UNSIGNED primary key auto_increment,
    table_id bigint UNSIGNED not null, table_name varchar(5050)
    table_name varchar(50) not null, columns varchar(200) not null, ``sql create table trace_tables (
    columns varchar(200)
)
```

Trace target Table, this table records the data of the trace's target table to reduce the amount of trace data.

5. trace_event_error
```sql
create table trace_event_error (
    ts bigint not null,
    txn_id varchar(50) not null,
    error_info varchar(1000) not null
)
```

Event data table for transaction correctness errors. This table is the starting point for our query on transaction correctness issues. Find the transaction that went wrong and analyze the data.


### How trace data is written
Trace data if written in real time then this performance is terrible and almost unusable. So we take an asynchronous write approach.

Our CN nodes come with a local disk, so we use the local disk as cache and use load csv file to write trace data.

Use single thread to write trace data to CSV file in sequential way, write to a certain size (default 16M), flush to disk, and load to MO.

### Dynamically enable and disable
Combine with `mo_ctl` function to provide dynamic enable, disable and modify the trace target. The specific commands are as follows:
```sql
# Enable Trace on
select mo_ctl('cn', 'txn-trace', 'enable')

# Disable Trace
select mo_ctl('cn', 'txn-trace', 'disable')

# Create the trace target
select mo_ctl('cn', 'txn-trace', 'add table_name [columns...]')

# Clear the trace target
select mo_ctl('cn', 'txn-trace', 'clear')
```