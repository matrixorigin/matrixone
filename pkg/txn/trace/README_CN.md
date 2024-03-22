## Txn trace framework
在MO的版本迭代过程中，可能会遇到各种事务正确性问题，比如 Duplicate 和 W-W Conflict 问题。

这些问题通常依靠日志和错误信息去分析很困难，通常会花费开发人员非常多的时间，因为Debug事务正确性问题是相当困难的。很多时候，这些问题可能非常难以复现。

事务trace框架就是为了是为了快速定位事务正确性问题而设计的。


## 设计思考
### Trace哪些数据
结合之前分析事务正确性问题的经验，我们通常需要如下的一些信息：

* 事务元数据

  事务的元数据，以及每次元数据变更的信息。毫无疑问，这个信息是非常重要的。至少包括了，事务的snapshot ts，commit ts等

* 事务Read的数据
  
  事务在执行过程中，所有读到数据的内容和版本

* 事务Commit的数据

  事务Commit到TN的数据内容

* Logtail数据
 
  CN收到的Logtail是什么，包括数据内容，以及顺序。

* 事务正确性错误
  
  已知的事务正确性的错误，内部包含发现问题的table以及row的信息。

* 冲突处理
  
  在悲观模式下，会发生锁冲突，冲突的处理的信息。这些信息决定了事务会不会lost update.

当我们具备了这些信息，并且按照发生的时间顺序排列，那么我们就具备了我们分析问题的所有信息，理论上只要发生了问题，我们就可以找到问题所在。

### 存储和访问
Trace的数据量是很大的，如何存储和使用这些数据决定了分析问题的难度。

分析问题的时候，我们需要检索数据，以及根据一些条件来过滤数据，最好的访问方式就是SQL。

一旦我们决定使用SQL来访问这些trace数据，那么这些数据自然而然的就应该存储在MO自己的内部表中。

## 设计方案

### Trace数据内部表

#### Features
```sql
create table trace_features (
    name    varchar(50) not null primary key,
    state   varchar(20) not null
);
```

* txn
  
  跟踪事务的生命周期中，事务元数据的变化事件。

  开启：`select mo_ctl('cn', 'txn-trace', 'enable txn')`
  
  关闭：`select mo_ctl('cn', 'txn-trace', 'disable txn')`


* data

  跟踪数据的变化事件，比如commit，apply logtail，flush等。
  
  开启：`select mo_ctl('cn', 'txn-trace', 'enable data')`
  
  关闭：`select mo_ctl('cn', 'txn-trace', 'disable data')`

* txn-action

  跟踪事务的生命周期中，所有动作的开销（耗时，读的数据块个数等等）。
  
  开启：`select mo_ctl('cn', 'txn-trace', 'enable txn-action')`
  
  关闭：`select mo_ctl('cn', 'txn-trace', 'disable txn-action')`

* txn-workspace

  跟踪事务的生命周期中，workspace的所有数据变更。
  
  开启：`select mo_ctl('cn', 'txn-trace', 'enable txn-workspace')`
  
  关闭：`select mo_ctl('cn', 'txn-trace', 'disable txn-workspace')`

* statement
 
  跟踪满足条件的statement的耗时。用于和txn-action特性配合，跟踪statement执行的每个阶段的开销，以便于性能调优。
  
  开启：`select mo_ctl('cn', 'txn-trace', 'enable statement')`
  
  关闭：`select mo_ctl('cn', 'txn-trace', 'disable statement')`


#### Trace数据
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

  事务事件表，记录了事务元数据的变更数据，以及事务的锁冲突处理的信息。`txn` feature 打开的时候生效。

* trace_event_data
 
  数据读写事件表，所有事务读取的数据，commit的数据以及Logtail的事件数据。`data` feature 打开的时候生效。

* trace_event_txn_action

  事务动作表，记录了事务生命周期内，所有动作的信息，包含了这些动作的开销（执行耗时，数据块个数等等）。`txn-action` feature 打开的时候生效。

* trace_event_error

  事务正确性错误的事件数据表。这个表是我们查询事务正确性问题的起点。找到出问题的事务以及数据分析。`txn` feature 打开的时候生效。

  遇到有复合主键的，使用`select mo_ctl('cn', 'txn-trace', 'decode-complex complex_pk_value')`来解码。

* trace_statement

  statement表，满足条件的statement会记录，包括sql内容，耗时，以及事务

#### Filters
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

过滤条件表，支持按照表，事务以及statement的条件过滤，用来trace需要关注的事务和数据。

* trace_table_filters
 
  表数据过滤，一行记录对应一张表，可以同时创建多条记录，用来trace多个表。columns字段不设置，trace这个表的所有的字段。`存在多行记录，满足任意一行的数据都会被trace；0行记录，不会对数据进行trace`。

  增加表所有列: `select mo_ctl('cn', 'txn-trace', 'add-table table_name')`

  增加表指定列: `select mo_ctl('cn', 'txn-trace', 'add-table table_name col1,col2')`，多个col用`,`分割

  清理：`select mo_ctl('cn', 'txn-trace', 'clear data')`

  刷新：`select mo_ctl('cn', 'txn-trace', 'refresh data')`


* trace_txn_filters
 
  事务过滤，一行记录代表一个对于事务的约束，可以同时创建多个约束，`当存在多行记录的时候，必须所有的约束都满足，事务才会被trace；0行记录，不会对事务进行trace`。

  过滤指定用户的事务：`select mo_ctl('cn', 'txn-trace', 'add-txn user user_name')`

  过滤指定租户的事务：`select mo_ctl('cn', 'txn-trace', 'add-txn tenant account_id user_name')`

  过滤指定session的事务：`select mo_ctl('cn', 'txn-trace', 'add-txn session session_id')`

  过滤指定connection的事务：`select mo_ctl('cn', 'txn-trace', 'add-txn connection session_id connection_id')`

  清理：`select mo_ctl('cn', 'txn-trace', 'clear txn')`

  刷新：`select mo_ctl('cn', 'txn-trace', 'refresh txn')`

* trace_statement_filters

  statement过滤，一行记录代表一个statement的约束，可以同时创建多个约束，`当存在多行记录的时候，必须所有的约束都满足，statement才会被trace；0行记录，不会对statement进行trace`。

  过滤执行时间超过阈值的statement：`select mo_ctl('cn', 'txn-trace', 'add-statement cost value[us|ms|s|m|h]')`

  过滤包含关键字的statement：`select mo_ctl('cn', 'txn-trace', 'add-statement contains value)`

  清理：`select mo_ctl('cn', 'txn-trace', 'clear statement')`

  刷新：`select mo_ctl('cn', 'txn-trace', 'refresh statement')`


### Trace数据如何写入
trace数据如果实时写入，那么这个性能是糟糕的，几乎不可用。所以我们采取异步写入的方式。

我们的CN节点都会带有一个本地磁盘，所以使用本地磁盘作为cache，使用load csv文件的方式，写入trace数据。

使用单线程的方式，采用顺序写的方式写入trace数据到CSV文件，写满一定大小（默认16M），flush到磁盘，并且load到MO。
