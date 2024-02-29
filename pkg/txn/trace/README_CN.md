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

1. trace_features 表
```sql
create table trace_features (
    name    varchar(50) not null primary key,
    state   varchar(20) not null
);

insert into trace_features (name, state) values ('txn', 'disable');
```

trace特性表，这个表中存储了trace特性的状态，默认都是关闭trace

2. trace_event_txn 表
```sql
create table trace_event_txn (
    ts 			          bigint       not null,
    txn_id                varchar(50)  not null,
    cn                    varchar(100) not null,
    event_type            varchar(50)  not null,
    txn_status			  varchar(10),
    snapshot_ts           varchar(50),
    commit_ts             varchar(50),
    check_changed		  varchar(100)
)
```

事务事件表，记录了事务元数据的变更数据，以及事务的锁冲突处理的信息。

3. trace_event_data
```sql
create table trace_event_data (
    ts 			          bigint          not null,
    cn                    varchar(100)    not null,
    event_type            varchar(50)     not null,
    entry_type			  varchar(50)     not null,
    table_id 	          bigint UNSIGNED not null,
    txn_id                varchar(50),
    row_data              varchar(500)    not null, 
    committed_ts          varchar(50),
    snapshot_ts           varchar(50)
)
```

数据读写事件表，所有事务读取的数据，commit的数据以及Logtail的事件数据。

4. trace_tables
```sql
create table trace_tables (
    id                    bigint UNSIGNED primary key auto_increment,
    table_id			  bigint UNSIGNED not null,
    table_name            varchar(50)     not null,
    columns               varchar(200)
)
```

Trace目标Table，这个表记录了trace的目标表的数据，来减少trace数据量。

5. trace_event_error
```sql
create table trace_event_error (
    ts 			          bigint          not null,
    txn_id                varchar(50)     not null,
    error_info            varchar(1000)   not null
)
```

事务正确性错误的事件数据表。这个表是我们查询事务正确性问题的起点。找到出问题的事务以及数据分析。


### Trace数据如何写入
trace数据如果实时写入，那么这个性能是糟糕的，几乎不可用。所以我们采取异步写入的方式。

我们的CN节点都会带有一个本地磁盘，所以使用本地磁盘作为cache，使用load csv文件的方式，写入trace数据。

使用单线程的方式，采用顺序写的方式写入trace数据到CSV文件，写满一定大小（默认16M），flush到磁盘，并且load到MO。

### 动态关闭和开启
结合`mo_ctl`函数，来提供动态开启，关闭以及修改Trace目标。具体命令如下：
```sql
# 开启Trace
select mo_ctl('cn', 'txn-trace', 'enable')

# 关闭Trace
select mo_ctl('cn', 'txn-trace', 'disable')

# 创建Trace对象
select mo_ctl('cn', 'txn-trace', 'add table_name [columns...]')

# 清除Trace对象
select mo_ctl('cn', 'txn-trace', 'clear')
```