# 分布式事务

## 什么是事务

事务是数据库中访问并可能更新数据库中各种数据项的一个程序执行单元，通常由一条或多条控制语句与 SQL 语句组合构成。通常事务需要具备ACID四个特征：

- **原子性（Atomicity）**

   事务的原子性是指事务是一个不可分割的单位，在一个事务中的操作要么都发生，要么都不发生。

- **一致性（Consistency）**

   事务的一致性是指在事务前后，数据必须是保持正确并遵守所有数据相关的约束。

- **隔离性（Isolation）**

   事务的隔离性是在多个用户并发访问时，事务之间要遵守规定好的隔离级别，在确定的隔离级别范围内，一个事务不能被另一个事务所干扰。

- **持久性（Durability）**

   事务的持久性是指，在数据库中一个事务被提交时，它对数据库中的数据的改变是永久性的，无论数据库软件是否重启。

## 隔离级别

在常见的数据库隔离级别包含 **读未提交（Read Uncommitted）**、**读已提交（Read Committed）**、**可重复读（Repeatable read）**、**串行化（Serializable）** 等几种，在 MatrixOne 0.6 中，支持的隔离级别是 **快照隔离（Snapshot Isolation）**。

与其他隔离级别有所区别的是，快照隔离具备如下特性：

- 快照隔离对于指定事务内读取的数据不会反映其他同步的事务对数据所做的更改。指定事务使用本次事务开始时读取的数据行。
- 读取数据时不会对数据进行锁定，因此快照事务不会阻止其他事务写入数据。
- 写入数据的事务也不会阻止快照事务读取数据。

## 事物的类型

在 MatrixOne 中，事务分为显式事务与隐式事务：

- 显式事务，以 `BEGIN/START TRANSACTION` 开头，以 `COMMIT/ROLLBACK` 结束，用户能够清晰明确地判断事务的起止与全部内容。
- 隐式事务，事务起始不包含以 `BEGIN/START TRANSACTION` 时，开启的事务即成为隐式事务。根据系统参数 `AUTOCOMMIT` 的值来判定判定事物的结束。

### 显式事务规则

- 显式事务是指以 `BEGIN...END` 或 `START TANSACTIONS...COMMIT/ROLLBACK` 作为起始结束。
- 在显式事务中，DML 与 DDL 可以同时存在，但是如果的出现 DDL 会影响到 DML 的结果，例如 `drop table` 或者 `alter table`，该 DDL 会被判定失败并报错，事务中未受影响的语句正常提交或回滚。
- 显式事务中，无法嵌套其他显式事务，例如 `START TANSACTIONS` 之后再遇到 `START TANSACTIONS`，两个 `START TANSACTIONS` 之间的所有语句都会强制提交，无论 `AUTOCOMMIT` 的值是 1 或 0。
- 显式事务中，只能包含 DML 与 DDL，不能带有修改参数配置或管理命令，如 `set [parameter] = [value]`， `create user` 等等。
- 显式事务中，如果某一条语句发生错误，为确保事务的原子性，单条语句的错误会强制让整个事务回滚。

### 隐式事务规则

- 在 `AUTOCOMMIT` 发生变化的时候，之前所有未提交的DML语句都会自动提交。
- 在 `AUTOCOMMIT=1` 的情况下，每一条 DML 语句都是一个单独的事务，在执行后立即提交。
- 在 `AUTOCOMMIT=0` 的情况下，每一条 DML 语句都不会在执行后立即提交，需要手动进行 `COMMIT` 或 `ROLLBACK`，如果在尚未提交或回滚的状态下退出客户端，则默认回滚。
- 在 `AUTOCOMMIT=0` 的情况下，在有未提交的 DML 的情况下，如果出现的 DDL 没有对之前DML的结果产生影响，那么 DDL 将会在提交时生效；如果对之前的 DML 产生影响，例如 `drop table` 或 `alter table`，则报错提示该条语句执行失败，在提交或回滚时，未受影响的语句正常提交或回滚。

## MVCC

MVCC（Multiversion Concurrency Control，多版本并发控制）应用于 MatrixOne，以保证事务快照隔离，实现事务的隔离性。

基于数据元组（Tuple，即表中的每行）的指针字段来创建一个 Latch Free 的链表，称为版本链。这个版本链允许数据库定位一个 Tuple 的所需版本。因此这些版本数据的存放机制是数据库存储引擎设计的一个重要考量。

一个方案是采用 Append Only 机制，一个表的所有 Tuple 版本都存储在同一个存储空间。这种方法被用于 Postgre SQL，为了更新一个现有的 Tuple，数据库首先为新的版本从表中获取一个空的槽（Slot），然后，它将当前版本的内容复制到新版本。最后，它在新分配的 Slot 中应用对 Tuple 的修改。Append Only 机制的关键决定是如何为 Tuple 的版本链排序，由于不可能维持一个无锁（Lock free）的双向链表，因此版本链只指向一个方向，或者从 Old 到 New（O2N），或者从 New 到 Old（N2O）。

另外一个类似的方案称为时间旅行（Time Travel），它会把版本链的信息单独存放，而主表维护主版本数据。

第三种方案，是在主表中维护 Tuple 的主版本，在一个单独的数据库对比工具（Delta）存储中维护一系列 Delta 版本。这种存储在 MySQL 和 Oracle 中被称为回滚段。为了更新一个现有的 Tuple，数据库从 Delta 存储中获取一个连续的空间来创建一个新的 Delta 版本。这个 Delta 版本包含修改过的属性的原始值，而不是整个 Tuple。然后数据库直接对主表中的主版本进行原地更新（In Place Update）。

![image-20221026152318567](https://github.com/matrixorigin/artwork/blob/main/docs/distributed-transaction/mvcc.jpg)

## MatrixOne 事务的流程

在 MatrixOne 中，CN（Coordinator Node，协调节点）与 DN（Data Node，数据节点）是重要的参与者，一个完整的事务流程如下：

1. 由某个 CN 接受事务的发起请求，生成事务的 TxID（Transaction ID，事务 ID）。
2. CN 根据查询判断需要从哪个 DN 同步日志采集（Logtail)，然后直接从特定的 DN 同步 Logtail。
3. DN 根据 CN 给的时间戳，返回日志采集（Logtail)，CN 收到后将 Logtail 应用到工作空间（Workspace）。
4. CN 将事务的提交或回滚请求推给 DN，由 DN 来进行事务裁决。
5. DN 做 MVCC 机制的版本控制，并将事务的日志持久化到日志服务器（Logservice)。
