# 事务概览

MatrixOne 支持将多个 SQL 语句绑定到单个 All-or-Nothing（即，要么都不做，要么做全套） 事务中。每个事务保证 ACID 语义跨越任意表和行。如果一个事务成功，所有的突变都应用在一起，并具有虚拟同时性。如果事务的任何部分失败，则整个事务将中止，数据库将保持不变。MatrixOne 保证当一个事务处于挂起状态时，它通过快照隔离与其他并发事务隔离。

在 0.5.0 版本中，MatrixOne 支持独立数据库事务。MatrixOne 支持乐观事务模式。

本篇文档介绍了常用的事务相关语句、显式和隐式事务、隔离级别、惰性检查约束以及事务大小。

## SQL 语句

以下 SQL 语句，用于控制事务的开启、提交、回滚等操作：

- `START TRANSACTION` 或 `BEGIN` ：开始新的事务。
- `COMMIT`：提交事务，并使事务的更改永久生效。
- `ROLLBACK` ：回滚事务，取消当前的更改。
- `SET autocommit`：禁用或启用当前会话的默认自动提交模式。(在0.5.0版本，MatrixOne 当前仅支持启用自动提交模式，暂不支持关闭此模式)。

### 启动事务

`BEGIN` 和 `START TRANSACTION` 语句均可用于显式启动一个新事务。

语法结构：

```sql
BEGIN;
```

```
START TRANSACTION;
```

使用 `START TRANSACTION` 或 `BEGIN` 语句时，自动提交模式将处于禁用状态，直至通过 `COMMIT` 或 `ROLLBACK` 语句结束事务。事务结束后，自动提交模式将恢复到禁用之前的状态。与 MySQL 不同，MatrixOne 中的 `START TRANSACTION` 语句没有控制事务特征的修饰符。

### 提交事务

`COMMIT` 语句表示事务的提交，事务提交后，MatrixOne 应用当前事务中所做的所有更改。

语法结构：

```sql
COMMIT;
```

### 回滚事务

`ROLLBACK` 语句表示事务的回滚，将清除当前事务的所有更改，回到原点。

语法结构：

```sql
ROLLBACK;
```

如果客户端连接中止或关闭，事务将会自动回滚。

### 自动提交

MatrixOne 运行时，默认启用自动提交模式。这表示，开启自动提交，每条 SOL 语句都会被当做一个单独的事务自动执行，即每执行一条 SQL 语句，事务都会提交一次，在自动模式开启期间，你不能通过 `ROLLBACK` 进行回滚；但是，如果在语句执行过程中发生错误，则会回滚该语句。

示例如下：

```sql
> SELECT @@autocommit;
+--------------+
| @@autocommit |
+--------------+
| on           |
+--------------+
1 row in set (0.01 sec)

> create table test (c int primary key,d int);
Query OK, 0 rows affected (0.03 sec)

> Insert into test values(1,1);
Query OK, 1 row affected (0.04 sec)

> rollback;
ERROR 1105 (HY000): the txn has not been began

> select * from test;
+------+------+
| c    | d    |
+------+------+
|    1 |    1 |
+------+------+
1 row in set (0.01 sec)
```

在上述所举的例子中，`ROLLBACK` 语句没有生效。这是因为开启了自动提交模式，而 `INSERT` 语句是在自动提交模式中执行的。`ROLLBACK` 只适用于 `BEGIN` 或 `START TRANSACTION` ，也就是说，它相当于下面的单语句事务：

```
START TRANSACTION;
Insert into test values(1,1);
COMMIT;
```

如果事务已显式启动，则自动提交模式将关闭。
在以下示例中，`ROLLBACK` 语句成功地还原了 `INSERT` 语句：

```sql
> SELECT @@autocommit;
+--------------+
| @@autocommit |
+--------------+
| on           |
+--------------+
1 row in set (0.01 sec)

> create table test (c int primary key,d int);
Query OK, 0 rows affected (0.03 sec)

> BEGIN;
Query OK, 0 rows affected (0.00 sec)

> Insert into test values(1,1);
Query OK, 1 row affected (0.01 sec)

> ROLLBACK;
Query OK, 0 rows affected (0.03 sec)

> SELECT * from test;
Empty set (0.01 sec)
```

`autocommit` 系统变量目前不支持在全局变量或会话变量基础上进行更改。

### 显式事务和隐式事务

MatrixOne 支持显式事务（即使用 `[BEGIN|START TRANSACTION]` 和 `COMMIT` 来定义事务的开始和结束）和隐式事务(默认)。

如果你通过 `[BEGIN|START TRANSACTION]` 语句启动一个新的事务，事务由默认的隐式事务切换到显式事务，自动提交模式会在 `COMMIT` 或' `ROLLBACK` 之前被禁用。

### 语句回滚

MatrixOne 支持语句在执行失败后进行原子回滚。如果一条语句导致错误，那么这条语句将不会生效。事务将保持打开状态，并且可以在 `COMMIT` 或 `ROLLBACK` 语句之前进行其他的更改。

```sql
> CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);
Query OK, 0 rows affected (0.04 sec)

> INSERT INTO t1 VALUES (1);
Query OK, 1 row affected (0.16 sec)

> BEGIN;
Query OK, 0 rows affected (0.00 sec)

> INSERT INTO t1 VALUES (1);
ERROR 1105 (HY000): tae data: duplicate
> INSERT INTO t1 VALUES (2);
Query OK, 1 row affected (0.03 sec)

> INSERT INTO t1_1 VALUES (3);
ERROR 1105 (HY000): tae catalog: not found
> INSERT INTO t1 VALUES (3);
Query OK, 1 row affected (0.00 sec)

> commit;
Query OK, 0 rows affected (0.03 sec)

> select * from t1;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.02 sec)
```

在上面的例子中，`INSERT` 语句执行失败，事务保持打开状态，并且可以在 `COMMIT` 或 `ROLLBACK` 语句之前进行其他的更改，如例子所示，最后一个 `INSERT` 语句执行成功，并提交了更改。

#### 快照隔离(Snapshot Isolation)级别

事务隔离是数据库事务处理的基础之一。隔离是事务的四个关键属性（ACID，即原子性 *Atomicity*，或称不可分割性、一致性 *Consistency*、隔离性 *Isolation*、持久性 *Durability*）之一。

SQL-92 标准定义了四种级别的事务隔离：读未提交、读已提交、可重复读和可序列化。下表为 SQL-92 标准事务隔离：

| 隔离级别  | 脏写(Dirty Write)  | 脏读(Dirty Read)   | 不可重复读(Fuzzy Read)  | 幻读(Phantom)    |
| :--------------- | :----------- | :----------- | :----------- | :----------- |
| READ UNCOMMITTED | Not Possible | Possible     | Possible     | Possible     |
| READ COMMITTED   | Not Possible | Not possible | Possible     | Possible     |
| REPEATABLE READ  | Not Possible | Not possible | Not possible | Possible     |
| SERIALIZABLE     | Not Possible | Not possible | Not possible | Not possible |

MatrixOne实现了快照隔离(SI，Snapshot Isolation)一致性，该级别的隔离在 SQL-92 标准的 `REPEATABLE READ` 和 `SERIALIZABLE` 之间。

在快照隔离的系统中，每个事务似乎都在数据库的独立、一致的快照上运行。在提交之前，事务的更改仅对该事务可见，此时所有更改对后续开始的任何事务都以原子方式可见。如果事务 T1 修改了一个对象 *x*，这时，另一个事务 T2 在 T1 的快照开始后和在 T1 提交之前，向 *x* 提交了一个写操作，那么 T1 必须中止。

[ANSI SQL隔离级别评论](https://arxiv.org/ftp/cs/papers/0701/0701157.pdf)提出的可能异常的隔离级别，MatrixOne 的隔离级别与[ANSI SQL隔离级别评论](https://arxiv.org/ftp/cs/papers/0701/0701157.pdf)所述略有不同，参见下表所示，可以查看 MatrixOne 的隔离级别：

| Isolation Level                | P0 Dirty Write | P1 Dirty Read | P4C Cursor Lost Update | P4 Lost Update | P2 Fuzzy Read | P3 Phantom   | A5A Read Skew | A5B Write Skew |
| ------------------------------ | -------------- | ------------- | ---------------------- | -------------- | ------------- | ------------ | ------------- | -------------- |
| MatrixOne's Snapshot Isolation | Not Possible   | Not Possible  | Not Possible           | Not Possible   | Not Possible  | Not Possible | Not Possible  | Possible       |

#### 乐观事务模型

MatrixOne 支持乐观事务模型。你在使用乐观并发读取一行时不会锁定该行。当你想要更新一行时，应用程序必须确定其他用户是否在读取该行后对该行进行了。乐观并发事务通常用于数据争用较低的环境中。

在乐观并发模型中，如果你从数据库接收到一个值后，另一个用户在你试图修改该值之前修改了该值，则产生报错。

下面给出乐观并发的示例，将为你展示服务器如何解决并发冲突。

在下午 1:00，用户1 从数据库中读取一行，其值如下:

**CustID LastName FirstName**

101 Smith Bob

| Column name | Original value | Current value | Value in database |
| :---------- | :------------- | :------------ | :---------------- |
| CustID      | 101            | 101           | 101               |
| LastName    | Smith          | Smith         | Smith             |
| FirstName   | Bob            | Bob           | Bob               |

在下午 1:01，User2 从数据库中读取同一行。

在下午 1:03，用户2 将 **FirstName** 行的“Bob”改为“Robert”，并更新到数据库里。

| Column name | Original value | Current value | Value in database |
| :---------- | :------------- | :------------ | :---------------- |
| CustID      | 101            | 101           | 101               |
| LastName    | Smith          | Smith         | Smith             |
| FirstName   | Bob            | Robert        | Bob               |

上表所示，更新成功，因为更新时数据库中的值与用户2 的原始值匹配。

在下午 1:05，用户1 将 **FirstName** 行的“Bob”改为“James”，并尝试进行更新。

| Column name | Original value | Current value | Value in database |
| :---------- | :------------- | :------------ | :---------------- |
| CustID      | 101            | 101           | 101               |
| LastName    | Smith          | Smith         | Smith             |
| FirstName   | Bob            | James         | Robert            |

此时，用户1 遇到了乐观并发冲突，因为数据库中的值“Robert”不再与用户1 期望的原始值“Bob”匹配，并发冲突提示更新失败。下一步需要决定，是采用用户1 的更改覆盖用户2 的更改，还是取消用户1 的更改。
