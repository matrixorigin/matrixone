# Distributed Transaction

## What is the transaction

The transaction is a program execution unit in the database that accesses and possibly updates various data items. It usually consists of one or more control statements combined with SQL statements. Usually, transactions need to have four characteristics of ACID:

- **Atomicity**

   The atomicity of a transaction means that a transaction is an indivisible unit, and the operations in a transaction either all or none of them occur.

- **Consistency**

   Transactional consistency means that data must remain correct and obey all data-related constraints before and after the transaction.

- **Isolation**

   The isolation of a transaction is that when multiple users access concurrently, the specified isolation level must be observed between transactions. Within the determined isolation level range, one transaction cannot be interfered with by another transaction.

- **Durability**
   Transaction durability means that when a transaction is committed in the database, its changes to the data are permanent, regardless of whether the database software is restarted.

## Isolation level

Standard database isolation levels include **Read Uncommitted**, **Read Committed**, **Repeatable read**, **Serializable**, etc. In MatrixOne 0.6, the supported isolation level is **Snapshot Isolation**.

Different from other isolation levels, snapshot isolation has the following characteristics:

- Snapshot isolation does not reflect changes made to data by other synchronized transactions for data read within a specified transaction. Specifies that the transaction uses the rows of data read at the beginning of this transaction.
- Data is not locked when read, so snapshot transactions do not prevent other transactions from writing data.
- Transactions that write data also do not prevent snapshot transactions from reading data.

## Transaction Types

In MatrixOne, transactions are divided into explicit and implicit transactions:

- Explicit transactions, which start with `BEGIN/START TRANSACTION` and end with `COMMIT/ROLLBACK`, allow users to clearly and unambiguously determine the start, end, and full content of the transaction.
- Implicit transaction, when the transaction start does not contain `BEGIN/START TRANSACTION`, the enabled transaction becomes an implicit transaction. The end of the transaction is determined according to the value of the system parameter `AUTOCOMMIT`.

### Explicit transaction rules

- An explicit transaction starts and ends with `BEGIN...END` or `START TRANSACTIONS...COMMIT/ROLLBACK`.
- In an explicit transaction, DML and DDL can exist simultaneously, but if the occurrence of DDL will affect the result of DML, such as `drop table` or `alter table`, the DDL will be judged to fail and report an error. Affected statements are committed or rolled back typically.
- In an explicit transaction, other straightforward transactions cannot be nested. For example, if `START TANSACTIONS` is encountered after `START TANSACTIONS`, all statements between two `START TANSACTIONS` will be forced to commit, regardless of the value of `AUTOCOMMIT` 1 or 0.
- In an explicit transaction, only DML and DDL can be included, and no parameter configuration or management commands can be modified, such as `set [parameter] = [value]`, `create user`, etc.

### 隐式事务规则

- 在 `AUTOCOMMIT` 发生变化的时候，之前所有未提交的DML语句都会自动提交。
- 在 `AUTOCOMMIT=1` 的情况下，每一条 DML 语句都是一个单独的事务，在执行后立即提交。
- 在 `AUTOCOMMIT=0` 的情况下，每一条 DML 语句都不会在执行后立即提交，需要手动进行 `COMMIT` 或 `ROLLBACK`，如果在尚未提交或回滚的状态下退出客户端，则默认回滚。
- 在 `AUTOCOMMIT=0` 的情况下，在有未提交的 DML 的情况下，如果出现的 DDL 没有对之前DML的结果产生影响，那么 DDL 将会在提交时生效；如果对之前的 DML 产生影响，例如 `drop table` 或 `alter table`，则报错提示该条语句执行失败，在提交或回滚时，未受影响的语句正常提交或回滚。

### Implicit Transaction Rules

- When `AUTOCOMMIT` changes, all previously uncommitted DML statements are automatically committed.
- With `AUTOCOMMIT=1`, each DML statement is a separate transaction, committed immediately after execution.
- In the case of `AUTOCOMMIT=0`, each DML statement will not be committed immediately after execution, you need to manually perform `COMMIT` or `ROLLBACK`, if the client exits without committing or rolling back, then Rollback by default.
- In the case of `AUTOCOMMIT=0`, in the case of uncommitted DML, if the DDL that appears has no effect on the result of the previous DML, then the DDL will take effect when it is submitted; if it has an effect on the previous DML , such as `drop table` or `alter table`, an error is reported to indicate that the statement failed to execute. When committing or rolling back, the unaffected statement is normally committed or rolled back.

## MVCC

MVCC (Multiversion Concurrency Control) is applied to MatrixOne to ensure transaction snapshot and achieve transaction isolation.

Create a Latch Free linked list based on the pointer field of the data tuple (Tuple, that is, each row in the table), called the version chain. This version chain allows the database to locate the desired version of a Tuple. Therefore, the storage mechanism of these versions of data is an essential consideration in the design of the database storage engine.

One solution is the Append Only mechanism, where all tuple versions of a table are stored in the same storage space. This method is used in Postgre SQL. To update an existing Tuple, the database first fetches an empty slot from the table for the new version; then, it copies the current version's contents to the latest version. Finally, it applies the modifications to the Tuple in the newly allocated Slot. The critical decision of the Append Only mechanism is how to order the version chain of Tuple. Since it is impossible to maintain a lock-free doubly linked list, the version chain only points in one direction, either from Old to New (O2N) or New to Old (N2O).

Another similar scheme is called Time Travel, which stores the information of the version chain separately, while the main table maintains the main version data.

The third option is to maintain the main version of the tuple in the main table, and maintain a series of delta versions in a separate database comparison tool (delta) store. This storage is called a rollback segment in MySQL and Oracle. To update an existing tuple, the database fetches a contiguous space from the delta store to create a new delta version. This delta version contains the original value of the modified property, not the entire tuple. Then the database directly updates the main version in the main table (In Place Update).

## MatrixOne transaction flow

MatrixOne's CN (Coordinator, node) and DN (Data Node, data node) are participants in important nodes, a complete transaction process.

1. The transaction CN accepts the transaction initiation request and generates the transaction TID (Transaction ID, ID).
2. CN determines which DN needs to synchronize log collection (Logtail) and then directly synchronizes Logtail from a specific DN.
3. According to the CN's post-tracking space log collection (log), the CN receives the Logtail application workspace (Workspace).
4. The CN pushes the commit or rollback request of the transaction to the DN, and the DN decides the transaction.
5. DN controls the MVCC mechanism and persists transaction logs to the server (Log service).
