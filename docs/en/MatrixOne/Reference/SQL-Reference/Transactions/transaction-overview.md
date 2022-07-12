## Transactions

MatrixOne supports bundling multiple SQL statements into a single all-or-nothing transaction. Each transaction guarantees ACID semantics spanning arbitrary tables and rows. If a transaction succeeds, all mutations are applied together with virtual simultaneity. If any part of a transaction fails, the entire transaction is aborted, and the database is left unchanged. MatrixOne guarantees that while a transaction is pending, it is isolated from other concurrent transactions with serializable isolation.

In 0.5.0 version, MatrixOne supports standalone database transaction. MatrixOne supports optimistic transaction mode.

This document introduces commonly used transaction-related statements, explicit and implicit transactions, isolation levels, lazy check for constraints, and transaction sizes.

### SQL statements

The following SQL statements control transactions：

- `START TRANSACTION` or `BEGIN` start a new transaction.
- `COMMIT` commits the current transaction, making its changes permanent.
- `ROLLBACK` rolls back the current transaction, canceling its changes.
- `SET autocommit` disables or enables the default autocommit mode for the current session. (It's not fully implemented in 0.5.0 version, MatrixOne only supports autocommit mode enabled, it cannot be switched off yet). 

#### Starting a transaction

The statements `BEGIN` and `START TRANSACTION` can be used interchangeably to explicitly start a new transaction.

Syntax:

```sql
BEGIN;
```

```
START TRANSACTION;
```

With `START TRANSACTION` or `BEGIN` , autocommit remains disabled until you end the transaction with `COMMIT` or `ROLLBACK`. The autocommit mode then reverts to its previous state. Unlike MySQL, `START TRANSACTION` in MatrixOne doesn't have a modifier that control transaction characteristics.

#### Committing a transaction

The statement `COMMIT` instructs MatrixOne to apply all changes made in the current transaction.

Syntax:

```sql
COMMIT;
```

#### Rolling back a transaction

The statement `ROLLBACK`rolls back and cancels all changes in the current transaction.

Syntax:

```sql
ROLLBACK;
```

Transactions are also automatically rolled back if the client connection is aborted or closed.

#### Autocommit

By default, MatrixOne runs with autocommit mode enabled. This means that, when not otherwise inside a transaction, each statement is atomic, as if it were surrounded by `START TRANSACTION` and `COMMIT`. You cannot use `ROLLBACK` to undo the effect; however, if an error occurs during statement execution, the statement is rolled back.

For example:

```sql
mysql> SELECT @@autocommit;
+--------------+
| @@autocommit |
+--------------+
| on           |
+--------------+
1 row in set (0.01 sec)

mysql> create table test (c int primary key,d int);
Query OK, 0 rows affected (0.03 sec)

mysql> Insert into test values(1,1);
Query OK, 1 row affected (0.04 sec)

mysql> rollback;
ERROR 1105 (HY000): the txn has not been began

mysql> select * from test;
+------+------+
| c    | d    |
+------+------+
|    1 |    1 |
+------+------+
1 row in set (0.01 sec)
```

In the above example, the `ROLLBACK` statement has no effect. This is because the `INSERT` statement is executed in autocommit. `ROLLBACK` only works with a `BEGIN` or `START TRANSACTION`. That is, it was the equivalent of the following single-statement transaction:

```
START TRANSACTION; 
Insert into test values(1,1);
COMMIT;
```

Autocommit will not apply if a transaction has been explicitly started. In the following example, the `ROLLBACK` statement successfully reverts the `INSERT` statement:

```
mysql> SELECT @@autocommit;
+--------------+
| @@autocommit |
+--------------+
| on           |
+--------------+
1 row in set (0.01 sec)

mysql> create table test (c int primary key,d int);
Query OK, 0 rows affected (0.03 sec)

mysql> BEGIN;
Query OK, 0 rows affected (0.00 sec)

mysql> Insert into test values(1,1);
Query OK, 1 row affected (0.01 sec)

mysql> ROLLBACK;
Query OK, 0 rows affected (0.03 sec)

mysql> SELECT * from test;
Empty set (0.01 sec)
```

The `autocommit` system variable cannot be changed on either a global nor session basis for now. 

#### Explicit and implicit transaction

MatrixOne supports explicit transactions (use `[BEGIN|START TRANSACTION]` and `COMMIT` to define the start and end of the transaction) and implicit transactions (by default).

If you start a new transaction through the `[BEGIN|START TRANSACTION]` statement, the autocommit is disabled before `COMMIT` or `ROLLBACK` which makes the transaction becomes explicit.

#### Statement rollback

MatrixOne supports atomic rollback after statement execution failure. If a statement results in an error, the changes it made will not take effect. The transaction will remain open, and additional changes can be made before issuing a `COMMIT` or `ROLLBACK` statement.

```
mysql> CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY);
Query OK, 0 rows affected (0.04 sec)

mysql> INSERT INTO t1 VALUES (1);
Query OK, 1 row affected (0.16 sec)

mysql> BEGIN;
Query OK, 0 rows affected (0.00 sec)

mysql> INSERT INTO t1 VALUES (1);
ERROR 1105 (HY000): tae data: duplicate
mysql> INSERT INTO t1 VALUES (2);
Query OK, 1 row affected (0.03 sec)

mysql> INSERT INTO t1_1 VALUES (3);
ERROR 1105 (HY000): tae catalog: not found
mysql> INSERT INTO t1 VALUES (3);
Query OK, 1 row affected (0.00 sec)

mysql> commit;
Query OK, 0 rows affected (0.03 sec)

mysql> select * from t1;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.02 sec)
```

In the above example, the transaction remains open after the failed `INSERT` statements. The final insert statement is then successful and changes are committed.
