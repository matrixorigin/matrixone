# **Parser**

## Golang and MO start

### 1. Make sure your golang environment

To build MatrixOne locally, golang 1.19 is required. You can follow these steps to make sure your golang whether is ready.

```
$ go version
```

If the result is like `'go version go1.19 xxxxxx'` , you can skip step 2 and build your MatrixOne. If lower than go 1.19 or no golang, your can follow step 2 to configure your own golang 1.19 environment locally.

### 2. Configure your golang environment

If your OS is Mac OS, download the installation package directly with this url:<https://go.dev/dl>
After installation, use step 1 to make sure again.

If your OS is Linux, still download golang package and modify your profile. Here is a sampe as following. The path can be modified as your preferred.

```
$ wget https://go.dev/dl/go1.19.1.linux-amd64.tar.gz
$ sudo tar -zxvf go1.18.3.linux-amd64.tar.gz -C /usr/local
$ cat >> /etc/profile << EOF
export GOROOT=/usr/local/go  
export PATH=$GOROOT/bin:$PATH
export GOPATH=/home/go
export GOPROXY=https://goproxy.cn,direct    
EOF
$ source /etc/profile
```

After these, your can check your golang version with go version.

### 3. Build and run your own MatrixOne

You can get the code from <https://github.com/matrixorigin/matrixone>. 
After download with git clone command, then run the these commands to run MatrixOne instance in the diretory of matrixone. 

```
$ make config
$ make build
$ ./mo-service -cfg etc/cn-standalone-test.toml
```

While notified `"Server Listening on : 0.0.0.0:6001"` , it means your MatirxOne is started.

### 4. Access MatrixOne database

To access MatrixOne, you need a MySQL client with this url: <https://dev.mysql.com/downloads/mysql>
After the MySQL client installation, you can use this command to access your MatrixOne locally in another CLI session.

```
$ mysql -P6001 -h127.0.0.1 -udump -p111
```

When you are in mysql command line, you can start your MatrixOne experience. More details can be found in <http://doc.matrixorigin.cn>

## Features Tasks

### 1. INSERT ... ON DUPLICATE KEY UPDATE Statement

If you specify an `ON DUPLICATE KEY UPDATE` clause and a row to be inserted would cause a duplicate value in a `UNIQUE` index or `PRIMARY KEY` , an `UPDATE` of the old row occurs. 

```
INSERT
    [INTO] tbl_name
    [(col_name [, col_name] ...)]
    { {VALUES | VALUE} (value_list) [, (value_list)] ... }
    [AS row_alias[(col_alias [, col_alias] ...)]]
    [ON DUPLICATE KEY UPDATE assignment_list]
    
assignment:
    col_name = 
          value
        | [row_alias.]col_name
        | [tbl_name.]col_name
        | [row_alias.]col_alias

assignment_list:
    assignment [, assignment] ...

```

For example, if column a is declared as UNIQUE and contains the value 1, the following two statements have similar effect:

```
INSERT INTO t1 (a,b,c) VALUES (1,2,3)
  ON DUPLICATE KEY UPDATE c=c+1;

UPDATE t1 SET c=c+1 WHERE a=1;
```

If column b is also unique, the INSERT is equivalent to this UPDATE statement instead:

```
UPDATE t1 SET c=c+1 WHERE a=1 OR b=2 LIMIT 1;
```

In assignment value expressions in the `ON DUPLICATE KEY UPDATE` clause, you can use the `VALUES(col_name)` function to refer to column values from the INSERT portion of the `INSERT ... ON DUPLICATE KEY UPDATE` statement. In other words, `VALUES(col_name)` in the `ON DUPLICATE KEY UPDATE` clause refers to the value of col_name that would be inserted, had no duplicate-key conflict occurred. This function is especially useful in multiple-row inserts. The `VALUES()` function is meaningful only in the `ON DUPLICATE KEY UPDATE` clause or `INSERT` statements and returns NULL otherwise. Example:

```
INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6)
  ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);
```

That statement is identical to the following two statements:

```
INSERT INTO t1 (a,b,c) VALUES (1,2,3)
  ON DUPLICATE KEY UPDATE c=3;
INSERT INTO t1 (a,b,c) VALUES (4,5,6)
  ON DUPLICATE KEY UPDATE c=9;
```

If the result of `INSERT ... ON DUPLICATE KEY UPDATE` is incorrect such as the following example, there should be notified error messages to users and this statement failed.

```
INSERT INTO t1 (a,b,c) VALUES (1,2,3)
  ON DUPLICATE KEY UPDATE c='a';
```

### 2. REPLACE Statement

`REPLACE` works exactly like `INSERT` , except that if an old row in the table has the same value as a new row for a `PRIMARY KEY` or a `UNIQUE` index, the old row is deleted before the new row is inserted.

```
REPLACE 
    INTO tbl_name
    [(col_name [, col_name] ...)]
    { {VALUES | VALUE} (value_list) [, (value_list)] ...
      |
      VALUES row_constructor_list
    }
```

Example:

```
CREATE TABLE test (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  data VARCHAR(64) DEFAULT NULL,
  ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);
```

When we create this table and run the statements shown in the mysql client, the result is as follows:

```
mysql> REPLACE INTO test VALUES (1, 'Old', '2014-08-20 18:47:00');
Query OK, 1 row affected (0.04 sec)

mysql> REPLACE INTO test VALUES (1, 'New', '2014-08-20 18:47:42');
Query OK, 2 rows affected (0.04 sec)

mysql> SELECT * FROM test;
+----+------+---------------------+
| id | data | ts                  |
+----+------+---------------------+
|  1 | New  | 2014-08-20 18:47:42 |
+----+------+---------------------+
1 row in set (0.00 sec)
```

Now we create a second table almost identical to the first, except that the primary key now covers 2 columns, as shown here (emphasized text):

```
CREATE TABLE test2 (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  data VARCHAR(64) DEFAULT NULL,
  ts TIMESTAMP NOT NULL,
  PRIMARY KEY (id, ts)
);
```

When we run on test2 the same two `REPLACE` statements as we did on the original test table, we obtain a different result:

```
mysql> REPLACE INTO test2 VALUES (1, 'Old', '2014-08-20 18:47:00');
Query OK, 1 row affected (0.05 sec)

mysql> REPLACE INTO test2 VALUES (1, 'New', '2014-08-20 18:47:42');
Query OK, 1 row affected (0.06 sec)

mysql> SELECT * FROM test2;
+----+------+---------------------+
| id | data | ts                  |
+----+------+---------------------+
|  1 | Old  | 2014-08-20 18:47:00 |
|  1 | New  | 2014-08-20 18:47:42 |
+----+------+---------------------+
2 rows in set (0.00 sec)
```

### 3. VALUES STATEMENT

`VALUES` is a DML statement which returns a set of one or more rows as a table. In other words, it is a table value constructor which also functions as a standalone SQL statement.

```
VALUES row_constructor_list [ORDER BY column_designator] [LIMIT number]

row_constructor_list:
    ROW(value_list)[, ROW(value_list)][, ...]

value_list:
    value[, value][, ...]

column_designator:
    column_index
```

The `VALUES` statement consists of the `VALUES` keyword followed by a list of one or more row constructors, separated by commas. A row constructor consists of the `ROW()` row constructor clause with a value list of one or more scalar values enclosed in the parentheses. A value can be a literal of any MatirxOne data type or an expression that resolves to a scalar value.
`ROW()` cannot be empty (but each of the supplied scalar values can be NULL). Each `ROW()` in the same `VALUES` statement must have the same number of values in its value list.
The `DEFAULT` keyword is not supported by `VALUES` and causes a syntax error, except when it is used to supply values in an INSERT statement.
The output of `VALUES` is a table:

```
mysql> VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8);
+----------+----------+----------+
| column_0 | column_1 | column_2 |
+----------+----------+----------+
|        1 |       -2 |        3 |
|        5 |        7 |        9 |
|        4 |        6 |        8 |
+----------+----------+----------+
3 rows in set (0.00 sec)
```

The columns of the table output from `VALUES` have the implicitly named columns column_0, column_1, column_2, and so on, always beginning with 0. This fact can be used to order the rows by column using an optional `ORDER BY` clause in the same way that this clause works with a `SELECT` statement, as shown here:

```
mysql> VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY column_1;
+----------+----------+----------+
| column_0 | column_1 | column_2 |
+----------+----------+----------+
|        1 |       -2 |        3 |
|        4 |        6 |        8 |
|        5 |        7 |        9 |
+----------+----------+----------+
3 rows in set (0.00 sec)

mysql> VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY column_1 limit 1;
+----------+----------+----------+
| column_0 | column_1 | column_2 |
+----------+----------+----------+
|        1 |       -2 |        3 |
+----------+----------+----------+
1 row in set (0.00 sec)
```

The `VALUES` statement is permissive regarding data types of column values; you can mix types within the same column, as shown here:

```
mysql> VALUES ROW("q", 42, '2019-12-18'),
    ->     ROW(23, "abc", 98.6),
    ->     ROW(27.0002, "Mary Smith", '{"a": 10, "b": 25}');
+----------+------------+--------------------+
| column_0 | column_1   | column_2           |
+----------+------------+--------------------+
| q        | 42         | 2019-12-18         |
| 23       | abc        | 98.6               |
| 27.0002  | Mary Smith | {"a": 10, "b": 25} |
+----------+------------+--------------------+
3 rows in set (0.00 sec)
```

With `UNION` , as shown here:

```
mysql> VALUES ROW(1,2), ROW(3,4), ROW(5,6)
     >     UNION VALUES ROW(10,15),ROW(20,25);
+----------+----------+
| column_0 | column_1 |
+----------+----------+
|        1 |        2 |
|        3 |        4 |
|        5 |        6 |
|       10 |       15 |
|       20 |       25 |
+----------+----------+
5 rows in set (0.00 sec)
```

### 4. DO Statement and DECLARE Statement

`DO` executes the expressions but does not return any results. In most respects, `DO` is shorthand for `SELECT expr, ...` , but has the advantage that it is slightly faster when you do not care about the result.

```
DO expr [, expr] ...
```

`DO` is useful primarily with functions that have side effects, such as `RELEASE_LOCK()` .
Example: This `SELECT` statement pauses, but also produces a result set:

```
mysql> SELECT SLEEP(5);
+----------+
| SLEEP(5) |
+----------+
|        0 |
+----------+
1 row in set (5.02 sec)
```

`DO` , on the other hand, pauses without producing a result set.

```
mysql> DO SLEEP(5);
Query OK, 0 rows affected (4.99 sec)
```

`DECLARE` statement declares local variables within stored programs. To provide a default value for a variable, include a `DEFAULT` clause. The value can be specified as an expression; it need not be a constant. If the `DEFAULT` clause is missing, the initial value is NULL.

```
DECLARE Statement
DECLARE var_name [, var_name] ... type [DEFAULT value]
```

### 5. HANDLER Statement

The `HANDLER` statement provides direct access to table storage engine interfaces. 

```
HANDLER tbl_name OPEN [ [AS] alias]
HANDLER tbl_name READ { FIRST | NEXT }
    [ WHERE where_condition ] [LIMIT ... ]
HANDLER tbl_name CLOSE
```

The `HANDLER ... OPEN` statement opens a table, making it accessible using subsequent `HANDLER ... READ` statements. This table object is not shared by other sessions and is not closed until the session calls `HANDLER ... CLOSE`  or the session terminates.
The `HANDLER ... READ` syntax fetches a row from the table in natural row order that matches the WHERE condition. Natural row order is the order in which rows are stored in a TAE table data file.
Without a `LIMIT` clause, all forms of `HANDLER ... READ` fetch a single row if one is available. To return a specific number of rows, include a `LIMIT` clause. It has the same syntax as for the SELECT statement. 
`HANDLER ... CLOSE` closes a table that was opened with `HANDLER ... OPEN` .
Example:

```
mysql> create table t1(a int ,b int);
Query OK, 0 rows affected (0.01 sec)
mysql> insert into t1 values(1,-1),(2,-2),(3,-3),(4,-4),(5,-5);
Query OK, 5 rows affected (0.04 sec)
Records: 5  Duplicates: 0  Warnings: 0
mysql> handler  t1 open as th;
Query OK, 0 rows affected (0.00 sec)
mysql> handler th read first;
+------+------+
| a    | b    |
+------+------+
|    1 |   -1 |
+------+------+
1 row in set (0.00 sec)
mysql> handler th read first limit 3;
+------+------+
| a    | b    |
+------+------+
|    1 |   -1 |
|    2 |   -2 |
|    3 |   -3 |
+------+------+
3 rows in set (0.00 sec)
mysql> handler th read next;
+------+------+
| a    | b    |
+------+------+
|    5 |   -5 |
+------+------+
2 rows in set (0.00 sec)
mysql> handler th read next limit 3;
+------+------+
| a    | b    |
+------+------+
|    5 |   -5 |
+------+------+
2 rows in set (0.00 sec)
mysql> handler th close;
Query OK, 0 rows affected (0.00 sec)
```

### 6. CREATE TABLE ... LIKE Statement and CREATE TABLE ... SELECT Statement

Use `CREATE TABLE ... LIKE` to create an empty table based on the definition of another table, including any column attributes and indexes defined in the original table:

```
CREATE TABLE new_tbl LIKE orig_tbl;
```

The copy is created using the same version of the table storage format as the original table.

```
create table test1(a int, b float);
create table test2 like test1;
show columns from test2;
+-------+-------+------+------+---------+---------+
| Field | Type  | Null | Key  | Default | Comment |
+-------+-------+------+------+---------+---------+
| a     | INT   | YES  |      | NULL    |         |
| b     | FLOAT | YES  |      | NULL    |         |
+-------+-------+------+------+---------+---------+
2 rows in set (0.11 sec)
```

You can create one table from another by adding a `SELECT` statement at the end of the `CREATE TABLE` statement:

```
CREATE TABLE new_tbl [AS] SELECT * FROM orig_tbl;
```

Create new columns for all elements in the `SELECT`. For example:

```
mysql> CREATE TABLE test (a INT NOT NULL AUTO_INCREMENT,
    ->        PRIMARY KEY (a), KEY(b))
    ->        SELECT b,c FROM test2;
```

This creates a table with three columns, a, b, and c. 
Notice that the columns from the `SELECT` statement are appended to the right side of the table, not overlapped onto it. Take the following example:

```
mysql> SELECT * FROM foo;
+---+
| n |
+---+
| 1 |
+---+
1 row in set (0.00 sec)

mysql> CREATE TABLE bar (m INT) SELECT n FROM foo;
Query OK, 1 row affected (0.02 sec)
Records: 1  Duplicates: 0  Warnings: 0

mysql> SELECT * FROM bar;
+------+---+
| m    | n |
+------+---+
| NULL | 1 |
+------+---+
1 row in set (0.00 sec)
```

### 7. CREATE TRIGGER/DROP TRIGGER

This statement creates a new trigger. A trigger is a named database object that is associated with a table, and that activates when a particular event occurs for the table. The trigger becomes associated with the table named tbl_name, which must refer to a permanent table. 

```
CREATE
    [DEFINER = user]
    TRIGGER [IF NOT EXISTS] trigger_name
    trigger_time trigger_event
    ON tbl_name FOR EACH ROW
    [trigger_order]
    trigger_body

trigger_time: { BEFORE | AFTER }

trigger_event: { INSERT | UPDATE | DELETE }

trigger_order: { FOLLOWS | PRECEDES } other_trigger_name
```

`IF NOT EXISTS` prevents an error from occurring if a trigger having the same name, on the same table, exists in the same schema. 

`trigger_time` is the trigger action time. It can be BEFORE or AFTER to indicate that the trigger activates before or after each row to be modified.

`trigger_event` indicates the kind of operation that activates the trigger. These `trigger_event` values are permitted:
`INSERT` : The trigger activates whenever a new row is inserted into the table (for example, through INSERT, LOAD DATA, and REPLACE statements).
`UPDATE`: The trigger activates whenever a row is modified (for example, through `UPDATE` statements).
`DELETE` : The trigger activates whenever a row is deleted from the table (for example, through DELETE and REPLACE statements). `DROP TABLE` and `TRUNCATE TABLE` statements on the table do not activate this trigger, because they do not use `DELETE` . Dropping a partition does not activate `DELETE` triggers, either.
The `trigger_event` does not represent a literal type of SQL statement that activates the trigger so much as it represents a type of table operation. For example, an `INSERT` trigger activates not only for `INSERT` statements but also `LOAD DATA` statements because both statements insert rows into a table.
`trigger_body` is the statement to execute when the trigger activates. To execute multiple statements, use the `BEGIN ... END` compound statement construct.

```
DROP TRIGGER [IF EXISTS] [schema_name.]trigger_name
```

This statement drops a trigger. The schema (database) name is optional. If the schema is omitted, the trigger is dropped from the default schema. 

### 8. TRUNCATE TABLE and KILL Statement

TRUNCATE TABLE Statement

```
TRUNCATE [TABLE] tbl_name
```

`TRUNCATE TABLE` empties a table completely. Logically, `TRUNCATE TABLE` is similar to a `DELETE` statement that deletes all rows, or a sequence of `DROP TABLE` and `CREATE TABLE` statements.

KILL Statement

```
KILL [CONNECTION | QUERY] processlist_id
```

Each connection to mysqld runs in a separate thread. You can kill a thread with the `KILL processlist_id` statement.