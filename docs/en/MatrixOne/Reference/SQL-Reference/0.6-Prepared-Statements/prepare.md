# **PREPARE**

## **Description**

The `PREPARE` statement prepares a SQL statement and assigns it a name, The prepared statement is executed with EXECUTE and released with `DEALLOCATE PREPARE`.

Statement names are not case-sensitive.

## **Syntax**

```
PREPARE stmt_name FROM preparable_stmt
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
|stmt_name | The name of the prepared statement. |
|preparable_stmt| a string literal or a user variable that contains the text of the SQL statement. The text must represent a single statement, not multiple statements. Within the statement, ? characters can be used as parameter markers to indicate where data values are to be bound to the query later when you execute it. The ? characters should not be enclosed within quotation marks, even if you intend to bind them to string values. Parameter markers can be used only where data values should appear, not for SQL keywords, identifiers, and so forth.|

If a prepared statement with the given name already exists, it is deallocated implicitly before the new statement is prepared. This means that if the new statement contains an error and cannot be prepared, an error is returned and no statement with the given name exists.

The scope of a prepared statement is the session within which it is created, which as several implications:

- A prepared statement created in one session is not available to other sessions.

- When a session ends, whether normally or abnormally, its prepared statements no longer exist. If auto-reconnect is enabled, the client is not notified that the connection was lost. For this reason, clients may wish to disable auto-reconnect.

A parameter used in a prepared statement has its type determined when the statement is first prepared, and retains this type whenever `EXECUTE` is invoked for this prepared statement. Rules for determining a parameter's type are listed here:

- A parameter which is an operand of a binary arithmetic operator has the same data type as the other operand.

- If both operands of a binary arithmetic operator are parameters, the type of the parameters is decided by the context of the operator.

- If a parameter is the operand of a unary arithmetic operator, the parameter's type is decided by the context of the operator.

- If an arithmetic operator has no type-determining context, the derived type for any parameters involved is `DOUBLE PRECISION`. This can happen, for example, when the parameter is a top-level node in a `SELECT` list, or when it is part of a comparison operator.

- A parameter which is an operand of a character string operator has the same derived type as the aggregated type of the other operands. If all operands of the operator are parameters, the derived type is `VARCHAR;` its collation is determined by the value of `collation_connection`.

- A parameter which is an operand of a temporal operator has type DATETIME if the operator returns a `DATETIME`, `TIME` if the operator returns a `TIME`, and `DATE` if the operator returns a `DATE`.

- A parameter which is an operand of a binary comparison operator has the same derived type as the other operand of the comparison.

- A parameter that is an operand of a ternary comparison operator such as `BETWEEN` has the same derived type as the aggregated type of the other operands.

- If all operands of a comparison operator are parameters, the derived type for each of them is `VARCHAR`, with collation determined by the value of `collation_connection`.

- A parameter that is an output operand of any of CASE, COALESCE, IF, IFNULL, or NULLIF has the same derived type as the aggregated type of the operator's other output operands.

- If all output operands of any of CASE, COALESCE, IF, IFNULL, or NULLIF are parameters, or they are all NULL, the type of the parameter is decided by the context of the operator.

- If the parameter is an operand of any of CASE, COALESCE(), IF, or IFNULL, and has no type-determining context, the derived type for each of the parameters involved is VARCHAR, and its collation is determined by the value of collation_connection.

- A parameter which is the operand of a CAST() has the same type as specified by the CAST().

- If a parameter is an immediate member of a SELECT list that is not part of an INSERT statement, the derived type of the parameter is VARCHAR, and its collation is determined by the value of collation_connection.

- If a parameter is an immediate member of a SELECT list that is part of an INSERT statement, the derived type of the parameter is the type of the corresponding column into which the parameter is inserted.

- If a parameter is used as source for an assignment in a SET clause of an UPDATE statement or in the ON DUPLICATE KEY UPDATE clause of an INSERT statement, the derived type of the parameter is the type of the corresponding column which is updated by the SET or ON DUPLICATE KEY UPDATE clause.

- If a parameter is an argument of a function, the derived type depends on the function's return type.

For some combinations of actual type and derived type, an automatic repreparation of the statement is triggered. Repreparation does not occur if any of the following conditions are true:

- NULL is used as the actual parameter value.

- A parameter is an operand of a CAST(). (Instead, a cast to the derived type is attempted, and an exception raised if the cast fails.)

- A parameter is a string. (In this case, an implicit CAST(? AS derived_type) is performed.)

- The derived type and actual type of the parameter are both INTEGER and have the same sign.

- The parameter's derived type is DECIMAL and its actual type is either DECIMAL or INTEGER.

- The derived type is DOUBLE and the actual type is any numeric type.

- Both the derived type and the actual type are string types.

- If the derived type is temporal and the actual type is temporal. Exceptions: The derived type is TIME and the actual type is not TIME; the derived type is DATE and the actual type is not DATE.

- The derived type is temporal and the actual type is numeric.

For cases other than those just listed, the statement is reprepared and the actual parameter types are used instead of the derived parameter types.

These rules also apply to a user variable referenced in a prepared statement.

Using a different data type for a given parameter or user variable within a prepared statement for executions of the statement subsequent to the first execution causes the statement to be reprepared. This is less efficient; it may also lead to the parameter's (or variable's) actual type to vary, and thus for results to be inconsistent, with subsequent executions of the prepared statement. For these reasons, it is advisable to use the same data type for a given parameter when re-executing a prepared statement.

## **Examples**

```sql
> create table t13 (a int primary key);
> insert into t13 values (1);
> select * from t13 where 3 in (select (1+1) union select 1);
Empty set (0.01 sec)

> select * from t13 where 3 in (select (1+2) union select 1);
+------+
| a    |
+------+
|    1 |
+------+
1 row in set (0.01 sec)

> prepare st_18492 from 'select * from t13 where 3 in (select (1+1) union select 1)';
Query OK, 0 rows affected (0.00 sec)

> execute st_18492;
Empty set (0.01 sec)

> prepare st_18493 from 'select * from t13 where 3 in (select (2+1) union select 1)';
Query OK, 0 rows affected (0.00 sec)

> execute st_18493;
+------+
| a    |
+------+
|    1 |
+------+
1 row in set (0.00 sec)

> deallocate prepare st_18492;
Query OK, 0 rows affected (0.00 sec)

> deallocate prepare st_18493;
Query OK, 0 rows affected (0.00 sec)
```
