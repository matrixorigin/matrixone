# **EXECUTE**

## **Description**

After preparing a statement with `PREPARE`, you execute it with an `EXECUTE` statement that refers to the prepared statement name. If the prepared statement contains any parameter markers, you must supply a USING clause that lists user variables containing the values to be bound to the parameters. Parameter values can be supplied only by user variables, and the USING clause must name exactly as many variables as the number of parameter markers in the statement.

You can execute a given prepared statement multiple times, passing different variables to it or setting the variables to different values before each execution.

## **Syntax**

```
EXECUTE stmt_name
    [USING @var_name [, @var_name] ...]
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
|stmt_name | The name of the prepared statement. |

## **Examples**

```sql
> CREATE TABLE numbers(pk INTEGER PRIMARY KEY, ui BIGINT UNSIGNED, si BIGINT);
> INSERT INTO numbers VALUES (0, 0, -9223372036854775808), (1, 18446744073709551615, 9223372036854775807);
> SET @si_min = -9223372036854775808;
> SET @si_max = 9223372036854775807;
> PREPARE s2 FROM 'SELECT * FROM numbers WHERE si=?';
Query OK, 0 rows affected (0.00 sec)

> EXECUTE s2 USING @si_min;
+------+------+----------------------+
| pk   | ui   | si                   |
+------+------+----------------------+
|    0 |    0 | -9223372036854775808 |
+------+------+----------------------+
1 row in set (0.01 sec)

> EXECUTE s2 USING @si_max;
+------+----------------------+---------------------+
| pk   | ui                   | si                  |
+------+----------------------+---------------------+
|    1 | 18446744073709551615 | 9223372036854775807 |
+------+----------------------+---------------------+
1 row in set (0.01 sec)

> DEALLOCATE PREPARE s2;
Query OK, 0 rows affected (0.00 sec)
```
