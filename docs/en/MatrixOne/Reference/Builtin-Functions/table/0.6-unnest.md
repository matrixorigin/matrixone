# **UNNEST()**

The `UNNEST` function takes an `ARRAY` and returns a table with a row for each element in the `ARRAY`.

Empty arrays and `NULL` as an input returns an empty table. An array containing `NULL` values will produce a row of `NULL` values.

## **Syntax**

```
> UNNEST(ARRAY) [WITH OFFSET]
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| ARRAY | Required. `ARRAY` as an input but can return a table of any structure. |
|WITH OFFSET |Optional. `WITH OFFSET` clause provides an additional column containing the position of each element in the array (starting at zero) for each row produced by `UNNEST`.|

## **Examples**

- Example 1:

```sql
> select * from unnest('{"a":1}') u;
+----------------+------+------+------+-------+-------+----------+
| col            | seq  | key  | path | index | value | this     |
+----------------+------+------+------+-------+-------+----------+
| UNNEST_DEFAULT |    0 | a    | $.a  |  NULL | 1     | {"a": 1} |
+----------------+------+------+------+-------+-------+----------+
1 row in set (0.00 sec)

> select * from unnest('[1,2,3]') u;
+----------------+------+------+------+-------+-------+-----------+
| col            | seq  | key  | path | index | value | this      |
+----------------+------+------+------+-------+-------+-----------+
| UNNEST_DEFAULT |    0 | NULL | $[0] |     0 | 1     | [1, 2, 3] |
| UNNEST_DEFAULT |    0 | NULL | $[1] |     1 | 2     | [1, 2, 3] |
| UNNEST_DEFAULT |    0 | NULL | $[2] |     2 | 3     | [1, 2, 3] |
+----------------+------+------+------+-------+-------+-----------+
3 rows in set (0.00 sec)

> select * from unnest('[1,2,3]','$') u;
+----------------+------+------+------+-------+-------+-----------+
| col            | seq  | key  | path | index | value | this      |
+----------------+------+------+------+-------+-------+-----------+
| UNNEST_DEFAULT |    0 | NULL | $[0] |     0 | 1     | [1, 2, 3] |
| UNNEST_DEFAULT |    0 | NULL | $[1] |     1 | 2     | [1, 2, 3] |
| UNNEST_DEFAULT |    0 | NULL | $[2] |     2 | 3     | [1, 2, 3] |
+----------------+------+------+------+-------+-------+-----------+
3 rows in set (0.01 sec)

> select * from unnest('[1,2,3]','$[0]',true) u;
+----------------+------+------+------+-------+-------+------+
| col            | seq  | key  | path | index | value | this |
+----------------+------+------+------+-------+-------+------+
| UNNEST_DEFAULT |    0 | NULL | $[0] |  NULL | NULL  | 1    |
+----------------+------+------+------+-------+-------+------+
1 row in set (0.00 sec)
```

- Example 2:

```sql
> create table t1 (a json,b int);
> insert into t1 values ('{"a":1,"b":[{"c":2,"d":3},false,4],"e":{"f":true,"g":[null,true,1.1]}}',1);
> insert into t1 values ('[1,true,false,null,"aaa",1.1,{"t":false}]',2);
> select * from t1;
+---------------------------------------------------------------------------------------+------+
| a                                                                                     | b    |
+---------------------------------------------------------------------------------------+------+
| {"a": 1, "b": [{"c": 2, "d": 3}, false, 4], "e": {"f": true, "g": [null, true, 1.1]}} |    1 |
| [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |    2 |
+---------------------------------------------------------------------------------------+------+
2 rows in set (0.00 sec)

> select * from unnest(t1.a) as f;
+------+------+------+------+-------+-------------------------------------+---------------------------------------------------------------------------------------+
| col  | seq  | key  | path | index | value                               | this                                                                                  |
+------+------+------+------+-------+-------------------------------------+---------------------------------------------------------------------------------------+
| a    |    0 | a    | $.a  |  NULL | 1                                   | {"a": 1, "b": [{"c": 2, "d": 3}, false, 4], "e": {"f": true, "g": [null, true, 1.1]}} |
| a    |    0 | b    | $.b  |  NULL | [{"c": 2, "d": 3}, false, 4]        | {"a": 1, "b": [{"c": 2, "d": 3}, false, 4], "e": {"f": true, "g": [null, true, 1.1]}} |
| a    |    0 | e    | $.e  |  NULL | {"f": true, "g": [null, true, 1.1]} | {"a": 1, "b": [{"c": 2, "d": 3}, false, 4], "e": {"f": true, "g": [null, true, 1.1]}} |
| a    |    1 | NULL | $[0] |     0 | 1                                   | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[1] |     1 | true                                | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[2] |     2 | false                               | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[3] |     3 | null                                | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[4] |     4 | "aaa"                               | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[5] |     5 | 1.1                                 | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
| a    |    1 | NULL | $[6] |     6 | {"t": false}                        | [1, true, false, null, "aaa", 1.1, {"t": false}]                                      |
+------+------+------+------+-------+-------------------------------------+---------------------------------------------------------------------------------------+
10 rows in set (0.01 sec)

> select * from unnest(t1.a, "$.b") as f;
+------+------+------+--------+-------+------------------+------------------------------+
| col  | seq  | key  | path   | index | value            | this                         |
+------+------+------+--------+-------+------------------+------------------------------+
| a    |    0 | NULL | $.b[0] |     0 | {"c": 2, "d": 3} | [{"c": 2, "d": 3}, false, 4] |
| a    |    0 | NULL | $.b[1] |     1 | false            | [{"c": 2, "d": 3}, false, 4] |
| a    |    0 | NULL | $.b[2] |     2 | 4                | [{"c": 2, "d": 3}, false, 4] |
+------+------+------+--------+-------+------------------+------------------------------+
3 rows in set (0.00 sec)

> select * from unnest(t1.a, "$.a", true) as f;
+------+------+------+------+-------+-------+------+
| col  | seq  | key  | path | index | value | this |
+------+------+------+------+-------+-------+------+
| a    |    0 | NULL | $.a  |  NULL | NULL  | 1    |
+------+------+------+------+-------+-------+------+
1 row in set (0.00 sec)
```
