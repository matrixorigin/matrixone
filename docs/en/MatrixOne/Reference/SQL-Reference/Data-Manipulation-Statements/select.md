# **SELECT**

## **Description**
Retrieves data from a table.

## **Syntax**

```
$ SELECT
    [ALL | DISTINCT]
    select_expr [[AS] alias], ...
    [INTO variable [, ...]]
    [ FROM table_references
    [WHERE expr]
    [GROUP BY {{col_name | expr | position}, ...
    | extended_grouping_expr}]
    [HAVING expr]
    [ORDER BY {col_name | expr} [ASC | DESC], ...]
    [LIMIT row_count]
    [OFFSET row_count]
    ]
```
## **Examples**

```
$ SELECT number FROM numbers(3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

$ SELECT * FROM t1 WHERE spID>2 AND userID <2 || userID >=2 OR userID < 2 LIMIT 3;

$ SELECT userID,MAX(score) max_score FROM t1 WHERE userID <2 || userID > 3 GROUP BY userID ORDER BY max_score;
```
