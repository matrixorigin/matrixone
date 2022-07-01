# **SELECT**

## **Description**

Retrieves data from a table.

## **Syntax**

``` sql
> SELECT
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

### **SELECT INTO**

`SELECT INTO` statement enables a query result to be stored in variables or written to a file  

* SELECT ... INTO ***var_list*** selects column values and stores them into variables.
* SELECT ... INTO OUTFILE writes the selected rows to a file. Column and line terminators can be specified to produce a specific output format.

``` sql
> SELECT <expr_list>
  INTO OUTFILE 'file_name'
    [{FIELDS}
        [TERMINATED BY 'char']
        [ENCLOSED BY 'char']
    ]
    [LINES
        [TERMINATED BY 'string']
    ]
    [HEADER 'bool']
    [MAX_FILE_SIZE long]
    [FORCE_QUOTE {'col1','col2',...}]
```

#### Explanations

* `<expr_list>` is the query result you want to export.
* `'file_name'` is the file name of the absolute path on the server. The query will fail if a file with the same filename already exists. And the front folder written in the absolute path must be created in advance, otherwise an error will occur.
* `TERMINATED BY` is an optional argument as the field separator. The default value is comma `,`.
* `ENCLOSED BY` is an optional argument as the inclusion character of column fields. The default value is double quotations `"`.
* `LINES TERMINATED BY` is an optional argument as the end of a line. The default value is `\n`.
* When `HEADER` is `TRUE`, the table's title will also be exported, vice versa.
* You can limit the maximum size of the file using `MAX_FILE_SIZE` in KB.
  For example, with `MAX_FILE_SIZE`=5242880(5GB), tables with size of 10GB are exported as two files distinguished  by the ordinal number in their name.  
  When this value is not set, one file will be exported by default.
* `FORCE_QUOTE` is used to add double quotes for every `NOT NULL` value in the specified column.
* `NULL` values will be exported as `\N`.
* `HAVING` specifies conditions on groups, typically formed by the `GROUP BY` clause. The query result includes only groups satisfying the `HAVING` conditions. (If no GROUP BY is present, all rows implicitly form a single aggregate group.)
  The `HAVING` clause, like the `WHERE` clause, specifies selection conditions. The `WHERE` clause specifies conditions on columns in the select list, but cannot refer to aggregate functions.
  The SQL standard requires that HAVING must reference only columns in the `GROUP BY` clause or columns used in aggregate functions.
  If the `HAVING` clause refers to a column that is ambiguous, a warning occurs.

!!! info Suggestions
    If `MAX_FILE_SIZE` is not set, a large file may be exported and the operation may fail. Therefore, we recommend you to set this value case by case.

#### Constraints  

 * The query will fail if a file with the same filename already exists.
 * Only ***.csv*** types of files are supported currently.
 * Currently, this command supports only the absolute path, and files can be exported only to the server host, not to the remote client.

## **Examples**

```sql
> SELECT number FROM numbers(3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

> SELECT * FROM t1 WHERE spID>2 AND userID <2 || userID >=2 OR userID < 2 LIMIT 3;

> SELECT userID,MAX(score) max_score FROM t1 WHERE userID <2 || userID > 3 GROUP BY userID ORDER BY max_score;
```

```sql
select * from t1 into outfile '/Users/tmp/test.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
header 'TRUE'
MAX_FILE_SIZE 9223372036854775807
FORCE_QUOTE (a, b)
```

```sql
> create table t1 (spID int,userID int,score smallint);
> insert into t1 values (1,1,1);
> insert into t1 values (2,2,2);
> insert into t1 values (2,1,4);
> insert into t1 values (3,3,3);
> insert into t1 values (1,1,5);
> insert into t1 values (4,6,10);
> insert into t1 values (5,11,99);
> select userID,count(score) from t1 group by userID having count(score)>1 order by userID;
+--------+--------------+
| userid | count(score) |
+--------+--------------+
|      1 |            3 |
+--------+--------------+
> select userID,count(score) from t1 where userID>2 group by userID having count(score)>1 order by userID;
Empty set (0.01 sec)s
```
