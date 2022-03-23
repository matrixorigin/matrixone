# **SELECT**

## **语法描述**
`SELECT`语句用于从表中检索数据。
## **语法结构**

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
`SELECT INTO` 语句将查询结果存储在变量或者导出为文件。

* SELECT ... INTO ***var_list*** selects column values and stores them into variables.
* SELECT ... INTO OUTFILE writes the selected rows to a file. Column and line terminators can be specified to produce a specific output format.
* SELECT ... INTO ***var_list*** 可以检索列值并将其存储到变量中。
* SELECT ... INTO OUTFILE 将检索结果输出为文件，可以使用行/列分隔符来生成特定的格式。 


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

#### 参数释义

* `<expr_list>` 是你想输出的检索结果，为必选参数。
* `'file_name'` 为本机上你所导出的文件的文件名，以绝对路径。若存在同名文件，则会导致导出失败。此外，路径中的所有前置文件夹都必须事先创建，该命令不会自动创建所需要的文件夹。
* `TERMINATED BY` 被用来指定字段分隔符，默认值为逗号 `,`。
* `ENCLOSED BY` 指定了列字段包括符，默认为双引号 `"`。
* `LINES TERMINATED BY` 表示行结束符，默认为换行符 `\n`。
* `HEADER` 用于设置文件中是否包含每一列名称的标题行，当为`TRUE`时，输出包含标题行，反之则不包含。
* 你可以通过`MAX_FILE_SIZE`限制输出文件的大小（以KB为单位）。
  例如，当`MAX_FILE_SIZE`=5242880(5GB)时，10GB的表就会被导出为两个文件，文件命名规则为原文件名后缀加上`.序号`。若不设置该值，那么默认输出一个文件。
* `FORCE_QUOTE`强制对每个指定列中所有非NULL值使用引用，使用双引号作为标识。
* `NULL` 值将被导出为`\N`。

!!! info 建议
    如果不设置`MAX_FILE_SIZE`，当表的数据量很大时会输出一个极大的文件，可能因此发生导出失败的情况；因此我们建议您根据实际情况设置该值。


#### 限制

 * 相同目录下若存在同名文件，则会导致导出失败。
 * 目前只支持导出 ***.csv*** 类型的文件。
 * 目前只支持通过绝对路径将文件导出到服务器主机，而不能导出到远程客户端。


## **示例**

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
