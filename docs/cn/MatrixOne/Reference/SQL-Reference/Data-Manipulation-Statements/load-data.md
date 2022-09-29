# **LOAD DATA**

## **语法说明**

`LOAD DATA`语句以超快的速度从一个 *.csv* 文件中读取数据然后存储到表中。

## **语法结构**

```
> LOAD DATA
    INFILE 'file_name'
    INTO TABLE tbl_name
    [{FIELDS | COLUMNS}
        [TERMINATED BY 'string']
        [[OPTIONALLY] ENCLOSED BY 'char']
        [ESCAPED BY 'char']
    ]
    [LINES
        [STARTING BY 'string']
        [TERMINATED BY 'string']
    ]
    [IGNORE number {LINES | ROWS}]
```

* `TERMINATED BY`，`ENCLOSED BY`等分隔符的意义与`SELECT INTO`一致。
* `IGNORE number`可用来忽略从文件开始的第`number`行/列。

## **示例**

可以在SSB测试中了解`LOAD DATA`语句的用法，请见
[Complete a SSB Test with MatrixOne
](../../../Get-Started/Tutorial/SSB-test-with-matrixone.md)

```sql
> LOAD DATA INFILE '/ssb-dbgen-path/lineorder_flat.tbl ' INTO TABLE lineorder_flat;
```

## **限制**

当前仅支持 `csv` 格式的文件。
