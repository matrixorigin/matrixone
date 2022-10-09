# **LOAD DATA**

## **Description**

The LOAD DATA statement reads rows from a csv file into a table at a very high speed.

## **Syntax**

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

- `TERMINATED BY`, `ENCLOSED BY`, and other separators in meanings are the same as `SELECT INTO`.

- The IGNORE number LINES clause can be used to ignore lines at the start of the file. For example, you can use `IGNORE 1 LINES` to skip an initial header line containing column names:

```
LOAD DATA INFILE '/tmp/test.txt' INTO TABLE test IGNORE 1 LINES;
```

## **Examples**

The SSB Test is an example of LOAD DATA syntax. [Complete a SSB Test with MatrixOne
](../../../Tutorial/SSB-test-with-matrixone.md)

```
> LOAD DATA INFILE '/ssb-dbgen-path/lineorder_flat.tbl ' INTO TABLE lineorder_flat;
```

## **Constraints**

Only CSV format file is supported for now.
