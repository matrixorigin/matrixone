# **LOAD DATA**

## **Description**

The LOAD DATA statement reads rows from a text file into a table at a very high speed.

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
```

## **Examples**

The SSB Test is an example of LOAD DATA syntax. [Complete a SSB Test with MatrixOne
](../../../Get-Started/Tutorial/SSB-test-with-matrixone.md)

```
> LOAD DATA INFILE '/ssb-dbgen-path/lineorder_flat.tbl ' INTO TABLE lineorder_flat;
```

## **Constraints**

Only CSV format file is supported for now. 