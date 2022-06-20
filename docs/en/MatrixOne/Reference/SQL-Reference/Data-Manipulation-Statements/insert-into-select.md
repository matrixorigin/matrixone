# **INSERT INTO SELECT**

## **Description**

Quickly insert many rows into a table from the result of a SELECT statement, which can select from one or many tables.

## **Syntax**

```
INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE]
    [INTO] tbl_name
    [PARTITION (partition_name [, partition_name] ...)]
    [(col_name [, col_name] ...)]
    {   SELECT ...
      | TABLE table_name
      | VALUES row_constructor_list
    }
    [ON DUPLICATE KEY UPDATE assignment_list]
```

## **Examples**

```sql
> INSERT INTO tbl_temp2 (fld_id)
> SELECT tbl_temp1.fld_order_id
> FROM tbl_temp1 WHERE tbl_temp1.fld_order_id > 100;
```
