# **COALESCE**

## **语法说明**

The `COALESCE()` function returns the first non-NULL value in the list, or NULL if there are no non-NULL values.

The return type of `COALESCE()` is the aggregated type of the argument types.

## **语法结构**

```
> COALESCE(value,...)
```

## **示例**

```sql
> SELECT COALESCE(NULL,1);
        -> 1
mysql> SELECT COALESCE(NULL,NULL,NULL);
        -> NULL
