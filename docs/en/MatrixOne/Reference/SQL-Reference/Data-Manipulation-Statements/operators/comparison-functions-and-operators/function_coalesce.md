# **COALESCE**

## **Description**

The `COALESCE()` function returns the first non-NULL value in the list, or NULL if there are no non-NULL values.

The return type of `COALESCE()` is the aggregated type of the argument types.

## **Syntax**

```
> COALESCE(value,...)
```

## **Examples**

```sql
> SELECT COALESCE(NULL,1);
        -> 1
mysql> SELECT COALESCE(NULL,NULL,NULL);
        -> NULL
