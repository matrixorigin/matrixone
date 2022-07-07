# **ISNULL**

## **Description**

The `ISNULL()` function can be used instead of = to test whether a value is NULL. (Comparing a value to `NULL` using = always yields `NULL`.)

If expression is `NULL`, this function returns `true`. Otherwise, it returns `false`.

The `ISNULL()` function shares some special behaviors with the `IS NULL` comparison operator. See the description of [`IS NULL`](is-null.md).

## **Syntax**

```
> ISNULL(expr)
```

## **Examples**

```sql
> SELECT ISNULL(1+1);
+---------------+
| isnull(1 + 1) |
+---------------+
| false         |
+---------------+
1 row in set (0.02 sec)
```
