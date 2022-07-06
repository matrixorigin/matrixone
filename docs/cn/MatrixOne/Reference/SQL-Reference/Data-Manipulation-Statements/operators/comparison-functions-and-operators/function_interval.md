# **INTERVAL**

## **语法说明**

The `INTERVAL` operator returns 0 if N < N1, 1 if N < N2 and so on or -1 if N is NULL. All arguments are treated as integers. It is required that N1 < N2 < N3 < ... < Nn for this function to work correctly. This is because a binary search is used (very fast).

## **语法结构**

```
> INTERVAL(N,N1,N2,N3,...)
```

## **示例**

```sql
>
```
