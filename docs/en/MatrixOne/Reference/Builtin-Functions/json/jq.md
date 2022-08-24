# **JQ()**

## **Description**

Extracts the value of the path expression expr from json.

## **Syntax**

```
> JQ(json, expr)
```

```

## **Arguments**

| Arguments | Description                                                                       |
|-----------|-----------------------------------------------------------------------------------|
| json      | Required. The source to extract the value from, optionally string or column name. |
| expr      | Required. The path expression to position the value.                              |

## **Examples**

```sql
> select jq('{"a":1}','$.a');
+---------------------+
| jq({"a":1},$.a) |
+---------------------+
| 1                   |
+---------------------+

> select jq('[1,2,3]', '$[1]');
+---------------------+
| jq([1,2,3], $[1]) |
+---------------------+
| 2                   |
+---------------------+

> select jq('{"a":1,"b":2}','$.*');
+-------------------------+
| jq({"a":1,"b":2},$.*) |
+-------------------------+
| [1, 2]                  |
+-------------------------+

> select jq('[1,2,3]','$[*]');
+---------------------+
| jq([1,2,3],$[*]) |
+---------------------+
| [1, 2, 3]           |
+---------------------+

> select jq('{"a":[1,2,3,{"a":4}]}','$**.a');
+----------------------------------+
| jq({"a":[1,2,3,{"a":4}]}, $**.a) |
+----------------------------------+
| [[1, 2, 3, {"a": 4}], 4]         |
+----------------------------------+


> drop table if exists t1;
> create table t1(a json, b int);
> insert into t1 values('{"a":1}',1),('{"a":2}',2);
> select jq(a,'$.a') from t1 where b=1;
+-----------------+
| jq(a,$.a)     |
+-----------------+
| 1               |
+-----------------+

> select jq(a,'$.a') from t1;
+-----------------+
| jq(a,$.a)     |
+-----------------+
| 1               |
| 2               |
+-----------------+


```

## Hint

If the query result is not a single value, the result will be automatically transformed into a JSON array.
eg: `select jq('[1,2,3]','$[*]')` will return `[1,2,3]` instead of `1,2,3`, but `select jq('[1,2,3]','$[0]')` will
return `1`.