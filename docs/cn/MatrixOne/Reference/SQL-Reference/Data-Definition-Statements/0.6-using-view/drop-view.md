# **DROP VIEW**

## **语法说明**

`DROP VIEW` 语句表示删除视图。

如果语法参数列表中命名的任何视图都不存在，语句报错，并提示无法删除哪些不存在的视图，并且不做任何更改。

`IF EXISTS` 子句表示防止对不存在的视图发生错误。给出该子句时，将为每个不存在的视图生成一个 `NOTE`。

## **语法结构**

```
> DROP VIEW [IF EXISTS]
    view_name [, view_name] ...
```

## **示例**

```sql
> CREATE TABLE t1(c1 INT PRIMARY KEY, c2 INT);
> CREATE VIEW v1 AS SELECT * FROM t1;
> DROP VIEW v1;
```
