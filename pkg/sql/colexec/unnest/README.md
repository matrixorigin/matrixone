# **UNNEST**

## **函数说明**

`UNNEST`是一个表函数,出现在sql的from子句中,用于将json[object|array]类型的数据展开为多行,每行包含json中的一个元素.

## **语法结构**

```
> UNNEST(src[, path[, outer]])
```

## **相关参数**

| 参数 | 说明                                       | 类型                                               |
|----------|------------------------------------------|--------------------------------------------------|
| src | 必要参数，待展开的数据源                             | 类型可以是json列或json字符串                               |
| path | 可选参数，指明待展开数据源的具体json路径。默认为"$",展开整个json数据 | [path字符串](../../../container/bytejson/README.md) |
| outer | 可选参数，如果数据源展开后结果行为0，是否加上一个空行作为标记。默认为false | bool类型                                           |

## **返回表结构**

| 字段名 | 类型 | 说明                                                |
|-----|---------|---------------------------------------------------|
| col | varchar | 数据源的名称。如果数据源是列，则是列名；如果数据源是json字符串，则是"UNNEST_DEFAULT" |
| seq | int32 | 数据源中元素的索引，从0开始                                    |
|key    |varchar    | 展开元素的键名，如果父级元素是数组，则为null                          |
|path | varchar | 展开元素的在数据源中的路径                                     |
|index | int32 | 展开元素在父级元素中的索引，如果数据源是对象，则为null                     |
|value | varchar | 展开元素的值                                            |
|this | varchar | 展开元素的父级元素值                                        |

## **示例**

```
> select *
> from unnest('{"a":1,"b":2,"c":3}') as u;
+----------------+------+------+------+-------+-------+--------------------------+
| col            | seq  | key  | path | index | value | this                     |
+----------------+------+------+------+-------+-------+--------------------------+
| UNNEST_DEFAULT |    0 | a    | $.a  |  NULL | 1     | {"a": 1, "b": 2, "c": 3} |
| UNNEST_DEFAULT |    0 | b    | $.b  |  NULL | 2     | {"a": 1, "b": 2, "c": 3} |
| UNNEST_DEFAULT |    0 | c    | $.c  |  NULL | 3     | {"a": 1, "b": 2, "c": 3} |
+----------------+------+------+------+-------+-------+--------------------------+

> select *
> from unnest('{"a":1,"b":2,"c":3}') as u
> where u.`key` = 'b';
+----------------+------+------+------+-------+-------+--------------------------+
| col            | seq  | key  | path | index | value | this                     |
+----------------+------+------+------+-------+-------+--------------------------+
| UNNEST_DEFAULT |    0 | b    | $.b  |  NULL | 2     | {"a": 1, "b": 2, "c": 3} |
+----------------+------+------+------+-------+-------+--------------------------+

> select *
> from unnest('{"a":1,"b":2,"c":3}',"$.b") as u;
Empty set (0.01 sec)

> select *
> from unnest('{"a":1,"b":2,"c":3}',"$.b",true) as u;
+----------------+------+------+------+-------+-------+--------------------------+
| col            | seq  | key  | path | index | value | this                     |
+----------------+------+------+------+-------+-------+--------------------------+
| UNNEST_DEFAULT |    0 | NULL | $.b  |  NULL | NULL  | 2                        |

> drop table if exists t1;
> create table t1 (a json,b int);
> insert into t1
> values ('{"a":1,"b":[{"c":2,"d":3},false,4],"e":{"f":true,"g":[null,true,1.1]}}',1);
> insert into t1
> values ('[1,true,false,null,"aaa",1.1,{"t":false}]',2);
> select * from unnest(t1.a, "$.b") as u;
+------+------+------+--------+-------+------------------+------------------------------+
| col  | seq  | key  | path   | index | value            | this                         |
+------+------+------+--------+-------+------------------+------------------------------+
| a    |    0 | NULL | $.b[0] |     0 | {"c": 2, "d": 3} | [{"c": 2, "d": 3}, false, 4] |
| a    |    0 | NULL | $.b[1] |     1 | false            | [{"c": 2, "d": 3}, false, 4] |
| a    |    0 | NULL | $.b[2] |     2 | 4                | [{"c": 2, "d": 3}, false, 4] |
+------+------+------+--------+-------+------------------+------------------------------+

> select * from unnest(t1.a, "$.b[0]") as u;
+------+------+------+----------+-------+-------+------------------+
| col  | seq  | key  | path     | index | value | this             |
+------+------+------+----------+-------+-------+------------------+
| a    |    0 | c    | $.b[0].c |  NULL | 2     | {"c": 2, "d": 3} |
| a    |    0 | d    | $.b[0].d |  NULL | 3     | {"c": 2, "d": 3} |
+------+------+------+----------+-------+-------+------------------+

> select distinct(f.seq) from unnest(t1.a, "$") as f;
+-------+
| f.seq |
+-------+
|     0 |
|     1 |
+-------+
```

## **注意事项**

* key,index和value全为null则代表当前行是outer为true时默认添加的空行

## **执行流程**

1. 数据源是json列
   `...unnest -> project(jsonCol) -> tableScan`
2. 数据源是json字符串
   `...unnest -> project(default) -> valueScan(parse jsonStr)`

## **实现细节**

### 数据源为json字符串

1. 构建plan时将存储在`tree.Unnest`中的参数序列化后存储到`unnestNode.TableDef.TableFunctionParam`中
2. 在`unnestNode`中添加`valueScan`节点
3. 给`valueScan.TableDef.TableFunctionParam`赋值为`tree.Unnest`中存储的json字符串转化的字节切片
4. 在编译阶段首先将`valueScan`的`TableDef.TableFunctionParam`存入`scope.Datasource.Bat`
5. 在scope中添加`vm.Unnest`指令，并通过`unnestNode.TableDef.TableFunctionParam`构建运行参数
6. 执行阶段通过`bytejson`包解析json字节切片，解析path字符串，通过json,path, outer,*filter*参数调用`bytejson.Unnest`函数,返回`UnnestResult`结果集
7. 通过`makeBatch`组装`UnnestResult`结果集为`batch`

### 数据源为json列

1. 构建plan时将存储在`tree.Unnest`中的参数序列化后存储到`unnestNode.TableDef.TableFunctionParam`中
2. 在`unnestNode`中添加`tableScan`节点,并根据`tree.Unnest`中的参数初始化`tableScan`的`TableDef`
3. 编译阶段在scope中添加`vm.Unnest`指令，并通过`unnestNode.TableDef.TableFunctionParam`构建运行参数
4. 执行阶段通过`bytejson`包解析由`tableScan`传递bytejson字节切片，解析path字符串，通过json,path, outer,*filter*参数调用`bytejson.Unnest`
   函数,返回`UnnestResult`结果集
5. 通过`makeBatch`组装`UnnestResult`结果集为`batch`

*filter*参数是根据`tree.Unnest`中的`Attrs`字段构建的string切片，其目的是为了在`bytejson.Unnest`函数中过滤不需要的结果集

