# 慢查询

当前 MatrixOne 的慢查询是超过 1000 毫秒的查询，暂不支持定向输出到对应的日志文件中，需要通过创建视图的方式进行过滤获取。

## 开启慢查询

MatrixOne 的慢查询功能在版本 0.6.0 中提供了如下几个基础信息：

- `statement`：即 SQL 文本，用于提供完整的 SQL 语句。
- `request_at`：SQL 语句的的起始时间。
- `duration_second`：SQL 语句的实际执行时间。
- `exec_plan`：SQL 语句的详细执行计划。

执行如下内容的脚本，开启慢查询：

```sql
> drop database if exists mo_ts;
> create database mo_ts;
> use mo_ts;
> create view slow_query as select statement,request_at,duration/1000000000 as duration_second from system.statement_info where statement like 'select%' and duration/1000000000>1  order by request_at desc;
> create view slow_query_with_plan as select statement,request_at,duration/1000000000 as duration_second,exec_plan from system.statement_info where statement like 'select%' and duration/1000000000>1  order by request_at desc;
```

对于所有超过1秒的查询，可以通过如下语句：

```sql
> select * from mo_ts.slow_query;
> select * from mo_ts.slow_query_with_plan;
```

**语句解释**

- `select * from mo_ts.slow_query;` ：不带执行计划。

- `select * from mo_ts.slow_query_with_plan;` ：带执行计划。

## 错误日志

在开启了慢查询的情况下，可以开启错误日志，检查日志，定位错误信息。

### 开启错误日志

执行如下内容脚本：

```sql
> create database mo_ts;
> use mo_ts;
> create view error_message as select timestamp,message from system.log_info where level in ('error','panic','faltal');
> create view error_sql as select si.request_at time_stamp,si.statement as SQL,el.err_code from statement_info si,error_info el where si.statement_id=el.statement_id and user<>'internal';
```

查询数据库服务错误，执行如下 SQL：

```sql
> select * from mo_ts.error_message;
```

查询 SQL 错误，执行如下命令：

```sql
> select * from mo_ts.error_sql;
```
