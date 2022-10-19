# Slow Query

Currently, the slow queries on a MatrixOne are longer than 1000 milliseconds and cannot be directly output to corresponding log files. You need to create a view to filter the query information.

## Enable Slow Query

MatrixOne slow query feature is available in version 0.6.0 with the following basic information:

- `statement`: indicates the SQL text that provides the complete SQL statement.
- `request_at`: indicates the start time of the SQL statement.
- `duration_second`: indicates the actual execution time of the SQL statement.
- `exec_plan`: indicates the detailed execution plan of the SQL statement.

To enable the slow query, execute the following SQL statements:

```sql
> drop database if exists mo_ts;
> create database mo_ts;
> use mo_ts;
> create view slow_query as select statement,request_at,duration/1000000000 as duration_second from system.statement_info where statement like 'select%' and duration/1000000000>1  order by request_at desc;
> create view slow_query_with_plan as select statement,request_at,duration/1000000000 as duration_second,exec_plan from system.statement_info where statement like 'select%' and duration/1000000000>1  order by request_at desc;
```

For all queries longer than 1 second, execute the following SQL statement to view them:

```sql
> select * from mo_ts.slow_query;
> select * from mo_ts.slow_query_with_plan;
```

**Explanations**

- `select * from mo_ts.slow_query;`: slow query without plan.

- `select * from mo_ts.slow_query_with_plan;`: slow query with plan.。

## Error Log

When slow query is enabled, you can enable error logs, check logs, and locate error information.

### Enable Error Log

To enable the error log, , execute the following SQL statements:

```sql
> create database mo_ts;
> use mo_ts;
> create view error_message as select timestamp,message from system.log_info where level in ('error','panic','faltal');
> create view error_sql as select si.request_at time_stamp,si.statement as SQL,el.err_code from statement_info si,error_info el where si.statement_id=el.statement_id and user<>'internal';
```

To query the error message of database, execute the following SQL statements:

```sql
> select * from mo_ts.error_message;
```

To query the error of SQL, execute the following SQL statements:

```sql
> select * from mo_ts.error_sql;
```
