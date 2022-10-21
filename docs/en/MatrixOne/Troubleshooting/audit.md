# Audit

## Overview

The audit function records user behaviors and major internal events in the database. After logging in to the database, it registers all users' database operations and major internal events. It is also one of the essential features of many enterprise databases. With MatrixOne 0.6.0, the audit capability is in its initial form and is available for testing. To facilitate users obtaining the required information, you can use this document to enable this function.

In daily database operation and maintenance, an audit is a very effective means to ensure that all behaviors of database users comply with the law. When important events occur in the database, such as start and stop, node breakdown, etc., the audit content can be very convenient for tracing the behavior of the protective device library before and after the period.

It is necessary to enable database audit when you need to effectively and completely monitor necessary service information tables or system configuration tables. For example, it monitors all user A's behaviors in the database to discover the source of data modification or deletion promptly. For the monitoring of major internal events in the database, the fault can be rectified immediately, and the root cause of the accident can be traced back.

## Enable Audit

To enable the audit, execute the following SQL statements:

```sql
> drop database if exists mo_audits;
> create database mo_audits;
> use mo_audits;
> create view mo_user_action as select request_at,user,host,statement,status from system.statement_info where user<>'internal' order by request_at desc;
> create view mo_events as select timestamp,level,message from system.log_info where level in ('error','panic','fatal') order by timestamp desc;
```

## Audit Query

To audit user behaviors, execute the following SQL statement:

```sql
> select * from mo_audits.mo_user_action;
```

The example query result as below:

```
| 2022-10-18 16:45:43.508278 | dump | 0.0.0.0 | show grants for test@localhost                                                                                                                                                                                                         | Success |
| 2022-10-18 16:45:26.058345 | dump | 0.0.0.0 | show grants for root@localhost                                                                                                                                                                                                         | Success |
| 2022-10-18 16:45:18.957485 | dump | 0.0.0.0 | show grants for root@localhost@%                                                                                                                                                                                                       | Success |
| 2022-10-18 16:45:12.331454 | dump | 0.0.0.0 | show grants for root@%                                                                                                                                                                                                                 | Success |
| 2022-10-18 16:43:44.027403 | dump | 0.0.0.0 | show grants                                                                                                                                                                                                                            | Success |
| 2022-10-18 16:31:18.406944 | dump | 0.0.0.0 | select date_sub(now(), interval(1, "hour"))                                                                                                                                                                                            | Success |
| 2022-10-18 16:28:51.355151 | dump | 0.0.0.0 | create view error_message as select timestamp, message from system.log_info where level in ("error", "panic", "faltal")                                                                                                                | Success |
| 2022-10-18 16:28:51.351457 | dump | 0.0.0.0 | create view slow_query_with_plan as select statement, request_at, duration / 1000000000 as duration_second, exec_plan from system.statement_info where statement like "select%" and duration / 1000000000 > 1 order by request_at desc | Success |
| 2022-10-18 16:28:51.347598 | dump | 0.0.0.0 | create view slow_query as select statement, request_at, duration / 1000000000 as duration_second from system.statement_info where statement like "select%" and duration / 1000000000 > 1 order by request_at desc                      | Success |
| 2022-10-18 16:28:51.343752 | dump | 0.0.0.0 | show tables                                                                                                                                                                                                                            | Success |
| 2022-10-18 16:28:51.341880 | dump | 0.0.0.0 | show databases                                                                                                                                                                                                                         | Success |
| 2022-10-18 16:28:51.340159 | dump | 0.0.0.0 | use mo_ts                                                                                                                                                                                                                              | Success |
| 2022-10-18 16:28:51.339825 | dump | 0.0.0.0 | select database()                                                                                                                                                                                                                      | Success |
| 2022-10-18 16:28:51.337843 | dump | 0.0.0.0 | create database mo_ts                                                                                                                                                                                                                  | Success |
| 2022-10-18 16:28:51.335967 | dump | 0.0.0.0 | drop database if exists mo_ts                                                                                                                                                                                                          | Success |
| 2022-10-18 16:28:21.830069 | dump | 0.0.0.0 | use mo_ts                                                                                                                                                                                                                              | Failed  |
| 2022-10-18 16:28:21.829747 | dump | 0.0.0.0 | select database()                                                                                                                                                                                                                      | Success |
| 2022-10-18 16:28:21.827240 | dump | 0.0.0.0 | drop database if exists mo_ts                                                                                                                                                                                                          | Success |
```

Query database internal status change query, execute the following SQL statement to view:

```sql
> select * from mo_events;
```

The example query result as below:

```
|
| 2022-10-18 15:26:20.293735 | error | error: timeout, converted to code 20429                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 2022-10-18 15:26:20.293725 | error | failed to propose initial cluster info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 2022-10-18 15:26:20.288695 | error | failed to set initial cluster info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 2022-10-18 15:26:20.288559 | error | failed to propose initial cluster info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 2022-10-18 15:26:20.285384 | error | failed to set initial cluster info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 2022-10-18 15:26:20.285235 | error | failed to propose initial cluster info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 2022-10-18 15:26:18.473472 | error | failed to join the gossip group, 1 error occurred:
	* Failed to join 127.0.0.1:32022: dial tcp 127.0.0.1:32022: connect: connection refused                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 2022-10-18 15:26:18.469029 | error | failed to join the gossip group, 1 error occurred:
	* Failed to join 127.0.0.1:32012: dial tcp 127.0.0.1:32012: connect: connection refused       
```

## Disable Audit

To disable the audit, execute the following SQL statement:

```sql
> drop database if exists mo_audits;
```
