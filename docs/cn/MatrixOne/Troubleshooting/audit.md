# 审计

## 概述

审计是用来记录数据库用户行为以及数据库内部重要事件的功能，它记录了所有用户在登录数据库后做出的所有数据库操作以及数据内部的重大事件。也是很多企业级数据库必备的功能之一。在 MatrixOne 0.6.0 的版本中，审计功能已经具备了最初的形态，可供用户进行测试体验。为了方便用户能够更方便地获取自己所需要的相关信息，可以通过本文档进行开启。

在日常的数据库运维中，为了确保数据库用户的所有行为合规合法，审计是非常有效的手段。在数据库发生重要事件时，例如启停、节点宕机等，审计内容可以非常方便地追踪到前后时段的护具库行为。

对于重要的业务信息表或系统配置表需要进行有效完整的行为监控时，数据库审计的开启十分有必要。例如监控对用户 A 在数据库中所有行为，以便于及时发现违规的数据修改或删除来源。对于数据库内部重大事件的监控，可以第一时间排查故障，并且追溯事故产生的根本原因。

## 开启审计

执行如下内容脚本，开启审计功能：

```sql
> drop database if exists mo_audits;
> create database mo_audits;
> use mo_audits;
> create view mo_user_action as select request_at,user,host,statement,status from system.statement_info where user<>'internal' order by request_at desc;
> create view mo_events as select timestamp,level,message from system.log_info where level in ('error','panic','fatal') order by timestamp desc;
```

## 审计查询

对用户行为进行审计时，执行下面的 SQL 语句进行查看：

```sql
> select * from mo_audits.mo_user_action;
```

查询示例结果如下：

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

查询数据库内部状态变更查询，执行下面的 SQL 语句进行查看：

```sql
> select * from mo_events;
```

查询示例结果如下：

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

## 关闭审计

执行下面的 SQL 语句，关闭审计：

```sql
> drop database if exists mo_audits;
```
