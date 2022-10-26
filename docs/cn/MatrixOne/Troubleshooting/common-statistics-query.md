# 常用统计数据查询

统计数据是数据库在运维使用的过程中周期性进行的常用查询，可以帮助数据库用户较为直观准确地掌握当前数据库的状态以及健康程度。

在 MatrixOne 中，统计数据包含了如下几方面的内容：

- 元数据（Meatadata）：描述数据库的数据，包含数据库信息、表信息、列信息。
- SQL 统计：在特定时间范围内，SQL 的执行成功与否、执行用户、起始与停止时间。
- 角色与权限信息：通过查询，获取到 MatrixOne 下所有角色的授权、权限、继承信息，以及执行时间与授权人。

## 查看当前租户下所有数据库基本信息

```sql
> select md.datname as database_name,md.created_time as created_time,mu.user_name as creator, mr.role_name as owner_role, count(mt.reldatabase) as total_tables
from mo_catalog.mo_database md,mo_catalog.mo_role mr, mo_catalog.mo_user mu, mo_catalog.mo_tables mt
where md.creator=mu.user_id and md.owner=mr.role_id and mt.reldatabase_id=md.dat_id
group by mt.reldatabase,md.datname,md.created_time,mu.user_name,mr.role_name
order by md.created_time asc;
```

## 查看所有的自增列相关信息

```sql
> select att_database as database_name,att_relname as table_name,attname as column_name
from mo_catalog.mo_columns
where att_is_auto_increment=1
order by att_database, att_relname asc;
```

## 查看所有视图

```sql
> select mt.relname as view_name, mt.reldatabase as database_name,mu.user_name as created_user,mr.role_name as owner_role,mt.created_time
from mo_catalog.mo_tables mt, mo_catalog.mo_user mu, mo_catalog.mo_role mr
where mt.relkind='v' and mt.creator=mu.user_id and mt.owner=mr.role_id
order by 1,2 asc;
```

## 查看所有外部表

```sql
> select mt.relname as view_name, mt.reldatabase as database_name,mu.user_name as created_user,mr.role_name as owner_role,mt.created_time
from mo_catalog.mo_tables mt, mo_catalog.mo_user mu, mo_catalog.mo_role mr
where mt.relkind='e' and mt.creator=mu.user_id and mt.owner=mr.role_id
order by 1,2 asc;
```

## 查看所有表的主键

```sql
> select att_database as database_name,att_relname as table_name,attname as column_name
from mo_catalog.mo_columns
where att_constraint_type='p' and att_relname not like '%!%'
order by att_database, att_relname asc;
```

## 查看所有没有主键的表

```sql
> select distinct att_database as database_name,att_relname as table_name
from mo_catalog.mo_columns
minus
select att_database as database_name,att_relname as table_name
from mo_catalog.mo_columns
where att_constraint_type='p'
order by database_name,table_name asc;
```

## 查看过去24小时内的sql统计（非sys租户暂不支持）

```sql
> select user,host,status,count(status) as count, date_sub(now(), interval 24 hour) as start_time, now() as end_time
from system.statement_info
where status in ('Success','Failed') and user <> 'internal'
and request_at between date_sub(now(), interval 24 hour) and now()
group by status,user,host
order by user,status asc;
```

## 查看所有角色授予用户信息

```sql
> select mu.user_name as user_name,mr.role_name as role_name,mug.with_grant_option
from mo_catalog.mo_user mu, mo_catalog.mo_role mr, mo_catalog.mo_user_grant mug
where mu.user_id=mug.user_id and mr.role_id=mug.role_id
order by mu.user_name,mr.role_name asc;
```

## 查看所有角色权限信息

```sql
> select mrp.role_name,mrp.privilege_name,mrp.obj_type,mrp.privilege_level,md.datname as object_name,with_grant_option
from mo_catalog.mo_role_privs mrp, mo_catalog.mo_database md
where mrp.obj_id=md.dat_id and mrp.obj_type='database'
union
select mrp.role_name,mrp.privilege_name,mrp.obj_type,mrp.privilege_level,'*',with_grant_option
from mo_catalog.mo_role_privs mrp
where obj_id=0
union
select mrp.role_name,mrp.privilege_name,mrp.obj_type,mrp.privilege_level,mt.relname as object_name,with_grant_option
from mo_catalog.mo_role_privs mrp, mo_catalog.mo_tables mt
where mrp.obj_id=mt.rel_id and mrp.obj_type='table'
order by 1,2 asc;
```

## 查看所有角色继承信息

```sql
> select mr1.role_name as inheritor_role,mr2.role_name as inheritee_role,mu.user_name as operator_user,mrg.granted_time,mrg.with_grant_option
from mo_catalog.mo_user mu, mo_catalog.mo_role mr1, mo_catalog.mo_role mr2,mo_catalog.mo_role_grant mrg
where mu.user_id=mrg.operation_user_id and mr1.role_id=mrg.grantee_id and mr2.role_id=mrg.granted_id
order by mr1.role_name,mr2.role_name asc;
```
