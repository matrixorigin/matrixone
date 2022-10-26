# Common statistic data query

Statistic data is a common query periodically performed during the operation and maintenance of a database. It helps the users intuitively and accurately grasp the current database status and health status.

In MatrixOne, statistics include the following:

- **Metadata**: data that describes the database, including database information, table information, and column information.

- **SQL Statistics**: indicates whether the SQL is executed successfully, by whom, and at the start and stop time in a specific time range.

- **Role and Permission information**: You can query the authorization, permission, inheritance, execution time, and authorization person of all roles in a MatrixOne.

## Check basic information on all databases

To view the basic information on all databases of the current account, execute the SQL as below:

```sql
> select md.datname as database_name,md.created_time as created_time,mu.user_name as creator, mr.role_name as owner_role, count(mt.reldatabase) as total_tables
from mo_catalog.mo_database md,mo_catalog.mo_role mr, mo_catalog.mo_user mu, mo_catalog.mo_tables mt
where md.creator=mu.user_id and md.owner=mr.role_id and mt.reldatabase_id=md.dat_id
group by mt.reldatabase,md.datname,md.created_time,mu.user_name,mr.role_name
order by md.created_time asc;
```

## View the information on all auto increment columns

To view the information on all the auto increment columns, execute the SQL as below:

```sql
> select att_database as database_name,att_relname as table_name,attname as column_name
from mo_catalog.mo_columns
where att_is_auto_increment=1
order by att_database, att_relname asc;
```

## View the information on all the views

To view the information on all the views, execute the SQL as below:

```sql
> select mt.relname as view_name, mt.reldatabase as database_name,mu.user_name as created_user,mr.role_name as owner_role,mt.created_time
from mo_catalog.mo_tables mt, mo_catalog.mo_user mu, mo_catalog.mo_role mr
where mt.relkind='v' and mt.creator=mu.user_id and mt.owner=mr.role_id
order by 1,2 asc;
```

## View the information on all the external tables

To view the information on all the external tables, execute the SQL as below:

```sql
> select mt.relname as view_name, mt.reldatabase as database_name,mu.user_name as created_user,mr.role_name as owner_role,mt.created_time
from mo_catalog.mo_tables mt, mo_catalog.mo_user mu, mo_catalog.mo_role mr
where mt.relkind='e' and mt.creator=mu.user_id and mt.owner=mr.role_id
order by 1,2 asc;
```

## View the information on all the primary keys of tables

To view the information on all the primary keys of tables, execute the SQL as below:

```sql
> select att_database as database_name,att_relname as table_name,attname as column_name
from mo_catalog.mo_columns
where att_constraint_type='p' and att_relname not like '%!%'
order by att_database, att_relname asc;
```

## View the information on all the tables without primary keys

To view the information on all the tables without primary keys, execute the SQL as below:

```sql
> select distinct att_database as database_name,att_relname as table_name
from mo_catalog.mo_columns
minus
select att_database as database_name,att_relname as table_name
from mo_catalog.mo_columns
where att_constraint_type='p'
order by database_name,table_name asc;
```

## View the sql statistics of the last 24 hours (not supported for non-sys account)

```sql
> select user,host,status,count(status) as count, date_sub(now(), interval 24 hour) as start_time, now() as end_time
from system.statement_info
where status in ('Success','Failed') and user <> 'internal'
and request_at between date_sub(now(), interval 24 hour) and now()
group by status,user,host
order by user,status asc;
```

## View the information on all role grant users

To view the information on all role grant users, execute the SQL as below:

```sql
> select mu.user_name as user_name,mr.role_name as role_name,mug.with_grant_option
from mo_catalog.mo_user mu, mo_catalog.mo_role mr, mo_catalog.mo_user_grant mug
where mu.user_id=mug.user_id and mr.role_id=mug.role_id
order by mu.user_name,mr.role_name asc;
```

## View the promission of all roles

To view the promission of all roles, execute the SQL as below:

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

## View all role inheritance information

To view all role inheritance information, execute the SQL as below:

```sql
> select mr1.role_name as inheritor_role,mr2.role_name as inheritee_role,mu.user_name as operator_user,mrg.granted_time,mrg.with_grant_option
from mo_catalog.mo_user mu, mo_catalog.mo_role mr1, mo_catalog.mo_role mr2,mo_catalog.mo_role_grant mrg
where mu.user_id=mrg.operation_user_id and mr1.role_id=mrg.grantee_id and mr2.role_id=mrg.granted_id
order by mr1.role_name,mr2.role_name asc;
```
