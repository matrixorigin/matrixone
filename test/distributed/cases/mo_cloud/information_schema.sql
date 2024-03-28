select
    collation_name,
    character_set_name,
    lower(is_default) = 'yes' as is_default
from
    information_schema.collations;

select
     grantee,
     table_schema,
     privilege_type,
     is_grantable
from
    information_schema.schema_privileges;

select
     from_host,
     from_user,
     to_host,
     to_user,
     with_admin_option
from
     mysql.role_edges
order by
     to_user,
     to_host;

select
     event_name,
     event_comment,
     definer,
     event_type = 'RECURRING' recurring,
     interval_value,
     interval_field,
     cast(coalesce(starts, execute_at) as char) starts,
     cast(ends as char) ends,
     status,
     on_completion = 'PRESERVE' preserve,
     last_executed
from
     information_schema.events
where
     event_schema = 'system';

select
     c.constraint_name,
     c.constraint_schema,
     c.table_name,
     c.constraint_type,
     c.enforced = 'YES' enforced
from
    information_schema.table_constraints c
where
    c.table_schema = 'system';

select
    from_host,
    from_user,
    to_host,
    to_user,
    with_admin_option
from
    mysql.role_edges
order by
    to_user,
    to_host;

select
    grantee,
    table_name,
    column_name,
    privilege_type,
    is_grantable
from
    information_schema.column_privileges
where
    table_schema = 'system'
    union all
select
    grantee,
    table_name,
    '' column_name,
    privilege_type,
    is_grantable
from
    information_schema.table_privileges
where
    table_schema = 'system'
order by
    table_name,
    grantee,
    privilege_type;

select
    grantee,
    table_name,
    column_name,
    privilege_type,
    is_grantable
from
    information_schema.column_privileges
where
    table_schema = 'system_metrics'
union all
select
    grantee,
    table_name,
    '' column_name,
    privilege_type,
    is_grantable
from
    information_schema.table_privileges
where
    table_schema = 'system_metrics'
order by
    table_name,
    grantee,
    privilege_type;
select table_name from information_schema.tables where table_schema collate utf8_general_ci = 'information_schema' and table_name collate utf8_general_ci = 'parameters';