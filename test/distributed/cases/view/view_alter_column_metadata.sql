drop database if exists view_alter_column_metadata;
drop database if exists view_alter_column_metadata_cross;
create database view_alter_column_metadata;
create database view_alter_column_metadata_cross;

use view_alter_column_metadata;
create table source_t (
    id int primary key,
    code varchar(5) not null unique,
    qty int not null default 1,
    price decimal(10, 2) not null
);
insert into source_t values (1, 'short', 2, 1.25);

create view v_source_t as
select id, code, qty, price, qty * price as total from source_t;
create view v_nested as select code, qty, total from v_source_t;
create view view_alter_column_metadata_cross.v_cross as
select code, price from view_alter_column_metadata.source_t;
create table invalid_source (a int);
create view invalid_view as select a from invalid_source;
drop table invalid_source;
create table original_view_ids as
select relname, rel_id
from mo_catalog.mo_tables
where reldatabase in ('view_alter_column_metadata', 'view_alter_column_metadata_cross')
  and relname in ('v_source_t', 'v_nested', 'v_cross');

alter table source_t modify column code varchar(60) not null;
alter table source_t modify column qty bigint not null default 1;
alter table source_t modify column price decimal(20, 5) not null;

insert into source_t values
    (2, 'this-code-is-longer-than-five-characters', 5000000000, 12345.67891);

select id, code, qty, price, total from v_source_t order by id;
desc v_source_t;
desc v_nested;
desc view_alter_column_metadata_cross.v_cross;
select
    table_schema,
    table_name,
    column_name,
    column_type
from information_schema.columns
where table_schema in ('view_alter_column_metadata', 'view_alter_column_metadata_cross')
  and table_name in ('v_source_t', 'v_nested', 'v_cross')
order by table_schema, table_name, ordinal_position;
select count(*) = 3 as view_ids_preserved
from mo_catalog.mo_tables current_view
join original_view_ids original_view
  on current_view.relname = original_view.relname
 and current_view.rel_id = original_view.rel_id
where current_view.reldatabase in (
    'view_alter_column_metadata',
    'view_alter_column_metadata_cross'
);

create table ctas_view as select id, code, qty, price from v_source_t;
desc ctas_view;

drop database view_alter_column_metadata;
drop database view_alter_column_metadata_cross;
