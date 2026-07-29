drop database if exists view_alter_column_metadata;
drop database if exists view_alter_column_metadata_cross;
create database view_alter_column_metadata;
create database view_alter_column_metadata_cross;

use view_alter_column_metadata;
create table source_t (
    id int primary key,
    code varchar(5) not null unique,
    qty int not null default 1,
    price decimal(10, 2) not null,
    unused_col int
);
insert into source_t values (1, 'short', 2, 1.25, 1);

create view v_source_t as
select id, code, qty, price, qty * price as total from source_t;
create view v_nested as select code, qty, total from v_source_t;
create view view_alter_column_metadata_cross.v_cross as
select code, price from source_t;
create table invalid_source (a int);
create view invalid_view as select a from invalid_source;
drop table invalid_source;

create table ambiguity_left (a int);
create table ambiguity_right (b int);
create view ambiguity_view as
select a from ambiguity_left, ambiguity_right;
alter table ambiguity_right rename column b to a;
-- @ignore:5,6
desc ambiguity_right;
alter table ambiguity_right rename column a to b;
-- @ignore:5,6
desc ambiguity_view;

create table original_view_metadata as
select relname, rel_id, creator, owner, created_time
from mo_catalog.mo_tables
where reldatabase in ('view_alter_column_metadata', 'view_alter_column_metadata_cross')
  and relname in ('v_source_t', 'v_nested', 'v_cross');

use view_alter_column_metadata_cross;
alter table view_alter_column_metadata.source_t modify column unused_col bigint;
alter table view_alter_column_metadata.source_t modify column code varchar(60) not null;
-- @ignore:5,6
desc view_alter_column_metadata.v_source_t;
alter table view_alter_column_metadata.source_t change column qty qty bigint not null default 1;
-- @ignore:5,6
desc view_alter_column_metadata.v_source_t;
alter table view_alter_column_metadata.source_t modify column price decimal(20, 5) not null;
-- @ignore:5,6
desc view_alter_column_metadata.v_source_t;

insert into view_alter_column_metadata.source_t values
    (2, 'this-code-is-longer-than-five-characters', 5000000000, 12345.67891, 2);

select id, code, qty, price, total from view_alter_column_metadata.v_source_t order by id;
-- @ignore:5,6
desc view_alter_column_metadata.v_source_t;
-- @ignore:5,6
desc view_alter_column_metadata.v_nested;
-- @ignore:5,6
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
select count(*) = 3 as view_metadata_preserved
from mo_catalog.mo_tables current_view
join view_alter_column_metadata.original_view_metadata original_view
  on current_view.relname = original_view.relname
 and current_view.rel_id = original_view.rel_id
 and current_view.creator = original_view.creator
 and current_view.owner = original_view.owner
 and current_view.created_time = original_view.created_time
where current_view.reldatabase in (
    'view_alter_column_metadata',
    'view_alter_column_metadata_cross'
);

create table view_alter_column_metadata.ctas_view as
select id, code, qty, price from view_alter_column_metadata.v_source_t;
-- @ignore:5,6
desc view_alter_column_metadata.ctas_view;

drop database view_alter_column_metadata;
drop database view_alter_column_metadata_cross;

drop account if exists view_alter_pub;
drop account if exists view_alter_sub;
create account view_alter_pub admin_name = 'admin' identified by '111';
create account view_alter_sub admin_name = 'admin' identified by '111';

-- @session:id=1&user=view_alter_pub:admin&password=111
create database pubdb;
use pubdb;
create table source_t (a int, b int);
create publication pub database pubdb account view_alter_sub;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
create database subdb from view_alter_pub publication pub;
create database localdb;
use localdb;
create view v as select a from subdb.source_t;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter table pubdb.source_t modify column a bigint;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
-- @ignore:5,6
desc localdb.v;
drop database localdb;
drop database subdb;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
drop publication pub;
drop database pubdb;
-- @session

drop account view_alter_pub;
drop account view_alter_sub;
