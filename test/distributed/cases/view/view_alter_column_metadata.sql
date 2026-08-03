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

create table recreated_source (a int);
create view recreated_view as select a from recreated_source;
drop table recreated_source;
create table recreated_source (a int);
alter table recreated_source modify column a bigint;
-- @ignore:5,6
desc recreated_view;

create table `recreated\%_source` (a int);
create view recreated_quoted_view as select a from `recreated\%_source`;
drop table `recreated\%_source`;
create table `recreated\%_source` (a int);
alter table `recreated\%_source` modify column a bigint;
-- @ignore:5,6
desc recreated_quoted_view;

create table recreated_pending_live (a int);
create table recreated_pending_source (b int);
create view recreated_pending_view as
select live.a, source.b from recreated_pending_live live, recreated_pending_source source;
drop table recreated_pending_source;
alter table recreated_pending_live modify column a bigint;
create table recreated_pending_source (b decimal(11, 2));
-- @ignore:5,6
desc recreated_pending_view;

create table recreated_view_source (b int);
create view recreated_view_base as select b from recreated_view_source;
create view recreated_view_dependent as select b from recreated_view_base;
drop view recreated_view_base;
alter table recreated_view_source modify column b decimal(12, 4);
create view recreated_view_base as select b from recreated_view_source;
-- @ignore:5,6
desc recreated_view_dependent;

create table altered_view_live (a int);
create table altered_view_missing (b int);
create view altered_view_base as
select live.a, missing.b from altered_view_live live, altered_view_missing missing;
create view altered_view_dependent as select a from altered_view_base;
drop table altered_view_missing;
alter table altered_view_live modify column a bigint;
alter view altered_view_base as select a from altered_view_live;
-- @ignore:5,6
desc altered_view_dependent;

create table renamed_live (a int);
create table renamed_missing (b int);
create view renamed_dependent as
select live.a, missing.b from renamed_live live, renamed_missing missing;
drop table renamed_missing;
alter table renamed_live modify column a bigint;
create table renamed_replacement (b decimal(13, 4));
rename table renamed_replacement to renamed_missing;
-- @ignore:5,6
desc renamed_dependent;

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

use view_alter_column_metadata;
create table snapshot_live (a int);
create table snapshot_frozen (b int);
create snapshot view_alter_column_metadata_sn for account sys;
create function refresh_udf() returns int language sql as '1';
create view mixed_snapshot_v as
select live.a, frozen.b
from snapshot_live live, snapshot_frozen { snapshot = 'view_alter_column_metadata_sn' } frozen;
create view snapshot_text_v as
select a, '{snapshot is only text' as marker from snapshot_live;
create view udf_v as select a, refresh_udf() as udf_value from snapshot_live;
create table sql_mode_source (a varchar(5));
set sql_mode = 'PIPES_AS_CONCAT';
create view sql_mode_v as select a || '-suffix' as combined from sql_mode_source;
set sql_mode = '';
alter table sql_mode_source modify column a varchar(20);
-- @ignore:5,6
desc sql_mode_v;
use view_alter_column_metadata_cross;
alter table view_alter_column_metadata.snapshot_live modify column a bigint;
-- @ignore:5,6
desc view_alter_column_metadata.mixed_snapshot_v;
-- @ignore:5,6
desc view_alter_column_metadata.snapshot_text_v;
-- @ignore:5,6
desc view_alter_column_metadata.udf_v;
drop function view_alter_column_metadata.refresh_udf();
alter table view_alter_column_metadata.snapshot_live modify column a decimal(12, 2);
-- @ignore:5,6
desc view_alter_column_metadata.udf_v;
drop snapshot view_alter_column_metadata_sn;

create table deleted_snapshot_live (a int);
create table deleted_snapshot_frozen (b int);
create snapshot deleted_snapshot_sn for account sys;
create view deleted_snapshot_v as
select live.a, frozen.b
from deleted_snapshot_live live,
deleted_snapshot_frozen {snapshot = 'deleted_snapshot_sn'} frozen;
drop snapshot deleted_snapshot_sn;
alter table deleted_snapshot_live modify column a bigint;
-- @ignore:5,6
desc deleted_snapshot_v;

create table pending_live_one (a int);
create table pending_live_two (a int);
create table pending_frozen (b int);
create snapshot pending_snapshot_one for account sys;
create snapshot pending_snapshot_two for account sys;
create view pending_view_one as
select live.a, frozen.b from pending_live_one live,
pending_frozen {snapshot = 'pending_snapshot_one'} frozen;
create view pending_view_two as
select live.a, frozen.b from pending_live_two live,
pending_frozen {snapshot = 'pending_snapshot_two'} frozen;
drop snapshot pending_snapshot_one;
drop snapshot pending_snapshot_two;
alter table pending_live_one modify column a bigint;
alter table pending_live_two modify column a decimal(10, 3);
create snapshot pending_snapshot_two for account sys;
create snapshot pending_snapshot_one for account sys;
-- @ignore:5,6
desc pending_view_one;
-- @ignore:5,6
desc pending_view_two;
drop snapshot pending_snapshot_one;
drop snapshot pending_snapshot_two;

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
create table excluded_t (a int);
create publication pub database pubdb table source_t, excluded_t account view_alter_sub;
create publication account_pub database * account view_alter_sub;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
create database subdb from view_alter_pub publication pub;
create database account_subdb from view_alter_pub publication account_pub;
drop database account_subdb;
create database localdb;
use localdb;
create table local_source_t (a int);
create view v as select a from subdb.source_t;
create snapshot view_alter_sub_sn for account view_alter_sub;
create view localdb.snapshot_sub_v as
select live.a, frozen.a as frozen_a
from localdb.local_source_t live,
subdb.source_t {snapshot = 'view_alter_sub_sn'} frozen;
alter table localdb.local_source_t modify column a bigint;
-- @ignore:5,6
desc localdb.snapshot_sub_v;
create snapshot view_alter_retry_sn for account view_alter_sub;
create view localdb.snapshot_retry_v as
select live.a, frozen.a as frozen_a
from localdb.local_source_t live,
subdb.source_t {snapshot = 'view_alter_retry_sn'} frozen;
drop snapshot view_alter_retry_sn;
alter table localdb.local_source_t modify column a decimal(9, 1);
create snapshot view_alter_retry_sn for account view_alter_sub;
-- @ignore:5,6
desc localdb.snapshot_retry_v;
drop snapshot view_alter_retry_sn;
create view localdb.subscription_recreate_v as select b from subdb.source_t;
drop database subdb;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter table pubdb.source_t modify column a bigint;
alter table pubdb.source_t modify column b decimal(13, 5);
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
create database subdb from view_alter_pub publication pub;
-- @ignore:5,6
desc localdb.subscription_recreate_v;
-- @ignore:5,6
desc localdb.v;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter publication pub database pubdb table excluded_t;
alter table pubdb.source_t modify column a decimal(10, 2);
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
select column_type
from information_schema.columns
where table_schema = 'localdb' and table_name = 'v' and column_name = 'a';
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter publication pub database pubdb table source_t, excluded_t;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
-- @ignore:5,6
desc localdb.v;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter table pubdb.source_t modify column a decimal(12, 3);
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
-- @ignore:5,6
desc localdb.v;

-- @session:id=1&user=view_alter_pub:admin&password=111
create database pubdb2;
create table pubdb2.source_t (a varchar(20));
create publication pub2 database pubdb2 table source_t account view_alter_sub;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
drop database subdb;
create database subdb from view_alter_pub publication pub2;
create snapshot view_alter_sub_sn2 for account view_alter_sub;
create view localdb.dual_snapshot_sub_v as
select live.a, old_source.a as old_a, new_source.a as new_a
from localdb.local_source_t live,
subdb.source_t {snapshot = 'view_alter_sub_sn'} old_source,
subdb.source_t {snapshot = 'view_alter_sub_sn2'} new_source;
alter table localdb.local_source_t modify column a decimal(10, 2);
-- @ignore:5,6
desc localdb.dual_snapshot_sub_v;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
alter table pubdb2.source_t modify column a bigint;
-- @session

-- @session:id=2&user=view_alter_sub:admin&password=111
-- @ignore:5,6
desc localdb.v;
drop snapshot view_alter_sub_sn2;
drop snapshot view_alter_sub_sn;
drop database localdb;
drop database subdb;
-- @session

-- @session:id=1&user=view_alter_pub:admin&password=111
drop publication pub2;
drop database pubdb2;
drop publication account_pub;
drop publication pub;
drop database pubdb;
-- @session

drop account view_alter_pub;
drop account view_alter_sub;
