-- Historical schema and data do not preserve withdrawn publication access.
drop snapshot if exists view_subscription_snapshot;
drop publication if exists view_subscription_foreign_pub;
drop database if exists view_subscription_foreign_source;
drop account if exists view_subscription_reader;
drop publication if exists view_subscription_pub;
drop publication if exists view_subscription_other_pub;
drop database if exists view_subscription_source;
drop database if exists view_subscription_remap;
create account view_subscription_reader admin_name 'admin' identified by '111';
create database view_subscription_source;
create table view_subscription_source.src (x varchar(8));
insert into view_subscription_source.src values ('old');
create view view_subscription_source.v as select x from view_subscription_source.src;
create database view_subscription_remap;
create table view_subscription_remap.src (x varchar(37));
insert into view_subscription_remap.src values ('remapped-value');
create view view_subscription_remap.v as select x from view_subscription_remap.src;
create publication view_subscription_pub database view_subscription_source account view_subscription_reader;

-- The subscriber has no local database matching the qualified publisher source.
-- @session:id=1&user=view_subscription_reader:admin&password=111
create database subscribed from sys publication view_subscription_pub;
select x from subscribed.v;
create snapshot view_subscription_snapshot for account;
-- @session
alter table view_subscription_source.src modify column x varchar(24);
update view_subscription_source.src set x='new';
-- @session:id=2&user=view_subscription_reader:admin&password=111
select x from subscribed.v;
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select character_maximum_length from information_schema.columns where table_schema='subscribed' and table_name='v';
select character_maximum_length from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session

alter publication view_subscription_pub account drop view_subscription_reader;
-- @session:id=3&user=view_subscription_reader:admin&password=111
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select count(*) from information_schema.columns where table_schema='subscribed' and table_name='v';
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session
alter publication view_subscription_pub account add view_subscription_reader;
-- @session:id=4&user=view_subscription_reader:admin&password=111
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session

alter publication view_subscription_pub table src;
-- @session:id=5&user=view_subscription_reader:admin&password=111
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
select character_maximum_length from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='src';
-- @session
drop publication view_subscription_pub;
create publication view_subscription_pub database view_subscription_source account view_subscription_reader;
-- @session:id=8&user=view_subscription_reader:admin&password=111
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session
drop publication view_subscription_pub;
-- @session:id=6&user=view_subscription_reader:admin&password=111
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session

-- Both sources existed at T. Metadata and SELECT must agree on the source admitted now.
create publication view_subscription_pub database view_subscription_remap account view_subscription_reader;
-- @session:id=7&user=view_subscription_reader:admin&password=111
select x from subscribed.v;
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select character_maximum_length from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- Alias membership is historical schema, not publication authorization.
drop database subscribed;
create database subscribed_late from sys publication view_subscription_pub;
select count(*) from information_schema.columns where table_schema='subscribed' and table_name='v';
select count(*) from information_schema.columns where table_schema='subscribed_late' and table_name='v';
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed_late' and table_name='v';
select x from subscribed.v {snapshot='view_subscription_snapshot'};
drop database subscribed_late;

-- A cached historical handle must ignore an unrelated, denied current alias.
create database subscribed from sys publication view_subscription_pub;
prepare view_subscription_history_stmt from 'select x from subscribed.v {snapshot=''view_subscription_snapshot''}';
execute view_subscription_history_stmt;
-- @session
create publication view_subscription_other_pub database view_subscription_source account view_subscription_reader;
-- @session:id=9&user=view_subscription_reader:admin&password=111
drop database subscribed;
create database subscribed from sys publication view_subscription_other_pub;
select x from subscribed.v;
-- @session
alter publication view_subscription_other_pub account drop view_subscription_reader;
-- @session:id=7&user=view_subscription_reader:admin&password=111
select x from subscribed.v;
select x from subscribed.v {snapshot='view_subscription_snapshot'};
select character_maximum_length from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
execute view_subscription_history_stmt;
prepare view_subscription_fresh_stmt from 'select x from subscribed.v {snapshot=''view_subscription_snapshot''}';
execute view_subscription_fresh_stmt;
-- @session
alter publication view_subscription_pub account drop view_subscription_reader;
-- @session:id=7&user=view_subscription_reader:admin&password=111
execute view_subscription_history_stmt;
execute view_subscription_fresh_stmt;
select count(*) from information_schema.columns {snapshot='view_subscription_snapshot'} where table_schema='subscribed' and table_name='v';
-- @session
alter publication view_subscription_pub account add view_subscription_reader;
-- @session:id=7&user=view_subscription_reader:admin&password=111
execute view_subscription_history_stmt;
execute view_subscription_fresh_stmt;
deallocate prepare view_subscription_history_stmt;
deallocate prepare view_subscription_fresh_stmt;
drop database subscribed;
drop snapshot view_subscription_snapshot;
-- @session

-- A remapped foreign source is identified by the live publication's database
-- ID, not by a same-named publisher database visible at historical T.
create database view_subscription_foreign_source;
create table view_subscription_foreign_source.src(x varchar(8)) comment='publisher-decoy';
insert into view_subscription_foreign_source.src values ('decoy-T');
create view view_subscription_foreign_source.v as select x from view_subscription_foreign_source.src;
-- @session:id=10&user=view_subscription_reader:admin&password=111
create database view_subscription_foreign_source;
create table view_subscription_foreign_source.src(x varchar(37)) comment='physical-owner';
insert into view_subscription_foreign_source.src values ('owner-before');
create view view_subscription_foreign_source.v as select x from view_subscription_foreign_source.src;
-- @session
create publication view_subscription_foreign_pub database view_subscription_source account view_subscription_reader;
-- @session:id=10&user=view_subscription_reader:admin&password=111
create database foreign_alias from sys publication view_subscription_foreign_pub;
create snapshot view_subscription_foreign_snapshot for account;
-- @session
drop publication view_subscription_foreign_pub;
drop database view_subscription_foreign_source;
create publication view_subscription_foreign_pub database view_subscription_foreign_source account view_subscription_reader;
create database view_subscription_foreign_source;
create table view_subscription_foreign_source.src(x varchar(16)) comment='current-decoy';
insert into view_subscription_foreign_source.src values ('decoy-now');
create view view_subscription_foreign_source.v as select x from view_subscription_foreign_source.src;
-- @session:id=10&user=view_subscription_reader:admin&password=111
alter table view_subscription_foreign_source.src modify x varchar(51);
update view_subscription_foreign_source.src set x='owner-now';
select x from foreign_alias.src;
select x from foreign_alias.src {snapshot='view_subscription_foreign_snapshot'};
select x from foreign_alias.v {snapshot='view_subscription_foreign_snapshot'};
select table_name, character_maximum_length from information_schema.columns where table_schema='foreign_alias' and column_name='x' order by table_name;
select table_name, character_maximum_length from information_schema.columns {snapshot='view_subscription_foreign_snapshot'} where table_schema='foreign_alias' and column_name='x' order by table_name;
select table_name, table_comment from information_schema.tables {snapshot='view_subscription_foreign_snapshot'} where table_schema='foreign_alias' and table_name='src';
prepare view_subscription_foreign_stmt from 'select x from foreign_alias.v {snapshot=''view_subscription_foreign_snapshot''}';
execute view_subscription_foreign_stmt;
-- @session
alter publication view_subscription_foreign_pub account drop view_subscription_reader;
-- @session:id=10&user=view_subscription_reader:admin&password=111
execute view_subscription_foreign_stmt;
select count(*) from information_schema.columns {snapshot='view_subscription_foreign_snapshot'} where table_schema='foreign_alias';
-- @session
alter publication view_subscription_foreign_pub account add view_subscription_reader;
-- @session:id=10&user=view_subscription_reader:admin&password=111
execute view_subscription_foreign_stmt;
deallocate prepare view_subscription_foreign_stmt;
drop database foreign_alias;
drop snapshot view_subscription_foreign_snapshot;
-- @session
drop publication view_subscription_foreign_pub;
-- @session:id=10&user=view_subscription_reader:admin&password=111
drop database view_subscription_foreign_source;
-- @session
drop database view_subscription_foreign_source;

drop account view_subscription_reader;
drop publication view_subscription_pub;
drop publication view_subscription_other_pub;
drop database view_subscription_source;
drop database view_subscription_remap;
