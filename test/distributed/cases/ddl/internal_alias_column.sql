drop database if exists internal_alias_column;
create database internal_alias_column;
use internal_alias_column;

-- __mo_alias_ is the internal suffix namespace used for regular-index primary
-- key storage. User columns must not be allowed to collide with it.
-- @pattern
create table create_reject (
    id int primary key,
    `__mo_alias_payload` varchar(32),
    key idx_alias(`__mo_alias_payload`(2))
);

-- CTAS has a separate column-construction path.
-- @pattern
create table ctas_reject as
select 1 as id, 'alpha-one' as `__mo_alias_payload`;

create table ctas_ok as
select 1 as id, 'alpha-one' as payload;
show create table ctas_ok;

create table alter_reject (
    id int primary key,
    payload varchar(32)
);
-- @pattern
alter table alter_reject rename column payload to `__mo_alias_payload`;
-- @pattern
alter table alter_reject change column payload `__mo_alias_payload` varchar(32);
-- @pattern
alter table alter_reject add column `__mo_alias_payload` varchar(32);
show create table alter_reject;

drop database internal_alias_column;
