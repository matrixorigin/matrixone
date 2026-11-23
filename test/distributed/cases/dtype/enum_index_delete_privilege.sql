-- Internal index maintenance must use the target table's authorization.
drop database if exists enum_delete_priv;
drop user if exists enum_delete_priv_user;
drop role if exists enum_delete_priv_role;
create database enum_delete_priv;
create table enum_delete_priv.modern_t (id int primary key, e enum('red','green'), n int);
insert into enum_delete_priv.modern_t values (1,'red',11);
alter table enum_delete_priv.modern_t add key ie(e);
alter table enum_delete_priv.modern_t add key inum(n);
insert into enum_delete_priv.modern_t values (2,'green',12),(3,'green',13);
create table enum_delete_priv.legacy_a (id int primary key, e enum('red','green'));
create table enum_delete_priv.legacy_b (id int primary key, e enum('red','green'));
insert into enum_delete_priv.legacy_a values (10,'red'),(11,'green');
insert into enum_delete_priv.legacy_b values (10,'red'),(11,'green');
alter table enum_delete_priv.legacy_a add key ie(e);
alter table enum_delete_priv.legacy_b add key ie(e);
create table enum_delete_priv.denied_t (id int primary key, e enum('red','green'), key ie(e));
insert into enum_delete_priv.denied_t values (1,'red');
create table enum_delete_priv.private_source (id int primary key);
insert into enum_delete_priv.private_source values (3);
create role enum_delete_priv_role;
grant connect on account * to enum_delete_priv_role;
grant select,delete on table enum_delete_priv.modern_t to enum_delete_priv_role;
grant select,delete on table enum_delete_priv.legacy_a to enum_delete_priv_role;
grant select on table enum_delete_priv.legacy_b to enum_delete_priv_role;
grant select on table enum_delete_priv.denied_t to enum_delete_priv_role;
create user enum_delete_priv_user identified by '111' default role enum_delete_priv_role;

-- @session:id=1&user=sys:enum_delete_priv_user:enum_delete_priv_role&password=111
delete from enum_delete_priv.modern_t where id=1 returning id,n;
-- @regex("do not have privilege", true)
delete from enum_delete_priv.denied_t where id=1;
-- @regex("do not have privilege", true)
delete a,b from enum_delete_priv.legacy_a a join enum_delete_priv.legacy_b b on a.id=b.id where a.id=10;
-- @regex("do not have privilege", true)
delete t from enum_delete_priv.modern_t t join enum_delete_priv.private_source s on t.id=s.id where t.id=3;
-- @session

-- Failed statements cannot change either their targets or associated indexes.
select id,e from enum_delete_priv.modern_t order by id;
select id,e from enum_delete_priv.denied_t order by id;
select id,e from enum_delete_priv.legacy_a order by id;
select id,e from enum_delete_priv.legacy_b order by id;
grant delete on table enum_delete_priv.legacy_b to enum_delete_priv_role;
grant select on table enum_delete_priv.private_source to enum_delete_priv_role;

-- @session:id=1&user=sys:enum_delete_priv_user:enum_delete_priv_role&password=111
delete a,b from enum_delete_priv.legacy_a a join enum_delete_priv.legacy_b b on a.id=b.id where a.id=10;
select row_count();
delete t from enum_delete_priv.modern_t t join enum_delete_priv.private_source s on t.id=s.id where t.id=3;
select row_count();
select id,e from enum_delete_priv.modern_t order by id;
select id,e from enum_delete_priv.legacy_a order by id;
select id,e from enum_delete_priv.legacy_b order by id;
-- @session

-- Count physical index rows as administrator after the authorized mutations.
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_delete_priv' and relname='modern_t') and name='ie' limit 1);
set @q=concat('select count(*) from enum_delete_priv.`',@idx,'`');
prepare idx_count from @q;
execute idx_count;
deallocate prepare idx_count;
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_delete_priv' and relname='legacy_a') and name='ie' limit 1);
set @q=concat('select count(*) from enum_delete_priv.`',@idx,'`');
prepare idx_count from @q;
execute idx_count;
deallocate prepare idx_count;
set @idx=(select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='enum_delete_priv' and relname='legacy_b') and name='ie' limit 1);
set @q=concat('select count(*) from enum_delete_priv.`',@idx,'`');
prepare idx_count from @q;
execute idx_count;
deallocate prepare idx_count;

drop user enum_delete_priv_user;
drop role enum_delete_priv_role;
drop database enum_delete_priv;
