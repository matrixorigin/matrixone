drop user if exists rr_dml_idx_33404_user;
drop role if exists rr_dml_idx_33404_role;
drop database if exists rr_dml_idx_33404;
create database rr_dml_idx_33404;
create table rr_dml_idx_33404.items (id int primary key, scope_key int, amount int);
create index rr_idx_scope on rr_dml_idx_33404.items (scope_key);
create index rr_idx_amount on rr_dml_idx_33404.items (amount);
insert into rr_dml_idx_33404.items values (1, 1, 10), (2, 2, 20), (3, 1, 30);
create table rr_dml_idx_33404.rates (id int primary key, adjustment int);
insert into rr_dml_idx_33404.rates values (1, 2);
create role rr_dml_idx_33404_role;
grant connect on account * to rr_dml_idx_33404_role;
grant select, update, delete on table rr_dml_idx_33404.items to rr_dml_idx_33404_role;
grant select on table rr_dml_idx_33404.rates to rr_dml_idx_33404_role;
alter role rr_dml_idx_33404_role add rule 'select amount as Amount, id as ID from rr_dml_idx_33404.items where scope_key = 1' on table rr_dml_idx_33404.items;
create user rr_dml_idx_33404_user identified by 'LocalFixturePass01' default role rr_dml_idx_33404_role;

-- Confirm the invoker sees the authorized rows and public index columns.
-- @session:id=1&user=sys:rr_dml_idx_33404_user:rr_dml_idx_33404_role&password=LocalFixturePass01
use rr_dml_idx_33404;
set enable_remap_hint = 0;
-- @metacmp(true)
select * from items order by id;
select index_name, column_name from information_schema.statistics
where table_schema = 'rr_dml_idx_33404' and table_name = 'items'
and index_name in ('rr_idx_amount', 'rr_idx_scope') order by index_name;
show index from items;
explain update items set amount = amount + 1 where id = 1;

-- Exercise governed writes and both RETURNING star forms.
update items set amount = amount + 1 where id = 2;
select row_count();
-- @metacmp(true)
update items set amount = amount + 1 where id = 1 returning *;
update items i join rates r on r.id = i.id
set i.amount = i.amount + r.adjustment where i.id = 1;
update items set amount = amount + 1 where scope_key = 1;
set enable_remap_hint = 1;
-- @metacmp(true)
/*+ {"rewrites":{"rr_dml_idx_33404.items":"select * from rr_dml_idx_33404.items"}} */ delete from items where id = 3 returning items.*;
-- @session

-- Verify committed rows and both physical indexes through the owner.
select id, scope_key, amount from rr_dml_idx_33404.items order by id;
select index_name, column_name from information_schema.statistics
where table_schema = 'rr_dml_idx_33404' and table_name = 'items'
and index_name in ('rr_idx_amount', 'rr_idx_scope') order by index_name;
drop user rr_dml_idx_33404_user;
drop role rr_dml_idx_33404_role;
drop database rr_dml_idx_33404;
