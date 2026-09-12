-- Regression for issue #26064: every copy-and-swap rebuild of an active
-- data branch must retain a live lineage edge and both protection snapshots
-- until DATA BRANCH DELETE reclaims the complete physical-generation chain.

drop database if exists issue26064_rebuild;
create database issue26064_rebuild;
use issue26064_rebuild;

-- TRUNCATE and DELETE without WHERE both use Scope.TruncateTable. Run them
-- consecutively to prove that a replacement generation can be rebuilt again.
create table rebuild_base(id int primary key, existing_value int default 5, removable_value int);
insert into rebuild_base values (1, 10, 100), (2, 20, 200);
data branch create table rebuild_branch from rebuild_base;

set @rebuild_before_truncate = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'rebuild_branch'
);
truncate table rebuild_branch;
set @rebuild_after_truncate = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'rebuild_branch'
);
delete from rebuild_branch;
set @rebuild_after_delete = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'rebuild_branch'
);

select @rebuild_before_truncate <> @rebuild_after_truncate
  and @rebuild_after_truncate <> @rebuild_after_delete
  as truncate_and_delete_replace_physical_tables;
select count(*) as retired_rebuild_generations
  from mo_catalog.mo_branch_metadata
 where table_id in (@rebuild_before_truncate, @rebuild_after_truncate)
   and table_deleted;
select p_table_id = @rebuild_after_truncate
  and level = 'alter:table'
  and not table_deleted as active_rebuild_branch
  from mo_catalog.mo_branch_metadata
 where table_id = @rebuild_after_delete;
select count(*) as rebuild_protection_snapshots
  from mo_catalog.mo_snapshots
 where kind = 'branch'
   and sname in (
     concat('__mo_branch_', cast(@rebuild_before_truncate as char)),
     concat('__mo_branch_', cast(@rebuild_after_truncate as char)),
     concat('__mo_branch_', cast(@rebuild_after_delete as char))
   );
data branch diff rebuild_branch against rebuild_base;
data branch delete table rebuild_branch;
select count(*) as rebuild_snapshots_after_branch_delete
  from mo_catalog.mo_snapshots
 where kind = 'branch'
   and sname in (
     concat('__mo_branch_', cast(@rebuild_before_truncate as char)),
     concat('__mo_branch_', cast(@rebuild_after_truncate as char)),
     concat('__mo_branch_', cast(@rebuild_after_delete as char))
   );

-- The four ALTER copy forms in the report already use the same lineage
-- protocol. Exercise them as one chain to keep their active ownership and
-- reclaim behavior covered alongside the TRUNCATE caller.
create table evolve_base(id int primary key, existing_value int default 5, removable_value int);
insert into evolve_base values (1, 10, 100), (2, 20, 200);
data branch create table evolve_branch from evolve_base;
set @evolve_initial = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'evolve_branch'
);
alter table evolve_branch add column extra_value int default 0;
set @evolve_after_add = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'evolve_branch'
);
data branch diff evolve_branch against evolve_base;
alter table evolve_branch alter column existing_value set default 7;
set @evolve_after_set_default = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'evolve_branch'
);
alter table evolve_branch drop column removable_value;
set @evolve_after_drop_column = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'evolve_branch'
);
alter table evolve_branch alter column existing_value drop default;
set @evolve_after_drop_default = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'evolve_branch'
);

select @evolve_initial <> @evolve_after_add
  and @evolve_after_add <> @evolve_after_set_default
  and @evolve_after_set_default <> @evolve_after_drop_column
  and @evolve_after_drop_column <> @evolve_after_drop_default
  as each_alter_rebuild_replaces_physical_table;
select count(*) as retired_alter_generations
  from mo_catalog.mo_branch_metadata
 where table_id in (
   @evolve_initial, @evolve_after_add, @evolve_after_set_default, @evolve_after_drop_column
 ) and table_deleted;
select p_table_id = @evolve_after_drop_column
  and level = 'alter:table'
  and not table_deleted as active_alter_branch
  from mo_catalog.mo_branch_metadata
 where table_id = @evolve_after_drop_default;
select count(*) as alter_protection_snapshots
  from mo_catalog.mo_snapshots
 where kind = 'branch'
   and sname in (
     concat('__mo_branch_', cast(@evolve_initial as char)),
     concat('__mo_branch_', cast(@evolve_after_add as char)),
     concat('__mo_branch_', cast(@evolve_after_set_default as char)),
     concat('__mo_branch_', cast(@evolve_after_drop_column as char)),
     concat('__mo_branch_', cast(@evolve_after_drop_default as char))
   );
data branch delete table evolve_branch;
select count(*) as alter_snapshots_after_branch_delete
  from mo_catalog.mo_snapshots
 where kind = 'branch'
   and sname in (
     concat('__mo_branch_', cast(@evolve_initial as char)),
     concat('__mo_branch_', cast(@evolve_after_add as char)),
     concat('__mo_branch_', cast(@evolve_after_set_default as char)),
     concat('__mo_branch_', cast(@evolve_after_drop_column as char)),
     concat('__mo_branch_', cast(@evolve_after_drop_default as char))
   );

-- A predicate keeps DELETE on the ordinary DML path: it must not create a
-- replacement generation or a second branch protection snapshot.
create table control_base(id int primary key, v int);
insert into control_base values (1, 10), (2, 20);
data branch create table control_branch from control_base;
set @control_before_delete = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'control_branch'
);
delete from control_branch where true;
set @control_after_delete = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'control_branch'
);
select @control_before_delete = @control_after_delete as predicate_delete_keeps_physical_table;
select level = 'table' and not table_deleted as predicate_delete_keeps_active_branch
  from mo_catalog.mo_branch_metadata
 where table_id = @control_after_delete;
select count(*) as predicate_delete_protection_snapshots
  from mo_catalog.mo_snapshots
 where kind = 'branch'
   and sname = concat('__mo_branch_', cast(@control_after_delete as char));
data branch delete table control_branch;

-- Explicit transactions cannot safely publish a replacement generation; the
-- statement is rejected before changing either table identity or metadata.
create table txn_base(id int primary key, v int);
insert into txn_base values (1, 10), (2, 20);
data branch create table txn_branch from txn_base;
set @txn_before_truncate = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'txn_branch'
);
begin;
-- @regex("TRUNCATE on a data-branch lineage is not supported inside an explicit transaction", true)
truncate table txn_branch;
rollback;
set @txn_after_truncate = (
  select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue26064_rebuild' and relname = 'txn_branch'
);
select @txn_before_truncate = @txn_after_truncate as rejected_truncate_is_atomic;
select level = 'table' and not table_deleted as rejected_truncate_keeps_active_branch
  from mo_catalog.mo_branch_metadata
 where table_id = @txn_after_truncate;
data branch delete table txn_branch;

drop database issue26064_rebuild;
