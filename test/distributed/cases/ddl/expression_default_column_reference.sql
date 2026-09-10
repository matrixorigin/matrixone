-- Expression defaults must be evaluated from the current materialized row.
-- Keep this case small and deterministic: every assertion is a persisted value
-- or an equality relation, not a plan-shape-only check.

drop database if exists expression_default_colref_28450;
create database expression_default_colref_28450;
use expression_default_colref_28450;

-- Multi-level defaults, explicit DEFAULT, an index consumer, and prepared INSERT.
create table t_chain (
    id int primary key,
    a int,
    b int default (a + 1),
    c int default (b + 1),
    key idx_b (b)
);
insert into t_chain (id, a) values (1, 10), (2, 20);
insert into t_chain (id, a, b, c) values (3, 30, default, default);
prepare expression_default_insert from 'insert into t_chain (id, a, b, c) values (?, ?, default, default)';
set @expression_default_id = 4;
set @expression_default_a = 40;
execute expression_default_insert using @expression_default_id, @expression_default_a;
deallocate prepare expression_default_insert;
select id, a, b, c from t_chain order by id;
select id, b from t_chain where b between 11 and 41 order by id;

-- A volatile source is evaluated once per row and its dependent default reads
-- that stored value. The explicit b override must remain untouched.
create table t_volatile (
    id int primary key,
    a double default (rand()),
    b double default (a)
);
insert into t_volatile (id) values (1), (2);
insert into t_volatile (id, b) values (3, default), (4, 7);
select id,
       case when a = b then 'same-source' else 're-evaluated' end as dependency,
       case when b = 7 then 'explicit' else 'default' end as b_origin
from t_volatile order by id;

-- A generated column must consume the same materialized volatile default,
-- rather than inlining and executing RAND() a second time. Cover INSERT and
-- REPLACE, which use separate projection builders.
create table t_generated_volatile (
    id int primary key,
    a double default (rand()),
    g double generated always as (a) stored
);
insert into t_generated_volatile (id) values (1), (2);
select id,
       case when a = g then 'same-source' else 're-evaluated' end as dependency
from t_generated_volatile order by id;
replace into t_generated_volatile (id) values (1);
select id,
       case when a = g then 'same-source' else 're-evaluated' end as dependency
from t_generated_volatile order by id;

-- Mixed explicit/DEFAULT rows prove that one row cannot trigger another row's
-- default expression. The NULL source remains NULL through the dependency.
create table t_rows (
    id int primary key,
    a int,
    b int default (a + 1)
);
insert into t_rows (id, a, b) values (1, 10, default), (2, 20, 99), (3, null, default);
insert into t_rows (b, id, a) values (default, 4, 40);
select id, a, b from t_rows order by id;

-- INSERT ... SELECT uses the source row as the materialized input for the
-- dependent default, including a NULL source value.
create table t_select_source (id int, a int);
insert into t_select_source values (5, 50), (6, null);
create table t_select (
    id int primary key,
    a int,
    b int default (a + 1)
);
insert into t_select (id, a) select id, a from t_select_source;
select id, a, b from t_select order by id;

-- UPDATE DEFAULT uses the row image produced by preceding assignments.
create table t_update (
    id int primary key,
    a int default 5,
    b int default (a + 1)
);
insert into t_update values (1, 10, 20);
update t_update set a = 100 where id = 1;
select id, a, b from t_update;
update t_update set a = default, b = default where id = 1;
select id, a, b from t_update;

-- REPLACE and ODKU both consume the same default dependency contract.
create table t_replace (
    id int primary key,
    a int default 7,
    b int default (a + 1)
);
insert into t_replace values (1, 10, 20);
replace into t_replace (id) values (1);
select id, a, b from t_replace;

create table t_odku (
    id int primary key,
    a int default 8,
    b int default (a + 1)
);
insert into t_odku values (1, 10, 20);
insert into t_odku (id) values (1) on duplicate key update a = default, b = default;
select id, a, b from t_odku;

-- ADD backfills existing rows, and MODIFY ... FIRST remaps the persisted
-- reference before a subsequent insert.
create table t_alter (a int, c int);
insert into t_alter values (1, 10), (2, 20);
alter table t_alter add column b int default (a + c);
select a, b, c from t_alter order by a;
alter table t_alter modify column b int default (c + 1) first;
insert into t_alter (a, c) values (10, 20);
select b, a, c from t_alter order by a;

-- CTAS can place target-only columns before SELECT columns. Persisted
-- DEFAULT coordinates must follow the final table order for later DML.
create table t_ctas (
    a int,
    b int default (a + 1)
) as select 10 as a;
insert into t_ctas (a) values (20);
select a, b from t_ctas order by a;

-- CTAS must not publish an order that SHOW CREATE/LIKE cannot replay.
-- @regex("defined after it", true)
create table t_ctas_expression (
    a int default (1 + 1),
    b int default (a + 1)
) as select 10 as a;
select count(*) as unpublished from information_schema.tables where table_schema = database() and table_name = 't_ctas_expression';

-- Inherited source defaults are bound against the source table order, not the
-- SELECT output order. Reordering the source columns must preserve b= a+1.
create table t_ctas_source (a int, b int default (a + 1));
insert into t_ctas_source (a) values (10);
create table t_ctas_source_reordered as select b, a from t_ctas_source;
insert into t_ctas_source_reordered (a) values (20);
select b, a from t_ctas_source_reordered order by a;

-- An explicit target default replaces an inherited source default. The source
-- dependency is not in the SELECT output, but it must not reject the valid
-- target definition or leak into the persisted target metadata.
create table t_ctas_explicit_override_source (a int, b int default (a + 1));
insert into t_ctas_explicit_override_source (a) values (10);
create table t_ctas_explicit_override (b int default 0)
as select b from t_ctas_explicit_override_source;
select b from t_ctas_explicit_override;
insert into t_ctas_explicit_override values ();
select b from t_ctas_explicit_override order by b;

-- Combine an explicit column-reference override with a target-only default.
-- The target-only x column is stored before the SELECT columns, while the
-- explicit b default must be bound to the final target row schema.
create table t_ctas_explicit_reference_source (a int, b int default (a + 1), c int);
insert into t_ctas_explicit_reference_source (a, c) values (10, 30);
create table t_ctas_explicit_reference (
    b int default (c + 10),
    c int,
    x int default (100)
) as select b, c from t_ctas_explicit_reference_source;
select x, b, c from t_ctas_explicit_reference;
insert into t_ctas_explicit_reference (c) values (50);
select x, b, c from t_ctas_explicit_reference order by c;

-- Without an explicit override, an inherited default whose source dependency
-- is omitted remains invalid and is rejected at DDL time.
-- @regex("cannot inherit default", true)
create table t_ctas_missing_source as select b from t_ctas_explicit_override_source;

-- Moving a source column must update references in neighboring DEFAULTs.
-- This specifically covers the old-position slot, which a delete/insert
-- shift cannot represent.
create table t_move (a int, b int default (a + 1), c int);
insert into t_move (a, c) values (10, 30);
alter table t_move modify column a int after b;
insert into t_move (a, c) values (20, 40);
select a, b, c from t_move order by a;

-- LIKE retains the dependency metadata and evaluates it on the new table.
create table t_like like t_chain;
insert into t_like (id, a) values (10, 50);
select id, a, b, c from t_like;

-- LOAD must normalize file order before evaluating dependency levels.
create table t_load(a bigint, b bigint, c bigint default (a+1), d bigint default (c+1));
load data inline format='csv', data='20,10\n40,30\n' into table t_load fields terminated by ',' (b,a);
select a,b,c,d from t_load order by a;
load data inline format='csv', data='50\n' into table t_load fields terminated by ',' (b);
select b, c is null as null_c, d is null as null_d from t_load where b=50;
create table t_load_volatile(id int, a double default (rand()), b double default (a));
load data inline format='csv', data='1\n2\n' into table t_load_volatile fields terminated by ',' (id);
select id, a=b as same_source from t_load_volatile order by id;
load data inline format='csv', data='3,7\n' into table t_load_volatile fields terminated by ',' (id,b);
select id,b from t_load_volatile where id=3;

-- CTAS rebinds inherited defaults against overridden target types and names.
create table t_ctas_src(a bigint, b bigint default (a+a));
insert into t_ctas_src(a) values(3);
create table t_ctas_type(a varchar(20)) as select a,b from t_ctas_src;
insert into t_ctas_type(a) values('10');
select a,b from t_ctas_type order by b;
create table t_ctas_alias as select a as x,b from t_ctas_src;
create table t_ctas_alias_copy like t_ctas_alias;
insert into t_ctas_alias_copy(x) values(7);
select x,b from t_ctas_alias_copy;

drop database expression_default_colref_28450;
