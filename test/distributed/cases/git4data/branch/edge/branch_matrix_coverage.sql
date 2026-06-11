-- Data branch coverage smoke for table types, column types and column constraints.

-- Case 1: remaining column type families can be branched and merged.
drop database if exists br_matrix_types;
create database br_matrix_types;
use br_matrix_types;

create table wide_base(
  id int primary key,
  b bit(10),
  u8 tinyint unsigned,
  u16 smallint unsigned,
  u32 int unsigned,
  u64 bigint unsigned,
  d256 decimal(65,30),
  y year,
  uid uuid,
  bin binary(4),
  vbin varbinary(8),
  e enum('red','blue','green'),
  bl blob,
  dl datalink,
  g geometry,
  vf vecf64(3)
);

insert into wide_base values
  (1, b'101', 250, 65000, 4000000000, 9000000000000000000,
   cast('12345678901234567890123456789012345.123456789012345678901234567890' as decimal(65,30)),
   2024, '6d1b1f73-2dbf-11ed-940f-000c29847904', 'ab', x'01020304',
   'red', 'blob-base', 'file:///tmp/mo_branch_type_base.csv', 'POINT(1 1)', '[1.1,2.2,3.3]'),
  (2, b'010', 1, 2, 3, 4,
   cast('-0.000000000000000000000000000001' as decimal(65,30)),
   1999, 'ad9f809f-2dbd-11ed-940f-000c29847904', 'cd', x'05060708',
   'blue', 'blob-two', 'file:///tmp/mo_branch_type_two.csv', 'LINESTRING(0 0,1 1)', '[4.4,5.5,6.6]');

data branch create table wide_branch from wide_base;
-- @bvt:issue#24924
update wide_branch set
  b = b'111',
  u8 = 251,
  u16 = 65001,
  u32 = 4000000001,
  u64 = 9000000000000000001,
  d256 = cast('42.000000000000000000000000000000' as decimal(65,30)),
  y = 2025,
  uid = '1b50c137-2dba-11ed-940f-000c29847904',
  bin = 'ef',
  vbin = x'090a0b0c',
  e = 'green',
  bl = 'blob-updated',
  dl = 'file:///tmp/mo_branch_type_updated.csv',
  g = 'POINT(2 2)',
  vf = '[7.7,8.8,9.9]'
where id = 1;
insert into wide_branch values
  (3, b'001', 2, 3, 4, 5,
   cast('7.000000000000000000000000000000' as decimal(65,30)),
   2000, '3ddf7b28-2dba-11ed-940f-000c29847904', 'gh', x'0d0e0f10',
   'red', 'blob-new', 'file:///tmp/mo_branch_type_new.csv', 'POINT(3 3)', '[0.1,0.2,0.3]');
delete from wide_branch where id = 2;
data branch merge wide_branch into wide_base;
select count(*) as wide_rows from wide_base;
select id, cast(b as int) as bit_i, u8, u16, u32, u64, y, e from wide_base order by id;
-- @bvt:issue
drop database br_matrix_types;

-- Case 2: core column constraints remain effective on a branch.
drop database if exists br_matrix_constraints;
create database br_matrix_constraints;
use br_matrix_constraints;

create table parent(id int primary key);
insert into parent values (1), (2), (3);
create table child(
  id int auto_increment primary key,
  parent_id int not null,
  code varchar(20) not null default 'seed' comment 'branch code',
  qty decimal(10,2) default 0.00,
  constraint chk_qty CHECK (qty IS NULL OR qty >= 0),
  unique key uq_code(code),
  constraint fk_child_parent foreign key(parent_id) references parent(id)
) comment = 'branch constraint table';
insert into child(parent_id, code, qty) values (1, 'base-a', 10.00), (2, 'base-b', 20.00);

data branch create table child_branch from child;
update child_branch set qty = qty + 5 where code = 'base-a';
insert into child_branch(parent_id, qty) values (2, 77.75);
-- @regex("Column 'code' cannot be null",true)
insert into child_branch(parent_id, code, qty) values (1, null, 1.00);
-- @regex("Duplicate entry 'base-a'",true)
insert into child_branch(parent_id, code, qty) values (1, 'base-a', 1.00);
-- @regex("foreign key constraint fails",true)
insert into child_branch(parent_id, code, qty) values (99, 'bad-fk', 1.00);
data branch merge child_branch into child;
select count(*) as child_rows from child;
select code from child where parent_id = 2 and qty = 77.75;

create table clustered_base(
  tenant int not null comment 'cluster tenant',
  seq int default 0,
  payload varchar(20)
) cluster by (tenant, seq);
insert into clustered_base values (1, 1, 'base'), (1, 2, 'old'), (2, 1, 'keep');
data branch create table clustered_branch from clustered_base;
update clustered_branch set payload = 'new' where tenant = 1 and seq = 2;
insert into clustered_branch(tenant, payload) values (3, 'default-seq');
delete from clustered_branch where tenant = 2;
-- @bvt:issue#24924
data branch merge clustered_branch into clustered_base;
select count(*) as clustered_rows from clustered_base;
select tenant, seq, payload from clustered_base order by tenant, seq;
-- @bvt:issue
drop database br_matrix_constraints;

-- Case 3: representative table types.
drop database if exists br_matrix_tables_copy;
drop database if exists br_matrix_tables;
create database br_matrix_tables;
use br_matrix_tables;

create table part_base(
  id int primary key,
  val int,
  note varchar(16)
) partition by range columns(id) (
  partition p0 values less than (10),
  partition p1 values less than (MAXVALUE)
);
insert into part_base values (1, 10, 'p0'), (11, 110, 'p1'), (12, 120, 'p1');
data branch create table part_branch from part_base;
update part_branch set val = val + 1 where id = 11;
insert into part_branch values (2, 20, 'new-p0'), (21, 210, 'new-p1');
delete from part_branch where id = 12;
-- @bvt:issue#24924
data branch merge part_branch into part_base;
select id, val, note from part_base order by id;
-- @bvt:issue

create table view_base(id int primary key, val int);
insert into view_base values (1, 10), (2, 20), (3, 30);
create view v_active as select id, val from view_base where val >= 20;
data branch create database br_matrix_tables_copy from br_matrix_tables;
insert into view_base values (4, 40);
select count(*) as copied_view_rows from br_matrix_tables_copy.v_active;
insert into br_matrix_tables_copy.view_base values (5, 50);
select count(*) as copied_view_rows_after_insert from br_matrix_tables_copy.v_active;

create external table ext_branch_base(id int) infile{"filepath"='$resources/external_table_file/extable.csv'} fields terminated by ',' lines terminated by '\n';
-- @regex("is not BASE TABLE",true)
data branch create table ext_branch_copy from ext_branch_base;

use mo_catalog;
drop table if exists br_cluster_branch_base;
create cluster table br_cluster_branch_base(a int);
insert into br_cluster_branch_base values (1, 0), (2, 0);
use br_matrix_tables;
data branch create table cluster_branch_copy from mo_catalog.br_cluster_branch_base;
select count(*) as cluster_rows from cluster_branch_copy;
drop table mo_catalog.br_cluster_branch_base;

drop database br_matrix_tables_copy;
drop database br_matrix_tables;

-- Case 4: quoted identifiers must not break data branch bookkeeping.
drop database if exists br_matrix_quoted;
create database br_matrix_quoted;
use br_matrix_quoted;

-- Regression for #24924.
create table `base table`(a int primary key, b varchar(20));
insert into `base table` values (1, 'space-name'), (2, 'space-name');
-- @bvt:issue#24924
data branch create table `branch table` from `base table`;
select count(*) as quoted_space_rows from `branch table`;
-- @bvt:issue

-- Regression for #24924.
-- @bvt:issue#24924
create table `quote'src`(a int primary key, b varchar(20));
insert into `quote'src` values (1, 'single-quote');
data branch create table `quote'dst` from `quote'src`;
select count(*) as quoted_apostrophe_rows from `quote'dst`;
-- @bvt:issue

drop database br_matrix_quoted;
