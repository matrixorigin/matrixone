-- DATA BRANCH must keep Decimal256 primary-key identity across diff, pick and
-- merge. Decimal256 keys (DECIMAL precision 39..76) decode from the branch
-- hashmaps as raw byte slices; the tombstone pipeline must convert them back to
-- the typed key before pairing deletes with inserts (#28589).

drop database if exists branch_decimal256_pk;
create database branch_decimal256_pk;
use branch_decimal256_pk;

-- #28589 update case: the unchanged DECIMAL(40,0) key must pair the old and new
-- row versions as one UPDATE; the fresh key stays an INSERT; WHEN CONFLICT
-- ACCEPT pick and merge must apply them without duplicate-key errors.
create table base (
  k decimal(40,0) primary key,
  d int,
  v int
);
insert into base values
  (9999999999999999999999999999999999999999, 1, 1);
data branch create table branch_t from base;
update branch_t set v = 11 where k = 9999999999999999999999999999999999999999;
insert into branch_t values
  (8888888888888888888888888888888888888888, 2, 22);
data branch diff branch_t against base output summary;
data branch diff branch_t against base output count;
data branch pick branch_t into base keys(9999999999999999999999999999999999999999) when conflict accept;
select k, d, v from base order by k;
data branch diff branch_t against base output count;
data branch merge branch_t into base when conflict accept;
select k, d, v from base order by k;
data branch diff branch_t against base output count;

-- #28589 comment case: a branch DELETE of a DECIMAL(65,30) key must be reported
-- by DIFF and synchronized by MERGE; the base must lose the deleted row.
create table del_base (
  d decimal(65,30) primary key,
  note varchar(8)
);
insert into del_base values
  (9007199254740992.000000000000000000000000000001, 'a'),
  (9007199254740993.000000000000000000000000000001, 'b');
data branch create table del_branch from del_base;
delete from del_branch where d = 9007199254740993.000000000000000000000000000001;
select count(*) as branch_rows from del_branch;
select d, note from del_branch order by d;
data branch diff del_branch against del_base output summary;
data branch diff del_branch against del_base output count;
data branch merge del_branch into del_base when conflict accept;
select count(*) as base_rows from del_base;
select d, note from del_base order by d;
data branch diff del_branch against del_base output count;

-- Decimal128 boundary control: 38-digit precision keys keep their existing
-- update-pairing behavior.
create table d38 (
  k decimal(38,0) primary key,
  d int,
  v int
);
insert into d38 values
  (99999999999999999999999999999999999999, 1, 1);
data branch create table d38_b from d38;
update d38_b set v = 11 where k = 99999999999999999999999999999999999999;
insert into d38_b values
  (88888888888888888888888888888888888888, 2, 22);
data branch diff d38_b against d38 output summary;
data branch merge d38_b into d38 when conflict accept;
data branch diff d38_b against d38 output count;
drop database if exists branch_decimal256_pk;
