-- issue#27049: database clone, data branch, and snapshot restore must preserve
-- sequence definitions and state, including sequences referenced by views.

drop snapshot if exists issue27049_sp;
drop snapshot if exists issue27049_table_sp;
drop database if exists issue27049_seq_only_src;
drop database if exists issue27049_seq_only_dst;
drop database if exists issue27049_src;
drop database if exists issue27049_live;
drop database if exists issue27049_snap;
drop database if exists issue27049_branch;
drop database if exists issue27049_atomic_src;
drop database if exists issue27049_atomic_dst;
drop account if exists issue27049_acc;

-- Minimal sequence-only database clone.
create database issue27049_seq_only_src;
create sequence issue27049_seq_only_src.seq1 as int unsigned
  increment by 2 minvalue 1 maxvalue 100 start with 11 no cycle;
create database issue27049_seq_only_dst clone issue27049_seq_only_src;
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_seq_only_dst'
order by relname;
select * from issue27049_seq_only_dst.seq1;
use issue27049_seq_only_dst;
select nextval('seq1');

-- A sequence-backed default and view, an uncalled sequence, and a sequence
-- whose stored catalog DDL is ALTER SEQUENCE rather than CREATE SEQUENCE.
create database issue27049_src;
use issue27049_src;
create sequence seq_state as bigint
  increment by 3 minvalue 1 maxvalue 100 start with 7 no cycle;
create sequence seq_uncalled as int unsigned
  increment by 2 minvalue 1 maxvalue 100 start with 11 cycle;
create sequence seq_altered as bigint start with 1;
alter sequence seq_altered as smallint increment by -2
  minvalue 3 maxvalue 9 start with 9 cycle;
create table t1(id bigint primary key default nextval('seq_state'), payload varchar(20));
insert into t1 values (1, 'snapshot-row');
create view seq_view as select nextval('seq_state') as n;
select * from seq_view;
select nextval('seq_altered');

create snapshot issue27049_sp for database issue27049_src;
select * from seq_view;
select nextval('seq_altered');
insert into t1 values (2, 'live-row');
create sequence seq_after_snapshot start with 31;

-- Live clone preserves current definitions/state and creates sequences before
-- dependent tables and views.
create database issue27049_live clone issue27049_src;
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_live'
order by relname;
select * from issue27049_live.seq_state;
select * from issue27049_live.seq_uncalled;
select * from issue27049_live.seq_altered;
use issue27049_live;
select * from seq_view;
select nextval('seq_uncalled');
select nextval('seq_altered');
insert into t1(payload) values ('cloned-default');
select * from t1 order by id;

-- Named-snapshot clone uses sequence definition and state from the snapshot,
-- not the current source relation.
create database issue27049_snap clone issue27049_src {snapshot = 'issue27049_sp'};
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_snap'
order by relname;
select * from issue27049_snap.seq_state;
select * from issue27049_snap.seq_uncalled;
select * from issue27049_snap.seq_altered;
use issue27049_snap;
select * from seq_view;
select nextval('seq_uncalled');
select nextval('seq_altered');
select * from t1 order by id;

-- Cross-account snapshot clone resolves sequence metadata in the source
-- tenant and restores the sequence relation in the target tenant.
create account issue27049_acc admin_name "root1" identified by "111";
create database issue27049_cross clone issue27049_src
  {snapshot = 'issue27049_sp'} to account issue27049_acc;
-- @session:id=1&user=issue27049_acc:root1&password=111
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_cross'
order by relname;
select * from issue27049_cross.seq_state;
use issue27049_cross;
select * from seq_view;
select nextval('seq_uncalled');
select nextval('seq_altered');
select * from t1 order by id;
-- @session

-- DATA BRANCH shares the live database source collector.
data branch create database issue27049_branch from issue27049_src;
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_branch'
order by relname;
select * from issue27049_branch.seq_state;
use issue27049_branch;
select * from seq_view;
select nextval('seq_uncalled');
select nextval('seq_altered');

-- Database snapshot restore restores sequence state and removes objects created
-- after the snapshot, while leaving dependent objects usable.
restore database issue27049_src{snapshot = 'issue27049_sp'};
select relname, relkind
from mo_catalog.mo_tables
where reldatabase = 'issue27049_src'
order by relname;
select * from issue27049_src.seq_state;
select * from issue27049_src.seq_uncalled;
select * from issue27049_src.seq_altered;
use issue27049_src;
select * from seq_view;
select nextval('seq_uncalled');
select nextval('seq_altered');
select * from t1 order by id;

-- Table-level snapshot restore uses the same sequence restore primitive.
create sequence seq_table_restore increment by 4 start with 21 no cycle;
select nextval('seq_table_restore');
create snapshot issue27049_table_sp for table issue27049_src seq_table_restore;
select nextval('seq_table_restore');
restore table issue27049_src.seq_table_restore{snapshot = 'issue27049_table_sp'};
select * from issue27049_src.seq_table_restore;
select nextval('seq_table_restore');

-- A failure after sequence creation must still roll back the whole database.
create database issue27049_atomic_src;
create sequence issue27049_atomic_src.seq1 start with 5;
create table issue27049_atomic_src.t1(a int);
select enable_fault_injection();
select add_fault_point('fj/cn/clone_fails',':::','echo',40,'issue27049_atomic_dst.t1');
-- @regex("internal error",true)
create database issue27049_atomic_dst clone issue27049_atomic_src;
select disable_fault_injection();
select count(*) as atomic_database_count
from mo_catalog.mo_database
where datname = 'issue27049_atomic_dst';
select count(*) as atomic_relation_count
from mo_catalog.mo_tables
where reldatabase = 'issue27049_atomic_dst';

drop snapshot if exists issue27049_table_sp;
drop snapshot if exists issue27049_sp;
drop database if exists issue27049_seq_only_src;
drop database if exists issue27049_seq_only_dst;
drop database if exists issue27049_src;
drop database if exists issue27049_live;
drop database if exists issue27049_snap;
drop database if exists issue27049_branch;
drop database if exists issue27049_atomic_src;
drop database if exists issue27049_atomic_dst;
drop account if exists issue27049_acc;
