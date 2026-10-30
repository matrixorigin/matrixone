drop database if exists issue_31614_branch;
create database issue_31614_branch;
use issue_31614_branch;

-- matrixone issue #31614: two sibling forks off one shared root; the
-- destination forked LATER and inherits the root's writes made between
-- the two forks (amt=555 on pk 1). Those inherited writes are
-- destination-side divergence and must participate in conflict handling.
create table s_base(id int primary key, amt int);
insert into s_base values (1,100),(2,200);
data branch create table s_cf from s_base;
update s_cf set amt=777 where id=1;
update s_cf set amt=222 where id=2;
update s_base set amt=555 where id=1;
data branch create table s_mf from s_base;

-- DIFF control: both sides of the shared-pk divergence are visible.
data branch diff s_cf against s_mf;

-- sibling fork, WHEN CONFLICT FAIL: the conflict fires and the whole
-- statement aborts; the conflicting row and the unrelated row stay intact.
-- @regex("s_cf UPDATE and s_mf UPDATE on pk\(1\)", true)
data branch merge s_cf into s_mf when conflict fail;
select * from s_mf order by id;

-- replay keeps failing while the conflict exists.
-- @regex("s_cf UPDATE and s_mf UPDATE on pk\(1\)", true)
data branch merge s_cf into s_mf when conflict fail;

-- the default policy is FAIL.
data branch create table s_md from s_base;
-- @regex("s_cf UPDATE and s_md UPDATE on pk\(1\)", true)
data branch merge s_cf into s_md;
select * from s_md order by id;

-- SKIP keeps the destination value on the conflicting row and applies
-- the non-conflicting row.
data branch create table s_ms from s_base;
data branch merge s_cf into s_ms when conflict skip;
select * from s_ms order by id;

-- ACCEPT keeps source-winning semantics.
data branch create table s_ma from s_base;
data branch merge s_cf into s_ma when conflict accept;
select * from s_ma order by id;

drop database issue_31614_branch;
