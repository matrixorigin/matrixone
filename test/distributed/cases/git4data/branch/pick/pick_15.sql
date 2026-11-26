-- @suitename: Data Branch Pick
-- @testcase: BETWEEN SNAPSHOT uses the source window's lower image

drop snapshot if exists pick_window_lo;
drop snapshot if exists pick_window_hi;
drop database if exists pick_window;
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
update src set v=20;
create snapshot pick_window_lo for table pick_window src;

-- k=1 returns to the fork value. k=2 returns to the value at lo.
update src set v=10 where k=1;
update src set v=25 where k=2;
update src set v=20 where k=2;
create snapshot pick_window_hi for table pick_window src;
update root set v=30;
data branch create table dst from root;
select k,v from src {snapshot = pick_window_lo} order by k;
select k,v from src {snapshot = pick_window_hi} order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: In-window rename preserves the source lo and destination fork baselines
-- k=3 equals lo on dst but changed after its own fork. SKIP preserves it.
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10),(3,10);
data branch create table src from root;
update src set v=20;
create snapshot pick_window_lo for table pick_window src;
alter table src algorithm=inplace, rename column v to v2;
update src set v2=10 where k in (1,3);
update src set v2=25 where k=2;
update src set v2=20 where k=2;
create snapshot pick_window_hi for table pick_window src;
update root set v=30;
data branch create table dst from root;
update dst set v=20 where k=3;
select k,v from src {snapshot = pick_window_lo} order by k;
select k,v2 from src {snapshot = pick_window_hi} order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: COPY rename after hi still probes the old source generation
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10),(3,10);
data branch create table src from root;
update src set v=20;
create snapshot pick_window_lo for table pick_window src;
update src set v=10 where k in (1,3);
update src set v=25 where k=2;
update src set v=20 where k=2;
create snapshot pick_window_hi for table pick_window src;
alter table src algorithm=copy, rename column v to v2;
update root set v=30;
data branch create table dst from root;
update dst set v=20 where k=3;
select k,v from src {snapshot = pick_window_lo} order by k;
select k,v from src {snapshot = pick_window_hi} order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window DELETE conflicts with a destination INSERT of the same key
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (2,10),(4,40);
data branch create table src from root;
insert into src values (1,20);
create snapshot pick_window_lo for table pick_window src;
delete from src where k in (1,2);
create snapshot pick_window_hi for table pick_window src;
data branch create table dst from root;
insert into dst values (1,99);
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: The window probe follows column identity when a name is reused
create database pick_window;
use pick_window;
create table root (k int primary key, v int, w int);
insert into root values (1,30,10);
data branch create table src from root;
data branch create table dst from root;
update src set v=20 where k=1;
alter table src algorithm=inplace, rename column w to v2;
create snapshot pick_window_lo for table pick_window src;
alter table src algorithm=inplace, rename column v2 to w;
alter table src algorithm=inplace, rename column v to v2;
update src set v2=10 where k=1;
create snapshot pick_window_hi for table pick_window src;
select k,v,v2 from src {snapshot = pick_window_lo} order by k;
select k,v2,w from src {snapshot = pick_window_hi} order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v,w from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window DELETE applies to a row inherited at the destination fork
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
data branch create table src from root;
insert into src values (1,20);
insert into root values (1,99),(2,40);
data branch create table dst_fail from root;
data branch create table dst_skip from root;
data branch create table dst_accept from root;
create snapshot pick_window_lo for table pick_window src;
delete from src where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst_fail between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst_fail order by k;
data branch pick src into dst_skip between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst_skip order by k;
data branch pick src into dst_accept between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst_accept order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window UPDATE of a key absent at the source fork is applied as an INSERT
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
data branch create table src from root;
insert into src values (1,20);
create snapshot pick_window_lo for table pick_window src;
update src set v=30 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch create table dst_absent from root;
data branch create table dst from root;
insert into dst values (1,99);
data branch pick src into dst_absent between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst_absent order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Destination fork resolves its COPY ALTER generation
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
data branch create table src from root;
insert into src values (1,20);
alter table root algorithm=copy, modify column v int;
insert into root values (1,99);
data branch create table dst_fail from root;
data branch create table dst_skip from root;
data branch create table dst_accept from root;
create snapshot pick_window_lo for table pick_window src;
delete from src where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst_fail between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst_fail;
data branch pick src into dst_skip between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst_skip;
data branch pick src into dst_accept between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst_accept;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Net-zero updates between snapshots remain no-ops with unrelated tables
create database pick_window;
use pick_window;
create table src (k int primary key, v int);
insert into src values (1,20),(2,20);
create table dst (k int primary key, v int);
insert into dst values (1,99),(2,99);
create snapshot pick_window_lo for table pick_window src;
update src set v=25;
update src set v=20 where k=1;
insert into src values (3,30);
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Restore one key and update the other in the same statement
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(5,10);
data branch create table src from root;
data branch create table dst from root;
delete from dst where k=1;
create snapshot pick_window_lo for table pick_window src;
update src set v=20;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A window update conflicts with a destination-only insert
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
data branch create table dst from root;
insert into dst values (1,99);
insert into root values (1,10);
data branch create table src from root;
update src set v=20;
create snapshot pick_window_lo for table pick_window src;
update src set v=10;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window start is in the parent before a later logical fork
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table par from root;
data branch create table dst from root;
update dst set v=99;
update par set v=20;
create snapshot pick_window_lo for table pick_window par;
update par set v=25;
update par set v=20 where k=1;
data branch create table src from par;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Prune net-zero updates when the common ancestor has no committed history
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
data branch create table dst from root;
drop table root;
commit;
update dst set v=99;
create snapshot pick_window_lo for table pick_window src;
update src set v=20;
update src set v=10 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A window update does not bypass an explicit destination delete
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
data branch create table src from root;
insert into src values (1,20),(2,20);
insert into root values (1,99);
data branch create table dst from root;
insert into dst values (2,99);
delete from dst where k>0;
create snapshot pick_window_lo for table pick_window src;
update src set v=30;
create snapshot pick_window_hi for table pick_window src;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Restore a key missing at the destination fork
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
delete from root where k=1;
data branch create table dst from root;
create snapshot pick_window_lo for table pick_window src;
update src set v=30 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window insert collides with an unchanged inherited destination key
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
data branch create table dst from root;
delete from src where k=1;
create snapshot pick_window_lo for table pick_window src;
insert into src values (1,30);
create snapshot pick_window_hi for table pick_window src;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: COPY ADD FIRST preserves shared column identities
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
data branch create table dst from root;
create snapshot pick_window_lo for table pick_window src;
alter table src algorithm=copy, add column extra int first;
update src set v=30 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Source-only rename after COPY must not block writes to shared columns
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
data branch create table dst from root;
create snapshot pick_window_lo for table pick_window src;
alter table src algorithm=copy, add column extra int;
alter table src algorithm=inplace, rename column extra to extra2;
update src set v=30 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: ACCEPT replaces indexed ENUM values that differ only at the destination
create database pick_window;
use pick_window;
create table root (k int primary key, v int, e enum('red','blue') unique);
data branch create table src from root;
data branch create table dst from root;
insert into dst values (1,99,'blue');
insert into src values (1,10,'red');
create snapshot pick_window_lo for table pick_window src;
update src set v=20 where k=1;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst where e='red';
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: An in-window reinsert conflicts with a destination delete
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10);
data branch create table src from root;
data branch create table dst from root;
delete from src where k=1;
create snapshot pick_window_lo for table pick_window src;
delete from dst where k=1;
insert into src values (1,30);
create snapshot pick_window_hi for table pick_window src;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Window includes parent updates before the first source fork
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10);
data branch create table dst from root;
update root set v=20;
create snapshot pick_window_lo for table pick_window root;
update root set v=30;
data branch create table src from root;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A child rename does not turn a parent's net-zero change into an update
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10);
data branch create table dst from root;
data branch create table par from root;
update par set v=20;
create snapshot pick_window_lo for table pick_window par;
update par set v=25;
data branch create table src from par;
alter table src algorithm=inplace, rename column v to vv;
update src set vv=20;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: COPY with an unambiguous later shared-column rename
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10);
data branch create table src from root;
data branch create table dst from root;
update src set v=20;
create snapshot pick_window_lo for table pick_window src;
alter table src algorithm=copy, modify column v int;
alter table src algorithm=inplace, rename column v to vv;
update src set vv=30;
create snapshot pick_window_hi for table pick_window src;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A conflict survives a lower bound in the pruned shared prefix
create database pick_window;
use pick_window;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
create snapshot pick_window_lo for account sys;
data branch create table src from root;
delete from root where k=1;
data branch create table dst from root;
insert into dst values (1,99);
delete from src where k=1;
update src set v=44 where k=2;
create snapshot pick_window_hi for account sys;
-- @regex("conflict: src DELETE and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Writing values from hi follows column identity after a name swap
create database pick_window;
use pick_window;
create table root (k int primary key, a int, b int);
insert into root values (1,40,50);
data branch create table src from root;
data branch create table dst from root;
data branch create table control from root;
create snapshot pick_window_lo for account sys;
update src set a=60;
create snapshot pick_window_hi for account sys;
data branch pick src into control between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,a,b from control order by k;
alter table src algorithm=inplace, rename column a to tmp;
alter table src algorithm=inplace, rename column b to a;
alter table src algorithm=inplace, rename column tmp to b;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A committed child of a zero-history ancestor can pick a window DELETE
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, v int);
insert into root values (1,10),(2,10);
data branch create table src from root;
data branch create table dst from root;
drop table root;
commit;
data branch create table leaf from dst;
create snapshot pick_window_lo for account sys;
delete from src where k=1;
update src set v=40 where k=2;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
data branch pick src into leaf between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from leaf order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A creation commit does not establish that a conflicting row was inherited
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, v int);
insert into root values (1,10);
data branch create table src from root;
delete from root where k=1;
data branch create table dst_equal from root;
insert into root values (2,99);
data branch create table dst from root;
insert into dst values (1,99);
drop table root;
commit;
insert into src values (2,20);
insert into dst_equal values (2,40);
create snapshot pick_window_lo for account sys;
delete from src where k=1;
update src set v=40 where k=2;
insert into src values (3,44);
create snapshot pick_window_hi for account sys;
-- @regex("conflict: src DELETE and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi keys (1,3) when conflict fail;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi keys (2,3) when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst_equal between snapshot pick_window_lo and pick_window_hi keys (2,3) when conflict fail;
select k,v from dst_equal order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Map destination column identity through COPY and distinct fork schemas
create database pick_window;
use pick_window;
create table root (k int primary key, a int, b int);
insert into root values (1,40,50);
data branch create table src from root;
data branch create table dst_copy from root;
alter table dst_copy algorithm=copy, modify column b int;
alter table root algorithm=inplace, rename column a to tmp;
alter table root algorithm=inplace, rename column b to a;
alter table root algorithm=inplace, rename column tmp to b;
data branch create table dst_fork from root;
create snapshot pick_window_lo for account sys;
update src set a=60;
create snapshot pick_window_hi for account sys;
alter table dst_copy algorithm=inplace, rename column a to tmp;
alter table dst_copy algorithm=inplace, rename column b to a;
alter table dst_copy algorithm=inplace, rename column tmp to b;
data branch pick src into dst_copy between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,a,b from dst_copy order by k;
data branch pick src into dst_fork between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,a,b from dst_fork order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Preserve committed source and destination history beyond a zero-history ancestor
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
data branch create table dst_alias from root;
drop table root;
commit;
delete from dst where k=1;
data branch create table leaf_dst from dst;
alter table dst_alias algorithm=inplace, rename column a to tmp;
alter table dst_alias algorithm=inplace, rename column b to a;
alter table dst_alias algorithm=inplace, rename column tmp to b;
update dst_alias set b=90 where k=4;
create snapshot pick_window_lo for account sys;
update parent_src set a=60 where k=1;
begin;
data branch create table leaf_src from parent_src;
insert into leaf_src values (3,70,80);
delete from leaf_src where k=2;
commit;
create snapshot pick_window_hi for account sys;
-- @regex("conflict: leaf_src INSERT and dst INSERT", true)
data branch pick leaf_src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
data branch pick leaf_src into leaf_dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,a,b from leaf_dst order by k;
data branch pick leaf_src into dst_alias between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst_alias order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Missing-key history uses the old primary key after a name swap
create database pick_window;
use pick_window;
begin;
create table root (k int not null primary key, a int not null, v int);
data branch create table src from root;
insert into root values (8,7,10);
data branch create table dst from root;
drop table root;
commit;
alter table src algorithm=inplace, rename column k to tmp;
alter table src algorithm=inplace, rename column a to k;
alter table src algorithm=inplace, rename column tmp to a;
alter table dst algorithm=inplace, rename column k to tmp;
alter table dst algorithm=inplace, rename column a to k;
alter table dst algorithm=inplace, rename column tmp to a;
insert into src (a,k,v) values (7,9,20);
create snapshot pick_window_lo for account sys;
update src set v=44 where a=7;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select a,k,v from dst order by a;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Project the retained parent segment by identity through a typed rename
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b varchar(10));
insert into root values (1,10,'old');
data branch create table parent_src from root;
data branch create table dst from root;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
insert into parent_src values (3,70,'x');
data branch create table src from parent_src;
alter table src algorithm=inplace, rename column a to tmp;
alter table src algorithm=inplace, rename column b to a;
alter table src algorithm=inplace, rename column tmp to b;
update src set b=75 where k=3;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,a from dst where (k=1 and b='old') or (k=3 and b='x') order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A replaced primary key does not turn an absent destination row into a conflict
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int not null, v int);
data branch create table src from root;
insert into root values (8,7,10);
data branch create table parent_dst from root;
drop table root;
commit;
data branch create table dst_fail from parent_dst;
data branch create table dst_skip from parent_dst;
alter table src drop primary key, add primary key(a);
alter table dst_fail drop primary key, add primary key(a);
alter table dst_skip drop primary key, add primary key(a);
insert into src values (9,8,20);
create snapshot pick_window_lo for account sys;
update src set v=44 where a=8;
create snapshot pick_window_hi for account sys;
data branch pick src into dst_fail between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,v from dst_fail order by k;
data branch pick src into dst_skip between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,a,v from dst_skip order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A fork-state comparison only reconciles the requested keys
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
update parent_src set b=80 where k=1;
begin;
data branch create table mid from parent_src;
insert into mid values (3,70,80);
delete from mid where k=2;
commit;
begin;
data branch create table src from mid;
insert into src values (5,90,99);
commit;
update src set b=44 where k=4;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi keys(4) when conflict skip;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Collect the writes inherited through two forks in the same commit
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from parent_src;
insert into mid values (3,70,80);
delete from mid where k=2;
data branch create table src from mid;
insert into src values (5,90,99);
insert into mid values (6,100,110);
commit;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Preserve column identity through a renamed intermediate fork dropped in the same commit
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from parent_src;
insert into mid values (3,70,80);
delete from mid where k=2;
alter table mid algorithm=inplace, rename column a to tmp;
alter table mid algorithm=inplace, rename column b to a;
alter table mid algorithm=inplace, rename column tmp to b;
data branch create table src from mid;
insert into src (k,b,a) values (5,90,99);
drop table mid;
commit;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Collect same-commit fork writes with a committed common ancestor and detect a destination update
create database pick_window;
use pick_window;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
update dst set b=121 where k=2;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from parent_src;
delete from mid where k=2;
insert into mid values (3,70,80);
data branch create table src from mid;
insert into src values (5,90,99);
commit;
create snapshot pick_window_hi for account sys;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: A rename on the intermediate table after cloning cannot change the window before-image
create database pick_window;
use pick_window;
begin;
create table root (k int primary key, a int, b int);
insert into root values (1,10,20),(2,30,40),(4,50,60);
data branch create table parent_src from root;
data branch create table dst from root;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from parent_src;
data branch create table src from mid;
alter table mid algorithm=inplace, rename column b to bb;
commit;
update src set b=62 where k=4;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Preserve destination writes inherited through a same-commit fork
create database pick_window;
use pick_window;
create table root(k int primary key, v int);
insert into root values(2,40);
data branch create table sp from root;
data branch create table dp from root;
begin;
data branch create table mm from dp;
update mm set v=121 where k=2;
data branch create table dst from mm;
commit;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from sp;
delete from mid where k=2;
insert into mid values(3,80);
data branch create table src from mid;
drop table mid;
commit;
create snapshot pick_window_hi for account sys;
select k,v from dst order by k;
-- @regex("conflict: src INSERT and dst INSERT", true)
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict skip;
select k,v from dst order by k;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict accept;
select k,v from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Use first committed clone column order when the common ancestor never committed
create database pick_window;
use pick_window;
begin;
create table root(k int primary key,a int,b int);
data branch create table dst from root;
data branch create table mid from root;
alter table mid algorithm=inplace,rename column a to tmp;
alter table mid algorithm=inplace,rename column b to a;
alter table mid algorithm=inplace,rename column tmp to b;
data branch create table src from mid;
drop table mid;
drop table root;
commit;
create snapshot pick_window_lo for account sys;
insert into src(k,b,a) values(2,31,41);
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;

-- @testcase: Project the shared historical segment by composite key identity
create database pick_window;
use pick_window;
create table root(k int,a int,b int, primary key(k,a));
insert into root values(1,10,20),(2,30,40),(4,50,60);
data branch create table par from root;
data branch create table dst from root;
create snapshot pick_window_lo for account sys;
begin;
data branch create table mid from par;
delete from mid where k=2;
insert into mid values(3,70,80);
alter table mid algorithm=inplace, rename column a to tmp;
alter table mid algorithm=inplace, rename column b to a;
alter table mid algorithm=inplace, rename column tmp to b;
data branch create table src from mid;
insert into src(k,b,a) values(5,90,99);
commit;
create snapshot pick_window_cut for account sys;
create snapshot pick_window_hi for account sys;
data branch pick src into dst between snapshot pick_window_lo and pick_window_hi when conflict fail;
select k,a,b from dst order by k;
drop snapshot pick_window_cut;
drop snapshot pick_window_hi;
drop snapshot pick_window_lo;
drop database pick_window;
