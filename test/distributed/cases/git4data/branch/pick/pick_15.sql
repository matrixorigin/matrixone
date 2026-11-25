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
