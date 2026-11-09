-- Rows an IVF-FLAT index cannot store (#31583). Each section proves the claim of
-- docs/design/20261028-index-ignored-rows.md it is headed by.
drop database if exists ivf_ignored;
create database ivf_ignored;
use ivf_ignored;

-- ---------------- I15: by default a build fails on a row the index cannot store -----
create table t(id bigint primary key, v vecf32(3));
insert into t values (1,'[1,2,3]'),(2,'[100000,1,1]'),(3,'[4,5,6]'),(4,'[7,8,9]');
create index i using ivfflat on t(v) lists 2 op_type 'vector_l2_ops' quantization 'float16';
show index ignored from t;
select count(*) from mo_catalog.mo_indexes where name = 'i' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'ivf_ignored' and relname = 't');
-- MAX_BAD_RECORDS lets the build leave out that many rows
create index i using ivfflat on t(v) lists 2 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 2;
-- @ignore:4
show index ignored from t;
select id from t order by l2_distance(v, '[1,2,3]') limit 3;
-- a build that can store none of the rows fails within the limit
create table z(id bigint primary key, v vecf32(2));
insert into z values (1,'[100000,1]'),(2,'[1,100000]');
create index iz using ivfflat on z(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 10;

-- ---------------- I16: DML on a synchronous index follows the limit -----
insert into t values (5,'[200000,1,1]');
-- @ignore:4
show index ignored from t;
insert into t values (6,'[300000,1,1]');
select count(*) from t;
-- an update to a value the index stores, or a delete, removes a row
update t set v = '[1,1,1]' where id = 2;
delete from t where id = 5;
show index ignored from t;
select id from t order by l2_distance(v, '[1,1,1]') limit 2;
-- an update to a value the index cannot store records it
update t set v = '[400000,1,1]' where id = 3;
-- @ignore:4
show index ignored from t;
-- REINDEX starts the rows over, TRUNCATE empties them
alter table t alter reindex i ivfflat lists 2;
-- @ignore:4
show index ignored from t;
truncate table t;
show index ignored from t;

-- a referential action keeps the record of a row the index still cannot store
create table fp(id int primary key);
create table fc(id int primary key, pid int, v vecf32(2), foreign key (pid) references fp(id) on delete set null);
insert into fp values (1),(2);
insert into fc values (1,1,'[1,1]'),(2,1,'[100000,1]'),(3,2,'[2,2]');
create index ifc using ivfflat on fc(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 5 include(pid);
delete from fp where id = 1;
select * from fc order by id;
-- @ignore:4
show index ignored from fc;
select id from fc order by l2_distance(v, '[1,1]') limit 3;

-- a quoted key name may hold dots
create table dk(`a.id` int primary key, v vecf32(2));
insert into dk values (1,'[1,1]'),(2,'[100000,1]');
create index idk using ivfflat on dk(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 5;
insert into dk values (3,'[200000,1]'),(4,'[2,2]');
-- @ignore:4
show index ignored from dk;
update dk set v = '[3,3]' where `a.id` = 2;
-- @ignore:4
show index ignored from dk;
select `a.id` from dk order by l2_distance(v, '[3,3]') limit 2;

-- ---------------- I17: an asynchronous build and its sync record the rows -----
create table c(a int, b varchar(10), v vecf64(2), primary key(a, b));
insert into c values (1,'x','[1,2]'),(2,'y','[3,4]'),(3,'z','[1e300,1]'),(4,'w','[5,6]');
create index ic using ivfflat on c(v) lists 1 op_type 'vector_l2_ops' quantization 'int8' max_bad_records 2 async;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from c;
insert into c values (5,'v','[1e200,2]');
delete from c where a = 3;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from c;
select a from c order by l2_distance(v, '[1,2]') limit 3;
-- one row more than the limit stops the sync
insert into c values (6,'u','[1e250,2]'),(7,'s','[1e260,2]');
-- @ignore:6
-- @wait_expect(2, 60)
show index status from c;
-- REINDEX with a higher limit restarts it
alter table c alter reindex ic ivfflat max_bad_records 5;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from c;
-- @ignore:6
-- @wait_expect(2, 60)
show index status from c;

-- an ENUM key is recorded by the build, DML and an asynchronous index
create table e(k enum('a','b','c','d') primary key, v vecf32(2));
insert into e values ('a','[1,1]'),('b','[100000,1]');
create index ie using ivfflat on e(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 5;
insert into e values ('c','[200000,1]');
-- @ignore:4
show index ignored from e;
create table ea(k enum('a','b','c','d') primary key, v vecf32(2));
insert into ea values ('a','[1,1]'),('b','[100000,1]');
create index iea using ivfflat on ea(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 1 async;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from ea;
insert into ea values ('c','[200000,1]');
-- @ignore:6
-- @wait_expect(2, 60)
show index status from ea;

-- an ENUM key that names no member, as INSERT IGNORE stores it, is recorded in strict mode
create table e0(k enum('a','b') primary key, v vecf32(2));
insert ignore into e0 values ('zz','[100000,1]');
insert into e0 values ('a','[1,1]');
create index ie0 using ivfflat on e0(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 1;
select cast(k as int) from e0;
-- @ignore:4
show index ignored from e0;
create table ea0(k enum('a','b') primary key, v vecf32(2));
insert into ea0 values ('a','[1,1]');
create index iea0 using ivfflat on ea0(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 1 async;
insert ignore into ea0 values ('zz','[100000,1]');
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from ea0;
delete from ea0 where cast(k as int) = 0;
-- @ignore:4
-- @wait_expect(2, 60)
show index ignored from ea0;

-- a composite key reads as the hex literal of its serial
create table ce(a int, e enum('x','y'), v vecf32(2), primary key(a, e));
insert into ce values (1,'x','[1,1]'),(2,'y','[100000,1]');
create index ice using ivfflat on ce(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 5;
-- including an ENUM part that names no member
insert ignore into ce values (3,'zz','[200000,1]');
-- @ignore:4
show index ignored from ce;
-- keys that differ in fractional seconds read as distinct literals, and a literal finds its row
create table cd(a int, d datetime(6), t time(6), v vecf32(2), primary key(a, d, t));
insert into cd values (1,'2026-11-05 10:00:00.100000','10:00:00.100000','[100000,1]'),(1,'2026-11-05 10:00:00.200000','10:00:00.200000','[200000,1]'),(2,'2026-11-05 10:00:00','10:00:00','[1,1]');
create index icd using ivfflat on cd(v) lists 1 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 2;
-- @ignore:4
show index ignored from cd;
-- @ignore:4
show index ignored from cd where `Primary_key` = 'X''3A1501421BE31F838E32F54047190861C77540''';
select a, d, t from cd where serial(a, d, t) = X'3A1501421BE31F838E32F54047190861C77540';
-- rows left out of the samples may leave fewer of them than lists
create table k(id bigint primary key, v vecf32(2));
insert into k values (1,'[1,1]'),(2,'[2,2]'),(3,'[3,3]'),(4,'[100000,1]'),(5,'[200000,1]');
create index ik using ivfflat on k(v) lists 4 op_type 'vector_l2_ops' quantization 'float16' max_bad_records 5;
-- @ignore:4
show index ignored from k;
select id from k order by l2_distance(v, '[1,1]') limit 2;
-- only internal SQL may name the index of a CENTROIDX join
select k.id from k centroidx('vector_l2_ops', '{}', '{"db":"ivf_ignored","ignored":"t"}') join k as k2 using (id);

-- ---------------- I18: a search vector the index cannot store is rejected -----
select id from t order by l2_distance(v, '[100000,1,1]') limit 1;

-- ---------------- I8: an index without an ignored table is unchanged -----
create table o(id bigint primary key, v vecf32(2));
insert into o values (1,'[1,1]');
create index io using ivfflat on o(v) lists 1 op_type 'vector_l2_ops';
insert into o values (2,'[2,2]');
show index ignored from o;

drop index ic on c;
show index ignored from c;
drop database ivf_ignored;
