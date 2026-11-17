-- Exercise byte-bounded CN spills through both sorted and no-sort writers.
drop database if exists bvt_cn_spill_bound;
create database bvt_cn_spill_bound;
use bvt_cn_spill_bound;
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',0,'bvt_cn_spill_bound');

create table sorted_rows(id int primary key, payload text, state int);
create table unsorted_rows(id int, payload text, state int);
insert into sorted_rows select result, concat(lpad(cast(result as char), 4, '0'), repeat('x',32768)), 0 from generate_series(1,64) g;
insert into unsorted_rows select result, concat(lpad(cast(result as char), 4, '0'), repeat('x',32768)), 0 from generate_series(1,64) g;

-- Values must remain attached to their IDs across several spill objects.
select count(*), count(distinct id), sum(length(payload)), sum(case when cast(left(payload,4) as unsigned) <> id then 1 else 0 end) from sorted_rows;
select count(*), count(distinct id), sum(length(payload)), sum(case when cast(left(payload,4) as unsigned) <> id then 1 else 0 end) from unsorted_rows;
-- The injected target must actually produce multiple CN-created objects.
select count(distinct object_name) > 1 as multiple_objects from metadata_scan('bvt_cn_spill_bound.sorted_rows', 'id') m;
select count(distinct object_name) > 1 as multiple_objects from metadata_scan('bvt_cn_spill_bound.unsorted_rows', 'id') m;

begin;
update sorted_rows set state=1;
update unsorted_rows set state=1;
rollback;
select sum(state) from sorted_rows;
select sum(state) from unsorted_rows;

update sorted_rows set state=2;
update unsorted_rows set state=2;
select count(*), sum(state), sum(case when cast(left(payload,4) as unsigned) <> id then 1 else 0 end) from sorted_rows;
select count(*), sum(state), sum(case when cast(left(payload,4) as unsigned) <> id then 1 else 0 end) from unsorted_rows;

-- Exercise variable-length primary keys through DELETE and rollback. The
-- small-object injection above applies to data writers; a unit test separately
-- checks multi-object tombstones at a small explicit threshold.
create table delete_rows(pk varchar(512) primary key, v int);
insert into delete_rows select concat(lpad(cast(result as char),4,'0'), repeat('k',300)), result from generate_series(1,32) g;
begin;
delete from delete_rows where mod(v,2)=0;
rollback;
select count(*), sum(v) from delete_rows;
delete from delete_rows where mod(v,2)=0;
select count(*), sum(v) from delete_rows;

-- Keep clean spill objects while compacting a dirty object in the same transaction.
create table compact_rows(id int primary key, payload text);
begin;
insert into compact_rows select result, concat(lpad(cast(result as char),4,'0'), repeat('x',32768)) from generate_series(1,64) g;
delete from compact_rows where id=64;
commit;
select count(*) as n, count(distinct id) as unique_ids, sum(id) as id_sum, sum(length(payload)) as bytes, sum(case when cast(left(payload,4) as unsigned) <> id then 1 else 0 end) as mismatches from compact_rows;

select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
drop database bvt_cn_spill_bound;
