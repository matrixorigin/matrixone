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

select remove_fault_point('fj/cn/flush_small_objs');
select disable_fault_injection();
drop database bvt_cn_spill_bound;
