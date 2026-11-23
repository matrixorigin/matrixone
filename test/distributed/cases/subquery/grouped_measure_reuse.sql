-- @suite
-- @case
-- @desc: grouped measure reuse preserves bag, NULL and fallback semantics
-- @label:bvt
drop database if exists grouped_measure_reuse;
create database grouped_measure_reuse;
use grouped_measure_reuse;
create table d(k int primary key, label varchar(10));
create table f(k int,v decimal(7,2));
insert into d values (1,'A'),(2,'B'),(3,'C');
insert into f values (1,10),(1,20),(1,null),(2,null),(3,5),(99,100),(null,100);
select d.k,d.label,sum(f.v) as qty from d join f on d.k=f.k where d.k in (select k from f group by k having sum(v)>10) group by d.k,d.label order by d.k;
select d.k,d.label,g.qty from d join (select k,sum(v) as qty from f group by k having sum(v)>10) g on d.k=g.k order by d.k;
select d.k,d.label,sum(f.v) as qty from d join f on d.k=f.k where d.k in (select k from f group by k having sum(v)>10) and f.v>=20 group by d.k,d.label order by d.k;
create table bag(k int,label varchar(10));
insert into bag values (1,'A'),(1,'A');
select d.k,d.label,sum(f.v) as qty from bag d join f on d.k=f.k where d.k in (select k from f group by k having sum(v)>10) group by d.k,d.label order by d.k;
select d.k,d.label,sum(f.v) as qty from d join f on d.k=f.k where d.k in (select k from f group by k having sum(v) is null) group by d.k,d.label order by d.k;
select d.k,d.label,sum(f.v) as qty from d join f on d.k=f.k where d.k in (select k from f group by k having sum(v)>0) group by d.k,d.label order by qty desc,d.k limit 1;
-- Qualification remains below a second unique dimension; either missing dimension filters the group.
create table p(k int primary key,label varchar(10));
insert into p values (1,'P'),(99,'orphan');
select d.k,p.label,sum(f.v) as qty from d join p on d.k=p.k join f on d.k=f.k where d.k in (select k from f group by k having sum(v)>0) group by d.k,p.label order by d.k;
drop database grouped_measure_reuse;
