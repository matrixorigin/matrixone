-- ivfflat: narrow base (direct match T->T) and QUANTIZATION down-cast of f32/f64
-- bases to f32/f16/bf16/int8 entries. Two well-separated clusters (1..5 and 50..54)
-- make the top-3 stable under quantization; query an exact cluster-A point.
drop database if exists ivfq;
create database ivfq;
use ivfq;
create table tbf16(a int primary key, v vecbf16(4));
insert into tbf16 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index i_bf16 using ivfflat on tbf16(v) lists=2 op_type 'vector_l2_ops';
select a from tbf16 order by l2_distance(v,'[1,1,1,1]') limit 3;
create table tf16(a int primary key, v vecf16(4));
insert into tf16 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index i_f16 using ivfflat on tf16(v) lists=2 op_type 'vector_l2_ops';
select a from tf16 order by l2_distance(v,'[1,1,1,1]') limit 3;
create table ti8(a int primary key, v vecint8(4));
insert into ti8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index i_i8 using ivfflat on ti8(v) lists=2 op_type 'vector_l2_ops';
select a from ti8 order by l2_distance(v,'[1,1,1,1]') limit 3;
create table q32(a int primary key, v vecf32(4));
insert into q32 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index q32f16 using ivfflat on q32(v) lists=2 op_type 'vector_l2_ops' quantization 'float16';
select a from q32 order by l2_distance(v,'[1,1,1,1]') limit 3;
alter table q32 drop index q32f16;
create index q32bf16 using ivfflat on q32(v) lists=2 op_type 'vector_l2_ops' quantization 'bf16';
select a from q32 order by l2_distance(v,'[1,1,1,1]') limit 3;
alter table q32 drop index q32bf16;
create index q32i8 using ivfflat on q32(v) lists=2 op_type 'vector_l2_ops' quantization 'int8';
select a from q32 order by l2_distance(v,'[1,1,1,1]') limit 3;

-- #29536: trained [0,1] clips row 5's source distance 2.5 to index
-- distance 1. SQL predicates must use the original source vector.
create table qrange(a int primary key, v vecf32(3));
insert into qrange values
    (1,'[0,0,0]'),
    (2,'[0.02745098,0.007843137,0.007843137]'),
    (3,'[1,1,0]'),
    (4,'[0.011764706,0.003921569,0.003921569]'),
    (5,'[2.5,0,0]');
create index qrangei8 using ivfflat on qrange(v) lists=1 op_type 'vector_l2_ops' quantization 'int8';
select a from qrange
where l2_distance(v,'[0,0,0]') <= 0.5
order by l2_distance(v,'[0,0,0]') limit 10;
select a from qrange
where l2_distance(v,'[0,0,0]') >= 0.02
order by l2_distance(v,'[0,0,0]') limit 10;
-- Explicit POST preserves indexed candidates on this small fixture.
select a from qrange where l2_distance(v,'[0,0,0]') > 1.5 and l2_distance(v,'[0,0,0]') < 3 order by l2_distance(v,'[0,0,0]') limit 10 by rank with option 'mode=post';
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='qrange' and i.name='qrangei8' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare entry_stats from @stats;
execute entry_stats;
deallocate prepare entry_stats;
select a from qrange where l2_distance_sq(v,'[0,0,0]') > 2.25 and l2_distance_sq(v,'[0,0,0]') < 9 order by l2_distance_sq(v,'[0,0,0]') limit 10 by rank with option 'mode=post';
select a from qrange where l2_distance(v,'[0,0,0]') < 1.1 order by l2_distance(v,'[0,0,0]') limit 10 by rank with option 'mode=post';
select a from qrange where l2_distance(v,'[0,0,0]') >= 2.5 and l2_distance(v,'[0,0,0]') <= 2.5 order by l2_distance(v,'[0,0,0]') limit 10 by rank with option 'mode=post';
prepare clipped_range from 'select a from qrange where l2_distance(v,''[0,0,0]'') > ? and l2_distance(v,''[0,0,0]'') < ? order by l2_distance(v,''[0,0,0]'') limit 10 by rank with option ''mode=post''';
set @lo = 1.5;
set @hi = 3;
execute clipped_range using @lo,@hi;
set @lo = NULL;
execute clipped_range using @lo,@hi;
set @lo = 1.5;
execute clipped_range using @lo,@hi;
deallocate prepare clipped_range;
-- PRE membership must filter before its bounded Top-K: 60 nearer nonmatches
-- exceed the 50-candidate budget for K=10. BF16 represents both 0 and 2 exactly.
set experimental_ivf_index = 1;
set ivf_preload_entries = 0;
create table qmembership(a int primary key, grp int, v vecf32(1));
insert into qmembership select result, 1, case when result <= 60 then '[0]' else '[2]' end from generate_series(1,70) g;
create index qmembershipbf16 using ivfflat on qmembership(v) lists=1 op_type 'vector_l2_ops' include(grp) quantization 'bf16';
select count(*) from (select a from qmembership where l2_distance(v,'[0]') > 1 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=pre') s;
select count(*) from (select a from qmembership where l2_distance(v,'[0]') > 1 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=include') s;
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='qmembership' and i.name='qmembershipbf16' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare entry_stats from @stats;
execute entry_stats;
deallocate prepare entry_stats;
select count(*) from (select a from qmembership where l2_distance(v,'[0]') > 1 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=pre') s;
select count(*) from (select a from qmembership where l2_distance(v,'[0]') > 1 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=include') s;
select count(*) from (select a from qmembership where v is not null and l2_distance(v,'[0]') > 1 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=pre') s;
select count(*) from (select a from qmembership where l2_distance(v,'[0]') > 2 and l2_distance(v,'[0]') < 3 order by l2_distance(v,'[0]') limit 10 by rank with option 'mode=include') s;
create table q64(a int primary key, v vecf64(4));
insert into q64 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index q64f32 using ivfflat on q64(v) lists=2 op_type 'vector_l2_ops' quantization 'float32';
select a from q64 order by l2_distance(v,'[1,1,1,1]') limit 3;
alter table q64 drop index q64f32;
create index q64i8 using ivfflat on q64(v) lists=2 op_type 'vector_l2_ops' quantization 'int8';
select a from q64 order by l2_distance(v,'[1,1,1,1]') limit 3;
-- uint8 QUANTIZATION (unsigned [0,255]) on f32 and f64 bases
alter table q32 drop index q32i8;
create index q32u8 using ivfflat on q32(v) lists=2 op_type 'vector_l2_ops' quantization 'uint8';
select a from q32 order by l2_distance(v,'[1,1,1,1]') limit 3;
alter table q64 drop index q64i8;
create index q64u8 using ivfflat on q64(v) lists=2 op_type 'vector_l2_ops' quantization 'uint8';
select a from q64 order by l2_distance(v,'[1,1,1,1]') limit 3;
-- an unsupported quantization name still errors
create table qbad(a int primary key, v vecf32(4));
create index qb using ivfflat on qbad(v) lists=1 op_type 'vector_l2_ops' quantization 'int16';

-- ============================================================
-- REINDEX over a quantized index.
--
-- CREATE INDEX and REINDEX share runCreateOrReindex, but only REINDEX takes the
-- rebuild branch: it deletes the old entries (ivfIndexDeleteOldEntries), bumps
-- the meta version, and repopulates. For int8/uint8 the entries build reads the
-- trained [min,max] back out of the meta table (readQuantizeBound) to rebuild
-- the same affine map — a path CREATE INDEX on an empty meta table never takes.
--
-- Rows are added between build and reindex so the rebuild has new data to place,
-- and the searches assert the index still returns cluster A after each step.
-- ============================================================
create table rq(a int primary key, v vecf32(4));
insert into rq values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index rqi8 using ivfflat on rq(v) lists=2 op_type 'vector_l2_ops' quantization 'int8';
select a from rq order by l2_distance(v,'[1,1,1,1]') limit 3;

insert into rq values (7,'[2,2,2,2]'),(8,'[56,56,56,56]');
alter table rq alter reindex rqi8 ivfflat lists=2;
select a from rq order by l2_distance(v,'[1,1,1,1]') limit 3;

-- uint8 takes the other arm of the same quantize branch
create table rqu(a int primary key, v vecf32(4));
insert into rqu values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index rqu8 using ivfflat on rqu(v) lists=2 op_type 'vector_l2_ops' quantization 'uint8';
insert into rqu values (7,'[2,2,2,2]');
alter table rqu alter reindex rqu8 ivfflat lists=2;
select a from rqu order by l2_distance(v,'[1,1,1,1]') limit 3;

drop database ivfq;
