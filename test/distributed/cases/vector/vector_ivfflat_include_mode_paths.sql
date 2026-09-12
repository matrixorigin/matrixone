drop database if exists vector_ivfflat_include_phase6;
create database vector_ivfflat_include_phase6;
use vector_ivfflat_include_phase6;

drop table if exists vector_ivfflat_include_phase6;
create table vector_ivfflat_include_phase6(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

create index idx_ivf_include_phase6 using ivfflat on vector_ivfflat_include_phase6(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

insert into vector_ivfflat_include_phase6 values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[1,2,4]", "beta", 20, "n2"),
    (3, "[9,9,9]", "gamma", 30, "n3"),
    (4, "[2,2,2]", "delta", 40, "n4");

-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id, title, category
from vector_ivfflat_include_phase6
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

select id, title, category
from vector_ivfflat_include_phase6
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id, title, category
from vector_ivfflat_include_phase6
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

select id, title, category
from vector_ivfflat_include_phase6
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id, title, note
from vector_ivfflat_include_phase6
where category >= 20 and note in ("n2", "n4")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

select id, title, note
from vector_ivfflat_include_phase6
where category >= 20 and note in ("n2", "n4")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

-- Scalar PRE must finish the non-covered note domain before candidate Top-K.
-- Reused prepared executions have independent domains, including exact empty.
set optimizer_hints = 'vectorLocalDOP=1';
prepare pre_domain from 'select id from vector_ivfflat_include_phase6 where note=? order by l2_distance(embedding,"[1,2,3]") limit 1 by rank with option ''mode=pre''';
set @pre_note='n2';
execute pre_domain using @pre_note;
set @pre_note='n4';
execute pre_domain using @pre_note;
set @pre_note='missing';
execute pre_domain using @pre_note;
deallocate prepare pre_domain;
set optimizer_hints = '';

-- Noninteger required domains use exact expressions, never a Bloom-only heap.
create table string_pre(id varchar(8) primary key, embedding vecf32(3), selected int);
insert into string_pre values ('near','[0,0,0]',0),('member','[2,0,0]',1),('far','[4,0,0]',1);
create index string_pre_idx using ivfflat on string_pre(embedding) lists=1 op_type 'vector_l2_ops';
select id from string_pre where selected=1 order by l2_distance(embedding,'[0,0,0]') limit 1 by rank with option 'mode=pre';
drop table string_pre;

drop table vector_ivfflat_include_phase6;
drop database vector_ivfflat_include_phase6;
