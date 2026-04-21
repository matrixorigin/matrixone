drop database if exists vector_ivfflat_include_phase8;
create database vector_ivfflat_include_phase8;
use vector_ivfflat_include_phase8;

drop table if exists phase8_main;
create table phase8_main(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

insert into phase8_main values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[1,2,4]", "beta", 20, "n2"),
    (3, "[9,9,9]", "gamma", 30, "n3"),
    (4, "[2,2,2]", "delta", 40, "n4"),
    (5, "[1,2,5]", "epsilon", 50, "n5");

create index idx_ivf_include_phase8 using ivfflat on phase8_main(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

show create table phase8_main;

-- @separator:table
explain select id, title, category
from phase8_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

select id, title, category
from phase8_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

-- @separator:table
explain select id, title, note
from phase8_main
where category >= 20 and note in ("n2", "n5")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

select id, title, note
from phase8_main
where category >= 20 and note in ("n2", "n5")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=include';

set @entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_include_phase8'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'phase8_main'
      )
    limit 1
);

set @entries_sql = concat(
    'select `__mo_index_pri_col`, `__mo_index_include_title`, `__mo_index_include_category` ',
    'from `', database(), '`.`', @entries, '` ',
    'order by `__mo_index_pri_col`'
);
prepare s_entries from @entries_sql;
execute s_entries;

update phase8_main
set title = 'beta2', category = 25, note = 'n2b'
where id = 2;
insert into phase8_main values
    (6, "[1,2,3.5]", "zeta", 35, "n6");
delete from phase8_main where id = 1;

execute s_entries;
deallocate prepare s_entries;

select id, title, category, note
from phase8_main
order by id;

alter table phase8_main alter reindex idx_ivf_include_phase8 ivfflat lists=3;

show create table phase8_main;
show index from phase8_main;

set @entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_include_phase8'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'phase8_main'
      )
    limit 1
);

set @entries_sql = concat(
    'select `__mo_index_pri_col`, `__mo_index_include_title`, `__mo_index_include_category` ',
    'from `', database(), '`.`', @entries, '` ',
    'order by `__mo_index_pri_col`'
);
prepare s_entries from @entries_sql;
execute s_entries;
deallocate prepare s_entries;

-- @separator:table
explain select id, title, category
from phase8_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

select id, title, category
from phase8_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

drop table phase8_main;

drop table if exists phase8_empty;
create table phase8_empty(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

create index idx_ivf_include_phase8_empty using ivfflat on phase8_empty(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

-- @separator:table
explain select id, title, category
from phase8_empty
order by l2_distance(embedding, "[1,1,1]")
limit 2 by rank with option 'mode=include';

select id, title, category
from phase8_empty
order by l2_distance(embedding, "[1,1,1]")
limit 2 by rank with option 'mode=include';

drop table phase8_empty;

drop table if exists phase8_nulls;
create table phase8_nulls(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

insert into phase8_nulls values
    (1, "[1,1,1]", null, 10),
    (2, "[1,1,2]", "beta", null),
    (3, "[2,2,2]", "gamma", 30);

create index idx_ivf_include_phase8_nulls using ivfflat on phase8_nulls(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

select id, title, category
from phase8_nulls
order by l2_distance(embedding, "[1,1,1]")
limit 3 by rank with option 'mode=include';

drop table phase8_nulls;

drop table if exists phase8_many_include;
create table phase8_many_include(
    id int primary key,
    embedding vecf32(3),
    c1 varchar(10),
    c2 varchar(10),
    c3 int,
    c4 int,
    c5 varchar(10),
    c6 varchar(10)
);

insert into phase8_many_include values
    (1, "[1,0,0]", "a1", "b1", 11, 21, "e1", "f1"),
    (2, "[2,0,0]", "a2", "b2", 12, 22, "e2", "f2");

create index idx_ivf_include_phase8_many using ivfflat on phase8_many_include(embedding)
lists=2 op_type "vector_l2_ops" include(c1, c2, c3, c4, c5, c6);

show create table phase8_many_include;
show index from phase8_many_include;

select id, c1, c2, c3, c4, c5, c6
from phase8_many_include
order by l2_distance(embedding, "[1,0,0]")
limit 2 by rank with option 'mode=include';

drop table phase8_many_include;

drop table if exists phase8_perf_include;
create table phase8_perf_include(
    id int primary key,
    embedding vecf32(4),
    title varchar(32),
    category int,
    note varchar(16)
);

drop table if exists phase8_perf_plain;
create table phase8_perf_plain(
    id int primary key,
    embedding vecf32(4),
    title varchar(32),
    category int,
    note varchar(16)
);

insert into phase8_perf_include
select
    result,
    case result % 4
        when 0 then "[0.1,0.2,0.3,0.4]"
        when 1 then "[0.1,0.2,0.3,0.5]"
        when 2 then "[0.2,0.2,0.3,0.4]"
        else "[0.3,0.2,0.3,0.4]"
    end,
    concat('title_', result),
    result % 200,
    case when result % 5 = 0 then 'keep' else 'skip' end
from generate_series(1, 8000) g;

insert into phase8_perf_plain
select * from phase8_perf_include;

create index idx_ivf_include_phase8_perf using ivfflat on phase8_perf_include(embedding)
lists=16 op_type "vector_l2_ops" include(title, category);

create index idx_ivf_plain_phase8_perf using ivfflat on phase8_perf_plain(embedding)
lists=16 op_type "vector_l2_ops";

-- @separator:table
explain select id, title, category
from phase8_perf_include
where category between 20 and 120
order by l2_distance(embedding, "[0.1,0.2,0.3,0.4]")
limit 20 by rank with option 'mode=include';

-- @separator:table
explain select id, title, category
from phase8_perf_plain
where category between 20 and 120
order by l2_distance(embedding, "[0.1,0.2,0.3,0.4]")
limit 20 by rank with option 'mode=post';

select count(*) from (
    select id, title, category
    from phase8_perf_include
    where category between 20 and 120
    order by l2_distance(embedding, "[0.1,0.2,0.3,0.4]")
    limit 20 by rank with option 'mode=include'
) as t;

select count(*) from (
    select id, title, category
    from phase8_perf_plain
    where category between 20 and 120
    order by l2_distance(embedding, "[0.1,0.2,0.3,0.4]")
    limit 20 by rank with option 'mode=post'
) as t;

drop table phase8_perf_include;
drop table phase8_perf_plain;

drop database vector_ivfflat_include_phase8;
