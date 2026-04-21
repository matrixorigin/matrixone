drop database if exists vector_ivfflat_include_phase3;
create database vector_ivfflat_include_phase3;
use vector_ivfflat_include_phase3;

drop table if exists vector_ivfflat_include_phase3;
create table vector_ivfflat_include_phase3(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

create index idx_ivf_include_dml using ivfflat on vector_ivfflat_include_phase3(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

insert into vector_ivfflat_include_phase3 values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[4,5,6]", "beta", 20, "n2"),
    (3, "[7,8,9]", "gamma", 30, "n3");

set @entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_include_dml'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'vector_ivfflat_include_phase3'
      )
    limit 1
);

set @q = concat(
    'select `__mo_index_pri_col`, `__mo_index_include_title`, `__mo_index_include_category` ',
    'from `', database(), '`.`', @entries, '` ',
    'order by `__mo_index_pri_col`'
);

prepare s1 from @q;
execute s1;

update vector_ivfflat_include_phase3
set title = 'beta2', category = 200, note = 'n2b'
where id = 2;
execute s1;

delete from vector_ivfflat_include_phase3 where id = 1;
execute s1;

deallocate prepare s1;

drop table vector_ivfflat_include_phase3;
drop database vector_ivfflat_include_phase3;
