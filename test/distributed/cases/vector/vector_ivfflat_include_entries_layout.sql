drop database if exists vector_ivfflat_include_phase2;
create database vector_ivfflat_include_phase2;
use vector_ivfflat_include_phase2;

drop table if exists vector_ivfflat_include_phase2;
create table vector_ivfflat_include_phase2(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

insert into vector_ivfflat_include_phase2 values
    (1, "[1,2,3]", "alpha", 10),
    (2, "[4,5,6]", "beta", 20),
    (3, "[7,8,9]", "gamma", 30);

create index idx_ivf_include using ivfflat on vector_ivfflat_include_phase2(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

set @entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_include'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'vector_ivfflat_include_phase2'
      )
    limit 1
);

select @entries is not null;

select attname, attnum, att_is_hidden
from mo_catalog.mo_columns
where att_database = database()
  and att_relname = @entries
order by attnum;

set @q = concat(
    'select `__mo_index_pri_col`, `__mo_index_include_title`, `__mo_index_include_category` ',
    'from `', database(), '`.`', @entries, '` ',
    'order by `__mo_index_pri_col`'
);
prepare s1 from @q;
execute s1;
deallocate prepare s1;

drop table vector_ivfflat_include_phase2;
drop database vector_ivfflat_include_phase2;
