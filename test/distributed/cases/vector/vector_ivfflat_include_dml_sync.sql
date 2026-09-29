drop database if exists vector_ivfflat_include_phase3;
create database vector_ivfflat_include_phase3;
use vector_ivfflat_include_phase3;

-- This CDC-only PK scenario is independent of the synchronous INCLUDE cases
-- below. Start its insert-then-async-index build now and verify it only after
-- the synchronous work, overlapping convergence without weakening either wait.
create table vector_ivfflat_async_pk(
    id bigint primary key,
    embedding vecf32(3)
);
insert into vector_ivfflat_async_pk values
    (10, "[1,2,3]"),
    (30, "[7,8,9]");
create index idx_ivf_async_pk using ivfflat on vector_ivfflat_async_pk(embedding)
lists=1 op_type "vector_l2_ops" async;

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
    'select `__mo_index_pri_col`, `__mo_index_centroid_fk_entry`, `__mo_index_include_title`, `__mo_index_include_category` ',
    'from `', database(), '`.`', @entries, '` ',
    'order by `__mo_index_pri_col`'
);

prepare s1 from @q;
-- JDBC reports hidden VARCHAR width differently through proxy and direct CN;
-- this regression checks exact index payload values, not transport metadata.
-- @metacmp(false)
execute s1;

-- ODKU updates only INCLUDE payload columns on the conflicting row. The
-- materialized final image must rebuild the whole logical IVF group so the
-- hidden entry cannot retain the stale covering values.
insert into vector_ivfflat_include_phase3 values
    (2, "[9,9,9]", "odku", 200, "odku")
    on duplicate key update title = values(title), category = values(category), note = values(note);
-- @metacmp(false)
execute s1;

update vector_ivfflat_include_phase3
set note = 'n2-only'
where id = 2;
-- @metacmp(false)
execute s1;

update vector_ivfflat_include_phase3
set title = 'beta2', category = 200, note = 'n2b'
where id = 2;
-- @metacmp(false)
execute s1;

update vector_ivfflat_include_phase3
set embedding = "[4,5,7]"
where id = 2;
-- @metacmp(false)
execute s1;

update vector_ivfflat_include_phase3
set id = 20
where id = 2;
select id from vector_ivfflat_include_phase3 order by id;
-- @metacmp(false)
execute s1;

delete from vector_ivfflat_include_phase3 where id = 1;
-- @metacmp(false)
execute s1;

deallocate prepare s1;

-- A table without an explicit primary key uses the hidden fake PK as the IVF
-- entry identity. Rewriting indexed and INCLUDE payload must keep one entry per
-- base row, and rollback must restore both payloads.
create table vector_ivfflat_fake_pk(
    k int unique,
    embedding vecf32(3),
    title varchar(20)
);
create index idx_ivf_fake_pk using ivfflat on vector_ivfflat_fake_pk(embedding)
lists=1 op_type "vector_l2_ops" include(title);
insert into vector_ivfflat_fake_pk values
    (1, "[1,1,1]", "before"),
    (2, "[9,9,9]", "peer");

set @fake_entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_fake_pk'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'vector_ivfflat_fake_pk'
      )
    limit 1
);
set @fake_q = concat(
    'select count(*) as entry_count, group_concat(`__mo_index_include_title` order by `__mo_index_include_title`) as titles ',
    'from `', database(), '`.`', @fake_entries, '`'
);
prepare s2 from @fake_q;
-- GROUP_CONCAT metadata width is transport-dependent; values stay exact.
-- @metacmp(false)
execute s2;

update vector_ivfflat_fake_pk
set embedding = "[2,2,2]", title = "after"
where k = 1;
-- @metacmp(false)
execute s2;

begin;
update vector_ivfflat_fake_pk
set embedding = "[3,3,3]", title = "rollback"
where k = 1;
rollback;
-- @metacmp(false)
execute s2;

deallocate prepare s2;
drop table vector_ivfflat_fake_pk;

-- Async IVF maintenance is CDC-only. Updating the source PK must replace the
-- active-version entry identity without retaining the old PK or duplicating
-- the new one.
set @async_entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_async_pk'
      and algo = 'ivfflat'
      and algo_table_type = 'entries'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'vector_ivfflat_async_pk'
      )
    limit 1
);
set @async_metadata = (
    select index_table_name
    from mo_catalog.mo_indexes
    where name = 'idx_ivf_async_pk'
      and algo = 'ivfflat'
      and algo_table_type = 'metadata'
      and table_id in (
          select rel_id
          from mo_catalog.mo_tables
          where reldatabase = database()
            and relname = 'vector_ivfflat_async_pk'
      )
    limit 1
);
set @async_q = concat(
    'select count(*) as entry_count, group_concat(`__mo_index_pri_col` order by `__mo_index_pri_col`) as identities ',
    'from `', database(), '`.`', @async_entries, '` ',
    'where `__mo_index_centroid_fk_version` = (',
    'select cast(`__mo_index_val` as bigint) from `', database(), '`.`', @async_metadata, '` ',
    'where `__mo_index_key` = ''version'')'
);
prepare s3 from @async_q;
-- @metacmp(false)
-- @wait_expect(2, 60)
execute s3;

update vector_ivfflat_async_pk set id = 20 where id = 10;
-- @metacmp(false)
-- @wait_expect(2, 60)
execute s3;

select id from vector_ivfflat_async_pk order by id;
deallocate prepare s3;
drop table vector_ivfflat_async_pk;

drop table vector_ivfflat_include_phase3;
drop database vector_ivfflat_include_phase3;
