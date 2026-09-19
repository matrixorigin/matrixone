-- @suit
-- @case
-- @desc: COPY ALTER supports adding regular, generated-column, and plugin indexes atomically
-- @label:bvt

drop database if exists alter_copy_add_index;
create database alter_copy_add_index;
use alter_copy_add_index;

-- A regular index can reference a column introduced by the same COPY ALTER.
create table ordinary(id bigint primary key, payload int);
insert into ordinary values (1, 10), (2, 20);
alter table ordinary add index idx_category(category), add column category int default 7;
select id, category from ordinary force index(idx_category) where category = 7 order by id;
update ordinary set category = 8 where id = 2;
select id, category from ordinary force index(idx_category) where category = 8;

-- Explicit COPY is also valid for an otherwise INPLACE-capable ADD INDEX.
alter table ordinary algorithm=copy, add index idx_payload(payload);
select id, payload from ordinary force index(idx_payload) where payload = 10;

-- STORED and VIRTUAL generated columns are bound against the evolving schema.
create table docs_stored(id bigint primary key, doc json);
insert into docs_stored values
    (1, '{"kind":"alpha"}'),
    (2, '{"kind":"beta"}');
alter table docs_stored
    add column kind varchar(20) generated always as (doc ->> '$.kind') stored,
    add index idx_kind(kind);
select id, kind from docs_stored force index(idx_kind) where kind = 'alpha';
update docs_stored set doc = '{"kind":"gamma"}' where id = 2;
select id, kind from docs_stored force index(idx_kind) where kind = 'gamma';

create table docs_virtual(id bigint primary key, doc json);
insert into docs_virtual values
    (1, '{"kind":"left"}'),
    (2, '{"kind":"right"}');
alter table docs_virtual
    add column kind varchar(20) generated always as (doc ->> '$.kind') virtual,
    add index idx_kind(kind);
select id, kind from docs_virtual force index(idx_kind) where kind = 'right';
update docs_virtual set doc = '{"kind":"updated"}' where id = 1;
select id, kind from docs_virtual force index(idx_kind) where kind = 'updated';

-- A new UNIQUE index still validates copied source rows. Failure is atomic.
create table duplicate_values(id bigint primary key, value_col int);
insert into duplicate_values values (1, 1), (2, 1);
alter table duplicate_values add column note int, add unique index uk_value(value_col);
select count(*) from information_schema.columns
where table_schema = 'alter_copy_add_index'
  and table_name = 'duplicate_values'
  and column_name = 'note';
select count(*) from information_schema.statistics
where table_schema = 'alter_copy_add_index'
  and table_name = 'duplicate_values'
  and index_name = 'uk_value';

-- A new plugin index on an existing column has no source hidden table to clone;
-- COPY must rebuild it even though the added column is unrelated.
create table fulltext_docs(id bigint primary key, body text);
insert into fulltext_docs values (1, 'alpha beta'), (2, 'beta gamma');
alter table fulltext_docs add column note int, add fulltext index ft_body(body);
set @ft_table = (
    select index_table_name
    from mo_catalog.mo_indexes
    where table_id = (
        select rel_id from mo_catalog.mo_tables
        where reldatabase = database() and relname = 'fulltext_docs'
    )
    and name = 'ft_body'
    limit 1
);
set @ft_count_sql = concat('select count(*) from `', database(), '`.`', @ft_table, '`');
prepare ft_count from @ft_count_sql;
execute ft_count;
deallocate prepare ft_count;
select id from fulltext_docs where match(body) against('alpha') order by id;
update fulltext_docs set body = 'alpha updated' where id = 2;
select id from fulltext_docs where match(body) against('alpha') order by id;

set experimental_hnsw_index = 1;
create table vectors(id bigint primary key, embedding vecf32(3));
insert into vectors values (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]');
alter table vectors
    add column note int,
    add index h_embedding using hnsw(embedding) op_type 'vector_l2_ops';
select algo, algo_table_type
from mo_catalog.mo_indexes
where table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'alter_copy_add_index' and relname = 'vectors'
)
and algo = 'hnsw'
order by algo_table_type;
select id from vectors order by l2_distance(embedding, '[1,0,0]') limit 1;
-- @regex("hnsw_search", true)
explain select id from vectors order by l2_distance(embedding, '[1,0,0]') limit 1;
set experimental_hnsw_index = 0;

-- Synchronous IVF-FLAT is also excluded from COPY INSERT maintenance and is
-- populated once by the post-copy rebuild.
set experimental_ivf_index = 1;
create table ivf_vectors(id bigint primary key, embedding vecf32(3));
insert into ivf_vectors values (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]');
alter table ivf_vectors
    add column note int,
    add index i_embedding using ivfflat(embedding)
        lists=1 op_type 'vector_l2_ops' kmeans_train_percent 100 kmeans_max_iteration 20;
set @ivf_entries = (
    select index_table_name
    from mo_catalog.mo_indexes
    where table_id = (
        select rel_id from mo_catalog.mo_tables
        where reldatabase = database() and relname = 'ivf_vectors'
    )
    and name = 'i_embedding'
    and algo_table_type = 'entries'
    limit 1
);
set @ivf_count_sql = concat('select count(*) from `', database(), '`.`', @ivf_entries, '`');
prepare ivf_count from @ivf_count_sql;
execute ivf_count;
deallocate prepare ivf_count;
select id from ivf_vectors order by l2_distance(embedding, '[1,0,0]') limit 1;
set experimental_ivf_index = 0;

-- Keep the base-table ids across DROP DATABASE so teardown can be checked
-- before the next test entry has a chance to clean up stale resources.
set @ordinary_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'ordinary'
);
set @docs_stored_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'docs_stored'
);
set @docs_virtual_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'docs_virtual'
);
set @duplicate_values_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'duplicate_values'
);
set @fulltext_docs_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'fulltext_docs'
);
set @vectors_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'vectors'
);
set @ivf_vectors_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = database() and relname = 'ivf_vectors'
);
select count(*) from mo_catalog.mo_tables
where rel_id in (
    @ordinary_id, @docs_stored_id, @docs_virtual_id, @duplicate_values_id,
    @fulltext_docs_id, @vectors_id, @ivf_vectors_id
);

drop database alter_copy_add_index;

select count(*) from mo_catalog.mo_tables
where reldatabase = 'alter_copy_add_index';
select count(*) from mo_catalog.mo_indexes
where table_id in (
    @ordinary_id, @docs_stored_id, @docs_virtual_id, @duplicate_values_id,
    @fulltext_docs_id, @vectors_id, @ivf_vectors_id
);
select count(*) from mo_catalog.mo_iscp_log
where table_id in (@fulltext_docs_id, @vectors_id, @ivf_vectors_id)
  and drop_at is null;
select count(*) from mo_catalog.mo_index_update
where db_name = 'alter_copy_add_index';
