drop database if exists prefix_index_quoted_identifier;
create database prefix_index_quoted_identifier;
use prefix_index_quoted_identifier;

-- Prefix metadata must support quoted column names that contain the legacy
-- metadata delimiters (':' and ',').
create table create_t (
    id int primary key,
    `a:b` varchar(32),
    `c,d` varchar(32),
    payload varchar(32),
    key idx_pair(`a:b`(3), `c,d`(2))
);
insert into create_t values
    (1, 'alpha-one', 'xy-one', 'p1'),
    (2, 'bravo-two', 'yz-two', 'p2');
show create table create_t;
select algo_params
from mo_catalog.mo_indexes
where table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'prefix_index_quoted_identifier' and relname = 'create_t'
) and name = 'idx_pair' and column_name = 'a:b';
select mo_ctl('dn', 'flush', 'prefix_index_quoted_identifier.create_t');
select id, `a:b`, `c,d`, payload
from create_t force index(idx_pair)
where `a:b` = 'alpha-one' and `c,d` = 'xy-one';
select id, `a:b`, `c,d`, payload
from create_t ignore index(idx_pair)
where `a:b` = 'alpha-one' and `c,d` = 'xy-one';

-- Exercise the CREATE INDEX path as well.
create table create_index_t (
    id int primary key,
    `a:b` varchar(32),
    `c,d` varchar(32)
);
create index idx_pair on create_index_t(`a:b`(3), `c,d`(2));
insert into create_index_t values (1, 'alpha-one', 'xy-one');
show create table create_index_t;
select id, `a:b`, `c,d`
from create_index_t force index(idx_pair)
where `a:b` = 'alpha-one' and `c,d` = 'xy-one';

-- ALTER TABLE ADD INDEX has a third builder path.
create table alter_add_t (
    id int primary key,
    `a:b` varchar(32),
    `c,d` varchar(32)
);
alter table alter_add_t add index idx_pair(`a:b`(3), `c,d`(2));
insert into alter_add_t values (1, 'alpha-one', 'xy-one');
show create table alter_add_t;
select id, `a:b`, `c,d`
from alter_add_t force index(idx_pair)
where `a:b` = 'alpha-one' and `c,d` = 'xy-one';

-- Renaming an existing prefix-indexed column into a delimiter-bearing quoted
-- name must migrate its legacy metadata to v2.
create table rename_t (
    id int primary key,
    plain varchar(32),
    key idx_plain(plain(3))
);
insert into rename_t values (1, 'alpha-one');
alter table rename_t rename column plain to `a:b`;
show create table rename_t;
select algo_params
from mo_catalog.mo_indexes
where table_id = (
    select rel_id from mo_catalog.mo_tables
    where reldatabase = 'prefix_index_quoted_identifier' and relname = 'rename_t'
) and name = 'idx_plain' and column_name = 'a:b';
select mo_ctl('dn', 'flush', 'prefix_index_quoted_identifier.rename_t');
select id, `a:b`
from rename_t force index(idx_plain) where `a:b` = 'alpha-one';

drop database prefix_index_quoted_identifier;
