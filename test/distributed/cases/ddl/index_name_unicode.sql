drop database if exists index_name_unicode;
create database index_name_unicode;
use index_name_unicode;

create table unicode_idx (
    a int,
    b int,
    key `Σ` (a),
    key `ς` (b)
);
show index from unicode_idx;

create index `σ` on unicode_idx(b);

drop index `ς` on unicode_idx;
show index from unicode_idx;

create index `ς` on unicode_idx(b);
show index from unicode_idx;

drop index `σ` on unicode_idx;
show index from unicode_idx;

drop database index_name_unicode;
