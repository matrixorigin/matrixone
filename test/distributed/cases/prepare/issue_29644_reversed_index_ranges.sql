-- @case
-- @desc: Reversed prepared secondary-index ranges are empty and preserve subsequent bindings.
-- @label:bvt

drop database if exists issue_29644_ranges;
create database issue_29644_ranges;
use issue_29644_ranges;
create table indexed_rows(id int primary key, k int, key numeric_k(k));
insert into indexed_rows select result, result from generate_series(1,256) g;
select mo_ctl('dn','flush','issue_29644_ranges.indexed_rows');
set @index_table = (select distinct index_table_name from mo_catalog.mo_indexes
  where name = 'numeric_k' and table_id = (select rel_id from mo_catalog.mo_tables
  where reldatabase = 'issue_29644_ranges' and relname = 'indexed_rows'));
select mo_ctl('dn','flush',concat('issue_29644_ranges.',@index_table));

prepare ranges from 'select k from indexed_rows where k between ? and ? or k between ? and ? order by k';
set @a=32, @b=34, @c=37, @d=38;
execute ranges using @a,@b,@c,@d;
set @a=34, @b=32, @c=38, @d=37;
execute ranges using @a,@b,@c,@d;
set @a=33, @b=33, @c=37, @d=37;
execute ranges using @a,@b,@c,@d;
set @a=34, @b=32, @c=37, @d=38;
execute ranges using @a,@b,@c,@d;
set @a=32, @b=34, @c=33, @d=35;
execute ranges using @a,@b,@c,@d;
set @a=null, @b=34, @c=37, @d=38;
execute ranges using @a,@b,@c,@d;
set @a=80, @b=82, @c=85, @d=86;
execute ranges using @a,@b,@c,@d;
deallocate prepare ranges;

prepare reversed_first from 'select k from indexed_rows where k between ? and ? or k between ? and ? order by k';
set @a=34, @b=32, @c=38, @d=37;
execute reversed_first using @a,@b,@c,@d;
deallocate prepare reversed_first;
select k from indexed_rows where k between 34 and 32 or k between 38 and 37 order by k;

create table plain_rows(id int primary key, k int);
insert into plain_rows select id,k from indexed_rows;
select mo_ctl('dn','flush','issue_29644_ranges.plain_rows');
prepare plain_ranges from 'select k from plain_rows where k between ? and ? or k between ? and ? order by k';
execute plain_ranges using @a,@b,@c,@d;
deallocate prepare plain_ranges;
set @index_table=null;
drop database issue_29644_ranges;
