-- Rebuilding after DML must compact sparse NULL metadata with the source rows.
set experimental_fulltext2_index = 1;
drop database if exists ft2_sparse_nulls;
create database ft2_sparse_nulls;
use ft2_sparse_nulls;
create table docs (id bigint primary key, body text, tag int);
insert into docs
select result,
       case when result = 66 then null when result > 49 then 'needle omega' else 'alpha plain' end,
       case when result = 66 or result % 17 = 0 then null else result % 7 end
from generate_series(1,66,1) g;
create fulltext2 index ix_ft on docs(body);
select id from docs where match(body) against('needle') order by id limit 7;

prepare sparse_match from 'select id from docs where match(body) against(?) order by id limit 7';
set @sparse_term = 'needle';
execute sparse_match using @sparse_term;

update docs set body = 'plain changed' where id = 50;
delete from docs where id = 51;
insert into docs values (70,'needle omega',2);
alter table docs alter reindex ix_ft fulltext2 force_sync;

-- Reuse the prepared query after synchronous publication; verify exact keys, not just no panic.
execute sparse_match using @sparse_term;
select id from docs where match(body) against('needle') order by id;
select id, body, tag from docs where body is null order by id;
deallocate prepare sparse_match;
set @sparse_term = null;
drop database ft2_sparse_nulls;
