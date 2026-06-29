-- @label:bvt
drop database if exists update_text_coalesce_cast;
create database update_text_coalesce_cast;
use update_text_coalesce_cast;

create table t1(id int primary key, txt text, vc varchar(255));
insert into t1 values(1, repeat('a', 260), null);

prepare s1 from 'update t1 set txt = concat(coalesce(txt, ''''), ?) where id = ?';
set @suffix = 'b';
set @id = 1;
execute s1 using @suffix, @id;
select length(txt) from t1;

update t1 set txt = null where id = 1;
execute s1 using @suffix, @id;
select txt from t1;

update t1 set txt = repeat('c', 260) where id = 1;
update t1 set txt = case when id = 1 then concat(txt, 'd') else '' end where id = 1;
select length(txt) from t1;

update t1 set txt = repeat('e', 260) where id = 1;
update t1 set txt = if(id = 1, concat(txt, 'f'), '') where id = 1;
select length(txt) from t1;

update t1 set txt = repeat('g', 260), vc = null where id = 1;
update t1 set txt = concat(coalesce(vc, txt, ''), 'h') where id = 1;
select length(txt) from t1;

update t1 set txt = repeat('i', 260) where id = 1;
select length(cast(txt as varchar(255))) from t1;

update t1 set txt = repeat('j', 260), vc = null where id = 1;
insert into t1(id, vc) select 2, txt from t1 where id = 1;

update t1 set txt = repeat('k', 260), vc = null where id = 1;
insert into t1 values(1, 'x', '') on duplicate key update vc = txt;

update t1 set txt = repeat('c', 260) where id = 1;
update t1 set vc = txt where id = 1;

-- multi-row mixed length in one UPDATE: an over-width TEXT row and a short row;
-- the COALESCE/CONCAT result must stay TEXT for every row (no narrowing).
drop table if exists t_multi;
create table t_multi(id int primary key, txt text);
insert into t_multi values(1, repeat('a', 260)), (2, 'short');
update t_multi set txt = concat(coalesce(txt, ''), 'x');
select length(txt) from t_multi order by id;
drop table t_multi;

drop table t1;
drop database update_text_coalesce_cast;
