-- @bvt:issue#23176
drop database if exists update_text_prepare_cast;
create database update_text_prepare_cast;
use update_text_prepare_cast;

create table t1(id int primary key, txt text);
insert into t1 values(1, repeat('a', 260));

prepare s1 from 'update t1 set txt = concat(coalesce(txt, ''''), ?) where id = ?';
set @suffix = 'b';
set @id = 1;
execute s1 using @suffix, @id;
select length(txt) from t1;

deallocate prepare s1;
drop table t1;
drop database update_text_prepare_cast;
