-- 清空表不应改变默认值元数据，尤其不能混淆 SQL NULL、字符串 'null' 和空字符串。
drop database if exists truncate_column_default;
create database truncate_column_default;
use truncate_column_default;

create table default_probe (
    id int primary key,
    missing varchar(32),
    explicit_null varchar(32) default null,
    quoted_null varchar(32) default 'null',
    empty_value varchar(32) default '',
    text_value varchar(32) default 'hello',
    numeric_value int default 7
);
select column_name, column_default, column_default is null as default_is_null,
       hex(column_default) as default_hex, is_nullable
from information_schema.columns
where table_schema = 'truncate_column_default' and table_name = 'default_probe'
order by ordinal_position;
show create table default_probe;
desc default_probe;
insert into default_probe(id) values (1);
select id, missing is null, explicit_null is null,
       hex(quoted_null) as quoted_null_hex, quoted_null is null as quoted_null_is_null,
       length(empty_value), text_value, numeric_value from default_probe order by id;

truncate table default_probe;
select count(*) from default_probe;
select column_name, column_default, column_default is null as default_is_null,
       hex(column_default) as default_hex, is_nullable
from information_schema.columns
where table_schema = 'truncate_column_default' and table_name = 'default_probe'
order by ordinal_position;
show create table default_probe;
desc default_probe;

-- 新连接必须看到相同的 NULL 位和字节，省略列插入仍使用原有默认值。
-- @session:id=1{
use truncate_column_default;
select column_name, column_default, column_default is null as default_is_null,
       hex(column_default) as default_hex, is_nullable
from information_schema.columns
where table_schema = 'truncate_column_default' and table_name = 'default_probe'
order by ordinal_position;
insert into default_probe(id) values (2);
select id, missing is null, explicit_null is null,
       hex(quoted_null) as quoted_null_hex, quoted_null is null as quoted_null_is_null,
       length(empty_value), text_value, numeric_value from default_probe order by id;

-- 再次重建后仍保持同一元数据契约。
truncate table default_probe;
select count(*) from default_probe;
select column_name, column_default, column_default is null as default_is_null,
       hex(column_default) as default_hex, is_nullable
from information_schema.columns
where table_schema = 'truncate_column_default' and table_name = 'default_probe'
order by ordinal_position;
-- @session}

drop database truncate_column_default;
