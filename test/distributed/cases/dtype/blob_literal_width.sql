-- issue #31808: an oversized BLOB literal reports ER_DATA_TOO_LONG (1406) like TEXT
-- in strict mode and is truncated with a warning when sql_mode is empty,
-- instead of an internal error that ignores sql_mode.
drop database if exists blob_literal_width;
create database blob_literal_width;
use blob_literal_width;
create table t(id int, b blob, s text);
insert into t(id, b) values (1, cast(repeat('y', 65535) as char));
-- 65536 bytes as a literal
set @stmt = concat("insert into t(id, b) values (2, '", repeat('y', 65536), "')");
prepare q from @stmt;
execute q;
set @stmt = concat("insert into t(id, s) values (2, '", repeat('y', 65536), "')");
prepare q2 from @stmt;
execute q2;
select id, length(b), length(s) from t order by id;
set session sql_mode = '';
execute q;
execute q2;
select id, length(b), length(s) from t order by id;
set session sql_mode = default;
deallocate prepare q;
deallocate prepare q2;
drop database blob_literal_width;
