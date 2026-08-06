-- MySQL user variables are lazily created: an unassigned variable reads NULL.
select @never_set;
select @never_set is null;

-- SELECT ... INTO @var returns a status packet and stores the single result row.
select abs(-5) into @out;
select @out, @OUT;

-- Multiple expressions map to multiple user variables.
select 7, 'ok', null into @out_number, @out_text, @out_null;
select @out_number, @out_text, @out_null is null;

drop database if exists mysql_compat_user_variables;
create database mysql_compat_user_variables;
use mysql_compat_user_variables;

create table uv_src(id int primary key, v varchar(10));
insert into uv_src values (1, 'one'), (2, 'two'), (3, 'three');

-- SELECT ... FROM ... INTO @var assigns the only result row.
select id, v from uv_src where id = 2 into @row_id, @row_v;
select @row_id, @row_v;

-- ORDER BY/LIMIT still feeds SELECT ... INTO as a single-row statement.
select v from uv_src order by id desc limit 1 into @last_v;
select @last_v;

-- A zero-row SELECT ... INTO leaves the previous variable value unchanged.
set @keep = 'old';
select v from uv_src where id = 100 into @keep;
select @keep;

-- Aggregates over empty input still produce one row and assign that value.
select count(*) from uv_src where id = 100 into @empty_count;
select @empty_count;

-- MySQL rejects SELECT ... INTO @var when more than one row is returned.
-- @regex("Result consisted of more than one row", true)
select id from uv_src into @too_many;

drop database mysql_compat_user_variables;
