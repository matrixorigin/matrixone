-- issue #31422: no-op and partially no-op UPDATE RETURNING return the final
-- image of every matched row; the row-count guard must not turn that into an
-- internal error, and write rejections keep surfacing the write error.
drop database if exists dml_returning_noop_update;
create database dml_returning_noop_update;
use dml_returning_noop_update;

create table noop_t(id int primary key, v int not null);
insert into noop_t values (1, 7), (2, 70), (3, 70);
update noop_t set v = 8 where id = 1 returning id, v;
select row_count();
update noop_t set v = 8 where id = 1 returning id, v;
update noop_t set v = case when id = 2 then v + 1 else v end where id in (2, 3) returning id, v;
update noop_t set v = 8 where id = 1 and v = 9 returning id, v;
select row_count();
update noop_t set v = null where id = 1 returning id, v;
select * from noop_t order by id;
begin;
update noop_t set v = 88 where id = 1 returning id, v;
rollback;
select v from noop_t where id = 1;
drop database if exists dml_returning_noop_update;
