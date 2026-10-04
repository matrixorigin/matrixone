-- Issue #26769: Power Query requires OFFSET without LIMIT for LimitOffset folding.
drop table if exists offset_without_limit;
create table offset_without_limit (id int primary key);
insert into offset_without_limit values (1), (2), (3);

select id from offset_without_limit order by id offset 1;
select id from offset_without_limit order by id offset 3;

prepare offset_without_limit_stmt from 'select id from offset_without_limit order by id offset ?';
set @skip_rows = 1;
execute offset_without_limit_stmt using @skip_rows;
set @skip_rows = 2;
execute offset_without_limit_stmt using @skip_rows;
deallocate prepare offset_without_limit_stmt;

drop table offset_without_limit;
