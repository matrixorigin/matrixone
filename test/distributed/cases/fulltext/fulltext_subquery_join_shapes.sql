-- #29079: classic FULLTEXT parity -- a top-level indexed MATCH must still be rewritten to the
-- fulltext index when the query block also contains NOT EXISTS (ANTI join) or correlated EXISTS
-- (MARK join). Previously these returned ERROR 20105.
drop database if exists ft_subq;
create database ft_subq;
use ft_subq;

create table docs(id bigint primary key, body text);
insert into docs values (1,'needle anchor'),(2,'needle anchor'),(3,'filler'),(5,'needle anchor');
create table q(name varchar(10) primary key, n int);
insert into q values ('lo', 0);
create fulltext index ft on docs(body);

-- Control: direct indexed MATCH uses fulltext_index_scan.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select id from docs where match(body) against('+needle +anchor' in boolean mode) order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode) order by id;

-- NOT EXISTS (ANTI join): MATCH on the preserved docs side. Was 20105.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='missing') order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='missing') order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='lo') order by id;

-- Correlated EXISTS (MARK join): MATCH on the preserved docs side. Was 20105.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;
select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;
update q set n=2 where name='lo';
select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;

drop database ft_subq;
