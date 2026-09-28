-- #29079: a top-level indexed MATCH must still be rewritten to the fulltext index when the query
-- block also contains a scalar subquery, NOT EXISTS (ANTI join), or correlated EXISTS (MARK join).
-- These subqueries do not touch fulltext membership, so the MATCH stays on the driving relation and
-- driving it is row-equivalent. Previously the ANTI/MARK shapes returned ERROR 20105.
drop database if exists ft2_subq;
create database ft2_subq;
use ft2_subq;
set experimental_fulltext2_index = 1;

create table docs(id bigint primary key, body text);
insert into docs values (1,'needle anchor'),(2,'needle anchor'),(3,'filler'),(5,'needle anchor');
create table q(name varchar(10) primary key, n int);
insert into q values ('lo', 0);
create fulltext2 index ft on docs(body);

-- Control: direct indexed MATCH uses fulltext2_search.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select id from docs where match(body) against('+needle +anchor' in boolean mode) order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode) order by id;

-- Scalar subquery in WHERE / projection / ORDER BY: still indexed, scalar applied normally.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and id > (select n from q where name='lo') order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and id > (select n from q where name='lo') order by id;
select id, (select n from q where name='lo') as marker from docs
  where match(body) against('+needle +anchor' in boolean mode) order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  order by (select n from q where name='lo'), id;

-- NOT EXISTS (ANTI join): the MATCH is on the preserved docs side. Was 20105.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='missing') order by id;
-- q has no 'missing' row -> NOT EXISTS true -> all matches returned.
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='missing') order by id;
-- q has 'lo' -> NOT EXISTS false -> empty (the ANTI filtering is preserved through the rewrite).
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and not exists (select 1 from q where name='lo') order by id;

-- Correlated EXISTS (MARK join): the MATCH is on the preserved docs side. Was 20105.
-- @separator:table
-- @regex("fulltext2_search", true)
explain select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;
-- q.n=0 < every matching id -> all matches returned.
select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;
-- q.n=2 -> only matching docs with id>2 qualify (id 5); the correlation is preserved.
update q set n=2 where name='lo';
select d.id from docs d where match(d.body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where q.n < d.id) order by d.id;
update q set n=0 where name='lo';

-- Independent EXISTS / IN already worked; keep as controls.
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and exists (select 1 from q where name='lo') order by id;
select id from docs where match(body) against('+needle +anchor' in boolean mode)
  and id in (select n from q) order by id;

-- Control: same subqueries without MATCH still work (no fulltext involved).
select id from docs where id > (select n from q where name='lo') and id < 3 order by id;

drop database ft2_subq;
