-- Issue #32691
drop database if exists issue32691_json_projection;
create database issue32691_json_projection;
use issue32691_json_projection;

create table docs (id int primary key, doc longtext);
create table tasks (id int primary key, a varchar(32), b varchar(32));
insert into docs values (1, '{"a":null,"b":"ready"}'), (2, '{"b":"missing-a"}'),
    (3, null), (4, '{bad');
insert into tasks values (1, 'before', 'before'), (2, 'before', 'before'),
    (3, 'before', 'before'), (4, 'before', 'before');

-- A present JSON null, a missing path, and a SQL NULL document.
select id, json_extract(doc, '$.a') is null as a_is_sql_null,
    json_unquote(json_extract(doc, '$.a')) = 'null' as a_is_json_null,
    json_unquote(json_extract(doc, '$.b')) as b
from docs where id < 4 order by id;

-- Both projections can read the same document. Row 4 still joins but its
-- malformed document is masked by both CASE branches.
update tasks t join docs d on t.id = d.id
set t.a = case when d.id < 4 then json_unquote(json_extract(d.doc, '$.a')) else t.a end,
    t.b = case when d.id < 4 then json_unquote(json_extract(d.doc, '$.b')) else t.b end;
select id, a, a is null as a_is_sql_null, b, b is null as b_is_sql_null
from tasks order by id;

-- Activate the malformed document. The valid rows would receive different
-- values if any part of this statement were committed.
update tasks t join docs d on t.id = d.id
set t.a = concat('retry:', json_unquote(json_extract(d.doc, '$.a'))),
    t.b = concat('retry:', json_unquote(json_extract(d.doc, '$.b')));
-- @session:id=1{
use issue32691_json_projection;
select id, a, a is null as a_is_sql_null, b, b is null as b_is_sql_null
from tasks order by id;
-- @session}

drop database issue32691_json_projection;
