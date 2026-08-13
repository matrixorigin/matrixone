-- fulltext2 half of #27027: MATCH() AGAINST() binds to the same unevaluable placeholders
-- as classic fulltext and is resolved by the same matcher, so a view definition no
-- fulltext2 index can serve is equally unrunnable and must be refused rather than
-- persisted. The plugin hook is implemented per algorithm, so fulltext2 needs its own
-- coverage -- a fulltext-only test would still pass if fulltext2's hook were missing.
set experimental_fulltext2_index = 1;

drop database if exists ft2_view_ddl;
create database ft2_view_ddl;
use ft2_view_ddl;

create table docs (id bigint primary key, body text, title text);
insert into docs values
    (1, 'hello matrixone', 'alpha'),
    (2, 'other text', 'hello title'),
    (3, 'hello hello database', 'gamma');

-- ---------------- no index: all three DDL forms are refused --------------------
create view v_create_bad as select id from docs where match(body) against('hello');
select count(*) as leftover from information_schema.views
where table_schema = 'ft2_view_ddl' and table_name = 'v_create_bad';

create view v_alter as select id, body from docs;
select count(*) as before_alter from v_alter;
alter view v_alter as select id from docs where match(body) against('hello');
select count(*) as after_alter from v_alter;

create view v_replace as select id, body from docs;
create or replace view v_replace as select id from docs where match(body) against('hello');
select count(*) as after_replace from v_replace;

-- an index on the wrong column does not satisfy MATCH(body)
create fulltext2 index ft2_title on docs(title);
create view v_wrong_col as select id from docs where match(body) against('hello');

-- ---------------- with a matching fulltext2 index it must WORK -----------------
create fulltext2 index ft2_body on docs(body);
create view v_good as select id from docs where match(body) against('hello');
select id from v_good order by id;

create view v_good_score as
select id, match(body) against('hello') as score from docs where match(body) against('hello');
select id from v_good_score order by id;

drop database ft2_view_ddl;
