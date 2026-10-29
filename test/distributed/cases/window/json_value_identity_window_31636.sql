-- issue #31636 persistent regression: JSON value identity must drive window
-- PARTITION BY segmentation and the correlated pagination rewrite exactly as
-- GROUP BY / DISTINCT / comparison operators treat JSON values.
drop database if exists issue31636_json_window;
create database issue31636_json_window;
use issue31636_json_window;

-- source data from the issue: JSON numeric twins, duplicate keys and NULL
create table src(id int primary key, j json, v int);
insert into src values
    (1, '1', 10),
    (2, '1.0', 20),
    (3, '1', 30),
    (4, '2', 40),
    (5, '2.0', 50),
    (6, null, 60);

-- controls: value semantics that main already gets right
select count(*) as cnt, sum(v) as total from src group by j order by sum(v), count(*);
select count(distinct j) as dj from src;

-- W1: ranked + running window over JSON partition keys
select id,
       row_number() over(partition by j order by id) as rn,
       sum(v) over(partition by j order by id rows unbounded preceding) as total
from src order by id;

-- W2: whole-partition window aggregates
select id,
       count(*) over(partition by j) as cnt,
       sum(v) over(partition by j) as total
from src order by id;

-- S1: correlated scalar with ORDER BY ... LIMIT 1
select l.id,
       (select r.id from src r where r.j = l.j order by r.id desc limit 1) as picked
from src l order by l.id;

-- S3: correlated scalar with ORDER BY ... LIMIT 1 OFFSET 1
select l.id,
       (select r.id from src r where r.j = l.j order by r.id desc limit 1 offset 1) as picked
from src l order by l.id;

-- S2 control: equivalent MAX form
select l.id,
       (select max(r.id) from src r where r.j = l.j) as picked
from src l order by l.id;

-- P1: INSERT SELECT persistence of window results
create table copied(id int primary key, rn bigint, total bigint);
insert into copied
select id,
       row_number() over(partition by j order by id),
       sum(v) over(partition by j order by id rows unbounded preceding)
from src;
select * from copied order by id;

-- P2: INSERT SELECT persistence of the correlated LIMIT scalar must persist
-- one row per outer row instead of the multi-row rewrite output
create table picked(id int primary key, picked int);
insert into picked
select l.id, (select r.id from src r where r.j = l.j order by r.id desc limit 1)
from src l;
select * from picked order by id;

-- J1: number spellings, objects, arrays and NULL share one JSON identity
create table jforms(id int primary key, j json);
insert into jforms values
    (1, '1'),
    (2, '1.0'),
    (3, '1e0'),
    (4, '{"a": 1}'),
    (5, '{"a": 1.0}'),
    (6, '[1]'),
    (7, '[1.0]'),
    (8, null),
    (9, null);
select id,
       count(*) over(partition by j) as cnt,
       row_number() over(partition by j order by id) as rn
from jforms order by id;

-- N1: non-JSON controls keep their own semantics
create table nform(id int primary key, s varchar(10), d decimal(10, 2));
insert into nform values (1, '1', 1), (2, '1.0', 1.0);
select id, count(*) over(partition by s) as cnt from nform order by id;
select id, count(*) over(partition by d) as cnt from nform order by id;

-- E1: statement atomicity of the window INSERT SELECT under a duplicate-PK
-- failure: the target keeps its prior state and the source is untouched
create table keyed(id int primary key, rn bigint, total bigint);
insert into keyed values (1, 1, 10);
insert into keyed
select id,
       row_number() over(partition by j order by id),
       sum(v) over(partition by j order by id rows unbounded preceding)
from src;
select count(*) as cnt from keyed;
select * from keyed order by id;
select count(*) as cnt, sum(v) as total from src group by j order by sum(v), count(*);

-- same-connection reuse after the failed statement
select 1;

drop database if exists issue31636_json_window;
