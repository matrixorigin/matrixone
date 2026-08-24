-- Multi-table INSERT: conditional routing (INSERT ALL vs INSERT FIRST, overlapping WHENs, ELSE)
drop database if exists mi_cond;
create database mi_cond;
use mi_cond;

-- deterministic source: 1000 rows, cat in 0..4, val = 3*id, nullable score (NULL for every 10th row)
create table src (id int primary key, cat int, val int, score int, txt varchar(20));
insert into src select result, result % 5, result * 3, if(result % 10 = 0, NULL, result % 100), concat('t', result) from generate_series(1, 1000) g;
select count(*), sum(val), count(score) from src;

create table t_even (id int primary key, val int);
create table t_div3 (id int primary key, val int);
create table t_big (id int primary key, val int);
create table t_rest (id int primary key, val int);

-- ================= INSERT ALL: overlapping conditions, a row lands in EVERY matching target
insert all
  when val % 2 = 0 then into t_even (id, val) values (id, val)
  when val % 3 = 0 then into t_div3 (id, val) values (id, val)
  when id > 900   then into t_big  (id, val) values (id, val)
  else                 into t_rest (id, val) values (id, val)
select id, val from src;
-- every target equals the plain filtered SELECT
select (select count(*) from t_even) = (select count(*) from src where val % 2 = 0) as even_ok;
select (select count(*) from t_div3) = (select count(*) from src where val % 3 = 0) as div3_ok;
select (select count(*) from t_big)  = (select count(*) from src where id > 900) as big_ok;
select count(*) from t_rest;
-- val is always a multiple of 3, so t_div3 holds the whole source and ELSE never fires
select count(*) from t_div3;
-- overlap really happened: rows present in several targets
select count(*) from t_even e join t_div3 d on e.id = d.id;
select count(*) from t_even e join t_big b on e.id = b.id;
select count(*) from t_div3 d join t_big b on d.id = b.id;
-- affected rows are counted per target
select (select count(*) from t_even) + (select count(*) from t_div3) + (select count(*) from t_big) + (select count(*) from t_rest) as total_written;

-- ================= INSERT FIRST: same conditions, a row lands in the FIRST matching target only
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert first
  when val % 2 = 0 then into t_even (id, val) values (id, val)
  when val % 3 = 0 then into t_div3 (id, val) values (id, val)
  when id > 900   then into t_big  (id, val) values (id, val)
  else                 into t_rest (id, val) values (id, val)
select id, val from src;
select count(*) from t_even;
select count(*) from t_div3;
select count(*) from t_big;
select count(*) from t_rest;
-- targets are disjoint and together cover the source exactly once
select count(*) from t_even e join t_div3 d on e.id = d.id;
select count(*) from t_even e join t_big b on e.id = b.id;
select count(*) from t_div3 d join t_big b on d.id = b.id;
select (select count(*) from t_even) + (select count(*) from t_div3) + (select count(*) from t_big) + (select count(*) from t_rest) = (select count(*) from src) as covers_source_once;
-- the third WHEN is shadowed: every id > 900 is even or a multiple of 3
select count(*) from t_big;

-- ================= ELSE receives rows matching no WHEN; NULL conditions never match
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert first
  when score > 90 then into t_big  (id, val) values (id, score)
  when score < 10 then into t_even (id, val) values (id, score)
  else                 into t_rest (id, val) values (id, coalesce(score, -1))
select id, score from src;
select count(*) from t_big;
select count(*) from t_even;
select count(*) from t_rest;
-- the 100 NULL scores all went to ELSE
select count(*) from t_rest where val = -1;
select (select count(*) from t_big) + (select count(*) from t_even) + (select count(*) from t_rest) = 1000 as covers_source_once;

-- INSERT ALL with NULL condition and no ELSE: NULL-score rows are dropped
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert all
  when score >= 50 then into t_big  (id, val) values (id, score)
  when score <  50 then into t_even (id, val) values (id, score)
select id, score from src;
select (select count(*) from t_big) + (select count(*) from t_even) as non_null_rows;
select count(*) from src where score is null;

-- ELSE only (no WHEN matches at all)
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert first
  when id < 0 then into t_big (id, val) values (id, val)
  else             into t_rest (id, val) values (id, val)
select id, val from src;
select count(*) from t_big;
select count(*) from t_rest;

-- ================= conditions of every shape
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
create table vip (id int primary key);
insert into vip values (5), (50), (500);
insert first
  when id in (select id from vip)          then into t_big  (id, val) values (id, val)
  when txt like 't1%' and cat between 1 and 2 then into t_even (id, val) values (id, val)
  when score is null or mod(id, 7) = 0     then into t_div3 (id, val) values (id, val)
  else                                          into t_rest (id, val) values (id, val)
select id, cat, val, score, txt from src;
select * from t_big order by id;
select (select count(*) from t_even) = (select count(*) from src where txt like 't1%' and cat between 1 and 2 and id not in (5, 50, 500)) as even_ok;
select (select count(*) from t_div3) = (select count(*) from src where (score is null or mod(id, 7) = 0) and not (txt like 't1%' and cat between 1 and 2) and id not in (5, 50, 500)) as div3_ok;
select (select count(*) from t_even) + (select count(*) from t_div3) + (select count(*) from t_big) + (select count(*) from t_rest) = 1000 as covers_source_once;

-- conditions on computed source columns (aliases) and expressions in VALUES
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert first
  when bucket = 'high' then into t_big  (id, val) values (id, doubled)
  when bucket = 'mid'  then into t_even (id, val) values (id, doubled + 1)
  else                      into t_rest (id, val) values (id, case when doubled > 100 then 100 else doubled end)
select id, val * 2 as doubled, case when val > 2000 then 'high' when val > 1000 then 'mid' else 'low' end as bucket from src;
select count(*), min(val), max(val) from t_big;
select count(*), min(val), max(val) from t_even;
select count(*), min(val), max(val) from t_rest;

-- ================= several INTO clauses under one WHEN; the same table under several WHENs
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
create table audit (seq int auto_increment primary key, id int, tag varchar(10));
insert first
  when cat = 0 then into t_even (id, val) values (id, val)
                    into audit (id, tag) values (id, 'zero')
  when cat = 1 then into t_even (id, val) values (id, -val)
                    into audit (id, tag) values (id, 'one')
  else              into audit (id, tag) values (id, 'other')
select id, cat, val from src;
select count(*), sum(case when val > 0 then 1 else 0 end), sum(case when val < 0 then 1 else 0 end) from t_even;
select tag, count(*) from audit group by tag order by tag;
select count(*) from audit;

-- ================= source shapes: aggregate, join, union, distinct, order by + limit
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
create table per_cat (cat int primary key, n int, total bigint);
insert all
  when n >= 200 then into per_cat (cat, n, total) values (cat, n, total)
select cat, count(*) as n, sum(val) as total from src group by cat;
select * from per_cat order by cat;

-- WHEN / VALUES see the source query's OUTPUT columns, not its table aliases
insert first
  when v.id is not null then into t_big  (id, val) values (s.id, s.val)
  else                       into t_rest (id, val) values (s.id, s.val)
select s.id, s.val, v.id as vid from src s left join vip v on s.id = v.id where s.id <= 60;
insert first
  when vid is not null then into t_big  (id, val) values (id, val)
  else                      into t_rest (id, val) values (id, val)
select s.id, s.val, v.id as vid from src s left join vip v on s.id = v.id where s.id <= 60;
select * from t_big order by id;
select count(*) from t_rest;

delete from t_big; delete from t_rest;
insert all
  when id <= 2 then into t_big (id, val) values (id, val)
  else              into t_rest (id, val) values (id, val)
select id, val from src where id <= 3 union all select id + 10, val from src where id <= 3;
select * from t_big order by id;
select * from t_rest order by id;

delete from t_big; delete from t_rest;
insert first
  when cat = 4 then into t_big (id, val) values (id, val)
  else              into t_rest (id, val) values (id, val)
select id, cat, val from src order by val desc limit 10;
select * from t_big order by id;
select * from t_rest order by id;

-- distinct source
delete from t_big;
insert all into t_big (id, val) values (cat, cat) select distinct cat from src;
select * from t_big order by id;

-- ================= empty source: nothing written, no error
delete from t_big; delete from t_rest;
insert first when id > 0 then into t_big (id, val) values (id, val) else into t_rest (id, val) values (id, val) select id, val from src where id > 100000;
select count(*) from t_big;
select count(*) from t_rest;

-- ================= a failure in one branch rolls back every branch
delete from t_even; delete from t_div3; delete from t_big; delete from t_rest;
insert into t_div3 values (996, 0);
insert first
  when cat = 0 then into t_even (id, val) values (id, val)
  when cat = 1 then into t_div3 (id, val) values (id, val)
  else              into t_rest (id, val) values (id, val)
select id, cat, val from src where id >= 990;
select count(*) from t_even;
select count(*) from t_rest;
select count(*) from t_div3;

-- ================= inside an explicit transaction, visible before commit, gone after rollback
begin;
insert first
  when cat = 0 then into t_even (id, val) values (id, val)
  else              into t_rest (id, val) values (id, val)
select id, cat, val from src where id <= 10;
select count(*) from t_even;
select count(*) from t_rest;
rollback;
select count(*) from t_even;
select count(*) from t_rest;
begin;
insert first
  when cat = 0 then into t_even (id, val) values (id, val)
  else              into t_rest (id, val) values (id, val)
select id, cat, val from src where id <= 10;
commit;
select count(*) from t_even;
select count(*) from t_rest;

drop database mi_cond;
