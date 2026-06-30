-- REPLACE into a table carrying an irregular (fulltext / IVF) index must maintain
-- that index synchronously on the modern path: the conflicting old row's entries
-- are dropped and the new row's entries inserted, so the new row is immediately
-- searchable and the old one is not. The delete is keyed by the matched old row's
-- PK, which can differ from the new row's PK when the conflict is on a non-PK
-- unique key.

-- ============================================================
-- fulltext
-- ============================================================
set experimental_fulltext_index = 1;
drop database if exists replace_irregular;
create database replace_irregular;
use replace_irregular;

drop table if exists t_ft;
create table t_ft(id int primary key, uk int unique, body text, fulltext(body));
insert into t_ft values (1, 10, 'hello world'), (2, 20, 'foo bar');

-- primary-key conflict: replace row 1
replace into t_ft values (1, 99, 'alpha beta');
select id, uk, body from t_ft order by id;
select id from t_ft where match(body) against('alpha') order by id;
select id from t_ft where match(body) against('hello') order by id;

-- unique-key conflict (uk=20 hits old id=2): old PK 2 differs from new PK 3, so
-- the old entries keyed by PK 2 must be dropped.
replace into t_ft values (3, 20, 'gamma delta');
select id, uk, body from t_ft order by id;
select id from t_ft where match(body) against('gamma') order by id;
select id from t_ft where match(body) against('foo') order by id;

-- no conflict: plain insert is indexed too
replace into t_ft values (4, 40, 'lambda mu');
select id from t_ft where match(body) against('lambda') order by id;

drop table if exists t_ft;

-- ============================================================
-- ivfflat vector
-- ============================================================
set experimental_ivf_index = 1;
drop table if exists t_vec;
create table t_vec(id int primary key, uk int unique, embedding vecf32(3));
insert into t_vec values (1, 10, '[1,1,1]'), (2, 20, '[9,9,9]');
create index idx_vec using ivfflat on t_vec(embedding) lists = 2 op_type 'vector_l2_ops';

-- primary-key conflict: move row 1 far away
replace into t_vec values (1, 99, '[100,100,100]');
select id, uk, embedding from t_vec order by id;
select id from t_vec order by l2_distance(embedding, '[100,100,100]') asc limit 1;
select id from t_vec order by l2_distance(embedding, '[1,1,1]') asc limit 1;

-- unique-key conflict (uk=20 hits old id=2): old PK 2 differs from new PK 3
replace into t_vec values (3, 20, '[5,5,5]');
select id, uk, embedding from t_vec order by id;
select id from t_vec order by l2_distance(embedding, '[5,5,5]') asc limit 1;

drop table if exists t_vec;

-- Merged-scan REPLACE: a real-PK table with no unique secondary key takes the
-- merged main-table scan path. Every conflict is a PK conflict, so the stale
-- irregular-index entries (keyed by the immutable PK) must still be dropped.
drop table if exists t_ft_nouk;
create table t_ft_nouk(id int primary key, body text, fulltext(body));
insert into t_ft_nouk values (1, 'hello world'), (2, 'foo bar');
-- PK conflict on id=1: old body's terms must be removed, new ones indexed.
replace into t_ft_nouk values (1, 'alpha beta');
select id from t_ft_nouk where match(body) against('alpha') order by id;
select id from t_ft_nouk where match(body) against('hello') order by id;
select id from t_ft_nouk where match(body) against('foo') order by id;
drop table if exists t_ft_nouk;

drop database if exists replace_irregular;
