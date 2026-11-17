-- Consumer compatibility, source identity, output-position mapping and private
-- delta columns share one two-row fixture. Each async oracle asserts the complete
-- tiny result; poll deadlines only bound liveness, never decide correctness.
drop database if exists mv_index_interop;
create database mv_index_interop;
use mv_index_interop;
create table src(id bigint primary key, k int, v int, body text, __mo_sign int, __MO_SIGN_1 int);
insert into src values (1,7,10,'needle',1,10),(2,7,20,'hay',1,20);
create fulltext index ft on src(body);

-- Explicit target names override inner SELECT headings by position.
create materialized view mv_complete(x,c) refresh complete on demand as select k,count(*) n from src group by k;
refresh materialized view mv_complete;
select count(*) = 1 and min(x) = 7 and min(c) = 2 as complete_matches from mv_complete;
create materialized view mv_expr(a,b) refresh complete on demand as select k,k+1 from src;
refresh materialized view mv_expr;
select count(*) = 2 and min(a) = 7 and max(a) = 7 and min(b) = 8 and max(b) = 8 as expression_matches from mv_expr;
create materialized view mv_fast(g,s,a,m) refresh fast on change as select k,sum(v),avg(v),min(v) from src group by k;
create materialized view mv_union(g,c) refresh fast on change as select k,count(*) n from src group by k union all select k,count(*) from src where v >= 20 group by k;
-- Both generated-name candidates are legal user columns; case is significant
-- for preserving the source value but not for avoiding an identifier collision.
create materialized view mv_sign(g,c,s,a,d) refresh fast on change as select __mo_sign,count(*),sum(__MO_SIGN_1),avg(__MO_SIGN_1),count(distinct v) from src where __MO_SIGN_1 >= 0 group by __mo_sign;
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 7 and min(s) = 30 and min(a) = 15 and min(m) = 10 as fast_matches from mv_fast;
-- @wait_expect(2, 60)
select count(*) = 2 and min(g) = 7 and max(g) = 7 and min(c) = 1 and max(c) = 2 as union_matches from mv_union;
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 1 and min(c) = 2 and min(s) = 30 and min(a) = 15 and min(d) = 2 as sign_matches from mv_sign;
-- @wait_expect(2, 60)
select count(*) = 1 and min(id) = 1 as index_matches from src where match(body) against('needle' in boolean mode);

-- Running MV and index jobs at the same frontier must keep compatible batches.
insert into src values (3,7,30,'needle',1,30);
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 7 and min(s) = 60 and min(a) = 20 and min(m) = 10 as fast_matches from mv_fast;
-- @wait_expect(2, 60)
select count(*) = 2 and min(g) = 7 and max(g) = 7 and min(c) = 2 and max(c) = 3 as union_matches from mv_union;
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 1 and min(c) = 3 and min(s) = 60 and min(a) = 20 and min(d) = 3 as sign_matches from mv_sign;
-- @wait_expect(2, 60)
select count(*) = 2 and min(id) = 1 and max(id) = 3 as index_matches from src where match(body) against('needle' in boolean mode);

-- Delete exercises the private negative sign, DISTINCT state and the MIN
-- affected-group rebuild, which also consumes the renamed refresh outputs.
delete from src where id = 1;
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 7 and min(s) = 50 and min(a) = 25 and min(m) = 20 as fast_matches from mv_fast;
-- @wait_expect(2, 60)
select count(*) = 2 and min(g) = 7 and max(g) = 7 and min(c) = 2 and max(c) = 2 as union_matches from mv_union;
-- @wait_expect(2, 60)
select count(*) = 1 and min(g) = 1 and min(c) = 2 and min(s) = 50 and min(a) = 25 and min(d) = 2 as sign_matches from mv_sign;
-- @wait_expect(2, 60)
select count(*) = 1 and min(id) = 3 as index_matches from src where match(body) against('needle' in boolean mode);
update src set __mo_sign = 2, __MO_SIGN_1 = 40, v = 40 where id = 3;
-- @wait_expect(2, 60)
select count(*) = 2 and sum(g = 1 and c = 1 and s = 20 and a = 20 and d = 1) = 1 and sum(g = 2 and c = 1 and s = 40 and a = 40 and d = 1) = 1 as moved_group_matches from mv_sign;

-- Source rename invalidates MVs by design. After dropping them, the existing
-- index job must continue through its resolved source projection at the same ID.
drop materialized view mv_fast;
drop materialized view mv_union;
drop materialized view mv_sign;
drop materialized view mv_complete;
drop materialized view mv_expr;
alter table src rename to renamed;
insert into renamed values (4,7,50,'needle',1,50);
-- @wait_expect(2, 60)
select count(*) = 2 and min(id) = 3 and max(id) = 4 as renamed_index_matches from renamed where match(body) against('needle' in boolean mode);
delete from renamed where id = 3;
-- @wait_expect(2, 60)
select count(*) = 1 and min(id) = 4 as renamed_index_matches from renamed where match(body) against('needle' in boolean mode);
drop database mv_index_interop;
