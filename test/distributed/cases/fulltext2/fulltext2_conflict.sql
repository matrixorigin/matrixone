-- fulltext2 DDL guard: a classic FULLTEXT and a FULLTEXT2 index must NOT coexist on
-- the same column set — they would both resolve the same MATCH(...) verb, making the
-- answering engine (classic SQL fulltext vs WAND positional) order-dependent. Creating
-- the second one (either order, CREATE INDEX or inline in CREATE TABLE) is rejected;
-- the two engines on DIFFERENT columns are fine.
set experimental_fulltext2_index = 1;
drop database if exists ft2_conflict;
create database ft2_conflict;
use ft2_conflict;

create table t (id bigint primary key, body text, title text);

-- classic first, then fulltext2 on the SAME column -> rejected
create fulltext index ft1 on t(body);
create fulltext2 index ft2 on t(body);

-- fulltext2 on a DIFFERENT column is allowed
create fulltext2 index ft3 on t(title);

-- reverse order: fulltext2 first, then classic on the same column -> rejected
create table t2 (id bigint primary key, body text);
create fulltext2 index f2 on t2(body);
create fulltext index f1 on t2(body);

-- CREATE TABLE with BOTH engines inline on the same column -> rejected
create table t3 (id bigint primary key, body text, fulltext(body), fulltext2(body));

-- ALTER with BOTH engines on the same column in ONE statement (multi-action) -> rejected
create table t4 (id bigint primary key, body text);
alter table t4 add fulltext ftx(body), add fulltext2 fty(body);

drop database ft2_conflict;
