-- Test: FULLTEXT INDEX with snapshot restore
-- Verifies that secondary fulltext index tables survive snapshot restore operations

drop database if exists db_ft_snap;
create database db_ft_snap;
use db_ft_snap;

-- ============================================================
-- Part 1: Basic fulltext index + snapshot restore
-- ============================================================
drop table if exists ft_test;
create table ft_test (
    id int primary key,
    content text,
    fulltext index ft_content (content) with parser ngram
);

insert into ft_test values (1, 'hello world');
insert into ft_test values (2, 'foo bar baz');

-- Verify fulltext search works before snapshot
select id from ft_test where match(content) against('hello' in boolean mode);

-- Create snapshot
create snapshot ft_snap1 for account sys;

-- Add more data after snapshot
insert into ft_test values (3, 'new data after snapshot');

-- Verify fulltext search includes new data
select id from ft_test where match(content) against('new' in boolean mode);

-- Restore from snapshot: delete current data and re-insert from snapshot
delete from ft_test;
insert into ft_test select * from db_ft_snap.ft_test {snapshot = 'ft_snap1'};

-- Verify: should only have rows 1 and 2 (snapshot state)
-- @sortkey:0
select id, content from ft_test;

-- Verify fulltext index still works after restore
select id from ft_test where match(content) against('hello' in boolean mode);
select id from ft_test where match(content) against('new' in boolean mode);

drop snapshot if exists ft_snap1;
drop table if exists ft_test;

-- ============================================================
-- Part 2: Multiple fulltext indexes + snapshot restore
-- ============================================================
drop table if exists ft_multi;
create table ft_multi (
    id int primary key,
    title varchar(200),
    body text,
    fulltext index ft_title (title) with parser ngram,
    fulltext index ft_body (body) with parser ngram
);

insert into ft_multi values (1, 'database tutorial', 'learn how to use databases');
insert into ft_multi values (2, 'search engine', 'fulltext search is powerful');

create snapshot ft_snap2 for account sys;

-- Modify data
update ft_multi set title = 'MODIFIED' where id = 1;
insert into ft_multi values (3, 'extra row', 'should not exist after restore');

-- Restore
delete from ft_multi;
insert into ft_multi select * from db_ft_snap.ft_multi {snapshot = 'ft_snap2'};

-- Verify both indexes work
select id from ft_multi where match(title) against('database' in boolean mode);
select id from ft_multi where match(body) against('search' in boolean mode);
-- Should return nothing (row 3 was post-snapshot)
select id from ft_multi where match(title) against('extra' in boolean mode);

drop snapshot if exists ft_snap2;
drop table if exists ft_multi;

-- ============================================================
-- Part 3: Snapshot restore with fulltext index - table-level
-- ============================================================
drop table if exists ft_restore;
create table ft_restore (
    id int primary key,
    doc text,
    fulltext index ft_doc (doc) with parser ngram
);

insert into ft_restore values (1, 'alpha beta gamma');
insert into ft_restore values (2, 'delta epsilon zeta');

create snapshot ft_snap3 for account sys;

-- Drop and recreate the table with different data
drop table ft_restore;
create table ft_restore (
    id int primary key,
    doc text,
    fulltext index ft_doc (doc) with parser ngram
);
insert into ft_restore values (10, 'completely different');

-- Restore from snapshot
delete from ft_restore;
insert into ft_restore select * from db_ft_snap.ft_restore {snapshot = 'ft_snap3'};

-- Verify original data is back
-- @sortkey:0
select id, doc from ft_restore;
select id from ft_restore where match(doc) against('alpha' in boolean mode);
select id from ft_restore where match(doc) against('different' in boolean mode);

drop snapshot if exists ft_snap3;
drop table if exists ft_restore;

-- ============================================================
-- Part 4: WITH PARSER gojieba — Chinese word segmentation survives restore
-- ============================================================
-- The ngram parser used in Parts 1-3 leaves the index dependent only on the
-- raw text. Gojieba is dictionary-backed, so this verifies that the index
-- rebuild on restore picks up the same parser configuration as the source.
drop table if exists ft_jieba;
create table ft_jieba (
    id int primary key,
    content text,
    fulltext index ft_content (content) with parser gojieba
);

insert into ft_jieba values (1, '我来到北京清华大学');
insert into ft_jieba values (2, '我来到上海');
insert into ft_jieba values (3, '清华大学很有名');

-- Pre-snapshot sanity: 我来到 phrase matches docs 1 and 2.
-- @sortkey:0
select id from ft_jieba where match(content) against('"我来到"' in boolean mode);

create snapshot ft_snap4 for account sys;

-- Mutate after snapshot.
insert into ft_jieba values (4, '崇文门');
update ft_jieba set content = 'modified' where id = 1;

-- Restore from snapshot.
delete from ft_jieba;
insert into ft_jieba select * from db_ft_snap.ft_jieba {snapshot = 'ft_snap4'};

-- Post-restore: index must once again be jieba-segmented. 我来到 phrase
-- should match docs 1 and 2; doc 4 must be gone; doc 1's content must
-- match its pre-snapshot value (not 'modified').
-- @sortkey:0
select id from ft_jieba where match(content) against('"我来到"' in boolean mode);
-- @sortkey:0
select id from ft_jieba where match(content) against('"清华大学"' in boolean mode);
select id from ft_jieba where match(content) against('modified' in boolean mode);

drop snapshot if exists ft_snap4;
drop table if exists ft_jieba;

drop database if exists db_ft_snap;
