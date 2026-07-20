-- Exact contracts for the dictionary-backed GoJieba tokenizer.

set experimental_fulltext_index = 1;

drop database if exists ft_gojieba_precise;
create database ft_gojieba_precise;
use ft_gojieba_precise;

-- Exact dictionary words, UTF-8 byte offsets, and trailing DocLen.
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, '我来到北京清华大学'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

-- ASCII tokens are lower-cased; punctuation is omitted; Chinese keeps byte offsets.
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, 'Hello, WORLD! MatrixOne数据库'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

-- Latin tokens are capped at 23 bytes.
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, 'abcdefghijklmnopqrstuvwxyzabcd'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

-- Repeated terms retain every position and contribute independently to DocLen.
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, 'red red red'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

-- Multiple indexed columns are joined by one boundary newline.
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body, column_2 as title
    from (values row(1, '我来到北京', 'MatrixOne 数据库'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body, title) as f;

-- The SQL index path uses dictionary-only segmentation (HMM disabled).
select f.pos, f.word
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, '他来到了网易杭研大厦'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

-- Inputs without searchable text emit neither tokens nor a DocLen row.
select count(*)
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, '     '))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

select count(*)
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, '，。！？；：、（）【】'))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

select count(*)
from (
    select cast(column_0 as bigint) as id, column_1 as body
    from (values row(1, ''))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

select count(*)
from (
    select cast(column_0 as bigint) as id, cast(column_1 as varchar) as body
    from (values row(1, null))
) as src
cross apply fulltext_index_tokenize('{"parser":"gojieba"}', 23, id, body) as f;

create table docs (
    id int primary key,
    title varchar(255),
    body text,
    tag int
);

insert into docs values
    (1, 'TitleOnlyToken', '', 0),
    (2, 'NullTitleToken', null, 0),
    (3, '', 'BodyOnlyToken', 0),
    (4, null, 'NullBodyToken', 0),
    (5, '校园', '我来到北京', 0),
    (6, null, null, 0);

create fulltext index ft_docs on docs(title, body) with parser gojieba;

-- Empty strings keep the other indexed column; NULL suppresses the composite document.
select id from docs
where match(title, body) against('+titleonlytoken' in boolean mode)
order by id;
select id from docs
where match(title, body) against('+nulltitletoken' in boolean mode)
order by id;
select id from docs
where match(title, body) against('+bodyonlytoken' in boolean mode)
order by id;
select id from docs
where match(title, body) against('+nullbodytoken' in boolean mode)
order by id;

-- A phrase must not cross the newline boundary between indexed columns.
select id from docs
where match(title, body) against('"校园我来到"' in boolean mode)
order by id;

-- Duplicate required terms must not duplicate a matching result row.
select id from docs
where match(title, body) against('+titleonlytoken +titleonlytoken' in boolean mode)
order by id;

-- Required terms from separate documents must not cross-match.
select id from docs
where match(title, body) against('+titleonlytoken +bodyonlytoken' in boolean mode)
order by id;

-- An empty boolean pattern is invalid rather than matching every document.
select id from docs
where match(title, body) against('' in boolean mode);

-- Updating a non-indexed column must preserve the fulltext entry.
update docs set tag = 7 where id = 1;
select id, tag from docs
where match(title, body) against('+titleonlytoken' in boolean mode);

-- NULL -> text creates an entry.
update docs set title = 'LifecycleTitle', body = 'LifecycleBody' where id = 6;
select id from docs
where match(title, body) against('+lifecycletitle +lifecyclebody' in boolean mode);

-- text -> NULL removes the whole composite entry.
update docs set title = null where id = 6;
select id from docs
where match(title, body) against('+lifecyclebody' in boolean mode);

-- NULL -> empty restores tokens from the populated column.
update docs set title = '' where id = 6;
select id from docs
where match(title, body) against('+lifecyclebody' in boolean mode);

-- TRUNCATE clears the hidden index; later inserts are indexed normally.
truncate table docs;
select count(*) from docs
where match(title, body) against('+titleonlytoken' in boolean mode);

insert into docs values (7, 'AfterTruncate', 'RestoredToken', 0);
select id from docs
where match(title, body) against('+aftertruncate +restoredtoken' in boolean mode);

drop database ft_gojieba_precise;
