-- #29271: a quoted BOOLEAN-mode phrase on the ""/default/ngram parser must be decomposed by
-- SimpleTokenizer (the index-build tokenizer), not looked up as the raw whole string, which is never
-- an indexed token. Covers a CJK phrase (stored as trigrams), a short CJK phrase (prefix of a stored
-- trigram), a hyphen/apostrophe phrase (breakers at index time), and a Latin run over the 23-byte
-- stored-token cap. The unquoted natural-language / +boolean forms already worked and stay as controls.

set experimental_fulltext_index = 1;

drop database if exists ft_bool_phrase;
create database ft_bool_phrase;
use ft_bool_phrase;

create table ft(id int primary key, body varchar(200));
insert into ft values
 (1, '苹果香蕉'),
 (2, '红苹果甜'),
 (3, 'full-text search'),
 (4, 'Ma''trix Origin'),
 (5, 'abcdefghijklmnopqrstuvwxyz'),
 (6, '苹果香瓜'),
 (7, 'a中'),
 (8, '中b'),
 (9, 'c-'),
 (10, '文。');
create fulltext index fi on ft(body) with parser ngram;

-- CJK phrase: was 0 rows (raw '苹果香蕉' lookup), now matches the trigram-stored row 1, not the
-- sibling 苹果香瓜 (row 6).
select id from ft where match(body) against('"苹果香蕉"' in boolean mode) order by id;
-- short CJK phrase: prefix, reaches 苹果甜 in 红苹果甜 (row 2) and 苹果香 in rows 1/6.
select id from ft where match(body) against('"苹果"' in boolean mode) order by id;
-- hyphen phrase: index stored full/text/search, so the phrase must split on '-' and match row 3.
select id from ft where match(body) against('"full-text search"' in boolean mode) order by id;
-- apostrophe phrase: index stored ma/trix/origin, so the phrase splits on '\'' and matches row 4.
select id from ft where match(body) against('"Ma''trix Origin"' in boolean mode) order by id;
-- Latin run over 23 bytes: stored token is truncated to 23, the phrase looks up the same 23 and hits row 5.
select id from ft where match(body) against('"abcdefghijklmnopqrstuvwxyz"' in boolean mode) order by id;

-- #29271 P2: SHORT (< ngram) phrases that mix scripts or contain a breaker must tokenize like the
-- index, not look up the raw whole string. Each was 0 rows before the fix.
-- mixed short Latin+CJK: index stored 'a'@0,'中'@1; phrase splits to 'a' + '中*' and hits row 7.
select id from ft where match(body) against('"a中"' in boolean mode) order by id;
-- mixed short CJK+Latin, reversed: '中*'@0 + 'b' hits row 8.
select id from ft where match(body) against('"中b"' in boolean mode) order by id;
-- hyphen short phrase: '-' is a discarded breaker, so the phrase is just 'c' and hits row 9.
select id from ft where match(body) against('"c-"' in boolean mode) order by id;
-- punctuation short phrase: '。' discarded, phrase is '文*' and hits row 10.
select id from ft where match(body) against('"文。"' in boolean mode) order by id;

-- controls: the unquoted natural-language and +boolean forms already matched and are unchanged.
select id from ft where match(body) against('苹果香蕉' in natural language mode) order by id;
select id from ft where match(body) against('+苹果香蕉' in boolean mode) order by id;
select id from ft where match(body) against('+苹果' in boolean mode) order by id;

drop database ft_bool_phrase;
