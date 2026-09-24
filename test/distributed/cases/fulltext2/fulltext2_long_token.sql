-- #29276: a Latin word longer than the stored-token cap (MAX_TOKEN_SIZE = 23 bytes) is indexed
-- truncated, but the natural-language, quoted-boolean, and BM25 lookups used the raw untruncated
-- word (ngramPhraseSlots did not apply the cap) and returned 0 rows. The query token must be
-- truncated the same way the index truncates.
drop database if exists ft2_longtok;
create database ft2_longtok;
use ft2_longtok;
set experimental_fulltext2_index = 1;

create table ft(id int primary key, body varchar(200));
insert into ft values
  (1, 'bbbbbbbbbbbbbbbbbbbbbbbbbb'),
  (2, 'aaaaaaaaaaaaaaaaaaaaaaa'),
  (3, 'hello bbbbbbbbbbbbbbbbbbbbbbbbbb'),
  (4, 'short');
create fulltext2 index fi on ft(body) with parser ngram;

-- 26 b's are stored as the first 23. NL / BM25 / quoted-boolean must find docs 1 and 3.
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb') order by id;
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb' in bm25 mode) order by id;
select id from ft where match(body) against('"bbbbbbbbbbbbbbbbbbbbbbbbbb"' in boolean mode) order by id;
-- multi-run: the long run truncates, hello stays a hit -> doc 3.
select id from ft where match(body) against('hello bbbbbbbbbbbbbbbbbbbbbbbbbb') order by id;

-- Controls (already worked): unquoted boolean truncates via the tokenizer; the exact 23-byte token hits.
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb' in boolean mode) order by id;
select id from ft where match(body) against('+bbbbbbbbbbbbbbbbbbbbbbbbbb' in boolean mode) order by id;
select id from ft where match(body) against('aaaaaaaaaaaaaaaaaaaaaaa') order by id;

drop database ft2_longtok;
