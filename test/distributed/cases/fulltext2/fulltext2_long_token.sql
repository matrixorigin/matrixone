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
  (4, 'short'),
  (5, 'aяяяяяяяяяяяя'),
  (6, 'ȺȺȺȺȺȺȺȺ');
create fulltext2 index fi on ft(body) with parser ngram;

-- 26 b's are stored as the first 23. NL / BM25 / quoted-boolean must find docs 1 and 3.
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb') order by id;
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb' in bm25 mode) order by id;
select id from ft where match(body) against('"bbbbbbbbbbbbbbbbbbbbbbbbbb"' in boolean mode) order by id;
-- multi-run: the long run truncates, hello stays a hit -> doc 3.
select id from ft where match(body) against('hello bbbbbbbbbbbbbbbbbbbbbbbbbb') order by id;

-- mixed-width run: a + 12 Cyrillic я (25 bytes) is stored truncated to a + 10 я (21 bytes) on the
-- tokenizer's byte boundary, not the clean 23-byte UTF-8 boundary. NL / BM25 / quoted-boolean must
-- reproduce that exact truncation and find doc 5 (#29276).
select id from ft where match(body) against('aяяяяяяяяяяяя') order by id;
select id from ft where match(body) against('aяяяяяяяяяяяя' in bm25 mode) order by id;
select id from ft where match(body) against('"aяяяяяяяяяяяя"' in boolean mode) order by id;

-- #29271 P2 (expanding fold): U+023A (Ⱥ, Latin <0x7FF, 2 bytes) folds to U+2C65 (ⱥ, 3 bytes). A run of
-- 8×Ⱥ (16 input bytes) folds to 24 bytes and is stored re-capped to 7×ⱥ (21 bytes). NL / BM25 /
-- quoted-boolean must build the query term with the SAME post-fold cap and find doc 6 -- each returned
-- 0 rows before the reader was synchronized with the folded-token cap (it looked up the unstored 24-byte
-- term). This row actually indexes the expanding value, which the classic panic test did not.
select id from ft where match(body) against('ȺȺȺȺȺȺȺȺ') order by id;
select id from ft where match(body) against('ȺȺȺȺȺȺȺȺ' in bm25 mode) order by id;
select id from ft where match(body) against('"ȺȺȺȺȺȺȺȺ"' in boolean mode) order by id;
-- 7×Ⱥ folds to exactly 21 bytes -- the SAME stored token as 8×Ⱥ -- so it also finds doc 6 (boundary control).
select id from ft where match(body) against('ȺȺȺȺȺȺȺ') order by id;

-- Controls (already worked): unquoted boolean truncates via the tokenizer; the exact 23-byte token hits.
select id from ft where match(body) against('bbbbbbbbbbbbbbbbbbbbbbbbbb' in boolean mode) order by id;
select id from ft where match(body) against('+bbbbbbbbbbbbbbbbbbbbbbbbbb' in boolean mode) order by id;
select id from ft where match(body) against('aaaaaaaaaaaaaaaaaaaaaaa') order by id;

drop database ft2_longtok;
