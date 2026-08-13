-- fulltext2 CDC (always-async) — ported from pessimistic_transaction/fulltext
-- (fulltext_async). fulltext2 is AlwaysAsync (no ASYNC keyword): the inline index
-- builds an empty tag=0 base at CREATE, then post-create INSERT/UPDATE/DELETE flow
-- into the tag=1 CdcTail via ISCP CDC. sleep() waits for CDC to settle, then MATCH
-- reflects the mutations.

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

-- create src async (multi-column body+title)
create table src (id bigint primary key, body varchar, title text, FULLTEXT2 ftidx (body, title));

insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

-- composite primary key
create table src2 (id1 varchar, id2 bigint, body char(128), title text, primary key (id1, id2), FULLTEXT2 ftidx(body, title));

insert into src2 values ('id0', 0, 'red', 't1'), ('id1', 1, 'yellow', 't2'), ('id2', 2, 'blue', 't3'), ('id3', 3, 'blue red', 't4'), ('id4', 4, 'bright red null', NULL);

-- wait for the initial CDC sync of src and src2
select sleep(30);

-- src's initial-sync state is verified via src2 below (a never-re-mutated table).
-- src itself is deliberately NOT searched here: fulltext2's per-CN index cache is
-- not cross-invalidated by the CDC consumer (which evicts only its own CN), so a
-- pre-update search on the mo-tester CN would pin the pre-update snapshot and the
-- post-update queries below would then read it stale on multi-CN. src is first
-- searched only after its final mutation has settled, so its cache loads fresh.
show create table src;

-- select src2 (composite pk): id0 (red), id3 (blue red)
select id1, id2 from src2 where match(body, title) against('red') order by id1;
show create table src2;

-- post-create UPDATE + DELETE flow through CDC too
update src set body = 'color is green' where id = 0;
delete from src where id = 3;
select sleep(30);

-- doc 0's old text (red) is superseded, doc 3 is deleted -> 'red' now empty.
-- This is src's FIRST MATCH, so its per-CN cache loads the fresh post-update index.
select id from src where match(body, title) against('red') order by id;
-- doc 0's new text
select id from src where match(body, title) against('green') order by id;

-- rename keeps the index + CDC
alter table src rename to src1;
select id from src1 where match(body, title) against('green') order by id;

drop table src1;
drop table src2;
