-- Direct MATCH score comparisons that a relevance of 0 can satisfy (#31604).
-- A row the index does not return has relevance 0, exactly like the
-- `... OR false` form. Rows are hand-labelled: 'anchor' is in every row
-- (score 0), 'rare' only in row 1, 'zzz' in no row.
set @saved_ft = @@experimental_fulltext_index;
set @saved_ft2 = @@experimental_fulltext2_index;
set experimental_fulltext_index = 1;
set experimental_fulltext2_index = 1;
drop database if exists ft_direct_score_compare;
create database ft_direct_score_compare;
use ft_direct_score_compare;

create table docs (id int primary key, body text);
insert into docs values (1, 'anchor rare'), (2, 'anchor common'), (3, 'anchor common');
create fulltext index ft on docs(body);
select id from docs where match(body) against('anchor') = 0 order by id;
select id from docs where match(body) against('anchor') = 0 or id = -1 order by id;
select id from docs where match(body) against('rare') = 0 order by id;
select id from docs where 0 = match(body) against('rare') order by id;
select id from docs where match(body) against('rare') < 0.01 order by id;
select id from docs where match(body) against('rare') >= 0 order by id;
select id from docs where match(body) against('rare') != 0 order by id;
select id from docs where match(body) against('zzz') = 0 order by id;
select id from docs where match(body) against('rare') = 0 and id > 2 order by id;
select id, match(body) against('rare') > 0 as hit from docs where match(body) against('rare') <= 0 order by id;
select count(*) from docs where match(body) against('rare') = 0;
-- controls: membership-implying shapes are unchanged
select id from docs where match(body) against('rare') > 0 order by id;
select id from docs where match(body) against('rare') order by id;

create table docs2 (id int primary key, body text);
insert into docs2 values (1, 'anchor rare'), (2, 'anchor common'), (3, 'anchor common');
create fulltext2 index ft2 on docs2(body) with parser gojieba;
alter table docs2 alter reindex ft2 fulltext2 force_sync;
select id from docs2 where match(body) against('anchor') = 0 order by id;
select id from docs2 where match(body) against('anchor') = 0 or id = -1 order by id;
select id from docs2 where match(body) against('rare') = 0 order by id;
select id from docs2 where match(body) against('rare') < 0.01 order by id;
select id from docs2 where match(body) against('rare') != 0 order by id;
select id from docs2 where match(body) against('zzz') = 0 order by id;
select id, match(body) against('rare') > 0 as hit from docs2 where match(body) against('rare') <= 0 order by id;
select id from docs2 where match(body) against('rare') > 0 order by id;

drop database ft_direct_score_compare;
set experimental_fulltext_index = @saved_ft;
set experimental_fulltext2_index = @saved_ft2;
