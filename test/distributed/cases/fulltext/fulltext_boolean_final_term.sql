-- #29288: classic-fulltext BOOLEAN mode dropped a final one-rune term after whitespace (it starts
-- and ends on the last rune, so it never reached the flush a multi-rune final term uses). MATCH then
-- missed documents that term would return, breaking the boolean-OR superset property.
drop database if exists ft_bfinal;
create database ft_bfinal;
use ft_bfinal;

create table src (id bigint primary key, body varchar(200));
create fulltext index ft on src (body);
insert into src values (1,'香蕉'),(2,'其他'),(3,'apple pie'),(4,'a cat');

-- Baseline: the single-rune term alone.
select id from src where match(body) against('蕉' in boolean mode) order by id;

-- The bug: a leading OR term must not drop the trailing single-rune term. `nope 蕉` must be a
-- superset of `蕉`. Was empty before the fix.
select id from src where match(body) against('nope 蕉' in boolean mode) order by id;
select id from src where match(body) against('+蕉' in boolean mode) order by id;

-- A trailing single-rune latin term is also kept: `apple a` returns the `apple` doc AND the `a` doc.
select id from src where match(body) against('apple a' in boolean mode) order by id;

-- Controls: a multi-rune final term must not double-emit or change, and an unmatched OR pair stays
-- empty.
select id from src where match(body) against('apple pie' in boolean mode) order by id;
select id from src where match(body) against('nope xy' in boolean mode) order by id;

drop database ft_bfinal;
