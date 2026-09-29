-- #29296: a classic-fulltext natural-language pattern shorter than 3 runes was not lowercased before
-- the STAR prefix search, so a capitalized short pattern (Hi / HI / Ab / AB) prefix-searched the raw
-- string and missed the lowercased stored token. It must match the same rows as the lowercase form.
drop database if exists ft_case;
create database ft_case;
use ft_case;

create table c (id int primary key, body varchar(64));
create fulltext index fc on c (body);
insert into c values (1,'high'),(2,'HI'),(3,'pineapple'),(4,'apple'),(5,'Abyss');

-- Lowercase controls (already worked).
select id from c where match(body) against('hi') order by id;
select id from c where match(body) against('ab') order by id;

-- The bug: capitalized short patterns must fold case and return the same rows.
select id from c where match(body) against('Hi') order by id;
select id from c where match(body) against('HI') order by id;
select id from c where match(body) against('Ab') order by id;
select id from c where match(body) against('AB') order by id;

-- A 3-rune pattern stays an exact token (the < 3-rune prefix rule does not apply): no match.
select id from c where match(body) against('hig') order by id;
-- A >= 3-rune pattern is tokenized (already case-folded): control.
select id from c where match(body) against('HIGH') order by id;

-- #29296 regression: the short-prefix fold must MATCH SimpleTokenizer, which folds only Latin-class
-- runes (<0x7FF) and preserves wider runes verbatim (outputCJK). A blanket lowercase folded fullwidth
-- / wide capitals like Ａ and ẞ, missing the cased token the index actually stored.
create table u (id int primary key, body varchar(64));
create fulltext index fu on u (body);
insert into u values (1,'Ａ'),(2,'ａ'),(3,'ẞ'),(4,'other');
-- Wide capitals are preserved, so they hit their own cased rows (1 and 3), not the folded ones.
select id from u where match(body) against('Ａ') order by id;
select id from u where match(body) against('ẞ') order by id;

-- #29296 regression: json_value stores values verbatim (no case folding), so its short query prefix
-- must preserve case. A blanket lowercase turned 'Hi' into 'hi' and matched the wrong stored value.
create table j (id int primary key, body json);
create fulltext index fj on j (body) with parser json_value;
insert into j values (1,'{"w":"Hi"}'),(2,'{"w":"hi"}'),(3,'{"w":"other"}');
select id from j where match(body) against('Hi') order by id;
select id from j where match(body) against('hi') order by id;

drop database ft_case;
