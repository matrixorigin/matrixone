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

drop database ft_case;
