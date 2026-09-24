-- #29273: a BOOLEAN-mode trailing `*` on a multi-trigram CJK stem must expand to a
-- positional phrase whose last token is a prefix_eq, instead of prefix_eq'ing the whole
-- (never-stored) stem and returning nothing. A Latin stem longer than the 23-byte stored
-- token cap must prefix_eq the truncated token. gojieba stars only the last segment.

set experimental_fulltext_index = 1;

drop database if exists ft_star_phrase;
create database ft_star_phrase;
use ft_star_phrase;

-- ngram CJK: 4-rune stem `苹果香蕉` stored as trigrams 苹果香/果香蕉/香蕉/蕉.
create table cjk(id int primary key, body varchar(200));
insert into cjk values (1,'苹果香蕉'),(2,'苹果香蕉西瓜'),(3,'苹果香瓜');
create fulltext index fi on cjk(body) with parser ngram;

-- trailing `*` on a 4-rune stem: matches the stem and any extension, not the sibling 苹果香瓜.
select id from cjk where match(body) against('苹果香蕉*' in boolean mode) order by id;
-- `+` form is identical.
select id from cjk where match(body) against('+苹果香蕉*' in boolean mode) order by id;
-- plain (no star) 4-rune control returns the same set.
select id from cjk where match(body) against('+苹果香蕉' in boolean mode) order by id;
-- 2-rune stem stays a plain prefix: 苹果* matches all three.
select id from cjk where match(body) against('苹果*' in boolean mode) order by id;
-- 6-rune stem: 苹果香蕉西瓜* matches only row 2.
select id from cjk where match(body) against('苹果香蕉西瓜*' in boolean mode) order by id;

-- Latin: stored token is capped at 23 bytes, so a longer stem must star the truncated token.
create table lat(id int primary key, body varchar(200));
insert into lat values
 (1,'abcdefghijklmnopqrstuvwxyz'),
 (2,'abcdefghijklmnopqrstuvwabc'),
 (3,'zzzabcdefghijklmnopqrstuvw');
create fulltext index fi on lat(body) with parser ngram;

-- 26-char stem shares the 23-byte prefix with rows 1 and 2; row 3 does not start with it.
select id from lat where match(body) against('abcdefghijklmnopqrstuvwxyz*' in boolean mode) order by id;
-- exact-cap 23-char stem behaves the same.
select id from lat where match(body) against('abcdefghijklmnopqrstuvw*' in boolean mode) order by id;

-- gojieba: last dictionary word becomes the prefix; 苹果香蕉* -> 苹果 + 香蕉*.
create table jb(id int primary key, body varchar(200));
insert into jb values (1,'苹果香蕉'),(2,'苹果香蕉西瓜'),(3,'苹果甜瓜');
create fulltext index fi on jb(body) with parser gojieba;
select id from jb where match(body) against('苹果香蕉*' in boolean mode) order by id;

drop database ft_star_phrase;
