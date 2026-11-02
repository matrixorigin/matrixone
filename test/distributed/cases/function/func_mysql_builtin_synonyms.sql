-- MySQL 8 builtin synonyms: LCASE/UCASE, CHARACTER_LENGTH, SCHEMA, SESSION_USER/SYSTEM_USER,
-- LOCALTIME/LOCALTIMESTAMP, UUID_SHORT, CONNECTION_ID and the logical XOR operator.
-- Each synonym is checked against the function it aliases (MySQL 8.0 reference manual semantics).
drop database if exists func_synonyms;
create database func_synonyms;
use func_synonyms;
create table s1 (id int primary key, v varchar(20), c char(8), t text);
insert into s1 values (1, 'Hello World', 'AbC', 'MiXeD Text'), (2, 'ÀÉÎ ñ', 'Ñandú', 'Ünïcödé'), (3, '', '', ''), (4, null, null, null), (5, 'trail  ', 'pad', '  lead');

-- LCASE()/UCASE() are synonyms for LOWER()/UPPER()
select lcase('AbC'), ucase('aBc'), lcase(null), ucase(null), lcase(''), ucase('');
select lcase('ÀÉÎ ñ'), ucase('àéî ñ'), lcase(123), ucase(12.5);
select id, lcase(v), ucase(v), lcase(c), ucase(c), lcase(t), ucase(t) from s1 order by id;
select count(*) from s1 where lcase(v) = lower(v) and ucase(v) = upper(v) and lcase(t) = lower(t) and ucase(c) = upper(c);
select id from s1 where ucase(v) = 'HELLO WORLD';
select lcase(ucase(v)) = lower(v) from s1 order by id;
select lcase(c) as k, count(*) from s1 group by lcase(c) order by k;

-- CHARACTER_LENGTH() is a synonym for CHAR_LENGTH(): characters, not bytes
select character_length('héllo'), char_length('héllo'), length('héllo');
select character_length(null), character_length(''), character_length('中文字符'), character_length(12345);
select id, character_length(v), char_length(v), length(v), character_length(c), character_length(t) from s1 order by id;
select count(*) from s1 where character_length(v) = char_length(v);

-- SCHEMA() is a synonym for DATABASE()
select schema(), database(), schema() = database();
select count(*) from information_schema.tables where table_schema = schema() and table_name = 's1';

-- SESSION_USER()/SYSTEM_USER() are synonyms for USER(); CURRENT_USER may be written without parentheses
select session_user() = user(), system_user() = user(), session_user() = system_user();
select current_user = current_user(), current_user() is not null;

-- LOCALTIME/LOCALTIMESTAMP (with or without parentheses) are synonyms for NOW()
select localtime = now(), localtime() = now(), localtimestamp = now(), localtimestamp() = now();
select length(localtime()), length(localtime(3)), length(localtimestamp(6)), length(localtimestamp(0));
select localtime(6) = now(6), localtimestamp(3) = now(3);

-- UUID_SHORT(): non-NULL, distinct per call/row
select uuid_short() is not null, uuid_short() > 0, uuid_short() <> uuid_short();
select count(distinct uuid_short()) from s1;

-- CONNECTION_ID(): stable within a session
select connection_id() > 0, connection_id() = connection_id();

-- XOR: NULL if either operand is NULL; otherwise true iff exactly one operand is true (non-zero)
select 1 xor 1, 1 xor 0, 0 xor 1, 0 xor 0;
select null xor 1, 1 xor null, null xor 0, null xor null;
select 1 xor 1 xor 1, 1 xor 1 xor 0, 2 xor 3, -1 xor 0, 0.5e0 xor 0, '1' xor 0;
select (1 xor 0) = ((1 and not 0) or (not 1 and 0));
create table x1 (a int, b int);
insert into x1 values (0,0),(0,1),(1,0),(1,1),(null,0),(0,null),(null,null),(5,0),(5,7);
select a, b, a xor b, not (a xor b) from x1 order by a, b;
select count(*) from x1 where a xor b;
select count(*) from x1 where not (a xor b);

drop table x1;
drop table s1;
drop database func_synonyms;
