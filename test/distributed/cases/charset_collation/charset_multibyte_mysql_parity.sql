-- Multibyte (CJK, accented, 4-byte emoji) string behavior under utf8mb4.
-- Every result here was checked against MySQL 8.4.9.
drop database if exists charset_multibyte_parity;
create database charset_multibyte_parity;
use charset_multibyte_parity;

select ord('中'), hex('中'), unhex('E4B8AD') = '中', length('中'), char_length('中'), bit_length('中'), octet_length('中');
select substr('中文字', 2, 1), left('中文字', 2), right('中文字', 1), reverse('中文'), lpad('中', 3, '文'), rpad('中', 3, 'é'), insert('中文字', 2, 1, 'X'), mid('中文字',-1,1);
select upper('中é'), lower('ÉÀ'), locate('文','中文'), instr('中文字','字'), position('字' in '中文字'), substring_index('中,文,字', ',', -2);
select hex(convert('中' using utf8mb4)), charset(convert('中' using utf8mb4)), charset(convert('中' using binary)), hex(convert('中' using binary));
select hex(cast('中' as binary)), charset(cast('a' as char));
select _utf8mb4'中' = '中', charset(_utf8mb4'a'), charset(_binary'a');
select to_base64('中'), from_base64(to_base64('中')) = '中';
select length(_utf8mb4 x'E4B8AD'), char_length(convert(x'E4B8AD' using utf8mb4));
select '😀' = '😀', char_length('😀'), length('😀'), hex('😀'), upper('😀'), substr('a😀b',2,1), reverse('a😀b');
select 'a' < 'B' collate utf8mb4_bin, 'a' = 'A' collate utf8mb4_bin, strcmp('a' collate utf8mb4_bin, 'A');
select 'abc' like 'ABC' collate utf8mb4_bin, 'a_c' like 'a\_c', 'ac' like 'a%c%', '中文' like '中_', '中文' like '_文%';
select replace('中文中', '中', 'x'), concat('中', 1, 'é'), concat_ws('、', '中', '文'), repeat('中', 3), space(2) = '  ';
select char_length(trim('  中  ')), ltrim('  中'), rtrim('中  '), trim(both '中' from '中文中'), trim(leading 'x' from 'xx中');
select field('文', '中', '文'), find_in_set('文', '中,文,字'), elt(2, '中', '文'), make_set(3, '中', '文', '字'), export_set(5, '是', '否', ',', 4);
select hex(char(228,184,173 using utf8mb4)), char(65,66 using utf8mb4), charset(char(65 using utf8mb4));

-- column lengths count characters, not bytes
create table c(a varchar(3), b char(3), c text);
insert into c values ('中文字','中文字','中文字'), ('😀','😀😀','😀');
select a, b, c, length(a), char_length(b), length(c) from c order by a collate utf8mb4_bin;
insert into c values ('中文字x','a','a');
select a from c where a like '中%';
select count(*) from c;

-- utf8mb4_bin columns are case sensitive in ORDER BY, DISTINCT and UNIQUE
create table d(a varchar(10) character set utf8mb4 collate utf8mb4_bin, b varchar(10) character set utf8mb4);
insert into d values ('x','x'),('X','X');
select a from d order by a;
select count(distinct a) from d;
create table u(a varchar(5) collate utf8mb4_bin, unique key(a));
insert into u values ('a'),('A');
insert into u values ('a');
select a from u order by a;

-- CHARACTER SET clauses in DDL and CAST/CONVERT
create table d2(a varchar(5)) character set utf8mb4;
alter table d2 convert to character set utf8mb4;
alter table d2 modify a varchar(5) character set utf8mb4;
select cast('a' as char(5) character set utf8mb4), convert('a', char character set utf8mb4);
select 'a' collate nonexist;

drop database charset_multibyte_parity;
