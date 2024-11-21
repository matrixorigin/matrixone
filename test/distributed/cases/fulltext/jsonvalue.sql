-- TODO: run all tests with both experimental_fulltext_index = 0 and 1
-- TODO: GENERATE the test case to cover all combinations of types (varchar, char and text)
set experimental_fulltext_index=1;

-- bytejson parser
create table src (id bigint primary key, json1 json, json2 json);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthdayhappybirthdayhappybirthday", "f":"winterautumnsummerspring"}'), 
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"redbluebrownyelloworange"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json_value;

select * from src where match(json1) against('redbluebrownyelloworange' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json_value;
select * from src where match(json1, json2) against('+redredredredredredredredredredrerr +winterautumnsummerspring' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in boolean mode);

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 text, json2 varchar);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthday.happy-birthday_happybirthday", "f":"winterautumnsummerspring"}'), 
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"redbluebrownyelloworange"}', '{"d":"兒童中文"}');

create fulltext index ftidx on src (json1) with parser json_value;

select * from src where match(json1) against('redredredredredredredredredredrerr' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json_value;
select * from src where match(json1, json2) against('+red +winter' in boolean mode);

select * from src where match(json1, json2) against('happybirthday.happy-birthday_happybirthday' in boolean mode);

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 json, json2 json, FULLTEXT(json1) with parser json_value);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthday.happy-birthday_happybirthday", "f":"winterautumnsummerspring"}'), 
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"redbluebrownyelloworange"}', '{"d":"兒童中文"}');

select * from src where match(json1) against('happybirthday.happy-birthday_happybirthday' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json_value;
select * from src where match(json1, json2) against('+happybirthday.happy-birthday_happybirthday +winterautumnsummerspring' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in boolean mode);

update src set json1='{"c":"update json"}' where id=0;

drop table src;

-- bytejson parser
create table src (id bigint primary key, json1 text, json2 varchar, fulltext(json1) with parser json_value);
insert into src values  (0, '{"a":1, "b":"redredredredredredredredredredrerr"}', '{"d": "happybirthday.happy-birthday_happybirthday", "f":"winterautumnsummerspring"}'), 
(1, '{"a":2, "b":"中文學習教材"}', '["apple", "orange", "banana", "指引"]'),
(2, '{"a":3, "b":"red..blue--brown@@yellow::orange"}', '{"d":"兒童中文"}');

select * from src where match(json1) against('red..blue--brown@@yellow::orange' in boolean mode);

select * from src where match(json1) against('中文學習教材' in boolean mode);

create fulltext index ftidx2 on src (json1, json2) with parser json_value;
select * from src where match(json1, json2) against('+red..blue--brown@@yellow::orange +兒童中文' in boolean mode);

select * from src where match(json1, json2) against('中文學習教材' in boolean mode);

update src set json1='{"c":"update_json"}' where id=0;

select * from src where match(json1, json2) against('"update_json"' in boolean mode);

drop table src;
