select json_unquote('"a\\n"');
select json_unquote('"a\n"');
select json_unquote('"a\t"');
select json_unquote('"a\\u0000"');
select json_unquote('"a\u0000"');
select json_unquote('"aaxa"');
select json_unquote('"a\\xa"');
select json_unquote('"a\\u0000a"');
select json_unquote('{"a":"b"}');
select json_unquote('[1,2,3,null,true,false,"a",1.2,{"a":"1","b":2}]');
select json_unquote('1');
select json_unquote('1.2');
select json_unquote('null');
select json_unquote('true');
select json_unquote('false');
select json_unquote('plain text');
select json_unquote('1e2');
select json_unquote('"leading');
select json_unquote('trailing"');
select hex(json_unquote(' "framed" '));
select json_unquote('"\\u554a\\u554a\\u5361\\u5361"');
select json_unquote('"\\u4f60\\u597d\\uff0c\\u006d\\u006f"');
select json_unquote('"\\u4f60\\u597d\\uff0cmo"');
select json_unquote('"\\u4f60\\u597d\\ufc"');
select json_unquote(json_extract('{"a":"b"}', '$.a'));
select json_unquote(json_extract('{"a":1}', '$.a'));
select json_unquote(null);

create table t1 (a varchar);
insert into t1 values  ('"a\\u0000"'), ('"a\u0000"'), ('"aaxa"'),  ('"a\\u0000a"'), ('{"a":"b"}'), ('[1,2,3,null,true,false,"a",1.2,{"a":"1","b":2}]'), ('1'), ('1.2'), ('null'), ('true'), ('false'), ('"\\u554a\\u554a\\u5361\\u5361"'), ('"\\u4f60\\u597d\\uff0c\\u006d\\u006f"'), ('"\\u4f60\\u597d\\uff0cmo"'), ('{"a":"b"}'), ('{"a":1}'),(null);
select json_unquote(a) from t1;
create table t2 (a json);
insert into t2 select a from t1;
select json_unquote(a) from t2;





-- typed scalars (DATE/TIME/DATETIME/BLOB) via json_array
select json_unquote(json_extract(json_array(cast('2021-02-01' as date)), '$[0]'));
select json_unquote(json_extract(json_array(cast('11:11:11' as time)), '$[0]'));
select json_unquote(json_extract(json_array(cast('2021-02-01 11:11:11' as datetime)), '$[0]'));
drop table t2;
drop table t1;

create table json_unquote_text_types (c char(8), v varchar(32), t text, mt mediumtext, lt longtext, b binary(8), vb varbinary(8), bl blob);
insert into json_unquote_text_types values ('plain', 'plain', 'plain', repeat('a', 70000), repeat('b', 70000), 'plain', 'plain', 'plain');
select json_unquote(c), json_unquote(v), json_unquote(t) from json_unquote_text_types;
select json_unquote(mt), json_unquote(lt) from json_unquote_text_types where false;
select length(json_unquote(mt)), length(json_unquote(lt)) from json_unquote_text_types;
create table json_unquote_text_ctas as select json_unquote(mt) as mt, json_unquote(lt) as lt from json_unquote_text_types;
select column_name, data_type, character_maximum_length from information_schema.columns where table_schema = database() and table_name = 'json_unquote_text_ctas' order by ordinal_position;
select length(mt), length(lt) from json_unquote_text_ctas;
select json_unquote(b) from json_unquote_text_types;
select json_unquote(vb) from json_unquote_text_types;
select json_unquote(bl) from json_unquote_text_types;
drop table json_unquote_text_ctas;
drop table json_unquote_text_types;
