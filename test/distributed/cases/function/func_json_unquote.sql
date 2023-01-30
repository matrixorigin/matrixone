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




