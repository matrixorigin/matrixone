create stage llmstage URL='file:///$resources/plugin/';

-- error
select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '[1,2,3]') as f;
select * from moplugin_table(cast('stage://llmstage/cat.wasm' as json), 'cat', null, '[1,2,3]') as f;
select * from moplugin_table('stage://llmstage/cat.wasm"') as f;
select * from moplugin_table('unknown', 'cat', null, '[1,2,3]') as f;
select * from moplugin_table('stage://llmstage/cat.wasm', '1') as f;
select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '[1a,2,3]') as f;
select * from moplugin_table('stage://llmstage/cat.wasm', 'cat',
        cast('["header", "footer"]' as JSON), '1,2,3') as f;

-- start test
-- index of multistream.json (offset, size) = [(0, 155), (155, 164)]

select * from moplugin_table(null, 'cat', null, null) as f;
select * from moplugin_table('stage://llmstage/cat.wasm', null, null, null) as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', 
	'{"header":"[", "footer":"]"}', '1,2,3') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', 
	cast('{"header":"[", "footer":"]"}' as JSON), '1,2,3') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '[1,2,3]') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '["1","2","3"]') as f;

select * from moplugin_table(cast('stage://llmstage/cat.wasm' as datalink), 'cat', null, '["a","b","c"]') as f;

select * from moplugin_table(cast('stage://llmstage/cat.wasm' as datalink), 'cat', null, '["a","b",null]') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '[false,true,null]') as f;

select json_extract(result, "$.id") from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, '[{"id":1},{"id":2},{"id":3}]') as f;

select * from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, cast('file:///$resources/plugin/result.json' as datalink)) as f;

select json_extract(result, "$.chunk"), json_extract(result, "$.e") from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, 
	cast('file:///$resources/plugin/multistream.json?offset=0&size=155' as datalink) ) as f;

select json_extract(result, "$.chunk"), json_extract(result, "$.e") from moplugin_table('stage://llmstage/cat.wasm', 'cat', null,
	cast('file:///$resources/plugin/multistream.json?offset=155&size=164' as datalink) ) as f;


create table t1 (chunk int, e vecf32(3));
insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e")) 
from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, cast('file:///$resources/plugin/result.json' as datalink)) as f;
select * from t1;

truncate t1;

insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e")) 
from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, 
	cast('file:///$resources/plugin/multistream.json?offset=0&size=155' as datalink) ) as f;

select * from t1;

insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e"))
from moplugin_table('stage://llmstage/cat.wasm', 'cat', null, cast('file:///$resources/plugin/multistream.json?offset=155&size=164' as datalink) ) as f;

select * from t1;

drop table t1;


create table src (pkey int primary key, dlink datalink);

create table embed (pkey int, chunk int, e vecf32(3), t varchar);

insert into src values 
(0, 'stage://llmstage/multistream.json?offset=0&size=155'),
(1, 'stage://llmstage/multistream.json?offset=155&size=164');

insert into embed select src.pkey, json_unquote(json_extract(f.result, "$.chunk")), 
json_unquote(json_extract(f.result, "$.e")), json_unquote(json_extract(f.result, "$.t"))
from src CROSS APPLY moplugin_table('stage://llmstage/cat.wasm', 'cat', null, src.dlink) as f;

select * from embed;

drop stage llmstage;
drop table src;
drop table embed;
