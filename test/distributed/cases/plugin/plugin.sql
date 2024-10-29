select * from plugin_exec('cat', '[1,2,3]') as f;

select * from plugin_exec('cat', '["1","2","3"]') as f;

select * from plugin_exec('cat', '["a","b","c"]') as f;

select * from plugin_exec('cat', '["a","b",null]') as f;

select * from plugin_exec('cat', '[false,true,null]') as f;

select json_extract(result, "$.id") from plugin_exec('cat', '[{"id":1},{"id":2},{"id":3}]') as f;

select * from plugin_exec('cat', cast('file:///$resources/plugin/result.json' as datalink)) as f;

select json_extract(result, "$.chunk"), json_extract(result, "$.e") from plugin_exec('cat', cast('file:///$resources/plugin/multistream.json?offset=0&size=116' as datalink) ) as f;

select json_extract(result, "$.chunk"), json_extract(result, "$.e") from plugin_exec('cat', cast('file:///$resources/plugin/multistream.json?offset=116&size=125' as datalink) ) as f;


create table t1 (chunk int, e vecf32(3));
insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e")) 
from plugin_exec('cat', cast('file:///$resources/plugin/result.json' as datalink)) as f;
select * from t1;

truncate t1;

insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e")) 
from plugin_exec('cat', cast('file:///$resources/plugin/multistream.json?offset=0&size=116' as datalink) ) as f;

insert into t1 select json_unquote(json_extract(result, "$.chunk")), json_unquote(json_extract(result, "$.e"))
from plugin_exec('cat', cast('file:///$resources/plugin/multistream.json?offset=116&size=125' as datalink) ) as f;

select * from t1;

drop table t1;
