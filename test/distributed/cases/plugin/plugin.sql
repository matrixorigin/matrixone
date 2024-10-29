select * from plugin_exec('cat', '[1,2,3]') as f;

select * from plugin_exec('cat', '["1","2","3"]') as f;

select * from plugin_exec('cat', '["a","b","c"]') as f;

select * from plugin_exec('cat', '["a","b",null]') as f;

select * from plugin_exec('cat', '[false,true,null]') as f;

select json_extract(result, "$.id") from plugin_exec('cat', '[{"id":1},{"id":2},{"id":3}]') as f;
