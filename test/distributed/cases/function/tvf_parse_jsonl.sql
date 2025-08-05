select * from parse_jsonl_data($$[1, 2, 3]
["foo", "bar", "zoo"]
{"foo": 1, "bar": "zoo"}
$$) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2,3.3]
$$, 'iii'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2,3.3]
$$, 'fIF'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, 'sss'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, '{"format":"array", "cols":[{"name":"x", "type":"int32"},{"name":"y","type":"float64"},{"name":"z","type":"string"}]}'
) t;

select * from parse_jsonl_data($${"x":1, "z":"zzz", "y":3.14}
{"x":2, "z":"zzz"}
{"x":3, "zzz":"z"}
{"y":2.7183, "zzz":666}
$$, '{"format":"object", "cols":[{"name":"x", "type":"int32"},{"name":"y","type":"float64"},{"name":"z","type":"string"}]}'
) t;

select * from parse_jsonl_data($$[true, 1, "2020-12-30 11:22:33"]
[false, 2, "2020-12-30 11:22:33"]
[null, 3, "2020-12-30 11:22:33"]
$$, 'bit'
) t;

select * from parse_jsonl_data($$[true, 1, "2020-12-30 11:22:33"]
[false, 2, "2020-12-30 11:22:33"]
[null, 3, "2020-12-30 11:22:33"]
$$, 'bIt'
) t;

-- error, invalid short format
select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, 'six'
) t;

-- error, invalid short format 
select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, '{"six'
) t;

-- error, invalid json
select * from parse_jsonl_data($$[true, 1, "2020-12-30 11:22:33"]
false, 2, "2020-12-30 11:22:33"
[null, 3, "2020-12-30 11:22:33"]
$$, 'bIt'
) t;

-- error, invalid timestamp
select * from parse_jsonl_data($$[1, 2, 3]
[true, 1, "foobar"]
$$, 'bIt'
) t;

-- error, too many columns
select * from parse_jsonl_data($$[true, 1, "2020-12-30 11:22:33", 42]
[false, 2, "2020-12-30 11:22:33"]
[null, 3, "2020-12-30 11:22:33"]
$$, 'bIt'
) t;

-- error, too few columns
select * from parse_jsonl_data($$[true, 1, "2020-12-30 11:22:33"]
[false, "2020-12-30 11:22:33"]
[null, 3, "2020-12-30 11:22:33"]
$$, 'bIt'
) t;

select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl') t;
select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl.gz') t;
select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl.bz2') t;
