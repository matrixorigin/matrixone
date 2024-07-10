--
-- jq test
--

select jq('{"foo": 128}', '.foo');
select try_jq('{"foo": 128}', '.foo');
select jq('{"a": {"b": 42}}', '.a.b');
select try_jq('{"a": {"b": 42}}', '.a.b');

select jq(null, '.foo');
select try_jq(null, '.foo');
select jq('{"a": {"b": 42}}', null);
select try_jq('{"a": {"b": 42}}', null);

select jq('{"id": "sample", "10": {"b": 42}}', '{(.id): .["10"].b}');
select jq('[{"id":1},{"id":2},{"id":3}]', '.[] | .id');
select jq('{"a":1, "b":2}', '.a += 1 | .b *= 2');
select jq('{"a":1} [2] 3', '. as {$a} ?// [$a] ?// $a | $a');
select jq('{"foo": 4722366482869645213696}', '.foo');
select jq('1', 'def fact($n): if $n < 1 then 1 else $n * fact($n - 1) end; fact(50)');

select jq('[1, 2, 3]', '.foo & .bar');
select try_jq('[1, 2, 3]', '.foo & .bar');

select jq('{"foo": {bar: []} }', '.');
select try_jq('{"foo": {bar: []} }', '.');

select jq($$
    {
        "a": 2
    }$$, '.a');

select jq('', '.');
select try_jq('', '.');
select jq('1', '');
select try_jq('1', '');

select jq('{"foo::bar": "zoo"}', '.["foo::bar"]');
select jq('{"foo::bar": "zoo"}', '.foo::bar');
select try_jq('{"foo::bar": "zoo"}', '.foo::bar');

select jq('["a", "b", "c", "d", "e"]', '.[2:4]');
select jq('["a", "b", "c", "d", "e"]', '.[:3]');
select jq('["a", "b", "c", "d", "e"]', '.[-2:]');

select jq('["a", "b", "c", "d", "e"]', '.[]');
select jq('[]', '.[]');
select jq('{"foo": ["a", "b", "c", "d", "e"]}', '.foo[]');
select jq('{"a":1, "b":2}', '.[]'); 

select jq('{"a":1, "b":2}', '.a, .b'); 
select jq('["a", "b", "c", "d", "e"]', '.[4,2]');
select jq('{"a": 1, "b": [2, 3]}', '[.a, .b[]]');
select jq('[1, 2, 3]', '[ .[] | . * 2]');

select jq('{"a":1, "b":2}', '{aa: .a, bb: .b}');
select jq('{"user":"stedolan","titles":["JQ Primer", "More JQ"]}', '{user, title: .titles[]}');
select jq('[[{"a":1}]]', '.. | .a');

-- expressions
select jq('{"a":1, "b":2}', '.a + .b'); 
select jq('{"a":1, "b":2}', '.a + null'); 
select jq('{"a":1, "b":2}', '. + {c: 3}'); 
select jq('{"a":1, "b":2}', '. + {a: 3, c: 3}'); 
select jq('0', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end');
select jq('1', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end');
select jq('2', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end');
select jq('[{}, true, {"a":1}]', '[.[]|try .a]');
select jq('[{}, true, {"a":1}]', '[.[]|.a?]');
select jq('[{}, true, {"a":1}]', '[.[]|try .a catch ". is not an object"]');

-- advanced 
select jq('[1, 2, 3]', 'reduce .[] as $item (0; + $item)'); 
select jq('[1, 2, 3]', 'foreach .[] as $item(0; . + $item; [$item, . * 2])');   

-- enough, move on to tables.
create table jqt(id int, data varchar(255), jq varchar(255));
insert into jqt values 
(1, '{"foo": 128}', '.foo'),
(2, '{"foo": 128}', '.foo'),
(3, '{"a": {"b": 42}}', '.a.b'),
(4, '{"a": {"b": 42}}', '.a.b'),
(5, null, '.foo'),
(6, '{"a": {"b": 42}}', null),
(7, '{"id": "sample", "10": {"b": 42}}', '{(.id): .["10"].b}'),
(8, '[{"id":1},{"id":2},{"id":3}]', '.[] | .id'),
(9, '{"a":1, "b":2}', '.a += 1 | .b *= 2'),
(10, '{"a":1} [2] 3', '. as {$a} ?// [$a] ?// $a | $a'),
(11, '{"foo": 4722366482869645213696}', '.foo'),
(12, '1', 'def fact($n): if $n < 1 then 1 else $n * fact($n - 1) end; fact(50)')
;


insert into jqt values
(100, '[1, 2, 3]', '.foo & .bar'),
(101, '[1, 2, 3]', '.foo & .bar'),
(102, '{"foo": {bar: []} }', '.'),
(103, '{"foo": {bar: []} }', '.');

insert into jqt values
(200, '{"a":1, "b":2}', '.a + .b'), 
(201, '{"a":1, "b":2}', '.a + null'), 
(202, '{"a":1, "b":2}', '. + {c: 3}'), 
(203, '{"a":1, "b":2}', '. + {a: 3, c: 3}'), 
(204, '0', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end'),
(205, '1', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end'),
(206, '2', 'if . == 0 then "zero" elif . == 1 then "one" else "many" end'),
(207, '[{}, true, {"a":1}]', '[.[]|try .a]'),
(208, '[{}, true, {"a":1}]', '[.[]|.a?]'),
(209, '[{}, true, {"a":1}]', '[.[]|try .a catch ". is not an object"]')
;

select count(*) from jqt;
select id, jq(data, '.') from jqt;
select id, jq(data, '.') from jqt where id < 100;
select id, try_jq(data, '.') from jqt;

select id, jq(null, jq) from jqt;
select id, jq(data, null) from jqt;

select id, jq(data, jq) from jqt;
select id, try_jq(data, jq) from jqt;

drop table jqt;

