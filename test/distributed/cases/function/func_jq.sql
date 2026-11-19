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

-- issue #30924/#30929: a query may yield zero, one or multiple results.
-- Successive results are separated by a newline, and integers pass through
-- without a float64 round trip, so values beyond 2^53 stay exact.
select jq('[1,2,3,4,5]', '.[]');
select jq('[1,2,3]', '.[] | . * 10');
select jq('[1,2,3,4,5]', '.[] | select(. > 2)');
select jq('[1,2,3]', '.[] | select(. > 10)');
select jq('{"n":9007199254740993}', '.n');
select jq('{"n":-9007199254740993}', '.n');
select jq('{"n":9007199254740993}', '.n * 1000000000000');
select jq('{"n":4722366482869645213696}', '.n + 1');
select try_jq('{"n":4722366482869645213696}', '.n');
select jq('[9007199254740993,4722366482869645213696]', '.[]');

-- Number tokens that overflow float64 keep the legacy rejection for both
-- fractional and integer-only forms: jq errors with the encoding/json message
-- and try_jq yields NULL (#31060 upstream contract). In-range integers stay
-- exact through json.Number and gojq's int/*big.Int normalization.
select jq('{"n":1e309}', '.n');
select try_jq('{"n":1e309}', '.n');
select jq('{"n":-1e309}', '.n');
select try_jq('{"n":-1e309}', '.n');
select jq('{"a":{"b":[1e309,2]}}', '.[]');
select jq('{"n":10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}', '.n');
select try_jq('{"n":10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}', '.n');



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

-- issue #30924/#30929: framing and exact integers through table rows
create table jq_num(id int, data varchar(255), query varchar(255));
insert into jq_num values
(1, '{"n": 9007199254740993}', '.n'),
(2, '{"n": -9007199254740993}', '.n'),
(3, '{"n": 4722366482869645213696}', '.n'),
(4, '[4722366482869645213696, 1]', '.[]'),
(5, '[1,2,3,4,5]', '.[] | select(. > 2)'),
(6, '{"n": 1.5}', '.n');
select id, jq(data, query) from jq_num order by id;
drop table jq_num;

-- Genuine server-side prepared execution (issue #30929 acceptance): the same
-- statement is executed with repeatedly rebound string parameters for both
-- jq and try_jq. concat() marks keep the values textual so the comparison is
-- exact rather than numerically tolerant.
prepare jq_p from 'select concat(''v='', jq(?, ?)) as jq_val';
prepare try_jq_p from 'select concat(''t='', try_jq(?, ?)) as jq_val';
set @js1 = '{"n":9007199254740993}';
set @qe = '.n';
execute jq_p using @js1, @qe;
execute try_jq_p using @js1, @qe;
set @js2 = '{"n":-9007199254740993}';
execute jq_p using @js2, @qe;
execute try_jq_p using @js2, @qe;
set @js3 = '{"n":4722366482869645213696}';
execute jq_p using @js3, @qe;
execute try_jq_p using @js3, @qe;
set @jm = '{"a":1,"b":2}';
set @qq = '.a, .b';
execute jq_p using @jm, @qq;
execute try_jq_p using @jm, @qq;
set @bad = 'oops';
execute jq_p using @bad, @qe;
execute try_jq_p using @bad, @qe;
deallocate prepare jq_p;
deallocate prepare try_jq_p;



drop table if exists jq_const_order_probe;
create table jq_const_order_probe(id int);
insert into jq_const_order_probe values (1), (2);
select try_jq(null, '.') as null_result from jq_const_order_probe order by id;
select try_jq('{"foo":128}', '.foo') as const_result from jq_const_order_probe order by id;
drop table jq_const_order_probe;

-- Teardown postconditions (issue #30924 group): the case must leave no
-- catalog objects behind, so repeating the file on the same instance proves
-- the teardown instead of being masked by entry-time recovery drops.
select table_name from information_schema.tables where table_schema = database() and table_name in ('jqt', 'jq_num', 'jq_const_order_probe') order by table_name;
