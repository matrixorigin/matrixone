--
-- wasm test
--

select wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_hello', 'world');
select wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_add', '[3, 5]');

select wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/notexist.wasm', 'mowasm_add', '[3, 5]');
select try_wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/notexist.wasm', 'mowasm_add', '[3, 5]');

select wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_add2', '[3, 5]');
select try_wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_add2', '[3, 5]');

select wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_add', '[1, 3, 5]');
select try_wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', 'mowasm_add', '[1, 3, 5]');

create table wasmt(id int, fn varchar(255), arg varchar(255));
insert into wasmt values
(1, 'mowasm_hello', '[1, 2]'),
(2, 'mowasm_add', '[1, 2]'),
(3, 'mowasm_hello', '[1, 2]'),
(4, 'mowasm_add', '[1, 2]'),
(5, 'mowasm_hello', '[1, 2]'),
(6, 'mowasm_add', '[1, 2]'),
(7, 'mowasm_hello', '[1, 2]'),
(8, 'mowasm_add', '[1, 2]'),
(9, 'mowasm_hello', '[1, 2]'),
(10, 'mowasm_add', '[1, 2]')
;

select count(*) from wasmt;
select id, wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', fn, arg)
from wasmt;
select id, try_wasm('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/hello.wasm', fn, arg)
from wasmt;

drop table wasmt;








