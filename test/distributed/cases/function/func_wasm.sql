--
-- moplugin test
--

create stage mystage URL='file:///$resources/plugin/';

select moplugin('stage://mystage/hello.wasm', 'mowasm_hello', 'world');

select moplugin('stage://mystage/hello.wasm', 'mowasm_hello', 'world');
select moplugin('stage://mystage/hello.wasm', 'mowasm_add', '[3, 5]');

-- select moplugin('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/notexist.wasm', 'mowasm_add', '[3, 5]');
-- select try_moplugin('https://github.com/matrixorigin/mojo/raw/main/plugin/hello/notexist.wasm', 'mowasm_add', '[3, 5]');

select moplugin('stage://mystage/notexist.wasm', 'mowasm_add', '[3, 5]');
select try_moplugin('stage://mystage/notexist.wasm', 'mowasm_add', '[3, 5]');

select moplugin('stage://mystage/hello.wasm', 'mowasm_add2', '[3, 5]');
select try_moplugin('stage://mystage/hello.wasm', 'mowasm_add2', '[3, 5]');

select moplugin('stage://mystage/hello.wasm', 'mowasm_add', '[1, 3, 5]');
select try_moplugin('stage://mystage/hello.wasm', 'mowasm_add', '[1, 3, 5]');

create table moplugint(id int, fn varchar(255), arg varchar(255));
insert into moplugint values
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

select count(*) from moplugint;
select id, moplugin('stage://mystage/hello.wasm', fn, arg)
from moplugint;
select id, try_moplugin('stage://mystage/hello.wasm', fn, arg)
from moplugint;

drop table moplugint;

drop stage mystage;

