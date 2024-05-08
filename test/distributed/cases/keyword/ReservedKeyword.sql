drop database if exists test;
create database test;

drop table if exists `_binary`;
create table `_binary` (`add` int, `all` bigint, `alter` smallint, `analyze` decimal, `and` char, `as` varchar, `asc` int, `begin` float);
show create table `_binary`;
drop table `_binary`;

drop table if exists `between`;
create table `between` (`binary` int, `both` bigint, `by` smallint, `call` decimal, `case` char, `change` varchar, `char` int, `character` float);
show create table `between`;
drop table `between`;

drop table if exists `check`;
create table `check` (`collate` int, `column` bigint, `config` decimal, `constraint` char, `convert` varchar, `create` int, `cross` float);
show create table `check`;
drop table `check`;

drop table if exists `current_date`;
create table `current_date` (`current_role` int, `current_time` bigint, `current_timestamp` smallint, `current_user` decimal, `database` char, `databases` varchar, `day_hour` int, `day_microsecond` float);
show create table `current_date`;
drop table `current_date`;

drop table if exists `day_minute`;
create table `day_minute` (`day_second` int, `declare` bigint, `default` smallint, `delayed` decimal, `delete` char, `dense_rank` varchar, `desc` int, `describe` float);
show create table `day_minute`;
drop table `day_minute`;

drop table if exists `distinct`;
create table `distinct` (`div` int, `drop` bigint, `else` smallint, `elseif` decimal, `enclosed` char, `end` varchar, `escape` int, `escaped` float);
show create table `distinct`;
drop table `distinct`;

drop table if exists `except`;
create table `except` (`exists` int, `explain` bigint, `false` smallint, `for` decimal, `force` char, `foreign` varchar, `from` int, `fulltext` float);
show create table `except`;
drop table `except`;

drop table if exists `function`;
create table `function` (`group` int, `groups` bigint, `having` smallint, `high_priority` decimal, `hour_microsecond` char, `hour_minute` varchar, `hour_second` int, `if` float);
show create table `function`;
drop table `function`;

drop table if exists `ignore`;
create table `ignore` (`ilike` int, `in` bigint, `index` smallint, `infile` decimal, `inner` char, `inout` varchar, `insert` int, `int1` float);
show create table `ignore`;
drop table `ignore`;

drop table if exists `int2`;
create table `int2` (`int3` int, `int4` bigint, `int8` smallint, `intersect` decimal, `interval` char, `into` varchar, `is` int, `iterate` float);
show create table `int2`;
drop table `int2`;

drop table if exists `join`;
create table `join` (`key` int, `kill` bigint, `leading` smallint, `leave` decimal, `left` char, `like` varchar, `limit` int, `lines` float);
show create table `join`;
drop table `join`;

drop table if exists `load`;
create table `load` (`localtime` int, `localtimestamp` bigint, `lock` smallint, `loop` decimal, `low_priority` char, `match` varchar, `maxvalue` int, `minus` float);
show create table `load`;
drop table `load`;

drop table if exists `minute_microsecond`;
create table `minute_microsecond` (`minute_second` int, `mod` bigint, `natural` smallint, `not` decimal, `null` char, `on` varchar, `optionally` int, `or` float);
show create table `minute_microsecond`;
drop table `minute_microsecond`;

drop table if exists `order`;
create table `order` (`out` int, `outer` bigint, `outfile` smallint, `over` decimal, `partition` char, `primary` varchar, `quick` int, `rank` float);
show create table `order`;
drop table `order`;

drop table if exists `recursive`;
create table `recursive` (`references` int, `regexp` bigint, `reindex` smallint, `rename` decimal, `repeat` char, `replace` varchar, `require` int, `right` float);
show create table `recursive`;
drop table `recursive`;

drop table if exists `rlike`;
create table `rlike` (`row` int, `row_number` bigint, `rows` smallint, `schema` decimal, `schemas` char, `second_microsecond` varchar, `select` int, `separator` float);
show create table `rlike`;
drop table `rlike`;

drop table if exists `set`;
create table `set` (`show` int, `sql_big_result` bigint, `sql_buffer_result` smallint, `sql_small_result` decimal, `ssl` char, `starting` varchar, `straight_join` int, `table` float);
show create table `set`;
drop table `set`;

drop table if exists `temporary`;
create table `temporary` (`terminated` int, `then` bigint, `to` smallint, `trailing` decimal, `true` char, `union` varchar, `unique` int, `until` float);
show create table `temporary`;
drop table `temporary`;

drop table if exists `update`;
create table `update` (`usage` int, `use` bigint, `using` smallint, `utc_date` decimal, `utc_time` char, `utc_timestamp` varchar, `values` int, `when` float);
show create table `update`;
drop table `update`;

drop table if exists `where`;
create table `where` (`while` int, `with` bigint, `xor` smallint, `year_month` decimal);
show create table `where`;
drop table `where`;

drop database test;