-- @suite
-- @case

drop database if exists group_concat_large_field_28653;
create database group_concat_large_field_28653;
use group_concat_large_field_28653;

set @group_concat_saved_max_len = @@group_concat_max_len;
set session group_concat_max_len = 100000;
create table large_values (
    id int primary key,
    t longtext,
    b longblob,
    j json
);
insert into large_values values
    (1, repeat('x', 65535), repeat('a', 65535), json_object('v', repeat('j', 65535))),
    (2, repeat('y', 65536), repeat('b', 65536), json_object('v', repeat('k', 65536))),
    (3, repeat('z', 70000), repeat('c', 70000), json_object('v', repeat('m', 70000)));

select id, length(group_concat(t)), length(group_concat(b)), length(group_concat(j))
from large_values group by id order by id;

set session group_concat_max_len = 10;
select length(group_concat(t order by id separator '')) from large_values;
show warnings;

set session group_concat_max_len = @group_concat_saved_max_len;
drop database group_concat_large_field_28653;
