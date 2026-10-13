drop database if exists window_char_pad_space_28023;
create database window_char_pad_space_28023;
use window_char_pad_space_28023;

create table window_char_pad_space (id int primary key, ch char(8), v int);
insert into window_char_pad_space values (1, 'a', 10), (2, 'a  ', 20), (3, 'b', 30);

select count(*) as group_count from (select ch from window_char_pad_space group by ch) g;

select id,
       count(*) over (partition by ch) as partition_count,
       rank() over (order by ch) as rank_value,
       sum(v) over (order by ch range between unbounded preceding and current row) as range_sum
from window_char_pad_space order by id;

select id,
       count(*) over (partition by cast(ch as varchar(8))) as partition_count,
       rank() over (order by cast(ch as varchar(8))) as rank_value,
       sum(v) over (order by cast(ch as varchar(8)) range between unbounded preceding and current row) as range_sum
from window_char_pad_space order by id;

drop database if exists window_char_pad_space_28023;
