-- @suite
-- @case

drop database if exists group_concat_temporal_28657;
create database group_concat_temporal_28657;
use group_concat_temporal_28657;

set @group_concat_saved_time_zone = @@time_zone;
set time_zone = '+00:00';
create table temporal_values (
    id int primary key,
    dt datetime(6),
    ts timestamp(6),
    tm time(6)
);
insert into temporal_values values
    (1, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456', '-12:34:56.123456'),
    (2, '2024-01-02 03:04:06.654321', '2024-01-02 03:04:06.654321', '12:34:56.654321');

select group_concat(dt order by id separator '|') from temporal_values;
select group_concat(tm order by id separator '|') from temporal_values;
select group_concat(ts order by id separator '|') from temporal_values;

prepare group_concat_timestamp_stmt from
    'select group_concat(ts order by id separator \'|\') from temporal_values';
set time_zone = '+08:00';
execute group_concat_timestamp_stmt;
select group_concat(dt order by id separator '|') from temporal_values;
select group_concat(tm order by id separator '|') from temporal_values;
deallocate prepare group_concat_timestamp_stmt;

set time_zone = @group_concat_saved_time_zone;
drop database group_concat_temporal_28657;
