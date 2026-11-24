-- Parallel REPLACE must release captured old rows with the worker's pool.
-- The composite key and column types reflect the incident's VALUES statement.
drop database if exists replace_parallel_capture_33124;
create database replace_parallel_capture_33124;
use replace_parallel_capture_33124;

create table telemetry (
    device varchar(64),
    seq bigint unsigned,
    observed_at datetime(6),
    state varchar(32),
    source varchar(32),
    reading1 double, reading2 double, reading3 double, reading4 double,
    reading5 double, reading6 double, reading7 double, reading8 double,
    quality varchar(32),
    attributes json,
    primary key (device, seq)
);
insert into telemetry values
    ('a', 1, '2025-01-01 00:00:00.000001', 'idle', 'test', 1, 2, 3, 4, 5, 6, 7, 8, 'old', '{"v":1}'),
    ('b', 2, '2025-01-01 00:00:00.000002', 'idle', 'test', 1, 2, 3, 4, 5, 6, 7, 8, 'old', '{"v":1}');

set @saved_parallel_capture_hints = @@session.optimizer_hints;
set @saved_parallel_capture_dop = @@session.max_dop;
set session optimizer_hints = 'execType=2';
set session max_dop = 2;

replace into telemetry
    (device, seq, observed_at, state, source,
     reading1, reading2, reading3, reading4, reading5, reading6, reading7, reading8,
     quality, attributes)
values
    ('b', 2, '2025-01-01 00:00:00.000003', 'active', 'test', 8, 7, 6, 5, 4, 3, 2, 1, 'new', '{"v":2}');
select row_count();
select * from telemetry order by device, seq;

set session optimizer_hints = @saved_parallel_capture_hints;
set session max_dop = @saved_parallel_capture_dop;
select @@session.optimizer_hints = @saved_parallel_capture_hints,
       @@session.max_dop = @saved_parallel_capture_dop;
drop database replace_parallel_capture_33124;
