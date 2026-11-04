-- #31661: FILL(LINEAR) must interpolate a single missing window between the
-- two real endpoints when the planner widens narrow integer operands of the
-- midpoint (agg + agg) / 2 with implicit casts.
drop database if exists fill_linear_int_endpoints;
create database fill_linear_int_endpoints;
use fill_linear_int_endpoints;

create table linear_series (
  series_id int not null,
  ts datetime not null,
  value int,
  primary key (series_id, ts)
);
insert into linear_series values
  (1, '2026-03-01 00:00:00', 100),
  (1, '2026-03-01 00:03:00', 130),
  (1, '2026-03-01 00:04:00', 140),
  (2, '2026-03-01 00:00:00', 200),
  (2, '2026-03-01 00:04:00', 120),
  (3, '2026-03-01 00:00:00', 0),
  (3, '2026-03-01 00:02:00', 20),
  (3, '2026-03-01 00:05:00', 50);

-- series 3 minute 1 is a single gap between 0 and 20: expect 10.
select series_id, date_format(_wstart, '%Y-%m-%d %H:%i:%s') as window_start,
       cast(max(value) as signed) as value
from linear_series
group by series_id interval(ts, 1, minute)
gapfill(partition) fill(linear)
order by series_id, _wstart;

select series_id, _wstart, max(value) as value
from linear_series
group by series_id interval(ts, 1, minute)
gapfill(partition) fill(linear)
order by series_id, _wstart;

-- Narrow and full-width integer controls, single gap and multi-gap.
create table narrow_series (g int, ts datetime, ti tinyint, si smallint, mi mediumint, bi bigint, ui int unsigned);
insert into narrow_series values
  (1, '2026-03-01 00:00:00', 0, -20, 1000, 10, 4),
  (1, '2026-03-01 00:02:00', 20, 20, 3000, 30, 8),
  (1, '2026-03-01 00:05:00', 50, 50, 6000, 60, 14);
select g, _wstart, max(ti), min(si), max(mi), max(bi), max(ui)
from narrow_series
group by g interval(ts, 1, minute)
gapfill(partition) fill(linear)
order by g, _wstart;

select g, _wstart, sum(ti) as s, max(ti) + 0 as m
from narrow_series
group by g interval(ts, 1, minute)
gapfill(partition) fill(linear)
order by g, _wstart;

drop database fill_linear_int_endpoints;
