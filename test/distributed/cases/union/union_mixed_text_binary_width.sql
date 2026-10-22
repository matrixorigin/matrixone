-- @suite
-- @case
-- @desc: Mixed text and binary set/VALUES columns retain the widest byte value (#30718)
-- @label:bvt

drop database if exists issue_30718;
create database issue_30718;
use issue_30718;

select ord, hex(d) as bytes, length(d) as width
from (select 1 as ord, cast('' as char(1)) as d
      union all select 2, cast('hello-long' as binary)
      union all select 3, cast('' as char(1))) as u
where ord = 2;

select ord, hex(d) as bytes, length(d) as width
from (select 1 as ord, cast('hello-long' as binary) as d
      union all select 2, cast('' as char(1))) as u
where ord = 1;

select n, hex(d) as bytes, length(d) as width
from (values row(1, cast('' as char(1))),
             row(2, cast('hello-long' as binary)),
             row(3, cast('' as char(1)))) as v(n,d)
where n = 2;

select ord from (select 1 as ord, cast('' as char(1)) as d
                 union all select 2, cast('hello-long' as binary)) as u
where length(d) > 1 order by ord;

create table source_char (d char(1));
create table source_binary (d varbinary(32));
insert into source_char values ('');
insert into source_binary values ('hello-long');
select hex(d) as bytes, length(d) as width
from (select d from source_char union all select d from source_binary) as u
where length(d) > 1;

create table saved as
select ord, d from (select 1 as ord, cast('' as char(1)) as d
                    union all select 2, cast('hello-long' as binary)) as u;
select ord, hex(d) as bytes, length(d) as width from saved where ord = 2;

create view shown as
select ord, d from (select 1 as ord, cast('' as char(1)) as d
                    union all select 2, cast('hello-long' as binary)) as u;
select ord, hex(d) as bytes, length(d) as width from shown where ord = 2;

select hex(d) as bytes, length(d) as width
from (select cast('hi' as binary) as d
      union all select cast('hello-long' as binary)) as u
where length(d) > 2;

select count(*) as matched
from (values row(cast('a' as char(1)) collate utf8mb4_unicode_ci),
             row(cast('A' as binary(1)))) as v(d)
where d = 'a';

select hex(d) as bytes, length(d) as width
from (values row(cast('a' as char(1)) collate utf8mb4_unicode_ci),
             row(cast('hello-long' as binary(10)))) as v(d)
where length(d) > 1;

select hex(d) as bytes
from (values row(cast('a' as char(1)) collate utf8mb4_unicode_ci),
             row(cast(x'FF' as binary(1)))) as v(d);

-- The short literal and BINARY(100) pair from #30726 must keep both bytes
-- and the fixed-width padding before and after persistence.
select hex(b) as bytes, length(b) as width
from (select 'x' as b union all select cast('ab' as binary(100))) as u
order by width;
create table bin_sink (v varbinary(200));
insert into bin_sink
select b from (select 'x' as b union all select cast('abcdefgh' as binary(100))) as u;
select hex(v) as bytes, length(v) as width from bin_sink order by width;

drop database issue_30718;
