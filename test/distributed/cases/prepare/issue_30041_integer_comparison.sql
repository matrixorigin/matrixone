-- @case
-- @desc: Exact symmetric signed/unsigned comparison and prepared reuse.
-- @label:bvt

drop database if exists issue_30041;
create database issue_30041;
use issue_30041;
create table s(v smallint);
create table u(v int unsigned);
insert into s values(-1);
insert into u values(4294967295);
select s.v<u.v as lt, u.v>s.v as gt, s.v=u.v as eq, s.v!=u.v as ne, s.v<=u.v as le, s.v>=u.v as ge from s cross join u;
select cast(-1 as tinyint)<cast(18446744073709551615 as bigint unsigned) as exact;
select cast(9007199254740993 as bigint)=cast(9007199254740992 as bigint unsigned) as precise;
select cast(-1 as smallint) between cast(0 as tinyint unsigned) and cast(65535 as smallint unsigned) as bounded;
prepare cmp from 'select ? < cast(18446744073709551615 as bigint unsigned) as less';
set @v=-1;
execute cmp using @v;
set @v=0;
execute cmp using @v;
set @v=-2;
execute cmp using @v;
deallocate prepare cmp;
drop database issue_30041;
