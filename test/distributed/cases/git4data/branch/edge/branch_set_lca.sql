-- SET values returned by the LCA SQL probe are labels, while branch comparison
-- stores them as uint64 bitmaps. Exercise every LCA-dependent statement.
drop database if exists br_set_lca;
create database br_set_lca;
use br_set_lca;

create table base(
    id int primary key,
    v set('a','b','c')
);
insert into base values (1,'a'),(2,null);

data branch create table src from base;
data branch create table pick_dst from base;
data branch create table merge_dst from base;

update src set v='b,c' where id=1;
insert into src values(3,'a,c');
delete from src where id=2;

data branch diff src against base output count;
data branch diff src against base;

data branch pick src into pick_dst keys(1,3);
select id, cast(v as char) as v, v is null as is_null from pick_dst order by id;

data branch merge src into merge_dst;
select id, cast(v as char) as v, v is null as is_null from merge_dst order by id;
data branch diff src against merge_dst output count;

-- Empty SET and NULL are distinct and both must survive LCA reconstruction.
create table boundary_base(
    id int primary key,
    v set('a','b','c')
);
insert into boundary_base values (1,''),(2,null);
data branch create table boundary_src from boundary_base;
data branch create table boundary_dst from boundary_base;
update boundary_src set v='a,b' where id=1;
update boundary_src set v='' where id=2;

data branch diff boundary_src against boundary_base output count;
data branch merge boundary_src into boundary_dst;
select id, cast(v as char) as v, v is null as is_null from boundary_dst order by id;
data branch diff boundary_src against boundary_dst output count;

-- An empty SET member and the empty SET both display as an empty string. The
-- LCA probe must carry the stored bitmap instead of round-tripping the label.
create table empty_member_base(
    id int primary key,
    v set('','a')
);
insert into empty_member_base values (1,1);
data branch create table empty_member_src from empty_member_base;
data branch create table empty_member_dst from empty_member_base;
update empty_member_src set v=0 where id=1;

select cast(v as unsigned) as bitmap from empty_member_base;
select cast(v as unsigned) as bitmap from empty_member_src;
data branch diff empty_member_src against empty_member_base output count;
data branch merge empty_member_src into empty_member_dst;
select cast(v as unsigned) as bitmap from empty_member_dst;
data branch diff empty_member_src against empty_member_dst output count;

-- Numeric consumers beyond the direct LCA expression must retain the same
-- physical bitmap across SELECT, INSERT ... SELECT, and pure SET UNION paths.
create table projection_src(
    id int primary key,
    v set('','a')
);
insert into projection_src values (1,0),(2,1);

select id, cast(name as unsigned) as bitmap
from (select id, v as name from projection_src) src
order by id;

create table projection_dst(
    id int primary key,
    bitmap bigint unsigned
);
insert into projection_dst select id, v from projection_src;
select * from projection_dst order by id;

select cast(name as unsigned) as bitmap
from (
    select v as name from projection_src
    union all
    select v from projection_src
) src
order by bitmap;

-- Once a set operation mixes SET and VARCHAR, its output is an ordinary
-- string. A SET label therefore follows VARCHAR cast semantics.
create table mixed_projection_src(v set('a',''));
insert into mixed_projection_src values (1);
select cast(name as unsigned) as bitmap
from (
    select v as name from mixed_projection_src
    union all
    select cast('1' as varchar) from mixed_projection_src
) src
order by bitmap;

drop database br_set_lca;
