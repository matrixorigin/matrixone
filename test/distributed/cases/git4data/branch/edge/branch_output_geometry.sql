-- DATA BRANCH DIFF OUTPUT AS must retain SRID metadata for GEOMETRY families.
drop database if exists br_output_geometry;
create database br_output_geometry;
use br_output_geometry;

create table base(
  a int primary key,
  g point srid 4326,
  g32 point32 srid 4326
);
insert into base values (
  1,
  st_geomfromtext('POINT(1 1)', 4326),
  st_geomfromtext('POINT(1 1)', 4326)
);

data branch create table geometry_left from base;
data branch create table geometry_right from base;
update geometry_left set
  g = st_geomfromtext('POINT(2 2)', 4326),
  g32 = st_geomfromtext('POINT(3 3)', 4326)
where a = 1;

data branch diff geometry_left against geometry_right output as geometry_diff_output;
show create table geometry_diff_output;
select __mo_diff_source, __mo_diff_flag,
       st_astext(g) as g_wkt, st_srid(g) as g_srid,
       st_astext(g32) as g32_wkt, st_srid(g32) as g32_srid
from geometry_diff_output;

drop database br_output_geometry;
