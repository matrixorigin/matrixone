-- GIS: GeoHash encode/decode (ST_GeoHash, ST_LatFromGeoHash, ST_LongFromGeoHash, ST_PointFromGeoHash).

-- Encode from a point and from explicit longitude/latitude.
select st_geohash(st_geomfromtext('POINT(-5.603 42.605)'), 5) as gh_point;
select st_geohash(-5.603, 42.605, 5) as gh_lonlat;
select st_geohash(0, 0, 11) as gh_origin;
select st_geohash(-180, -90, 1) as gh_southwest, st_geohash(180, 90, 1) as gh_northeast;
select length(st_geohash(12.5, -7.25, 100)) as gh_max_length;

-- Both SQL overloads reject invalid coordinates and lengths instead of
-- returning a plausible boundary hash or allocating an unbounded result.
-- @regex("longitude must be finite",true)
select st_geohash(180.0001, 0, 12);
-- @regex("latitude must be finite",true)
select st_geohash(0, 90.0001, 12);
-- @regex("longitude must be finite",true)
select st_geohash(cast('NaN' as double), 0, 12);
-- @regex("latitude must be finite",true)
select st_geohash(0, cast('Inf' as double), 12);
-- @regex("longitude must be finite",true)
select st_geohash(st_geomfromtext('POINT(180.0001 0)'), 12);
-- @regex("got 101",true)
select st_geohash(st_geomfromtext('POINT(0 0)'), 101);
-- @regex("got 0",true)
select st_geohash(0, 0, 0);
-- @regex("got 101",true)
select st_geohash(0, 0, 101);

-- NULL arguments remain NULL and do not undergo coordinate/length checks.
select st_geohash(null, 0, 12) as null_lon,
       st_geohash(0, null, 12) as null_lat,
       st_geohash(0, 0, null) as null_length;

-- A real table column path and a valid row followed by an invalid row.
drop temporary table if exists geohash_issue_28189;
create temporary table geohash_issue_28189 (
    lon double,
    lat double,
    max_length bigint
);
insert into geohash_issue_28189 values (-5.603, 42.605, 5);
select st_geohash(lon, lat, max_length) as gh from geohash_issue_28189;
insert into geohash_issue_28189 values (180.0001, 0, 12);
-- @regex("longitude must be finite",true)
select st_geohash(lon, lat, max_length) as gh from geohash_issue_28189 order by max_length;
drop temporary table geohash_issue_28189;
select 1 as after_geohash_error;

-- Prepared markers use the same admission rules as constants and columns.
prepare geohash_issue_28189_stmt from 'select st_geohash(?, ?, ?) as gh';
set @geohash_issue_28189_lon = -5.603;
set @geohash_issue_28189_lat = 42.605;
set @geohash_issue_28189_len = 5;
execute geohash_issue_28189_stmt using @geohash_issue_28189_lon, @geohash_issue_28189_lat, @geohash_issue_28189_len;
set @geohash_issue_28189_lon = 180.0001;
-- @regex("longitude must be finite",true)
execute geohash_issue_28189_stmt using @geohash_issue_28189_lon, @geohash_issue_28189_lat, @geohash_issue_28189_len;
deallocate prepare geohash_issue_28189_stmt;

-- Decode back to latitude / longitude (center of the cell).
select st_latfromgeohash('ezs42') as lat;
select st_longfromgeohash('ezs42') as lon;
select st_longfromgeohash('EZS42') as uppercase_lon;

-- Empty strings are not the geohash for the origin.
-- @regex("invalid geohash",true)
select st_latfromgeohash('');
-- @regex("invalid geohash",true)
select st_longfromgeohash('');
-- @regex("invalid geohash",true)
select st_pointfromgeohash('', 4326);

-- Decode validates at most the first 433 characters, per MySQL compatibility.
-- @regex("invalid geohash",true)
select st_latfromgeohash(concat(repeat('s', 432), '!'));
select st_longfromgeohash(concat(repeat('s', 433), '!')) is not null as gh_433_prefix_valid;

-- Build a point from a geohash.
select st_astext(st_pointfromgeohash('ezs42', 4326)) as pt;
select st_srid(st_pointfromgeohash('ezs42', 4326)) as pt_srid;

-- Round-trip: encode then decode is close to the original.
select st_longfromgeohash(st_geohash(12.5, -7.25, 20)) as rt_lon;
select st_latfromgeohash(st_geohash(12.5, -7.25, 20)) as rt_lat;

-- Invalid geohash characters are rejected.
-- @regex("invalid geohash",true)
select st_latfromgeohash('ail');
