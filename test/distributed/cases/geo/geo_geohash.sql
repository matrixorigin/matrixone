-- GIS: GeoHash encode/decode (ST_GeoHash, ST_LatFromGeoHash, ST_LongFromGeoHash, ST_PointFromGeoHash).

-- Encode from a point and from explicit longitude/latitude.
select st_geohash(st_geomfromtext('POINT(-5.603 42.605)'), 5) as gh_point;
select st_geohash(-5.603, 42.605, 5) as gh_lonlat;
select st_geohash(0, 0, 11) as gh_origin;

-- Decode back to latitude / longitude (center of the cell).
select st_latfromgeohash('ezs42') as lat;
select st_longfromgeohash('ezs42') as lon;

-- Build a point from a geohash.
select st_astext(st_pointfromgeohash('ezs42', 4326)) as pt;
select st_srid(st_pointfromgeohash('ezs42', 4326)) as pt_srid;

-- Round-trip: encode then decode is close to the original.
select st_longfromgeohash(st_geohash(12.5, -7.25, 20)) as rt_lon;
select st_latfromgeohash(st_geohash(12.5, -7.25, 20)) as rt_lat;

-- Invalid geohash characters are rejected.
-- @regex("invalid geohash",true)
select st_latfromgeohash('ail');
