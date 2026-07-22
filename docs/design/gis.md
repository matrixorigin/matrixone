# GIS type support in MatrixOne

We will implement a GEOMETRY type.  A Geometry type can have two properties, 
   1. type, it could either be 
            0: GENERIC,
            1: POINT,
            2. LINESTRING,
            3. POLYGON,
            4. MULTIPOINT,
            5. MULTILINESTRING,
            6. MULTIPOLYGON,
            7. GEOMETRYCOLLECTION
    2. SRID, it could be,
            0: Unitless planar geometry
            4326: WGS 84 longitude/latitude.

```
Create table (g GEOMETRY(Type='point', SRID=4326));
-- or simpler
Create table (g GEOMETRY('point', 4326));
```

Will create a geometry column g, whith type point, SRID 4326.  If not specified, type default to 0 (generic) and 
srid default to 0 (planar).  Use SQL type scale to store type, precision to store srid.   

Also create aliased types of GEOMETRY, 
    POINT: type = POINT, SRID = 0
    LINESTRING: type = LINESTRING, SRID = 0
    POLYGON: type = POLYGON, SRID = 0
    MULTIPOINT: type = MULTIPOINT, SRID = 0
    MULTILINESTRING: type = MULTILINESTRING, SRID = 0
    MULTIPOLYGON: type = MULTIPOLYGON, SRID = 0
    GEOMETRYCOLLECTION: type = GEOMETRYCOLLECTION, SRID = 0
And 
    GEOGRAPHY: type = GENERIC, SRID=4326

A Geometry type is stored as varlena, using the so called [Well Known Binary Format](https://libgeos.org/specification/wkb). 
We only need to implement the Standard WKB format.   We always use Little Endian (native for both x64 and arm64).

Read, write, load, and convert GEOMETRY type should use the [Well Known Text Format](https://libgeos.org/specification/wkt).
We will not need to implement POINTZ and POINTM and related.

Also implement a GEOMETRY32 type, and the aliased types.   The well known binary format stores a number using 64 bit floating point,
and these XXX32 types uses float32.

Implement all gis related function in its own directory.   Add bvt tests.  You can take test cases from mysql 9.7 and postgis.

We split this work into two steps.  First step, build these types, and converting from/to text format.  Insert, load, select, etc.

Next step, implement all [spatial functions](https://dev.mysql.com/doc/refman/9.7/en/spatical-function-reference.html).  
Note that if SRID is 0, it should do the computation is a Cartesian Cordinate system.   If SRID is 4326, it should compute using
geodetic algorithms and return result is meters or square meters.   





