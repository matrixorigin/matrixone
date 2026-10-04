-- GIS: polygon Boolean overlay (ST_Union, ST_Intersection, ST_Difference, ST_SymDifference).
-- Areas are asserted via ST_Area over a deterministic two-square overlap.

-- A = [0,4]^2 (area 16), B = [2,6]^2 (area 16), overlap [2,4]^2 (area 4).
select st_area(st_intersection(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), st_geomfromtext('POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))'))) as inter_area;
select st_area(st_union(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), st_geomfromtext('POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))'))) as union_area;
select st_area(st_difference(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), st_geomfromtext('POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))'))) as diff_area;
select st_area(st_symdifference(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), st_geomfromtext('POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))'))) as xor_area;

-- Crossing rectangles whose XOR has two polygon components that touch at one point. Check both
-- operand orders, component validity, and expected area without depending on ring serialization.
select st_geometrytype(xor_g) as xor_type,
       st_numgeometries(xor_g) as component_count,
       st_area(xor_g) as xor_area,
       st_isvalid(st_geometryn(xor_g, 1)) as first_component_valid,
       st_isvalid(st_geometryn(xor_g, 2)) as second_component_valid
from (select st_symdifference(
        st_geomfromtext('POLYGON((0 0,4 0,4 2,0 2,0 0))'),
        st_geomfromtext('POLYGON((-2 1,1 1,1 2,-2 2,-2 1))')) as xor_g) xor_case;
select st_geometrytype(xor_g) as xor_type,
       st_numgeometries(xor_g) as component_count,
       st_area(xor_g) as xor_area,
       st_isvalid(st_geometryn(xor_g, 1)) as first_component_valid,
       st_isvalid(st_geometryn(xor_g, 2)) as second_component_valid
from (select st_symdifference(
        st_geomfromtext('POLYGON((-2 1,1 1,1 2,-2 2,-2 1))'),
        st_geomfromtext('POLYGON((0 0,4 0,4 2,0 2,0 0))')) as xor_g) xor_case;

-- Disjoint squares: intersection empty (area 0), union is the sum (a multipolygon).
select st_area(st_intersection(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), st_geomfromtext('POLYGON((5 5,6 5,6 6,5 6,5 5))'))) as disjoint_inter;
select st_area(st_union(st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'), st_geomfromtext('POLYGON((5 5,6 5,6 6,5 6,5 5))'))) as disjoint_union;

-- Contained: difference leaves a polygon with a hole (area 100 - 16 = 84).
select st_area(st_difference(st_geomfromtext('POLYGON((0 0,10 0,10 10,0 10,0 0))'), st_geomfromtext('POLYGON((3 3,7 3,7 7,3 7,3 3))'))) as ring_area;

-- Two squares sharing an edge merge into one polygon under union (area 32).
select st_area(st_union(st_geomfromtext('POLYGON((0 0,4 0,4 4,0 4,0 0))'), st_geomfromtext('POLYGON((4 0,8 0,8 4,4 4,4 0))'))) as shared_edge_union;

-- Non-areal input is rejected.
-- @regex("POLYGON or MULTIPOLYGON",true)
select st_union(st_geomfromtext('POINT(0 0)'), st_geomfromtext('POLYGON((0 0,1 0,1 1,0 1,0 0))'));
