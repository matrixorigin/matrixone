-- Cross-path boundary regression for the int8/uint8 quantizer entry encoding.
--
-- The canonical entry form is `cast(cast(v as vecf32) * mul + add as vec{int8,uint8})`,
-- evaluated in FLOAT32 (VECF32 <op> Scalar => VECF32, two roundings). This BVT executes
-- the ACTUAL entry expression and asserts the code is independent of the base column
-- type: a vecf32 base and a vecf64 base must produce the SAME code, because the inner
-- `cast(... as vecf32)` narrows the f64 base to the same float32 the query is narrowed
-- to. Before the fix a vecf64 base evaluated the affine map in float64 and could bucket a
-- boundary component to a different code than the float32 query, so an exact query could
-- match a neighbor instead of its own row.
--
-- Params are the trained bounds [0.1,0.99] emitted at %.9g by Int8/Uint8EntrySQL:
--   int8:  mul=286.516854 add=-156.651685
--   uint8: mul=286.516854 add=-28.6516854
-- The query-side encoder (ApplyInt8/ApplyUint8) is pinned to these same codes by the Go
-- unit test TestQueryEntryEncodingContract (quantizer_test.go).

drop database if exists ivf_quant_boundary;
create database ivf_quant_boundary;
use ivf_quant_boundary;

create table t(id int primary key, vf32 vecf32(1), vf64 vecf64(1));
insert into t values
 (1, '[0.367]',     '[0.367]'),
 (2, '[0.3670001]', '[0.3670001]'),
 (3, '[0.1]',       '[0.1]'),
 (4, '[0.99]',      '[0.99]'),
 (5, '[0.5]',       '[0.5]');

-- int8 entry: the vecf32-base and vecf64-base codes must match, column for column.
select id,
  cast(cast(vf32 as vecf32(1)) * 286.516854 + (-156.651685) as vecint8(1)) as i8_f32,
  cast(cast(vf64 as vecf32(1)) * 286.516854 + (-156.651685) as vecint8(1)) as i8_f64
from t order by id;

-- uint8 entry: same cross-base consistency (add = -min*mul = -28.6516854).
select id,
  cast(cast(vf32 as vecf32(1)) * 286.516854 + (-28.6516854) as vecuint8(1)) as u8_f32,
  cast(cast(vf64 as vecf32(1)) * 286.516854 + (-28.6516854) as vecuint8(1)) as u8_f64
from t order by id;

drop database ivf_quant_boundary;
