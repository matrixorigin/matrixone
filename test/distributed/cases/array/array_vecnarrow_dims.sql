-- vector_dims on the narrow vector types (vecbf16/vecf16/vecint8/vecuint8).
-- Regression: vector_dims previously only had float32/float64 overloads and
-- errored on narrow types ("invalid argument function vector_dims, bad value
-- [VECINT8]"). It now returns the element count (= content bytes / sizeof(elem)),
-- like vecf32; a NULL vector yields NULL dims, matching vecf32 behavior.
drop database if exists nvdims;
create database nvdims;
use nvdims;
create table t(a int, bf vecbf16(3), hf vecf16(5), i8 vecint8(4), u8 vecuint8(2));
insert into t values(1, '[1,2,3]', '[1,2,3,4,5]', '[1,2,3,4]', '[1,2]');
insert into t values(2, '[4,5,6]', '[6,7,8,9,10]', '[5,6,7,8]', '[9,8]');
select a, vector_dims(bf) as bf, vector_dims(hf) as hf, vector_dims(i8) as i8, vector_dims(u8) as u8 from t order by a;
insert into t values(3, null, null, null, null);
select a, vector_dims(bf) as bf, vector_dims(u8) as u8 from t order by a;
drop database nvdims;
