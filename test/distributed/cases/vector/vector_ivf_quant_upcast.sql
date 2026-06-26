-- ivfflat QUANTIZATION is downcast-only. A narrow base column (vecbf16/vecf16/
-- vecint8/vecuint8) with a QUANTIZATION wider than the base element is rejected at
-- plan time -- it would store upcast entries for no precision gain and force the
-- f32 distance kernel over narrow data. Regression for the schema.go upcast guard.
-- Equal-width / narrower quantization (and omitting it) are allowed. All cases here
-- fail before any index build, so no GPU is required.
drop database if exists ivf_qup;
create database ivf_qup;
use ivf_qup;
create table i8(a int, v vecint8(4));
create table bf(a int, v vecbf16(4));
create table hf(a int, v vecf16(4));
create table u8(a int, v vecuint8(4));
-- int8 base (1 byte) + wider quantization -> rejected
create index x using ivfflat on i8(v) lists=1 op_type 'vector_l2_ops' quantization 'float32';
create index x using ivfflat on i8(v) lists=1 op_type 'vector_l2_ops' quantization 'bf16';
-- bf16 base (2 bytes) + float32 (4 bytes) -> rejected
create index x using ivfflat on bf(v) lists=1 op_type 'vector_l2_ops' quantization 'float32';
-- f16 base (2 bytes) + float32 (4 bytes) -> rejected
create index x using ivfflat on hf(v) lists=1 op_type 'vector_l2_ops' quantization 'float32';
-- uint8 base (1 byte) + float16 (2 bytes) -> rejected
create index x using ivfflat on u8(v) lists=1 op_type 'vector_l2_ops' quantization 'float16';
drop database ivf_qup;
