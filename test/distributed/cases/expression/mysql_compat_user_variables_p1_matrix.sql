-- @suite
-- @case
-- @desc: MySQL-compatible user-variable P1 regression matrix
-- @label:bvt
-- MySQL reference: 8.4.10, verified with a temporary local Docker server.

drop database if exists mysql_compat_user_variables_p1_matrix;
create database mysql_compat_user_variables_p1_matrix;
use mysql_compat_user_variables_p1_matrix;

-- Matrix A: MySQL varlen controls. MatrixOne vector cases below are MO-only,
-- so these keep the MySQL-native string/binary user-variable behavior nearby.
set @uv_p1_json_text = '[1,2,3]';
select @uv_p1_json_text, json_extract(@uv_p1_json_text, '$[0]');
set @uv_p1_binary = cast('abc' as binary);
select @uv_p1_binary, hex(@uv_p1_binary);

-- Matrix B: MO vector user variables must round-trip with valid internal
-- bytes, not a display-string payload such as "[1 2 3]".
set @uv_p1_v32 = cast('[1,2,3]' as vecf32(3));
select @uv_p1_v32;
set @uv_p1_v64 = cast('[1,2,3]' as vecf64(3));
select @uv_p1_v64;
set @uv_p1_i8 = cast('[1,2,3]' as vecint8(3));
select @uv_p1_i8;
set @uv_p1_u8 = cast('[1,128,255]' as vecuint8(3));
select @uv_p1_u8;
set @uv_p1_bf16 = cast('[1,2,3]' as vecbf16(3));
select @uv_p1_bf16;
set @uv_p1_f16 = cast('[1,2,3]' as vecf16(3));
select @uv_p1_f16;

-- Matrix C: TIMESTAMP user variables assigned in a non-local session time
-- zone must not be reconstructed through process-local time.Local.
set @uv_p1_old_time_zone = @@time_zone;
set time_zone = '+08:00';
set @uv_p1_ts_p8 = cast('2026-01-01 00:00:00' as timestamp);
select date_format(@uv_p1_ts_p8, '%Y-%m-%d %H:%i:%s') as ts_p8_same_zone;
set time_zone = '+00:00';
select date_format(@uv_p1_ts_p8, '%Y-%m-%d %H:%i:%s') as ts_p8_after_tz_change;
set time_zone = '-05:00';
set @uv_p1_ts_m5 = cast('2026-01-01 00:00:00' as timestamp);
select date_format(@uv_p1_ts_m5, '%Y-%m-%d %H:%i:%s') as ts_m5_same_zone;
set time_zone = '+08:00';
set @uv_p1_dt_p8 = cast('2026-01-01 00:00:00' as datetime);
select date_format(@uv_p1_dt_p8, '%Y-%m-%d %H:%i:%s') as dt_p8_control;
set time_zone = @uv_p1_old_time_zone;

-- Matrix D: parenthesized SELECT ... INTO propagates to execution and
-- preserves the zero-row warning behavior.
set @uv_p1_paren = 0;
(select 3 into @uv_p1_paren);
select @uv_p1_paren;
set @uv_p1_nested_paren = 0;
((select 4 into @uv_p1_nested_paren));
select @uv_p1_nested_paren;
set @uv_p1_empty = 9;
(select 1 from dual where false into @uv_p1_empty);
show warnings;
select @uv_p1_empty;

-- Matrix E: UNION SELECT ... INTO follows MySQL's final-result semantics.
-- A nested final query block is accepted with MySQL warning 3962.
set @uv_p1_union_nested = 0;
select 1 union (select 1 into @uv_p1_union_nested);
show warnings;
select @uv_p1_union_nested;

set @uv_p1_union_terminal = 0;
select 1 union select 1 into @uv_p1_union_terminal;
select @uv_p1_union_terminal;

set @uv_p1_union_too_many = 0;
-- @regex("Result consisted of more than one row", true)
select 1 union (select 2 into @uv_p1_union_too_many);
select @uv_p1_union_too_many;

-- Matrix F: non-final INTO clauses are rejected instead of being silently
-- dropped from the enclosing UNION tree.
set @uv_p1_bad_non_last = 0;
-- @regex("Misplaced INTO clause|INTO is not allowed|syntax error", true)
select 1 into @uv_p1_bad_non_last union select 1;
select @uv_p1_bad_non_last;

set @uv_p1_bad_middle = 0;
-- @regex("Misplaced INTO clause|INTO is not allowed|syntax error", true)
select 1 union (select 1 into @uv_p1_bad_middle) union select 1;
select @uv_p1_bad_middle;

drop database mysql_compat_user_variables_p1_matrix;
