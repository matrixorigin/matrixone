// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// 新增注册项必须明确选择证明或可空原因，不能靠 STRICT 继承乐观默认值。
func TestNullabilityRegistryCoverage(t *testing.T) {
	nullable := map[int]bool{}
	for _, fid := range []int{
		// 空聚合、无值窗口或空 frame。
		MIN, MAX, SUM, AVG, AVG_TW_CACHE, AVG_TW_RESULT, GROUP_CONCAT, JSON_ARRAYAGG, JSON_OBJECTAGG,
		ANY_VALUE, BIT_AND, BIT_OR, BIT_XOR, VAR_POP, STDDEV_POP, VAR_SAMPLE, STDDEV_SAMPLE,
		MEDIAN, BITMAP_CONSTRUCT_AGG, BITMAP_OR_AGG, FIRST_VALUE, LAST_VALUE, NTH_VALUE,
		// 非法值、未匹配内容、空/不存在的成员或执行错误被转换为 NULL。
		DIV, INTEGER_DIV, MOD, ELT, JSON_EXTRACT, JSON_EXTRACT_STRING, JSON_EXTRACT_FLOAT64,
		JSON_LENGTH, JSON_KEYS, JSON_VALUE, JSON_SET, JSON_INSERT, JSON_REPLACE,
		LPAD, RPAD, REGEXP_SUBSTR, REPEAT, SERIAL_EXTRACT, SPLIT_PART, FROM_BASE64,
		COMPRESS, UNCOMPRESS, UNCOMPRESSED_LENGTH, AES_ENCRYPT, AES_DECRYPT, SHA2, UNHEX,
		SQRT, ACOS, ASIN, COT, LN, LOG, LOG2, LOG10, ST_VALIDATE, ST_NUMGEOMETRIES,
		CONVERT_TZ, TIMESTAMPADD, FROM_UNIXTIME, STR_TO_DATE, FROM_DAYS, GET_FORMAT,
		ADDTIME, SUBTIME, LAST_DAY, MAKEDATE, MAKETIME, PERIOD_ADD, PERIOD_DIFF, SEC_TO_TIME,
		INET6_ATON, INET6_NTOA, INET_ATON,
		// 同名函数仅部分类型化 overload 有证明。
		MONTH, MICROSECOND, TIMEDIFF, TIMESTAMPDIFF, YEARWEEK, DATE_ADD, DATE_SUB, UNIX_TIMESTAMP,
		// 状态缺失、无此类元数据、无文件内容或自定义结果可产生 NULL。
		MO_LOG_DATE, MO_CU, MO_CU_V1, MO_EXPLAIN_PHY, RELEASE_LOCK, IS_USED_LOCK, TRIGGER_FAULT_POINT,
		INTERNAL_AUTO_INCREMENT, INTERNAL_CHAR_LENGTH, INTERNAL_CHAR_SIZE, INTERNAL_COLUMN_CHARACTER_SET,
		INTERNAL_DATETIME_SCALE, INTERNAL_NUMERIC_PRECISION, INTERNAL_NUMERIC_SCALE,
		LAST_QUERY_ID, LOAD_FILE, MO_SHOW_VISIBLE_BIN, MO_SHOW_VISIBLE_BIN_ENUM,
		MO_TABLE_COL_MAX, MO_TABLE_COL_MIN, MO_TUPLE_EXPR,
		TRY_JQ, TRY_WASM, STARLARK, TRY_STARLARK, PYTHON_UDF,
	} {
		nullable[fid] = true
	}
	for fid, fn := range allSupportedFunctions {
		if fn.testFlag(plan.Function_PRODUCE_NULL) || fn.testFlag(plan.Function_PRODUCE_NO_NULL) {
			continue
		}
		for _, overload := range fn.Overloads {
			if fn.nonNullResult == nil && overload.nonNullResult == nil {
				require.True(t, nullable[fid], "function %d overload %d needs an audited nullability contract", fid, overload.overloadId)
			}
		}
	}
}

func TestFunctionResultNullability(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []types.T
		want bool
	}{
		{"/", []types.T{types.T_float64, types.T_float64}, false},
		{"div", []types.T{types.T_int64, types.T_int64}, false},
		{"%", []types.T{types.T_int64, types.T_int64}, false},
		{"max", []types.T{types.T_int64}, false},
		{"min", []types.T{types.T_int64}, false},
		{"sum", []types.T{types.T_int64}, false},
		{"avg", []types.T{types.T_int64}, false},
		{"lag", []types.T{types.T_int64}, false},
		{"lead", []types.T{types.T_int64}, false},
		{"nth_value", []types.T{types.T_int64, types.T_int64}, false},
		{"regexp_substr", []types.T{types.T_varchar, types.T_varchar}, false},
		{"unhex", []types.T{types.T_varchar}, false},
		{"json_extract", []types.T{types.T_varchar, types.T_varchar}, false},
		{"month", []types.T{types.T_varchar}, false},
		{"month", []types.T{types.T_date}, true},
		{"microsecond", []types.T{types.T_varchar}, false},
		{"microsecond", []types.T{types.T_time}, true},
		{"timediff", []types.T{types.T_varchar, types.T_varchar}, false},
		{"timediff", []types.T{types.T_time, types.T_time}, true},
		{"yearweek", []types.T{types.T_varchar}, false},
		{"yearweek", []types.T{types.T_date}, true},
		{"sqrt", []types.T{types.T_float64}, false},
		{"sqrt", []types.T{types.T_array_float32}, false},
		{"split_part", []types.T{types.T_varchar, types.T_varchar, types.T_int64}, false},
		{"replace", []types.T{types.T_varchar, types.T_varchar, types.T_varchar}, true},
		{"md5", []types.T{types.T_varchar}, true},
		{"format", []types.T{types.T_varchar, types.T_varchar}, true},
		{"conv", []types.T{types.T_int64, types.T_int64, types.T_int64}, true},
		{"conv", []types.T{types.T_varchar, types.T_int64, types.T_int64}, false},
		{"json_type", []types.T{types.T_varchar}, true},
		{"encode", []types.T{types.T_varchar, types.T_varchar}, true},
		{"abs", []types.T{types.T_int64}, true},
		{"+", []types.T{types.T_int64, types.T_int64}, true},
		{"count", []types.T{types.T_int64}, true},
		{"row_number", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := make([]types.Type, len(tc.args))
			args := make([]*plan.Expr, len(tc.args))
			for i, typ := range tc.args {
				inputs[i] = typ.ToType()
				args[i] = &plan.Expr{Typ: plan.Type{Id: int32(typ), NotNullable: true}}
			}
			fn, err := GetFunctionByName(context.Background(), tc.name, inputs)
			require.NoError(t, err)
			require.Equal(t, tc.want, DeduceNotNullable(fn.GetEncodedOverloadID(), args))
			if len(args) > 0 && tc.name != "count" {
				args[0].Typ.NotNullable = false
				require.False(t, DeduceNotNullable(fn.GetEncodedOverloadID(), args))
			}
		})
	}
}

func TestCastResultNullability(t *testing.T) {
	for _, tc := range []struct {
		from, to types.T
		want     bool
	}{
		{types.T_bool, types.T_varchar, true},
		{types.T_bool, types.T_decimal64, false},
		{types.T_bit, types.T_time, true},
		{types.T_bit, types.T_timestamp, false},
		{types.T_int64, types.T_timestamp, false},
		{types.T_int64, types.T_year, false},
		{types.T_float64, types.T_decimal128, true},
		{types.T_decimal128, types.T_timestamp, false},
		{types.T_decimal64, types.T_year, true},
		{types.T_varchar, types.T_float64, true},
		{types.T_varchar, types.T_date, false},
		{types.T_varchar, types.T_datetime, false},
		{types.T_varchar, types.T_timestamp, false},
		{types.T_varchar, types.T_time, false},
		{types.T_varchar, types.T_year, false},
		{types.T_varchar, types.T_json, true},
		{types.T_varchar, types.T_uuid, true},
		{types.T_varchar, types.T_array_float32, true},
		{types.T_varchar, types.T_geometry, true},
		{types.T_datalink, types.T_varchar, true},
		{types.T_date, types.T_varchar, true},
		{types.T_date, types.T_timestamp, true},
		{types.T_datetime, types.T_decimal128, true},
		{types.T_timestamp, types.T_date, true},
		{types.T_time, types.T_int64, true},
		{types.T_year, types.T_varchar, true},
		{types.T_year, types.T_date, false},
		{types.T_json, types.T_float64, false},
		{types.T_json, types.T_varchar, true},
		{types.T_uuid, types.T_varchar, true},
		{types.T_enum, types.T_uint64, true},
		{types.T_geometry, types.T_geometry32, true},
		{types.T_array_float32, types.T_array_float64, true},
		{types.T_TS, types.T_timestamp, true},
		{types.T_Rowid, types.T_Rowid, true},
		{types.T_any, types.T_int64, false},
	} {
		for _, fid := range []int32{CAST, CAST_STRICT} {
			args := []*plan.Expr{{Typ: plan.Type{Id: int32(tc.from), NotNullable: true}}, {Typ: plan.Type{Id: int32(tc.to)}}}
			require.Equal(t, tc.want, DeduceNotNullable(EncodeOverloadID(fid, 0), args), "%v -> %v", tc.from, tc.to)
			args[0].Typ.NotNullable = false
			require.False(t, DeduceNotNullable(EncodeOverloadID(fid, 0), args))
		}
	}
}

func TestCastLiteralResultNullability(t *testing.T) {
	for _, tc := range []struct {
		value string
		to    types.T
		want  bool
	}{
		{"2026-01-01", types.T_date, true},
		{"bad", types.T_date, false},
		{"", types.T_date, false},
		{"2026", types.T_year, true},
		{"bad", types.T_year, false},
		{"2026-01-01", types.T_datetime, true},
		{"bad", types.T_datetime, true}, // 成功不产生 NULL，但执行仍然返回原来的错误。
		{"", types.T_datetime, false},
		{"", types.T_timestamp, false},
		{"12:34:56", types.T_time, true},
		{"", types.T_time, false},
	} {
		arg := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tc.value}}}}
		args := []*plan.Expr{arg, {Typ: plan.Type{Id: int32(tc.to)}}}
		require.Equal(t, tc.want, DeduceNotNullable(EncodeOverloadID(CAST, 0), args), "%q -> %v", tc.value, tc.to)
		arg.GetLit().Isnull = true
		require.False(t, DeduceNotNullable(EncodeOverloadID(CAST, 0), args))
	}
	require.False(t, DeduceNotNullable(EncodeOverloadID(CAST, 0), nil))
}

func TestConditionalResultNullability(t *testing.T) {
	nonnull := &plan.Expr{Typ: plan.Type{NotNullable: true}}
	nullable := &plan.Expr{}
	for _, tc := range []struct {
		name string
		fid  int32
		args []*plan.Expr
		want bool
	}{
		{"coalesce empty", COALESCE, nil, false},
		{"count nullable input", COUNT, []*plan.Expr{nullable}, true},
		{"coalesce all nullable", COALESCE, []*plan.Expr{nullable, nullable}, false},
		{"coalesce dynamic fallback", COALESCE, []*plan.Expr{nullable, nonnull}, true},
		{"case nullable condition", CASE, []*plan.Expr{nullable, nonnull, nonnull}, true},
		{"case nullable result", CASE, []*plan.Expr{nonnull, nullable, nonnull}, false},
		{"case nullable else", CASE, []*plan.Expr{nonnull, nonnull, nullable}, false},
		{"case multiple branches", CASE, []*plan.Expr{nullable, nonnull, nullable, nonnull, nonnull}, true},
		{"case missing else", CASE, []*plan.Expr{nonnull, nonnull}, false},
		{"if nullable condition", IFF, []*plan.Expr{nullable, nonnull, nonnull}, true},
		{"if nullable result", IFF, []*plan.Expr{nonnull, nullable, nonnull}, false},
		{"lag boundary", LAG, []*plan.Expr{nonnull}, false},
		{"lag default", LAG, []*plan.Expr{nonnull, nullable, nonnull}, true},
		{"lag nullable source", LAG, []*plan.Expr{nullable, nonnull, nonnull}, false},
		{"lead nullable default", LEAD, []*plan.Expr{nonnull, nonnull, nullable}, false},
		{"lead default", LEAD, []*plan.Expr{nonnull, nonnull, nonnull}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DeduceNotNullable(EncodeOverloadID(tc.fid, 0), tc.args))
		})
	}
	for _, id := range []int64{-1, EncodeOverloadID(1<<30, 0), EncodeOverloadID(MD5, 1000)} {
		require.False(t, DeduceNotNullable(id, []*plan.Expr{nonnull}))
	}
}

func TestWindowResultNullability(t *testing.T) {
	require.False(t, DeduceWindowNotNullable(nil))
	require.False(t, DeduceWindowNotNullable(&plan.WindowSpec{}))
	require.False(t, DeduceWindowNotNullable(&plan.WindowSpec{WindowFunc: &plan.Expr{}}))
	for _, fid := range []int32{FIRST_VALUE, LAST_VALUE} {
		for _, sourceNonNull := range []bool{false, true} {
			for _, tc := range []struct {
				name       string
				start, end *plan.FrameBound
				want       bool
			}{
				{"default", &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true}, &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, true},
				{"whole partition", &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true}, &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true}, true},
				{"current row", &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, true},
				{"preceding end can be empty", &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true}, &plan.FrameBound{Type: plan.FrameBound_PRECEDING}, false},
				{"following start can be empty", &plan.FrameBound{Type: plan.FrameBound_FOLLOWING}, &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true}, false},
				{"offset not proven", &plan.FrameBound{Type: plan.FrameBound_PRECEDING}, &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, false},
				{"missing bound", nil, nil, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					w := &plan.WindowSpec{
						WindowFunc: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
							Func: &plan.ObjectRef{Obj: EncodeOverloadID(fid, 0)},
							Args: []*plan.Expr{{Typ: plan.Type{NotNullable: sourceNonNull}}},
						}}},
						Frame: &plan.FrameClause{Start: tc.start, End: tc.end},
					}
					require.Equal(t, tc.want && sourceNonNull, DeduceWindowNotNullable(w))
				})
			}
		}
	}
	w := &plan.WindowSpec{WindowFunc: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: EncodeOverloadID(ROW_NUMBER, 0)}}}}}
	require.True(t, DeduceWindowNotNullable(w))
}
