// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// LAG/LEAD must reconcile the value argument (1st arg) and the default value
// (3rd arg) into one common result type. Narrowing the default to the value
// type truncates longer string defaults and rounds decimal ones (#29974).
func TestBindLagLeadDefaultCapacity(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				value, def   *planpb.Expr
				oid          types.T
				width, scale int32
			}{
				{"text boundary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, makePlan2Int64ConstExprWithType(0), types.T_text, 0, 0},
				{"mediumtext", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 16777215}}, makePlan2Int64ConstExprWithType(0), types.T_text, 16777215, 0},
				{"longtext", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: types.MaxLongTextLen}}, makePlan2Int64ConstExprWithType(0), types.T_text, types.MaxLongTextLen, 0},
				{"unbounded text", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}}, makePlan2Int64ConstExprWithType(0), types.T_text, types.MaxLongTextLen, 0},
				{"text default", makePlan2Int64ConstExprWithType(0), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 16777215}}, types.T_text, 16777215, 0},
				{"unknown char varchar", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_char)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}}, types.T_text, types.MaxLongTextLen, 0},
				{"json varchar 254", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 254}}, types.T_text, types.MaxLongTextLen, 0},
				{"json varchar 255", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 255}}, types.T_text, types.MaxLongTextLen, 0},
				{"json varchar 256", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}, types.T_text, types.MaxLongTextLen, 0},
				{"varchar 254 json", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 254}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, types.T_text, types.MaxLongTextLen, 0},
				{"varchar 255 json", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 255}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, types.T_text, types.MaxLongTextLen, 0},
				{"varchar 256 json", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, types.T_text, types.MaxLongTextLen, 0},
				{"mediumtext json", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: types.MaxMediumTextLen}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, types.T_text, types.MaxLongTextLen, 0},
				{"json mediumtext", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: types.MaxMediumTextLen}}, types.T_text, types.MaxLongTextLen, 0},

				{"uuid binary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uuid)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary), Width: 36}}, types.T_blob, types.MaxTinyTextLen, 0},
				{"binary uuid", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary), Width: 36}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uuid)}}, types.T_blob, types.MaxTinyTextLen, 0},
				{"mediumblob binary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: types.MaxMediumTextLen}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary), Width: 36}}, types.T_blob, types.MaxMediumTextLen, 0},
				{"binary mediumblob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_binary), Width: 36}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: types.MaxMediumTextLen}}, types.T_blob, types.MaxLongTextLen, 0},
				{"mediumblob char", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: types.MaxMediumTextLen}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_char), Width: 1}}, types.T_blob, types.MaxMediumTextLen, 0},
				{"char mediumblob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_char), Width: 1}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: types.MaxMediumTextLen}}, types.T_blob, types.MaxMediumTextLen, 0},
				{"mediumblob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, makePlan2Int64ConstExprWithType(0), types.T_blob, 16777215, 0},
				{"unbounded blob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob)}}, makePlan2Int64ConstExprWithType(0), types.T_blob, types.MaxLongTextLen, 0},
				{"blob default", makePlan2Int64ConstExprWithType(0), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, types.T_blob, 16777215, 0},
				{"blob text", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, types.T_blob, 16777215, 0},
				{"text blob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, types.T_blob, types.MaxLongTextLen, 0},
				{"varchar tinytext", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 100}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255}}, types.T_text, 0, 0},
				{"tinytext varchar", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 100}}, types.T_text, 0, 0},
				{"utf8 boundary 256", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64, Charset: uint32(types.CharsetUTF8)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255}}, types.T_text, 0, 0},
				{"mb3 boundary 255", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 85, Charset: uint32(types.CharsetUTF8MB3UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255, Charset: uint32(types.CharsetUTF8MB3UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}}, types.T_text, 255, 0},
				{"mb3 boundary 258", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 86, Charset: uint32(types.CharsetUTF8MB3UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255, Charset: uint32(types.CharsetUTF8MB3UnicodeCI), CollationVersion: uint32(types.CollationVersionV1)}}, types.T_text, 0, 0},
				{"binary charset boundary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 255, Charset: uint32(types.CharsetBinary)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255, Charset: uint32(types.CharsetBinary)}}, types.T_text, 255, 0},
				{"medium text byte capacity", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 20000}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 255}}, types.T_text, types.MaxMediumTextLen, 0},
				{"varchar tinyblob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 100}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 255}}, types.T_blob, 0, 0},
				{"tinyblob varchar", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 255}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 100}}, types.T_blob, 0, 0},
				{"integer year", makePlan2Int64ConstExprWithType(1000000000000), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}}, types.T_int64, 0, 0},
				{"year integer", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}}, makePlan2Int64ConstExprWithType(1000000000000), types.T_int64, 0, 0},
				{"decimal year", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 4}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}}, types.T_decimal128, 20, 4},
				{"year decimal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 4}}, types.T_decimal64, 8, 4},
				{"decimal null year", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, types.T_decimal256, 65, 65},
				{"decimal bit", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 2}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, types.T_decimal64, 5, 2},
				{"bit decimal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 2}}, types.T_decimal64, 3, 2},
				{"decimal bit64", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 2}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 64}}, types.T_decimal128, 22, 2},
				{"bit64 decimal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 64}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 2}}, types.T_decimal128, 22, 2},
				{"unknown bit decimal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 5, Scale: 2}}, types.T_decimal128, 22, 2},
				{"integer bit", makePlan2Int64ConstExprWithType(42), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, types.T_decimal128, 20, 0},
				{"bit integer", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, makePlan2Int64ConstExprWithType(42), types.T_decimal128, 20, 0},
				{"integer bool", makePlan2Int64ConstExprWithType(42), makePlan2BoolConstExprWithType(true), types.T_int64, 0, 0},
				{"unsigned bool", makePlan2Uint64ConstExprWithType(42), makePlan2BoolConstExprWithType(false), types.T_uint64, 0, 0},
				{"float bool", makePlan2Float32ConstExprWithType(42.5), makePlan2BoolConstExprWithType(true), types.T_float32, 0, 0},
				{"double bool", makePlan2Float64ConstExprWithType(42.5), makePlan2BoolConstExprWithType(false), types.T_float64, 0, 0},
				{"bit bool", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, makePlan2BoolConstExprWithType(true), types.T_bit, 3, 0},
				{"bool bit", makePlan2BoolConstExprWithType(false), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 3}}, types.T_bit, 3, 0},
				{"bool integer", makePlan2BoolConstExprWithType(true), makePlan2Int64ConstExprWithType(42), types.T_int64, 0, 0},
				{"bool double", makePlan2BoolConstExprWithType(false), makePlan2Float64ConstExprWithType(42.5), types.T_float64, 0, 0},
				{"decimal bool true", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 2}}, makePlan2BoolConstExprWithType(true), types.T_decimal64, 3, 2},
				{"decimal bool column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 2}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}}, types.T_decimal64, 3, 2},
				{"decimal bool false", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2BoolConstExprWithType(false), types.T_decimal256, 65, 65},
				{"decimal bool null", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, types.T_decimal256, 65, 65},
				{"bool decimal", makePlan2BoolConstExprWithType(true), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 2}}, types.T_decimal64, 3, 2},
				{"decimal signed zero", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Int64ConstExprWithType(0), types.T_decimal256, 65, 65},
				{"decimal unsigned zero", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Uint64ConstExprWithType(0), types.T_decimal256, 65, 65},
				{"decimal typed null", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, types.T_decimal256, 65, 65},
				{"decimal public boundary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 64, Scale: 63}}, makePlan2Int64ConstExprWithType(10), types.T_decimal256, 65, 63},
				{"varchar numeric", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}}, makePlan2Int64ConstExprWithType(0), types.T_varchar, 10, 0},
				{"decimal integer literal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, makePlan2Int64ConstExprWithType(1000), types.T_decimal64, 5, 1},
				{"decimal negative literal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, makePlan2Int64ConstExprWithType(-1000), types.T_decimal64, 5, 1},
				{"decimal integer column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}, types.T_decimal128, 20, 1},
				{"decimal uint64 column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 4}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}}, types.T_decimal128, 24, 4},
				{"decimal256 promotion", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 30}}, makePlan2Int64ConstExprWithType(999999999999), types.T_decimal256, 42, 30},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{tc.value, makePlan2Int64ConstExprWithType(1), tc.def})
					require.NoError(t, err)
					for i, typ := range []planpb.Type{expr.Typ, expr.GetF().Args[0].Typ, expr.GetF().Args[2].Typ} {
						require.Equal(t, int32(tc.oid), typ.Id)
						require.Equal(t, tc.scale, typ.Scale)
						if tc.value.Typ.Charset == uint32(types.CharsetUTF8MB3UnicodeCI) {
							require.Equal(t, tc.value.Typ.Charset, typ.Charset)
							require.Equal(t, tc.value.Typ.CollationVersion, typ.CollationVersion)
						}
						// Same-family decimals and BLOBs need no metadata-only
						// cast when their original capacity fits the result.
						if i == 0 || (!tc.oid.IsDecimal() && tc.oid != types.T_blob) {
							require.Equal(t, tc.width, typ.Width)
						}
					}
				})
			}
			_, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}},
				makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(999999999999),
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
			for _, tc := range []struct {
				name       string
				value, def *planpb.Expr
			}{
				{"year precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 62}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_year)}}},
				{"bit default precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 1}}},
				{"bit source precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 64}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}},
				{"bool default precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2BoolConstExprWithType(true)},
				{"bool value precision overflow", makePlan2BoolConstExprWithType(true), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}},
				{"nonzero integer precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Int64ConstExprWithType(1)},
				{"integer domain precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}},
				{"aligned decimal SQL precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 64}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 3, Scale: 1}}},
				{"aligned decimal physical precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 0}}},
				{"integer value decimal default precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{tc.value, makePlan2Int64ConstExprWithType(1), tc.def})
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				})
			}
		})
	}
}

func TestBindLagLeadDefaultStringDomain(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, stringOID := range []types.T{types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary} {
			for _, scalarOID := range []types.T{types.T_bit, types.T_bool, types.T_date, types.T_datetime, types.T_time, types.T_timestamp} {
				for _, reverse := range []bool{false, true} {
					t.Run(name+"/"+stringOID.String()+"/"+scalarOID.String()+map[bool]string{false: "/source", true: "/default"}[reverse], func(t *testing.T) {
						value := &planpb.Expr{Typ: planpb.Type{Id: int32(stringOID), Width: 100}}
						def := &planpb.Expr{Typ: planpb.Type{Id: int32(scalarOID)}}
						if reverse {
							value, def = def, value
						}
						expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{value, makePlan2Int64ConstExprWithType(0), def})
						require.NoError(t, err)
						oid := types.T(expr.Typ.Id)
						require.True(t, oid.IsMySQLString(), "string domain lost: %v", oid)
						if stringOID == types.T_blob {
							require.Equal(t, types.T_blob, oid)
						}
						for _, idx := range []int{0, 2} {
							require.Equal(t, expr.Typ.Id, expr.GetF().Args[idx].Typ.Id)
						}
					})
				}
			}
		}
	}
}

func TestBindLagLeadDefaultStringCollation(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, oid := range []types.T{types.T_varchar, types.T_text} {
			for _, scalar := range []types.T{types.T_bit, types.T_bool} {
				for _, charset := range []uint8{types.CharsetUTF8MB4UnicodeCI, types.CharsetUTF8MB3UnicodeCI, types.CharsetUTF8MB4Bin, types.CharsetUTF8} {
					for _, reverse := range []bool{false, true} {
						label := fmt.Sprintf("%s/%s/%s/charset=%d/reverse=%t", name, oid, scalar, charset, reverse)
						t.Run(label, func(t *testing.T) {
							stringType := types.NewWithCharset(oid, 100, 0, charset)
							value := &planpb.Expr{Typ: makePlan2Type(&stringType)}
							def := &planpb.Expr{Typ: planpb.Type{Id: int32(scalar)}}
							if reverse {
								value, def = def, value
							}
							expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{value, makePlan2Int64ConstExprWithType(0), def})
							require.NoError(t, err)
							for _, typ := range []planpb.Type{expr.Typ, expr.GetF().Args[0].Typ, expr.GetF().Args[2].Typ} {
								require.Equal(t, uint32(charset), typ.Charset)
								require.Equal(t, uint32(stringType.CollationVersion), typ.CollationVersion)
							}
						})
					}
				}
			}
		}
	}
}

func TestBindLagLeadDefaultTemporalDomain(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, tc := range []struct {
			name             string
			value, def, want types.Type
		}{
			{"integer timestamp", types.T_int64.ToType(), types.New(types.T_timestamp, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"decimal timestamp", types.New(types.T_decimal128, 20, 4), types.New(types.T_timestamp, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"integer date", types.T_int64.ToType(), types.T_date.ToType(), types.New(types.T_varchar, 20, 0)},
			{"bit datetime", types.New(types.T_bit, 64, 0), types.New(types.T_datetime, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"float time", types.T_float64.ToType(), types.New(types.T_time, 6, 6), types.New(types.T_text, types.MaxLongTextLen, 0)},
			{"bool date", types.T_bool.ToType(), types.T_date.ToType(), types.New(types.T_varchar, 10, 0)},
			{"year timestamp", types.T_year.ToType(), types.New(types.T_timestamp, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"time date", types.New(types.T_time, 6, 6), types.T_date.ToType(), types.New(types.T_varchar, 24, 0)},
			{"time datetime", types.New(types.T_time, 6, 6), types.New(types.T_datetime, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"time timestamp", types.New(types.T_time, 6, 6), types.New(types.T_timestamp, 6, 6), types.New(types.T_varchar, 26, 0)},
			{"time precision", types.New(types.T_time, 6, 6), types.New(types.T_time, 0, 0), types.New(types.T_time, 6, 6)},
			{"date datetime", types.T_date.ToType(), types.New(types.T_datetime, 6, 6), types.New(types.T_datetime, 6, 6)},
			{"date timestamp", types.T_date.ToType(), types.New(types.T_timestamp, 6, 6), types.New(types.T_datetime, 6, 6)},
			{"timestamp precision", types.New(types.T_timestamp, 6, 6), types.New(types.T_timestamp, 0, 0), types.New(types.T_timestamp, 6, 6)},
		} {
			for _, reverse := range []bool{false, true} {
				t.Run(name+tc.name+map[bool]string{false: "/source", true: "/default"}[reverse], func(t *testing.T) {
					value, def := tc.value, tc.def
					if reverse {
						value, def = def, value
					}
					expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
						{Typ: makePlan2Type(&value)}, makePlan2Int64ConstExprWithType(0), {Typ: makePlan2Type(&def)},
					})
					require.NoError(t, err)
					for _, typ := range []planpb.Type{expr.Typ, expr.GetF().Args[0].Typ, expr.GetF().Args[2].Typ} {
						require.Equal(t, int32(tc.want.Oid), typ.Id)
						require.Equal(t, tc.want.Scale, typ.Scale)
					}
					require.Equal(t, tc.want.Width, expr.Typ.Width)
				})
			}
		}
	}
}

func TestLagLeadPersistedDDLRebuild(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, tc := range []struct {
			sourceType, defaultSQL string
			oid                    types.T
			width, scale           int32
		}{
			{"varchar(100) collate utf8mb4_unicode_ci", "cast(7 as bit(3))", types.T_text, types.MaxLongTextLen, 0},
			{"text collate utf8mb4_unicode_ci", "true", types.T_text, types.MaxLongTextLen, 0},
			{"decimal(20,4)", "cast(2024 as year)", types.T_decimal128, 20, 4},
			{"bigint", "cast('2024-01-02 03:04:05.123456' as timestamp(6))", types.T_varchar, 26, 0},
			{"uuid", "cast('00000000-0000-0000-0000-000000000001' as binary(36))", types.T_blob, types.MaxTinyTextLen, 0},
			{"time(6)", "cast('2024-01-02' as date)", types.T_varchar, 24, 0},
			{"date", "cast('12:34:56.123456' as time(6))", types.T_varchar, 24, 0},
			{"datetime(6)", "cast('2024-01-02' as date)", types.T_datetime, 6, 6},
			{"mediumblob", "0", types.T_blob, 16777215, 0},
			{"mediumblob", "cast('x' as char(1))", types.T_blob, types.MaxMediumTextLen, 0},
			{"decimal(65,65)", "0", types.T_decimal256, 65, 65},
			{"mediumtext", "cast('{}' as json)", types.T_text, types.MaxLongTextLen, 0},
			{"varchar(100)", "cast('x' as tinytext)", types.T_text, 0, 0},
			{"decimal(5,2)", "cast(7 as bit(3))", types.T_decimal64, 5, 2},
			{"varchar(20)", "cast(7 as bit(3))", types.T_text, types.MaxLongTextLen, 0},
			{"text", "true", types.T_text, types.MaxLongTextLen, 0},
			{"blob", "false", types.T_blob, types.MaxLongTextLen, 0},
			{"text", "cast('2024-01-01' as date)", types.T_text, types.MaxLongTextLen, 0},
		} {
			t.Run(name+tc.sourceType+tc.defaultSQL, func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				source, err := buildTestCreateTableStmt(mock, "create table lag_lead_source(v "+tc.sourceType+")")
				require.NoError(t, err)
				mock.ctxt.tables["lag_lead_source"] = source
				mock.ctxt.objects["lag_lead_source"] = &ObjectRef{DbName: mock.ctxt.DefaultDatabase(), ObjName: "lag_lead_source"}
				p, err := runOneStmt(mock, t, "create table lag_lead_persisted as select "+name+"(v,0,"+tc.defaultSQL+") over () as v from lag_lead_source")
				require.NoError(t, err)
				persisted := p.GetDdl().GetCreateTable().GetTableDef()
				require.NotNil(t, persisted)
				require.Equal(t, int32(tc.oid), persisted.Cols[0].Typ.Id)
				require.Equal(t, tc.width, persisted.Cols[0].Typ.Width)
				require.Equal(t, tc.scale, persisted.Cols[0].Typ.Scale)
				// Use the same exporter as SHOW CREATE TABLE, then bind its
				// actual output rather than a hand-written equivalent schema.
				persisted.Name = "lag_lead_rebuilt"
				ddl, _, err := ConstructCreateTableSQL(&mock.ctxt, persisted, nil, false, nil)
				require.NoError(t, err)
				p, err = runOneStmt(mock, t, ddl)
				require.NoError(t, err, ddl)
				rebuilt := p.GetDdl().GetCreateTable().GetTableDef()
				require.NotNil(t, rebuilt)
				require.Equal(t, persisted.Cols[0].Typ.Id, rebuilt.Cols[0].Typ.Id)
				require.Equal(t, persisted.Cols[0].Typ.Width, rebuilt.Cols[0].Typ.Width)
				require.Equal(t, persisted.Cols[0].Typ.Scale, rebuilt.Cols[0].Typ.Scale)
				require.Equal(t, persisted.Cols[0].Typ.Charset, rebuilt.Cols[0].Typ.Charset)
				require.Equal(t, persisted.Cols[0].Typ.CollationVersion, rebuilt.Cols[0].Typ.CollationVersion)
			})
		}
	}
}

func TestLagLeadWindowLiteralIdentity(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, tc := range []struct {
			args    string
			oids    []types.T
			windows int
		}{
			{"(7,0,0.5) over (), " + name + "(7,0,'0.5') over ()", []types.T{types.T_decimal128, types.T_varchar}, 2},
			{"(7,0,'0.5') over (), " + name + "(7,0,0.5) over ()", []types.T{types.T_varchar, types.T_decimal128}, 2},
			{"(7,0,0.5) over ()", []types.T{types.T_decimal128}, 1},
			{"(7,0,'0.5') over ()", []types.T{types.T_varchar}, 1},
			{"(7,0,'0.5') over (), " + name + "(7,0,'0.5') over ()", []types.T{types.T_varchar, types.T_varchar}, 1},
		} {
			t.Run(name+tc.args, func(t *testing.T) {
				p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+name+tc.args)
				require.NoError(t, err)
				specs := allWindowSpecs(p)
				require.Len(t, specs, tc.windows)
				q := p.GetQuery()
				outputs := q.Nodes[q.Steps[0]].ProjectList
				require.Len(t, outputs, len(tc.oids))
				for i, oid := range tc.oids {
					require.Equal(t, int32(oid), outputs[i].Typ.Id)
				}
			})
		}
	}
}

func TestBindLagLeadDefaultCommonType(t *testing.T) {
	t.Run("long string default widens varchar result", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(12), expr.Typ.Width, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[0].Typ.Width, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[2].Typ.Width, name)
		}
	})

	t.Run("decimal default keeps scale over integer input", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_decimal64), expr.Typ.Id, name)
			require.Equal(t, int32(1), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal64), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(1), fn.Args[0].Typ.Scale, name)
		}
	})

	t.Run("decimal value keeps declared precision with integer default", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 4}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				makePlan2Int64ConstExprWithType(-1),
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			// -1 fits the declared integer capacity; do not widen to all of BIGINT.
			require.Equal(t, int32(types.T_decimal128), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
			require.Equal(t, int32(4), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal128), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(4), fn.Args[2].Typ.Scale, name)
		}
	})

	t.Run("domainless null default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_any)}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("shorter default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 3}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("two arg form keeps value type", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
		}
	})
}
