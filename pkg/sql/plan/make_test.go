// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func Test_rewriteDecimalTypeIfNecessary(t *testing.T) {
	t1 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id: int32(types.T_decimal64),
	})
	require.Equal(t, []int32{t1.Scale, t1.Width}, []int32{2, 6})
	t2 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id: int32(types.T_decimal128),
	})
	require.Equal(t, []int32{t2.Scale, t2.Width}, []int32{10, 38})
	t3 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id:    int32(types.T_decimal64),
		Scale: 10,
	})
	require.Equal(t, []int32{t3.Scale, t3.Width}, []int32{10, 0})
	t4 := rewriteDecimalTypeIfNecessary(&plan.Type{
		Id:    int32(types.T_decimal128),
		Width: 18,
	})
	require.Equal(t, []int32{t4.Scale, t4.Width}, []int32{0, 18})
}

func TestPlanTypeCharsetRoundTrip(t *testing.T) {
	for _, charset := range []uint8{
		types.CharsetLegacy,
		types.CharsetBinary,
		types.CharsetUTF8MB4Bin,
		types.CharsetUTF8,
	} {
		original := types.NewWithCharset(types.T_varchar, 32, 0, charset)
		planType := makePlan2Type(&original)
		require.Equal(t, uint32(charset), planType.Charset)
		require.Equal(t, original, makeTypeByPlan2Type(planType))
		encoded, err := planType.Marshal()
		require.NoError(t, err)
		var decoded plan.Type
		require.NoError(t, decoded.Unmarshal(encoded))
		require.Equal(t, planType, decoded)
	}

	// Charset was absent from older plans. Keep the OID-derived binary default.
	legacyBinary := makeTypeByPlan2Type(plan.Type{Id: int32(types.T_binary), Width: 8})
	require.Equal(t, types.CharsetBinary, legacyBinary.Charset)
	legacyText := makeTypeByPlan2Type(plan.Type{Id: int32(types.T_varchar), Width: 8})
	require.Equal(t, types.CharsetLegacy, legacyText.Charset)
}

func TestNewStringExpressionsAndSerializedColumnsHaveExplicitCharsets(t *testing.T) {
	require.Equal(t, uint32(types.CharsetUTF8),
		makePlan2StringConstExprWithType("value").Typ.Charset)
	require.Equal(t, plan.StringLiteralForm_STRING_LITERAL_TEXT,
		makePlan2StringConstExprWithType("value").GetLit().GetLiteralForm())
	require.Equal(t, uint32(types.CharsetBinary),
		makePlan2StringConstExprWithType("\xff", true).Typ.Charset)
	require.Equal(t, plan.StringLiteralForm_STRING_LITERAL_HEX,
		makePlan2StringConstExprWithType("\xff", true).GetLit().GetLiteralForm())
	require.Equal(t, uint32(types.CharsetBinary),
		MakeHiddenColDefByName("__mo_serialized").Typ.Charset)
}

func TestStringLiteralFormRoundTripAndDeepCopy(t *testing.T) {
	original := makePlan2VarBinaryConstExprWithType("value")
	original.GetLit().LiteralForm = plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER

	encoded, err := original.Marshal()
	require.NoError(t, err)
	decoded := &plan.Expr{}
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, original.GetLit().GetLiteralForm(), decoded.GetLit().GetLiteralForm())

	copied := DeepCopyExpr(original)
	require.NotSame(t, original.GetLit(), copied.GetLit())
	require.Equal(t, original.GetLit().GetLiteralForm(), copied.GetLit().GetLiteralForm())
}

func TestMakeGeneratedPlan2TypeUsesExplicitTextCharset(t *testing.T) {
	varchar := makeGeneratedPlan2Type(types.T_varchar, 128, 0, true)
	require.Equal(t, int32(types.T_varchar), varchar.Id)
	require.Equal(t, int32(128), varchar.Width)
	require.True(t, varchar.NotNullable)
	require.Equal(t, uint32(types.CharsetUTF8), varchar.Charset)

	text := makeGeneratedPlan2Type(types.T_text, types.MaxVarcharLen, 0, false)
	require.Equal(t, uint32(types.CharsetUTF8), text.Charset)

	integer := makeGeneratedPlan2Type(types.T_int64, 0, 0, false)
	require.Equal(t, uint32(types.CharsetLegacy), integer.Charset)
}

func Test_MakePlan2Vecf32ConstExprWithType(t *testing.T) {
	t1 := MakePlan2Vecf32ConstExprWithType("[1,2,3]", 3)
	actual := t1.Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_Sval).Sval
	require.Equal(t, "[1,2,3]", actual)
}

func Test_MakePlan2Vecf64ConstExprWithType(t *testing.T) {
	t1 := MakePlan2Vecf64ConstExprWithType("[1,2,3]", 3)
	actual := t1.Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_Sval).Sval
	require.Equal(t, "[1,2,3]", actual)
}

func Test_MakePlan2VecNarrowConstExprWithType(t *testing.T) {
	cases := []struct {
		name string
		fn   func(string, int32) *plan.Expr
		oid  types.T
	}{
		{"bf16", MakePlan2VecBf16ConstExprWithType, types.T_array_bf16},
		{"f16", MakePlan2VecF16ConstExprWithType, types.T_array_float16},
		{"int8", MakePlan2VecInt8ConstExprWithType, types.T_array_int8},
		{"uint8", MakePlan2VecUint8ConstExprWithType, types.T_array_uint8},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := c.fn("[1,2,3]", 3)
			actual := e.Expr.(*plan.Expr_Lit).Lit.GetValue().(*plan.Literal_Sval).Sval
			require.Equal(t, "[1,2,3]", actual)
			require.Equal(t, int32(c.oid), e.Typ.Id)
			require.Equal(t, int32(3), e.Typ.Width)
			require.True(t, e.Typ.NotNullable)
		})
	}
}

func Test_isSameColumnType(t *testing.T) {
	require.True(t, isSameColumnType(
		plan.Type{Id: int32(types.T_varchar), Width: 32},
		plan.Type{Id: int32(types.T_varchar), Width: 32},
	))

	require.False(t, isSameColumnType(
		plan.Type{Id: int32(types.T_varchar), Width: 32},
		plan.Type{Id: int32(types.T_varchar), Width: 64},
	))

	require.False(t, isSameColumnType(
		plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2},
		plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 4},
	))

	require.False(t, isSameColumnType(
		plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"},
		plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,c"},
	))

	require.False(t, isSameColumnType(
		plan.Type{Id: int32(types.T_varchar), Width: 32, Charset: uint32(types.CharsetUTF8)},
		plan.Type{Id: int32(types.T_varchar), Width: 32, Charset: uint32(types.CharsetBinary)},
	))

	require.False(t, isSameColumnType(
		*geometryPlanType(types.T_geometry, "POINT", 4326, true),
		*geometryPlanType(types.T_geometry, "POINT", 0, true),
	))
}
