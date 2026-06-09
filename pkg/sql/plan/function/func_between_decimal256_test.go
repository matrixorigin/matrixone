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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue #24565: BETWEEN over decimals with mixed scales must align the three
// operands to a common decimal type (max integral width + max scale), promoting
// to decimal256 when decimal128 would overflow, instead of rejecting them.

func findBetweenFunc(t *testing.T) *FuncNew {
	for i := range supportedOperators {
		if supportedOperators[i].functionId == BETWEEN {
			return &supportedOperators[i]
		}
	}
	require.FailNow(t, "BETWEEN operator should be defined")
	return nil
}

func Test_BetweenCheckFn_Decimal256Promotion(t *testing.T) {
	betweenFunc := findBetweenFunc(t)

	// DECIMAL(38,0) BETWEEN DECIMAL(38,30) AND DECIMAL(38,30): 38 integral + 30
	// scale = 68 digits -> DECIMAL256(68,30). Previously rejected because the
	// value scale (0) differed from the bound scale (30).
	res := betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 38, 0),
		types.New(types.T_decimal128, 38, 30),
		types.New(types.T_decimal128, 38, 30),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.Equal(t, types.T_decimal256, ft.Oid)
		require.Equal(t, int32(30), ft.Scale)
		require.Equal(t, int32(68), ft.Width)
	}

	// DECIMAL(38,0) BETWEEN DECIMAL(38,38) AND DECIMAL(38,38): 38 + 38 = 76 ->
	// DECIMAL256(76,38).
	res = betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 38, 0),
		types.New(types.T_decimal128, 38, 38),
		types.New(types.T_decimal128, 38, 38),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.Equal(t, types.T_decimal256, ft.Oid)
		require.Equal(t, int32(38), ft.Scale)
		require.Equal(t, int32(76), ft.Width)
	}
}

func Test_BetweenCheckFn_Decimal_MixedFamilyAndInteger(t *testing.T) {
	betweenFunc := findBetweenFunc(t)

	// mixed family (dec64 value, dec128 bounds) must not be rejected; all three
	// aligned to a common decimal type at the max scale.
	res := betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal64, 18, 2),
		types.New(types.T_decimal128, 38, 4),
		types.New(types.T_decimal128, 38, 4),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.True(t, ft.Oid.IsDecimal())
		require.Equal(t, res.finalType[0].Oid, ft.Oid)
		require.Equal(t, int32(4), ft.Scale)
	}

	// decimal value with integer bounds stays numeric and binds to a decimal.
	res = betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 20, 2),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	for _, ft := range res.finalType {
		require.True(t, ft.Oid.IsDecimal())
	}
}

// Test_BetweenCheckFn_NonDecimalUnchanged ensures the decimal fast-path does not
// disturb the existing non-decimal behaviour (a float bound keeps the original
// fixedTypeCastRule1 path).
func Test_BetweenCheckFn_NonDecimalUnchanged(t *testing.T) {
	betweenFunc := findBetweenFunc(t)

	// decimal value with float bounds: not all-decimal/integer, so it falls
	// through to the float64 comparison path instead of the decimal fast-path.
	res := betweenFunc.checkFn(betweenFunc.Overloads, []types.Type{
		types.New(types.T_decimal128, 20, 2),
		types.T_float64.ToType(),
		types.T_float64.ToType(),
	})
	require.Equal(t, succeedWithCast, res.status)
	require.Len(t, res.finalType, 3)
	require.Equal(t, types.T_float64, res.finalType[0].Oid)
}

func Test_BetweenDecimalHelpers(t *testing.T) {
	require.True(t, isDecimalOrInteger(types.New(types.T_decimal128, 38, 2)))
	require.True(t, isDecimalOrInteger(types.T_int64.ToType()))
	require.False(t, isDecimalOrInteger(types.T_float64.ToType()))
	require.False(t, isDecimalOrInteger(types.T_varchar.ToType()))

	require.Equal(t, types.T_decimal64, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal64, 18, 2), types.T_int64.ToType()}))
	require.Equal(t, types.T_decimal128, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 38, 2)}))
	require.Equal(t, types.T_decimal256, widestDecimalFamily([]types.Type{
		types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 76, 2)}))
}

// Test_BetweenImpl_Decimal256_NonConst exercises the runtime decimal256 path of
// BETWEEN with a non-constant left operand.
func Test_BetweenImpl_Decimal256_NonConst(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	p0 := vector.NewVec(types.New(types.T_decimal256, 76, 0))
	require.NoError(t, vector.AppendFixedList(p0, []types.Decimal256{
		types.Decimal256FromInt64(5),
		types.Decimal256FromInt64(50),
		types.Decimal256FromInt64(500),
	}, nil, mp))
	p1, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 0), types.Decimal256FromInt64(1), 3, mp)
	require.NoError(t, err)
	p2, err := vector.NewConstFixed(types.New(types.T_decimal256, 76, 0), types.Decimal256FromInt64(100), 3, mp)
	require.NoError(t, err)

	w := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, w.PreExtendAndReset(3))
	require.NoError(t, betweenImpl([]*vector.Vector{p0, p1, p2}, w, proc, 3, nil))

	rs := vector.MustFunctionResult[bool](w)
	col := vector.MustFixedColWithTypeCheck[bool](rs.GetResultVector())
	require.Equal(t, []bool{true, true, false}, col[:3])
}
