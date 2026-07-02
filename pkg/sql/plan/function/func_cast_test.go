// Copyright 2022 Matrix Origin
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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_StringToFloatInvalidConversion(t *testing.T) {
	// Test invalid string to float conversion (should return 0, not error)
	// This matches MySQL behavior where invalid strings convert to 0
	proc := testutil.NewProcess(t)

	testCases := []tcTemp{
		{
			info: "cast invalid string 'a' to float64 should return 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a"}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{false}),
		},
		{
			info: "cast invalid string 'abc123' to float64 should return 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abc123"}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{false}),
		},
		{
			info: "cast empty string to float64 should return 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{false}),
		},
		{
			info: "cast date string '2020-01-01' to float64 should return 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2020-01-01"}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{false}),
		},
		{
			info: "cast datetime string '2020-01-01 12:34:56' to float64 should return 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2020-01-01 12:34:56"}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := tcc.Run()
			require.True(t, succeed, tc.info, info)
		})
	}
}

func Test_BinaryToFloatInvalidConversion(t *testing.T) {
	// Test binary to float conversion with invalid hex values
	// Should return 0, not error (MySQL non-strict mode behavior)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	tests := []struct {
		name      string
		inputs    []string
		nulls     []uint64
		want      []float64
		wantNulls []uint64
	}{
		{
			name:      "overflow binary (>8 bytes) should return 0",
			inputs:    []string{"this-is-a-very-long-string"}, // > 8 bytes, will overflow uint64
			nulls:     []uint64{},
			want:      []float64{0},
			wantNulls: []uint64{}, // Not null, just 0
		},
		{
			name:      "date string as binary should work",
			inputs:    []string{"2020-01"}, // 7 bytes, fits in uint64
			nulls:     []uint64{},
			want:      []float64{1.4126740950298672e+16}, // hex of "2020-01"
			wantNulls: []uint64{},
		},
		{
			name:      "single char should work",
			inputs:    []string{"A"}, // 'A' = 0x41
			nulls:     []uint64{},
			want:      []float64{65}, // 0x41 = 65
			wantNulls: []uint64{},
		},
		{
			name:      "empty binary should return 0",
			inputs:    []string{""},
			nulls:     []uint64{},
			want:      []float64{0},
			wantNulls: []uint64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert strings to [][]byte
			inputs := make([][]byte, len(tt.inputs))
			for i, s := range tt.inputs {
				inputs[i] = []byte(s)
			}

			inputVec := testutil.MakeVarlenaVector(inputs, tt.nulls, types.T_blob.ToType(), mp)
			defer inputVec.Free(mp)
			inputVec.SetIsBin(true)

			from := vector.GenerateFunctionStrParameter(inputVec)

			resultType := types.T_float64.ToType()
			to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[float64])
			defer to.Free()
			err := to.PreExtendAndReset(len(tt.inputs))
			require.NoError(t, err)

			err = strToFloat(ctx, from, to, 64, len(tt.inputs), nil)
			require.NoError(t, err, "should not return error for invalid binary")

			resultVec := to.GetResultVector()
			result := vector.MustFixedColNoTypeCheck[float64](resultVec)
			require.Equal(t, tt.want, result)

			nulls := resultVec.GetNulls()
			require.Equal(t, tt.wantNulls, nulls.ToArray())
		})
	}
}

func Test_CastToDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal256Type := types.New(types.T_decimal256, 65, 30)
	expectedFromString, err := types.ParseDecimal256("12345678901234567890123456789012345.123456789012345678901234567890", 65, 30)
	require.NoError(t, err)
	expectedFromInt := types.Decimal256FromInt64(42)
	expectedFromInt, err = expectedFromInt.Scale(30)
	require.NoError(t, err)

	testCases := []tcTemp{
		{
			info: "cast string to decimal256",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"12345678901234567890123456789012345.123456789012345678901234567890"}, []bool{false}),
				NewFunctionTestInput(decimal256Type, []types.Decimal256{}, []bool{}),
			},
			expect: NewFunctionTestResult(decimal256Type, false,
				[]types.Decimal256{expectedFromString}, []bool{false}),
		},
		{
			info: "cast int64 to decimal256",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{42}, []bool{false}),
				NewFunctionTestInput(decimal256Type, []types.Decimal256{}, []bool{}),
			},
			expect: NewFunctionTestResult(decimal256Type, false,
				[]types.Decimal256{expectedFromInt}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := tcc.Run()
			require.True(t, succeed, tc.info, info)
		})
	}
}

func Test_CastFromDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimal256Type := types.New(types.T_decimal256, 65, 2)
	value, err := types.ParseDecimal256("42.00", 65, 2)
	require.NoError(t, err)

	testCases := []tcTemp{
		{
			info: "cast decimal256 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(decimal256Type, []types.Decimal256{value}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{42}, []bool{false}),
		},
		{
			info: "cast decimal256 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(decimal256Type, []types.Decimal256{value}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{42}, []bool{false}),
		},
		{
			info: "cast decimal256 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(decimal256Type, []types.Decimal256{value}, []bool{false}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{42}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := tcc.Run()
			require.True(t, succeed, tc.info, info)
		})
	}
}

func TestStAsWKBAndGeomFromWKB(t *testing.T) {
	proc := testutil.NewProcess(t)

	pointWKB := string(encodeGeometryPayload("POINT(1 2)", 0, false))
	lineWKB := string(encodeGeometryPayload("LINESTRING(0 0,1 1)", 0, false))

	// ST_AsWKB(geometry) -> standard WKB blob equal to the stored WKB.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{false})},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{pointWKB}, []bool{false}),
		StAsWKB)
	ok, info := tc.Run()
	require.True(t, ok, info)

	// ST_GeomFromWKB(wkb) -> geometry rendering as the original WKT.
	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{lineWKB}, []bool{false})},
		NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"LINESTRING(0 0,1 1)"}, []bool{false}),
		StGeomFromWKB)
	ok2, info2 := tc2.Run()
	require.True(t, ok2, info2)
}

func TestGeometry32EncodeDecode(t *testing.T) {
	// Float32 WKB is shorter than float64 (4 vs 8 bytes per ordinate) and both
	// decode back to the same WKT.
	f32 := encodeGeometryPayloadFloat32("POINT(1 2)")
	f64 := encodeGeometryPayload("POINT(1 2)", 0, false)
	require.Len(t, f32, 13)
	require.Len(t, f64, 21)

	got32, _, _, err := decodeGeometryPayload(f32)
	require.NoError(t, err)
	require.Equal(t, "POINT(1 2)", got32)

	got64, _, _, err := decodeGeometryPayload(f64)
	require.NoError(t, err)
	require.Equal(t, "POINT(1 2)", got64)
}

func Test_CastGeometry32(t *testing.T) {
	proc := testutil.NewProcess(t)

	// A geometry32 (float32 WKB) source value.
	pointF32 := string(encodeGeometryPayloadFloat32("POINT(5 6)"))

	testCases := []tcTemp{
		{
			info: "cast varchar to geometry32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT(1 2)"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry32.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_geometry32.ToType(), false, []string{"POINT(1 2)"}, []bool{false}),
		},
		{
			info: "cast geometry to geometry32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0,1 1)"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry32.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_geometry32.ToType(), false, []string{"LINESTRING(0 0,1 1)"}, []bool{false}),
		},
		{
			info: "cast geometry32 to geometry (float32 WKB source)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_geometry32.ToType(), []string{pointF32}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(5 6)"}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := tcc.Run()
			require.True(t, succeed, tc.info, info)
		})
	}
}

// Test_CastGeometryPrecisionBothWays proves the cast actually converts the
// coordinate width in each direction: a POINT is 13 bytes as float32 WKB and
// 21 bytes as float64 WKB.
func Test_CastGeometryPrecisionBothWays(t *testing.T) {
	proc := testutil.NewProcess(t)

	pointF32 := string(encodeGeometryPayloadFloat32("POINT(5 6)"))    // float32 WKB
	pointF64 := string(encodeGeometryPayload("POINT(5 6)", 0, false)) // float64 WKB

	castLen := func(srcType types.Type, src string, dstType types.Type) int {
		t.Helper()
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(srcType, []string{src}, []bool{false}),
				NewFunctionTestInput(dstType, []string{}, []bool{}),
			},
			NewFunctionTestResult(dstType, false, []string{"POINT(5 6)"}, []bool{false}), NewCast)
		ok, info := tc.Run()
		require.True(t, ok, info)
		return len(tc.GetResultVectorDirectly().GetBytesAt(0))
	}

	geom := types.T_geometry.ToType()
	geom32 := types.T_geometry32.ToType()

	// geometry (float64 WKB) -> geometry32 must shrink to float32 WKB.
	require.Equal(t, 13, castLen(geom, pointF64, geom32))
	// geometry32 (float32 WKB) -> geometry must grow to float64 WKB.
	require.Equal(t, 21, castLen(geom32, pointF32, geom))
	// idempotent same-type casts keep their width.
	require.Equal(t, 13, castLen(geom32, pointF32, geom32))
	require.Equal(t, 21, castLen(geom, pointF64, geom))
}

func Test_CastVarcharToGeometry(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []tcTemp{
		{
			info: "cast varchar to geometry",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT(1 1)"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 1)"}, []bool{false}),
		},
		{
			info: "cast geometry to geometry",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(2 2)"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(2 2)"}, []bool{false}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := tcc.Run()
			require.True(t, succeed, tc.info, info)
		})
	}

	require.True(t, IfTypeCastSupported(types.T_varchar, types.T_geometry))
	require.True(t, IfTypeCastSupported(types.T_geometry, types.T_geometry))

	t.Run("cast null to geometry", func(t *testing.T) {
		nullVec := vector.NewConstNull(types.T_any.ToType(), 1, proc.Mp())
		defer nullVec.Free(proc.Mp())

		targetType := vector.NewConstNull(types.T_geometry.ToType(), 1, proc.Mp())
		defer targetType.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_geometry.ToType(), proc.Mp())
		defer result.Free()

		err := result.PreExtendAndReset(1)
		require.NoError(t, err)
		err = NewCast([]*vector.Vector{nullVec, targetType}, result, proc, 1, nil)
		require.NoError(t, err)

		resultVec := result.GetResultVector()
		require.Equal(t, types.T_geometry, resultVec.GetType().Oid)
		require.True(t, resultVec.GetNulls().Contains(0))
	})

	t.Run("reject internal geometry payload syntax", func(t *testing.T) {
		inputVec := testutil.MakeVarcharVector([]string{"SRID=4326;POINT(1 1)"}, nil, proc.Mp())
		defer inputVec.Free(proc.Mp())

		targetType := vector.NewConstNull(types.T_geometry.ToType(), 1, proc.Mp())
		defer targetType.Free(proc.Mp())

		result := vector.NewFunctionResultWrapper(types.T_geometry.ToType(), proc.Mp())
		defer result.Free()

		err := result.PreExtendAndReset(1)
		require.NoError(t, err)
		err = NewCast([]*vector.Vector{inputVec, targetType}, result, proc, 1, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid geometry type")
	})
}

func initCastTestCase() []tcTemp {
	var testCases []tcTemp

	// used on case.
	f1251ToDec128, _ := types.Decimal128FromFloat64(125.1, 38, 1)
	s01date, _ := types.ParseDateCast("2004-04-03")
	s02date, _ := types.ParseDateCast("2021-10-03")
	s01ts, _ := types.ParseTimestamp(time.Local, "2020-08-23 11:52:21", 6)

	castToSameTypeCases := []tcTemp{
		// cast to same type.
		{
			info: "int8 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{-23}, []bool{false}),
		},
		{
			info: "int16 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{-23}, []bool{false}),
		},
		{
			info: "int32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{-23}, []bool{false}),
		},
		{
			info: "int64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-23}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-23}, []bool{false}),
		},
		{
			info: "uint8 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{23}, []bool{false}),
		},
		{
			info: "uint16 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{23}, []bool{false}),
		},
		{
			info: "uint32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{23}, []bool{false}),
		},
		{
			info: "uint64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{23}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{23}, []bool{false}),
		},
		{
			info: "float32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.5}, []bool{false}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{23.5}, []bool{false}),
		},
		{
			info: "float64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.5}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{23.5}, []bool{false}),
		},
		{
			info: "date to date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{729848}, []bool{false}),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false,
				[]types.Date{729848}, []bool{false}),
		},
		{
			info: "datetime to datetime",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{66122056321728512}, []bool{false}),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, []bool{}),
			},
			// When casting to datetime(0), .728512 microseconds should round to next second
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false,
				[]types.Datetime{66122056322000000}, []bool{false}),
		},
		{
			info: "timestamp to timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(),
					[]types.Timestamp{66122026122739712}, []bool{false}),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, []bool{}),
			},
			// When casting to timestamp(0), .739712 microseconds should round to next second
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false,
				[]types.Timestamp{66122026123000000}, []bool{false}),
		},
		{
			info: "time to time",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_time.ToType(),
					[]types.Time{661220261227}, []bool{false}),
				NewFunctionTestInput(types.T_time.ToType(), []types.Time{}, []bool{}),
			},
			// When casting to time(0), .261227 microseconds should be truncated (2 < 5, no rounding)
			expect: NewFunctionTestResult(types.T_time.ToType(), false,
				[]types.Time{661220000000}, []bool{false}),
		},
		{
			info: "vecf32 to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{}}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "vecf64 to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}}, []bool{false}),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{}}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}}, []bool{false}),
		},
		{
			info: "bit to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{23}, []bool{false}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false, []uint64{23}, []bool{false}),
		},
	}
	castInt8ToOthers := []tcTemp{
		// test cast int8 to others.
		{
			info: "int8 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int8 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int8 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(), []int8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt16ToOthers := []tcTemp{
		// test cast int16 to others.
		{
			info: "int16 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int16 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(),
					[]int16{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int16 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int16.ToType(), []int16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt32ToOthers := []tcTemp{
		// test cast int32 to others.
		{
			info: "int32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(), []int32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castInt64ToOthers := []tcTemp{
		// test cast int64 to others.
		{
			info: "int64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "int64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "int64 to char truncates to width",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{12345}, []bool{false}),
				NewFunctionTestInput(types.New(types.T_char, 3, 0), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.New(types.T_char, 3, 0), false,
				[]string{"123"}, []bool{false}),
		},
	}
	castUint8ToOthers := []tcTemp{
		// test cast uint8 to others.
		{
			info: "uint8 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint8 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(),
					[]uint8{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint8 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint16ToOthers := []tcTemp{
		// test cast uint16 to others.
		{
			info: "uint16 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint16 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(),
					[]uint16{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint16 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint32ToOthers := []tcTemp{
		// test cast uint32 to others.
		{
			info: "uint32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castUint64ToOthers := []tcTemp{
		// test cast uint64 to others.
		{
			info: "uint64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "uint64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "uint64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.New(types.T_bit, 5, 0), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), true, []uint64{}, []bool{}),
		},
	}
	castFloat32ToOthers := []tcTemp{
		// test cast float32 to others.
		{
			info: "float32 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float32 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{23.56, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float32 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{125.1, 0}, []bool{false, true}),
				NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float32 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(), []float32{125.4999, 125.55555, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castFloat64ToOthers := []tcTemp{
		// test cast float64 to others.
		{
			info: "float64 to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "float64 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{23.56, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_char.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_char.ToType(), false,
				[]string{"23.56", "126", "0"}, []bool{false, false, true}),
		},
		{
			info: "float64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{125.1, 0}, []bool{false, true}),
				NewFunctionTestInput(types.New(types.T_decimal128, 8, 1), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 8, 1), false,
				[]types.Decimal128{f1251ToDec128, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
		{
			info: "float64 to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{125.4999, 125.55555, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{125, 126, 0}, []bool{false, false, true}),
		},
	}
	castStrToOthers := []tcTemp{
		{
			info: "str type to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1501, 16, 0}, []bool{false, false, true}),
		},
		{
			info: "str type to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "16"}, nil),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{15, 16}, []bool{false, false}),
		},
		{
			info: "str type to uint64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501", "+0012", "16", ""}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{1501, 12, 16, 0}, []bool{false, false, false, true}),
		},
		{
			info: "str type to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"15", "  -7.5e1", "16"}, nil),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{15, -75, 16}, []bool{false, false, false}),
		},
		{
			info: "str type to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "  +7e0", "16", ""}, []bool{false, false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1501.12, 7, 16, 0}, []bool{false, false, false, true}),
		},
		{
			info: "str type to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"1501.12", "16", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_text.ToType(), []string{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_text.ToType(), false,
				[]string{"1501.12", "16", ""}, []bool{false, false, true}),
		},
		{
			info: "str type to date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2004-04-03", "2004-04-03", "2021-10-03"},
					[]bool{false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
			},
			expect: NewFunctionTestResult(
				types.T_date.ToType(), false,
				[]types.Date{
					s01date, s01date, s02date,
				},
				[]bool{false, false, false}),
		},
		{
			info: "str type to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"[1,2,3]", "[4,5,6]"}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
		{
			info: "str type to bit",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"15", "ab"}, nil),
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_bit.ToType(), false,
				[]uint64{12597, 24930}, []bool{false, false}),
		},
	}
	castDecToOthers := []tcTemp{
		{
			info: "decimal64 to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 0),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 0),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 0), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal64(10,5) to decimal64(10, 4)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(33333300)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 4),
					[]types.Decimal64{0}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal64, 10, 4), false,
				[]types.Decimal64{types.Decimal64(3333330)}, nil),
		},
		{
			info: "decimal64(10,5) to decimal128(20, 5)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(333333000)}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 5), false,
				[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
		},
		{
			info: "decimal128(20,5) to decimal128(20, 4)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 5),
					[]types.Decimal128{{B0_63: 333333000, B64_127: 0}}, nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 20, 4), false,
				[]types.Decimal128{{B0_63: 33333300, B64_127: 0}}, nil),
		},
		{
			info: "decimal64 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 10, 5),
					[]types.Decimal64{types.Decimal64(1234)}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"0.01234"}, nil),
		},
		{
			info: "decimal128 to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal128, 20, 2),
					[]types.Decimal128{{B0_63: 1234, B64_127: 0}}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"12.34"}, nil),
		},
	}
	castTimestampToOthers := []tcTemp{
		{
			info: "timestamp to str type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.T_timestamp.ToType(),
					[]types.Timestamp{s01ts}, nil),
				NewFunctionTestInput(
					types.T_varchar.ToType(), []string{}, nil),
			},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"2020-08-23 11:52:21"}, nil),
		},
	}

	castArrayFloat32ToOthers := []tcTemp{
		{
			info: "vecf32 type to vecf64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castArrayFloat64ToOthers := []tcTemp{
		{
			info: "vecf64 type to vecf32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{{1, 2, 3}, {4, 5, 6}}, []bool{false, false}),
		},
	}

	castBitToOthers := []tcTemp{
		// test cast bit to others.
		{
			info: "bit to int8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 256, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), true, []int8{}, []bool{}),
		},
		{
			info: "bit to int16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int16.ToType(), []int16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int16.ToType(), false,
				[]int16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int32.ToType(), []int32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int32.ToType(), false,
				[]int32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint8",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint16",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint16.ToType(), []uint16{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint16.ToType(), false,
				[]uint16{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to uint32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_uint32.ToType(), []uint32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_uint32.ToType(), false,
				[]uint32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float32.ToType(), []float32{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 126, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{125, 126, 0}, []bool{false, false, true}),
		},
		{
			info: "bit to decimal128",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bit.ToType(), []uint64{125, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_decimal128.ToType(), []types.Decimal128{}, []bool{}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 125, B64_127: 0}, {B0_63: 0, B64_127: 0}},
				[]bool{false, true}),
		},
	}

	// init the testCases
	testCases = append(testCases, castFloat64ToOthers...)
	testCases = append(testCases, castFloat32ToOthers...)
	testCases = append(testCases, castStrToOthers...)
	testCases = append(testCases, castDecToOthers...)
	testCases = append(testCases, castTimestampToOthers...)
	testCases = append(testCases, castArrayFloat32ToOthers...)
	testCases = append(testCases, castArrayFloat64ToOthers...)
	testCases = append(testCases, castBitToOthers...)
	testCases = append(testCases, castToSameTypeCases...)
	testCases = append(testCases, castInt8ToOthers...)
	testCases = append(testCases, castInt16ToOthers...)
	testCases = append(testCases, castInt32ToOthers...)
	testCases = append(testCases, castInt64ToOthers...)
	testCases = append(testCases, castUint8ToOthers...)
	testCases = append(testCases, castUint16ToOthers...)
	testCases = append(testCases, castUint32ToOthers...)
	testCases = append(testCases, castUint64ToOthers...)

	return testCases
}

func TestCast(t *testing.T) {
	testCases := initCastTestCase()

	// do the test work.
	proc := testutil.NewProcess(t)
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func BenchmarkCast(b *testing.B) {
	testCases := initCastTestCase()
	proc := testutil.NewProcess(b)

	b.StartTimer()
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, NewCast)
		_ = fcTC.BenchMarkRun()
	}
	b.StopTimer()
}

func Test_strToSigned_Binary(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	tests := []struct {
		name      string
		inputs    [][]byte
		nulls     []uint64
		bitSize   int
		want      []int64
		wantNulls []uint64
		wantErr   bool
		errMsg    string
	}{
		{
			name:    "empty slice",
			inputs:  [][]byte{{}},
			bitSize: 64,
			wantErr: true,
			errMsg:  "invalid arg",
		},
		{
			name:    "out of range length",
			inputs:  [][]byte{{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}},
			bitSize: 64,
			wantErr: true,
			errMsg:  "out of range",
		},
		{
			name:    "max int64",
			inputs:  [][]byte{{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
			bitSize: 64,
			want:    []int64{math.MaxInt64},
		},
		{
			name:    "valid binary",
			inputs:  [][]byte{{0x12, 0x34}},
			bitSize: 64,
			want:    []int64{0x1234},
		},
		{
			name:      "null value",
			inputs:    [][]byte{nil, {0x12}},
			nulls:     []uint64{0},
			bitSize:   64,
			want:      []int64{0, 0x12},
			wantNulls: []uint64{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVec := testutil.MakeVarlenaVector(tt.inputs, tt.nulls, types.T_blob.ToType(), mp)
			defer inputVec.Free(mp)
			inputVec.SetIsBin(true)

			from := vector.GenerateFunctionStrParameter(inputVec)

			resultType := types.T_int64.ToType()
			to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[int64])
			defer to.Free()
			err := to.PreExtendAndReset(len(tt.inputs))
			require.NoError(t, err)

			err = strToSigned(ctx, from, to, tt.bitSize, len(tt.inputs), nil)

			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)

			resultVec := to.GetResultVector()

			r := vector.GenerateFunctionFixedTypeParameter[int64](resultVec)

			for i := 0; i < len(tt.want); i++ {
				want := tt.want[i]
				get, null := r.GetValue(uint64(i))

				if contains(tt.wantNulls, uint64(i)) {
					require.True(t, null, "row %d should be null", i)
				} else {
					require.False(t, null, "row %d should not be null", i)
					require.Equal(t, want, get, "row %d value not match", i)
				}
			}

			resultNulls := to.GetResultVector().GetNulls()
			if len(tt.wantNulls) > 0 {
				for _, pos := range tt.wantNulls {
					require.True(t, resultNulls.Contains(pos))
				}
			} else {
				require.True(t, resultNulls.IsEmpty())
			}
		})
	}
}

func contains(slice []uint64, item uint64) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func Benchmark_strToSigned_Binary(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	benchCases := []struct {
		name    string
		inputs  [][]byte
		nulls   []uint64
		bitSize int
	}{
		{
			name:    "small_binary",
			inputs:  [][]byte{{0x12, 0x34}, {0x56, 0x78}},
			bitSize: 64,
		},
		{
			name:    "max_int64",
			inputs:  [][]byte{{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
			bitSize: 64,
		},
		{
			name:    "mixed_with_nulls",
			inputs:  [][]byte{nil, {0x12}, {0x34}, nil, {0x56}},
			nulls:   []uint64{0, 3},
			bitSize: 64,
		},
		{
			name: "large_input_set",
			inputs: func() [][]byte {
				data := make([][]byte, 1000)
				for i := range data {
					data[i] = []byte{byte(i % 256), byte((i + 1) % 256)}
				}
				return data
			}(),
			bitSize: 64,
		},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			inputVec := testutil.MakeVarlenaVector(bc.inputs, bc.nulls, types.T_blob.ToType(), mp)
			defer inputVec.Free(mp)
			inputVec.SetIsBin(true)

			resultType := types.T_int64.ToType()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[int64])
				err := to.PreExtendAndReset(len(bc.inputs))
				if err != nil {
					b.Fatalf("PreExtendAndReset failed: %v", err)
				}

				from := vector.GenerateFunctionStrParameter(inputVec)

				err = strToSigned(ctx, from, to, bc.bitSize, len(bc.inputs), nil)
				if err != nil {
					b.Fatalf("strToSigned failed: %v", err)
				}

				to.Free()
			}
		})
	}
}

// Test_strToStr_TextToCharVarchar tests that TEXT type can be cast to CHAR/VARCHAR
// without length validation errors, even when the string length exceeds the target length.
// This is important for UPDATE operations on TEXT columns with CONCAT operations.
func Test_strToStr_TextToCharVarchar(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// Helper function to create long strings
	longString260 := strings.Repeat("a", 260) // 260 characters
	longString100 := strings.Repeat("b", 100) // 100 characters

	tests := []struct {
		name      string
		inputs    []string
		nulls     []uint64
		fromType  types.Type
		toType    types.Type
		want      []string
		wantNulls []uint64
		wantErr   bool
		errMsg    string
	}{
		{
			name:     "TEXT to CHAR(255) with length 260 - should truncate",
			inputs:   []string{longString260},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 255, 0),
			want:     []string{strings.Repeat("a", 255)},
			wantErr:  false,
		},
		{
			name:     "TEXT to VARCHAR(255) with length 260 - should truncate",
			inputs:   []string{longString260},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 255, 0),
			want:     []string{strings.Repeat("a", 255)},
			wantErr:  false,
		},
		{
			name:      "TEXT to CHAR(255) with NULL - should handle NULL",
			inputs:    []string{"", "test"},
			nulls:     []uint64{0},
			fromType:  types.T_text.ToType(),
			toType:    types.New(types.T_char, 255, 0),
			want:      []string{"", "test"},
			wantNulls: []uint64{0},
			wantErr:   false,
		},
		{
			name:     "VARCHAR to CHAR(10) with length 100 - should truncate",
			inputs:   []string{longString100},
			fromType: types.New(types.T_varchar, 100, 0),
			toType:   types.New(types.T_char, 10, 0),
			want:     []string{strings.Repeat("b", 10)},
		},
		{
			name:     "TEXT to CHAR(1) with length > 1 - should truncate",
			inputs:   []string{"ab"},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 1, 0),
			want:     []string{"a"},
		},
		{
			name:     "TEXT to CHAR(10) with length 100 - should truncate",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 10, 0),
			want:     []string{strings.Repeat("b", 10)},
		},
		{
			name:     "TEXT to VARCHAR(10) with length 100 - should truncate",
			inputs:   []string{longString100},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 10, 0),
			want:     []string{strings.Repeat("b", 10)},
		},
		{
			name:     "TEXT to TEXT - should succeed",
			inputs:   []string{"test text"},
			fromType: types.T_text.ToType(),
			toType:   types.T_text.ToType(),
			want:     []string{"test text"},
			wantErr:  false,
		},
		{
			name:     "TEXT to CHAR(255) with multiple values - over-width truncates",
			inputs:   []string{"short", longString260, "medium length string"},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_char, 255, 0),
			want:     []string{"short", strings.Repeat("a", 255), "medium length string"},
			wantErr:  false,
		},
		{
			name:     "TEXT to VARCHAR(3) with multibyte value - should truncate by rune",
			inputs:   []string{"你好世界"},
			fromType: types.T_text.ToType(),
			toType:   types.New(types.T_varchar, 3, 0),
			want:     []string{"你好世"},
		},
		{
			name:     "VARCHAR to GEOMETRY with valid point",
			inputs:   []string{"POINT(1 2)"},
			fromType: types.T_varchar.ToType(),
			toType:   types.T_geometry.ToType(),
			want:     []string{"POINT(1 2)"},
			wantErr:  false,
		},
		{
			name:     "VARCHAR to GEOMETRY rejects invalid payload",
			inputs:   []string{"INVALID"},
			fromType: types.T_varchar.ToType(),
			toType:   types.T_geometry.ToType(),
			wantErr:  true,
			errMsg:   "invalid geometry payload",
		},
		{
			name:     "VARCHAR to GEOMETRY rejects non-finite coordinates",
			inputs:   []string{"POINT(NaN 1)"},
			fromType: types.T_varchar.ToType(),
			toType:   types.T_geometry.ToType(),
			wantErr:  true,
			errMsg:   "invalid geometry payload",
		},
		{
			name:     "VARCHAR to GEOMETRY rejects malformed structure",
			inputs:   []string{"POINT(1"},
			fromType: types.T_varchar.ToType(),
			toType:   types.T_geometry.ToType(),
			wantErr:  true,
			errMsg:   "invalid geometry payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input vector based on source type
			var inputVec *vector.Vector
			if tt.fromType.Oid == types.T_text {
				inputVec = testutil.MakeTextVector(tt.inputs, tt.nulls, mp)
			} else {
				inputVec = testutil.MakeVarcharVector(tt.inputs, tt.nulls, mp)
				// Set the type explicitly for non-TEXT types
				inputVec.SetType(tt.fromType)
			}
			defer inputVec.Free(mp)

			from := vector.GenerateFunctionStrParameter(inputVec)

			resultType := tt.toType
			to := vector.NewFunctionResultWrapper(resultType, mp).(*vector.FunctionResult[types.Varlena])
			defer to.Free()
			err := to.PreExtendAndReset(len(tt.inputs))
			require.NoError(t, err)

			err = strToStr(ctx, nil, from, to, len(tt.inputs), tt.toType, false)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					require.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}
			require.NoError(t, err)

			resultVec := to.GetResultVector()
			r := vector.GenerateFunctionStrParameter(resultVec)

			for i := 0; i < len(tt.want); i++ {
				want := tt.want[i]
				get, null := r.GetStrValue(uint64(i))

				if contains(tt.wantNulls, uint64(i)) {
					require.True(t, null, "row %d should be null", i)
				} else {
					require.False(t, null, "row %d should not be null", i)
					if tt.toType.Oid == types.T_geometry || tt.toType.Oid == types.T_geometry32 {
						// Geometry is stored as WKB; compare canonical WKT.
						require.Equal(t, geometryComparisonWKT([]byte(want)), geometryComparisonWKT(get), "row %d value not match", i)
					} else {
						require.Equal(t, want, string(get), "row %d value not match", i)
					}
				}
			}

			resultNulls := to.GetResultVector().GetNulls()
			if len(tt.wantNulls) > 0 {
				for _, pos := range tt.wantNulls {
					require.True(t, resultNulls.Contains(pos))
				}
			} else {
				require.True(t, resultNulls.IsEmpty())
			}
		})
	}
}

// Test_strToStr_StrictStringWidth covers the strict assignment path
// (strictStringWidth=true, used by cast_strict): an over-width CHAR/VARCHAR
// value is rejected with "larger than Dest length" instead of being truncated,
// while a value that fits is stored unchanged. The lenient path
// (strictStringWidth=false) truncates the same over-width value. Width is
// measured in runes, so multibyte boundaries are honored.
func Test_strToStr_StrictStringWidth(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	tests := []struct {
		name    string
		input   string
		toType  types.Type
		strict  bool
		want    string
		wantErr bool
	}{
		{name: "strict varchar over-width rejected", input: "abcd", toType: types.New(types.T_varchar, 3, 0), strict: true, wantErr: true},
		{name: "strict char over-width rejected", input: "abcd", toType: types.New(types.T_char, 3, 0), strict: true, wantErr: true},
		{name: "strict varchar fits", input: "abc", toType: types.New(types.T_varchar, 3, 0), strict: true, want: "abc"},
		{name: "strict char fits", input: "abc", toType: types.New(types.T_char, 3, 0), strict: true, want: "abc"},
		{name: "non-strict varchar over-width truncates", input: "abcd", toType: types.New(types.T_varchar, 3, 0), strict: false, want: "abc"},
		{name: "strict multibyte over-width rejected", input: "你好世", toType: types.New(types.T_varchar, 2, 0), strict: true, wantErr: true},
		{name: "strict multibyte fits", input: "你好", toType: types.New(types.T_varchar, 2, 0), strict: true, want: "你好"},
		{name: "non-strict multibyte over-width truncates by rune", input: "你好世", toType: types.New(types.T_varchar, 2, 0), strict: false, want: "你好"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputVec := testutil.MakeTextVector([]string{tt.input}, nil, mp)
			defer inputVec.Free(mp)
			from := vector.GenerateFunctionStrParameter(inputVec)

			to := vector.NewFunctionResultWrapper(tt.toType, mp).(*vector.FunctionResult[types.Varlena])
			defer to.Free()
			require.NoError(t, to.PreExtendAndReset(1))

			err := strToStr(ctx, nil, from, to, 1, tt.toType, tt.strict)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "larger than Dest length")
				return
			}
			require.NoError(t, err)
			get, null := vector.GenerateFunctionStrParameter(to.GetResultVector()).GetStrValue(0)
			require.False(t, null)
			require.Equal(t, tt.want, string(get))
		})
	}
}

func Test_CastVarcharToGeometryRejectTooManyPoints(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "max_points_in_geometry" {
			return int64(3), nil
		}
		return nil, nil
	})

	inputVec := testutil.MakeVarcharVector([]string{"LINESTRING(0 0,1 1,2 2,3 3)"}, nil, mp)
	defer inputVec.Free(mp)
	inputVec.SetType(types.T_varchar.ToType())

	from := vector.GenerateFunctionStrParameter(inputVec)
	to := vector.NewFunctionResultWrapper(types.T_geometry.ToType(), mp).(*vector.FunctionResult[types.Varlena])
	defer to.Free()
	err := to.PreExtendAndReset(1)
	require.NoError(t, err)

	err = strToStr(context.Background(), proc, from, to, 1, types.T_geometry.ToType(), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_points_in_geometry=3")
}

func TestCastNumericToDecimal256Dispatcher(t *testing.T) {
	proc := testutil.NewProcess(t)
	toType := types.New(types.T_decimal256, 65, 4)
	var zero types.Decimal256
	toDecimal256 := func(s string) types.Decimal256 {
		v, err := types.ParseDecimal256(s, 65, 4)
		require.NoError(t, err)
		return v
	}
	run := func(name string, input FunctionTestInput, expected types.Decimal256) {
		t.Run(name, func(t *testing.T) {
			tc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					input,
					NewFunctionTestInput(toType, []types.Decimal256{}, nil),
				},
				NewFunctionTestResult(toType, false, []types.Decimal256{expected, zero}, []bool{false, true}),
				NewCast,
			)
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}

	run("bit", NewFunctionTestInput(types.T_bit.ToType(), []uint64{42, 0}, []bool{false, true}),
		toDecimal256("42.0000"))
	run("int8", NewFunctionTestInput(types.T_int8.ToType(), []int8{-12, 0}, []bool{false, true}),
		toDecimal256("-12.0000"))
	run("int16", NewFunctionTestInput(types.T_int16.ToType(), []int16{-1234, 0}, []bool{false, true}),
		toDecimal256("-1234.0000"))
	run("int32", NewFunctionTestInput(types.T_int32.ToType(), []int32{-123456, 0}, []bool{false, true}),
		toDecimal256("-123456.0000"))
	run("int64", NewFunctionTestInput(types.T_int64.ToType(), []int64{-1234567890123, 0}, []bool{false, true}),
		toDecimal256("-1234567890123.0000"))
	run("uint8", NewFunctionTestInput(types.T_uint8.ToType(), []uint8{12, 0}, []bool{false, true}),
		toDecimal256("12.0000"))
	run("uint16", NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1234, 0}, []bool{false, true}),
		toDecimal256("1234.0000"))
	run("uint32", NewFunctionTestInput(types.T_uint32.ToType(), []uint32{123456, 0}, []bool{false, true}),
		toDecimal256("123456.0000"))
	run("uint64", NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1234567890123, 0}, []bool{false, true}),
		toDecimal256("1234567890123.0000"))
}

func TestCastFloatToDecimal256Dispatcher(t *testing.T) {
	proc := testutil.NewProcess(t)
	toType := types.New(types.T_decimal256, 65, 4)
	var zero types.Decimal256
	toDecimal256 := func(s string) types.Decimal256 {
		v, err := types.ParseDecimal256(s, 65, 4)
		require.NoError(t, err)
		return v
	}
	run := func(name string, input FunctionTestInput, expected types.Decimal256) {
		t.Run(name, func(t *testing.T) {
			tc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					input,
					NewFunctionTestInput(toType, []types.Decimal256{}, nil),
				},
				NewFunctionTestResult(toType, false, []types.Decimal256{expected, zero}, []bool{false, true}),
				NewCast,
			)
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}

	run("float32", NewFunctionTestInput(types.T_float32.ToType(), []float32{1.25, 0}, []bool{false, true}),
		toDecimal256("1.2500"))
	run("float64", NewFunctionTestInput(types.T_float64.ToType(), []float64{-2.5, 0}, []bool{false, true}),
		toDecimal256("-2.5000"))
}

func TestDecimal64ToDecimal256Scalar(t *testing.T) {
	mp := mpool.MustNewZero()
	fromType := types.New(types.T_decimal64, 18, 2)
	toType := types.New(types.T_decimal256, 65, 4)
	positive, err := types.ParseDecimal64("12.34", 18, 2)
	require.NoError(t, err)
	negative, err := types.ParseDecimal64("-56.78", 18, 2)
	require.NoError(t, err)
	expectedPositive, err := types.ParseDecimal256("12.3400", 65, 4)
	require.NoError(t, err)
	expectedNegative, err := types.ParseDecimal256("-56.7800", 65, 4)
	require.NoError(t, err)

	srcVec := vector.NewVec(fromType)
	defer srcVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(srcVec, []types.Decimal64{positive, negative, 0},
		[]bool{false, false, true}, mp))
	src := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](srcVec)

	to := vector.NewFunctionResultWrapper(toType, mp).(*vector.FunctionResult[types.Decimal256])
	defer to.Free()
	require.NoError(t, to.PreExtendAndReset(3))
	require.NoError(t, decimal64ToDecimal256(src, to, 3))

	got := vector.MustFixedColNoTypeCheck[types.Decimal256](to.GetResultVector())
	require.Equal(t, []types.Decimal256{expectedPositive, expectedNegative, {}}, got)
	require.Equal(t, []uint64{2}, to.GetResultVector().GetNulls().ToArray())
}

func TestCastDecimalToDecimal256Dispatcher(t *testing.T) {
	proc := testutil.NewProcess(t)
	var zero types.Decimal256

	decimal64Type := types.New(types.T_decimal64, 18, 2)
	decimal64ToType := types.New(types.T_decimal256, 65, 4)
	d64Positive, err := types.ParseDecimal64("12.34", 18, 2)
	require.NoError(t, err)
	d64Negative, err := types.ParseDecimal64("-56.78", 18, 2)
	require.NoError(t, err)
	d64ExpectedPositive, err := types.ParseDecimal256("12.3400", 65, 4)
	require.NoError(t, err)
	d64ExpectedNegative, err := types.ParseDecimal256("-56.7800", 65, 4)
	require.NoError(t, err)

	t.Run("decimal64", func(t *testing.T) {
		tc := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(decimal64Type, []types.Decimal64{d64Positive, d64Negative, 0}, []bool{false, false, true}),
				NewFunctionTestInput(decimal64ToType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(decimal64ToType, false,
				[]types.Decimal256{d64ExpectedPositive, d64ExpectedNegative, zero}, []bool{false, false, true}),
			NewCast,
		)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	decimal128Type := types.New(types.T_decimal128, 38, 3)
	decimal128ToType := types.New(types.T_decimal256, 65, 6)
	d128Positive, err := types.ParseDecimal128("123456789.123", 38, 3)
	require.NoError(t, err)
	d128Negative, err := types.ParseDecimal128("-987654321.500", 38, 3)
	require.NoError(t, err)
	d128ExpectedPositive, err := types.ParseDecimal256("123456789.123000", 65, 6)
	require.NoError(t, err)
	d128ExpectedNegative, err := types.ParseDecimal256("-987654321.500000", 65, 6)
	require.NoError(t, err)

	t.Run("decimal128", func(t *testing.T) {
		tc := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(decimal128Type,
					[]types.Decimal128{d128Positive, d128Negative, {}}, []bool{false, false, true}),
				NewFunctionTestInput(decimal128ToType, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(decimal128ToType, false,
				[]types.Decimal256{d128ExpectedPositive, d128ExpectedNegative, zero}, []bool{false, false, true}),
			NewCast,
		)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})
}

// makeJSONEncodedFromText parses JSON text strings and returns their bytejson-encoded form as []string for vector.
func makeJSONEncodedFromText(t *testing.T, jsonTexts []string, nulls []bool) []string {
	t.Helper()
	out := make([]string, len(jsonTexts))
	for i, s := range jsonTexts {
		if len(nulls) > i && nulls[i] {
			out[i] = ""
			continue
		}
		bj, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		enc, err := types.EncodeJson(bj)
		require.NoError(t, err)
		out[i] = string(enc)
	}
	return out
}

// TestDecimalToFloatRangeCheckUsesFullPrecision exercises the boundary case
// where a decimal literal rounds to a float(W,S) range violation only when
// compared at full precision. 999.995 stored in decimal(30,3) must be
// rejected when cast to float(5,2) — but if the fixed-range check sees the
// float32-quantized value (~999.9949951) it will truncate to 999.99 and let
// it through. This guards the intentional re-parse-at-64 that
// decimal128ToFloat / decimal256ToFloat do when bitSize==32.
func TestDecimalToFloatRangeCheckUsesFullPrecision(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	// Custom-Width/Scale float types can't be used at vector allocation time
	// (PreExtend trips a div-by-zero), so we create the result wrapper with
	// the default float32 type and override with SetType before the cast.
	floatTyp := types.Type{Oid: types.T_float32, Width: 5, Scale: 2}

	// decimal128 -> float32(5,2): 999.995 must raise out-of-range
	d128Vec := vector.NewVec(types.T_decimal128.ToType())
	defer d128Vec.Free(mp)
	d128, err := types.ParseDecimal128("999.995", 30, 3)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixedList(d128Vec, []types.Decimal128{d128}, nil, mp))
	d128Vec.SetType(types.Type{Oid: types.T_decimal128, Width: 30, Scale: 3})

	f32Res := vector.NewFunctionResultWrapper(types.T_float32.ToType(), mp).(*vector.FunctionResult[float32])
	defer f32Res.Free()
	require.NoError(t, f32Res.PreExtendAndReset(1))
	f32Res.GetResultVector().SetType(floatTyp)
	err = decimal128ToFloat[float32](
		ctx,
		vector.GenerateFunctionFixedTypeParameter[types.Decimal128](d128Vec),
		f32Res, 1, 32)
	require.Error(t, err, "decimal128(30,3)=999.995 must overflow float(5,2)")
	require.Contains(t, err.Error(), "999.995",
		"range-check error should reference the original decimal literal")

}

// TestDecimal256ToOthersRouting sanity-checks the cast-target matrix wired
// through decimal256ToOthers by invoking each helper with a small input.
// Having these helpers in place means CAST(decimal256_col AS VARCHAR) and
// similar statements no longer hit "unsupported cast".
func TestDecimal256ToOthersRouting(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	d256Typ := types.T_decimal256.ToType()
	d256, err := types.ParseDecimal256("42", 65, 0)
	require.NoError(t, err)
	srcVec := vector.NewVec(d256Typ)
	defer srcVec.Free(mp)
	require.NoError(t, vector.AppendFixedList(srcVec, []types.Decimal256{d256}, nil, mp))
	src := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](srcVec)

	// int64
	intRes := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp).(*vector.FunctionResult[int64])
	defer intRes.Free()
	require.NoError(t, intRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToSigned[int64](ctx, src, intRes, 64, 1, nil))
	require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](intRes.GetResultVector())[0])

	// uint32
	uRes := vector.NewFunctionResultWrapper(types.T_uint32.ToType(), mp).(*vector.FunctionResult[uint32])
	defer uRes.Free()
	require.NoError(t, uRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToUnsigned[uint32](ctx, src, uRes, 32, 1, nil))
	require.Equal(t, uint32(42), vector.MustFixedColNoTypeCheck[uint32](uRes.GetResultVector())[0])

	// bit
	bitRes := vector.NewFunctionResultWrapper(types.T_bit.ToType(), mp).(*vector.FunctionResult[uint64])
	defer bitRes.Free()
	require.NoError(t, bitRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToBit(ctx, src, bitRes, 64, 1, nil))
	require.Equal(t, uint64(42), vector.MustFixedColNoTypeCheck[uint64](bitRes.GetResultVector())[0])

	// decimal64
	d64Res := vector.NewFunctionResultWrapper(types.T_decimal64.ToType(), mp).(*vector.FunctionResult[types.Decimal64])
	defer d64Res.Free()
	require.NoError(t, d64Res.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal64(src, d64Res, 1, nil))

	// decimal128
	d128Res := vector.NewFunctionResultWrapper(types.T_decimal128.ToType(), mp).(*vector.FunctionResult[types.Decimal128])
	defer d128Res.Free()
	require.NoError(t, d128Res.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal128(src, d128Res, 1, nil))

	// decimal256 -> decimal256 narrow
	d256bRes := vector.NewFunctionResultWrapper(types.T_decimal256.ToType(), mp).(*vector.FunctionResult[types.Decimal256])
	defer d256bRes.Free()
	require.NoError(t, d256bRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToDecimal256(src, d256bRes, 1, nil))

	// varchar
	vcTyp := types.T_varchar.ToType()
	vcTyp.Width = 64
	vcRes := vector.NewFunctionResultWrapper(vcTyp, mp).(*vector.FunctionResult[types.Varlena])
	defer vcRes.Free()
	require.NoError(t, vcRes.PreExtendAndReset(1))
	require.NoError(t, decimal256ToStr(ctx, src, vcRes, 1, vcTyp))
	strParam := vector.GenerateFunctionStrParameter(vcRes.GetResultVector())
	got, null := strParam.GetStrValue(0)
	require.False(t, null)
	require.Equal(t, "42", string(got))
}

// TestDecimal256ToOthersDispatcher drives every arm of decimal256ToOthers,
// including the null/error branches inside each helper and the unsupported
// target-type fallback. This is what lights up the routing table's
// coverage the most — calling the helpers directly (as the routing test
// above does) skips the dispatcher case rows.
func TestDecimal256ToOthersDispatcher(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	d256Typ := types.T_decimal256.ToType()
	buildSrc := func(values []types.Decimal256, nulls []bool) vector.FunctionParameterWrapper[types.Decimal256] {
		srcVec := vector.NewVec(d256Typ)
		t.Cleanup(func() { srcVec.Free(mp) })
		require.NoError(t, vector.AppendFixedList(srcVec, values, nulls, mp))
		return vector.GenerateFunctionFixedTypeParameter[types.Decimal256](srcVec)
	}

	d42, err := types.ParseDecimal256("42", 65, 0)
	require.NoError(t, err)
	src := buildSrc([]types.Decimal256{d42}, nil)

	// Feed every supported Oid. We're not asserting numeric correctness here
	// (that belongs in per-helper tests); the point is to execute each case
	// branch of the switch so the dispatcher table is covered.
	targets := []types.T{
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_float32, types.T_float64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink,
	}
	for _, oid := range targets {
		toType := oid.ToType()
		if oid == types.T_char || oid == types.T_varchar || oid == types.T_binary ||
			oid == types.T_varbinary {
			toType.Width = 64
		}
		res := vector.NewFunctionResultWrapper(toType, mp)
		require.NoError(t, res.PreExtendAndReset(1))
		err := decimal256ToOthers(ctx, src, toType, res, 1, nil)
		// Some combinations (e.g. 42 fits int8) are ok; others reject by
		// design (e.g. too-narrow decimal). Either is fine — we just want
		// the case to execute.
		_ = err
		res.Free()
	}

	// Unsupported target hits the default arm.
	err = decimal256ToOthers(ctx, src, types.T_uuid.ToType(),
		vector.NewFunctionResultWrapper(types.T_uuid.ToType(), mp), 1, nil)
	require.Error(t, err)

	// Null input lights up every helper's null-append arm.
	srcNull := buildSrc([]types.Decimal256{{}}, []bool{true})
	for _, oid := range []types.T{
		types.T_bit, types.T_int16, types.T_uint16,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_float32, types.T_varchar,
	} {
		toType := oid.ToType()
		if oid == types.T_varchar {
			toType.Width = 64
		}
		res := vector.NewFunctionResultWrapper(toType, mp)
		require.NoError(t, res.PreExtendAndReset(1))
		require.NoError(t, decimal256ToOthers(ctx, srcNull, toType, res, 1, nil))
		res.Free()
	}

	// Out-of-range for narrow integer types goes through each helper's
	// ParseInt/ParseUint error branch.
	dLarge, err := types.ParseDecimal256("99999999999999", 65, 0)
	require.NoError(t, err)
	srcLarge := buildSrc([]types.Decimal256{dLarge}, nil)
	for _, oid := range []types.T{types.T_int8, types.T_uint8, types.T_bit} {
		toType := oid.ToType()
		if oid == types.T_bit {
			toType.Width = 4
		}
		res := vector.NewFunctionResultWrapper(toType, mp)
		require.NoError(t, res.PreExtendAndReset(1))
		require.Error(t, decimal256ToOthers(ctx, srcLarge, toType, res, 1, nil))
		res.Free()
	}

	// decimal256ToStr with a small binary target exercises the Width-bound
	// rejection path inside decimal256ToStr.
	tinyBin := types.T_binary.ToType()
	tinyBin.Width = 1
	res := vector.NewFunctionResultWrapper(tinyBin, mp)
	require.NoError(t, res.PreExtendAndReset(1))
	// 42 is 2 characters, exceeds Width=1 — expect error.
	require.Error(t, decimal256ToStr(ctx, src, res.(*vector.FunctionResult[types.Varlena]), 1, tinyBin))
	res.Free()
}

func TestCastJsonToNumeric(t *testing.T) {
	proc := testutil.NewProcess(t)

	// run runs one JSON->numeric cast test. jsonTexts are JSON literal strings (e.g. "1", "\"a\""); toType is target type.
	run := func(t *testing.T, name string, jsonTexts []string, nulls []bool, toType types.Type, expectAny any, expectNulls []bool, wantErr bool) {
		t.Helper()
		if nulls == nil {
			nulls = make([]bool, len(jsonTexts))
		}
		encoded := makeJSONEncodedFromText(t, jsonTexts, nulls)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encoded, nulls),
			NewFunctionTestInput(toType, emptySliceForCastTarget(toType.Oid), []bool{}),
		}
		expect := NewFunctionTestResult(toType, wantErr, expectAny, expectNulls)
		fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := fcTC.Run()
		if wantErr {
			require.True(t, succeed, "case %s: expected cast to fail with error, but: %s", name, info)
			return
		}
		require.True(t, succeed, "case %s: %s", name, info)
	}

	t.Run("json_number_to_int64", func(t *testing.T) {
		run(t, "int64", []string{"1", "2", "-3"}, nil,
			types.T_int64.ToType(),
			[]int64{1, 2, -3}, []bool{false, false, false}, false)
	})

	t.Run("json_number_to_int8", func(t *testing.T) {
		run(t, "int8", []string{"10", "20"}, nil,
			types.T_int8.ToType(),
			[]int8{10, 20}, []bool{false, false}, false)
	})

	t.Run("json_string_number_to_int64", func(t *testing.T) {
		run(t, "string_to_int64", []string{`"42"`, `"100"`}, nil,
			types.T_int64.ToType(),
			[]int64{42, 100}, []bool{false, false}, false)
	})

	t.Run("json_null_to_int64", func(t *testing.T) {
		run(t, "null", []string{"null", "null"}, nil,
			types.T_int64.ToType(),
			[]int64{0, 0}, []bool{true, true}, false)
	})

	t.Run("json_number_to_float64", func(t *testing.T) {
		run(t, "float64", []string{"1.5", "2.7", "3"}, nil,
			types.T_float64.ToType(),
			[]float64{1.5, 2.7, 3}, []bool{false, false, false}, false)
	})

	t.Run("json_string_number_to_float64", func(t *testing.T) {
		run(t, "string_float", []string{`"3.14"`, `"0"`}, nil,
			types.T_float64.ToType(),
			[]float64{3.14, 0}, []bool{false, false}, false)
	})

	t.Run("json_number_to_uint64", func(t *testing.T) {
		run(t, "uint64", []string{"1", "2", "99"}, nil,
			types.T_uint64.ToType(),
			[]uint64{1, 2, 99}, []bool{false, false, false}, false)
	})

	t.Run("json_object_to_int64_err", func(t *testing.T) {
		run(t, "object_err", []string{"{}"}, nil,
			types.T_int64.ToType(),
			nil, nil, true)
	})

	t.Run("json_array_to_int64_err", func(t *testing.T) {
		run(t, "array_err", []string{"[1,2]"}, nil,
			types.T_int64.ToType(),
			nil, nil, true)
	})

	t.Run("json_string_non_number_to_int64_err", func(t *testing.T) {
		run(t, "string_non_number_err", []string{`"abc"`}, nil,
			types.T_int64.ToType(),
			nil, nil, true)
	})

	// --- jsonAppendNull coverage: all numeric types get null from JSON null or vector null ---
	t.Run("json_null_to_int16", func(t *testing.T) {
		run(t, "null_int16", []string{"null", "null"}, nil,
			types.T_int16.ToType(),
			[]int16{0, 0}, []bool{true, true}, false)
	})
	t.Run("json_null_to_int32", func(t *testing.T) {
		run(t, "null_int32", []string{"null"}, nil,
			types.T_int32.ToType(),
			[]int32{0}, []bool{true}, false)
	})
	t.Run("json_null_to_uint8", func(t *testing.T) {
		run(t, "null_uint8", []string{"null", "null"}, nil,
			types.T_uint8.ToType(),
			[]uint8{0, 0}, []bool{true, true}, false)
	})
	t.Run("json_null_to_uint16", func(t *testing.T) {
		run(t, "null_uint16", []string{"null"}, nil,
			types.T_uint16.ToType(),
			[]uint16{0}, []bool{true}, false)
	})
	t.Run("json_null_to_uint32", func(t *testing.T) {
		run(t, "null_uint32", []string{"null", "null"}, nil,
			types.T_uint32.ToType(),
			[]uint32{0, 0}, []bool{true, true}, false)
	})
	t.Run("json_null_to_float32", func(t *testing.T) {
		run(t, "null_float32", []string{"null"}, nil,
			types.T_float32.ToType(),
			[]float32{0}, []bool{true}, false)
	})
	t.Run("json_null_to_decimal64", func(t *testing.T) {
		run(t, "null_decimal64", []string{"null", "null"}, nil,
			types.T_decimal64.ToType(),
			[]types.Decimal64{0, 0}, []bool{true, true}, false)
	})
	t.Run("json_null_to_decimal128", func(t *testing.T) {
		run(t, "null_decimal128", []string{"null"}, nil,
			types.T_decimal128.ToType(),
			[]types.Decimal128{{B0_63: 0, B64_127: 0}}, []bool{true}, false)
	})
	// Vector null (row is null): first row null triggers jsonAppendNull
	t.Run("vector_null_to_int64", func(t *testing.T) {
		nulls := []bool{true, false}
		run(t, "vector_null", []string{"1", "2"}, nulls,
			types.T_int64.ToType(),
			[]int64{0, 2}, []bool{true, false}, false)
	})

	// --- jsonAppendValue coverage: all numeric types with values ---
	t.Run("json_number_to_int16", func(t *testing.T) {
		run(t, "int16", []string{"100", "-200", "0"}, nil,
			types.T_int16.ToType(),
			[]int16{100, -200, 0}, []bool{false, false, false}, false)
	})
	t.Run("json_number_to_int32", func(t *testing.T) {
		run(t, "int32", []string{"100000", "-99999"}, nil,
			types.T_int32.ToType(),
			[]int32{100000, -99999}, []bool{false, false}, false)
	})
	t.Run("json_number_to_uint8", func(t *testing.T) {
		run(t, "uint8", []string{"0", "255", "100"}, nil,
			types.T_uint8.ToType(),
			[]uint8{0, 255, 100}, []bool{false, false, false}, false)
	})
	t.Run("json_number_to_uint16", func(t *testing.T) {
		run(t, "uint16", []string{"1000", "65535"}, nil,
			types.T_uint16.ToType(),
			[]uint16{1000, 65535}, []bool{false, false}, false)
	})
	t.Run("json_number_to_uint32", func(t *testing.T) {
		run(t, "uint32", []string{"100000", "4294967295"}, nil,
			types.T_uint32.ToType(),
			[]uint32{100000, 4294967295}, []bool{false, false}, false)
	})
	t.Run("json_number_to_float32", func(t *testing.T) {
		run(t, "float32", []string{"1.5", "2.5", "-0.5"}, nil,
			types.T_float32.ToType(),
			[]float32{1.5, 2.5, -0.5}, []bool{false, false, false}, false)
	})
	t.Run("json_number_to_decimal64", func(t *testing.T) {
		d1, _ := types.Decimal64FromFloat64(12.5, 18, 0)
		d2, _ := types.Decimal64FromFloat64(0, 18, 0)
		d3, _ := types.Decimal64FromFloat64(-3, 18, 0)
		run(t, "decimal64", []string{"12.5", "0", "-3"}, nil,
			types.T_decimal64.ToType(),
			[]types.Decimal64{d1, d2, d3}, []bool{false, false, false}, false)
	})
	t.Run("json_number_to_decimal128", func(t *testing.T) {
		d1, _ := types.Decimal128FromFloat64(12.5, 38, 0)
		d2, _ := types.Decimal128FromFloat64(0, 38, 0)
		d3, _ := types.Decimal128FromFloat64(-3, 38, 0)
		run(t, "decimal128", []string{"12.5", "0", "-3"}, nil,
			types.T_decimal128.ToType(),
			[]types.Decimal128{d1, d2, d3}, []bool{false, false, false}, false)
	})

	// --- jsonAppendValue error/overflow cases ---
	t.Run("json_int8_overflow_err", func(t *testing.T) {
		run(t, "int8_overflow", []string{"1000"}, nil,
			types.T_int8.ToType(),
			nil, nil, true)
	})
	t.Run("json_int16_overflow_err", func(t *testing.T) {
		run(t, "int16_overflow", []string{"100000"}, nil,
			types.T_int16.ToType(),
			nil, nil, true)
	})
	t.Run("json_int32_overflow_err", func(t *testing.T) {
		run(t, "int32_overflow", []string{"999999999999"}, nil,
			types.T_int32.ToType(),
			nil, nil, true)
	})
	t.Run("json_uint_negative_err", func(t *testing.T) {
		run(t, "uint_negative", []string{"-1"}, nil,
			types.T_uint64.ToType(),
			nil, nil, true)
	})
	t.Run("json_uint8_overflow_err", func(t *testing.T) {
		run(t, "uint8_overflow", []string{"256"}, nil,
			types.T_uint8.ToType(),
			nil, nil, true)
	})
	t.Run("json_float32_overflow_err", func(t *testing.T) {
		run(t, "float32_overflow", []string{"1e100"}, nil,
			types.T_float32.ToType(),
			nil, nil, true)
	})
}

func TestCastJsonToJson(t *testing.T) {
	proc := testutil.NewProcess(t)
	jsonTexts := []string{`{"a":1}`, `[1,true,{"b":"x"}]`, `null`}
	nulls := []bool{false, false, false}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nulls)

	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), encoded, nulls),
		NewFunctionTestInput(types.T_json.ToType(), []string{}, []bool{}),
	}
	expect := NewFunctionTestResult(types.T_json.ToType(), false, encoded, []bool{false, false, false})
	fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
	succeed, info := fcTC.Run()
	require.True(t, succeed, info)
}

func TestCastJsonToJsonOverloadResolution(t *testing.T) {
	require.True(t, IfTypeCastSupported(types.T_json, types.T_json))

	_, err := GetFunctionByName(context.Background(), "cast", []types.Type{types.T_json.ToType(), types.T_json.ToType()})
	require.NoError(t, err)
}

// TestCastJsonToVarchar verifies that casting a JSON value to VARCHAR uses JSON_UNQUOTE semantics,
// i.e. JSON strings lose their outer double-quotes (MySQL-compatible behavior).
func TestCastJsonToVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)

	jsonTexts := []string{`"active"`, `42`, `true`, `null`, `[1,2,3]`, `{"k":"v"}`}
	// After unquote: JSON strings lose outer quotes; other types keep their JSON text representation.
	expected := []string{"active", "42", "true", "null", "[1, 2, 3]", `{"k": "v"}`}
	nulls := []bool{false, false, false, false, false, false}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nulls)

	toType := types.New(types.T_varchar, 256, 0)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), encoded, nulls),
		NewFunctionTestInput(toType, []string{}, []bool{}),
	}
	expect := NewFunctionTestResult(toType, false, expected, nulls)
	fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
	succeed, info := fcTC.Run()
	require.True(t, succeed, info)
}

// emptySliceForCastTarget returns an empty slice of the right type for the second (target type) cast parameter.
func emptySliceForCastTarget(oid types.T) any {
	switch oid {
	case types.T_int8:
		return []int8{}
	case types.T_int16:
		return []int16{}
	case types.T_int32:
		return []int32{}
	case types.T_int64:
		return []int64{}
	case types.T_uint8:
		return []uint8{}
	case types.T_uint16:
		return []uint16{}
	case types.T_uint32:
		return []uint32{}
	case types.T_uint64:
		return []uint64{}
	case types.T_float32:
		return []float32{}
	case types.T_float64:
		return []float64{}
	case types.T_decimal64:
		return []types.Decimal64{}
	case types.T_decimal128:
		return []types.Decimal128{}
	default:
		return []int64{}
	}
}

// TestDecimal64ToDecimal128FastPaths tests the optimized decimal64→128 cast
// paths added in the decimal-perf PR.
func TestDecimal64ToDecimal128FastPaths(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Negative decimal64 value (sign extension matters).
	neg100 := types.Decimal64(^uint64(99)) // -100

	cases := []tcTemp{
		{
			info: "d64→d128 same-scale with nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 2),
					[]types.Decimal64{100, 200, 300, 400},
					[]bool{false, true, false, true}),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 38, 2),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 38, 2), false,
				[]types.Decimal128{{B0_63: 100}, {}, {B0_63: 300}, {}},
				[]bool{false, true, false, true}),
		},
		{
			info: "d64→d128 same-scale negative values with nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 4),
					[]types.Decimal64{neg100, 50},
					[]bool{false, false}),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 38, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 38, 4), false,
				[]types.Decimal128{{B0_63: ^uint64(99), B64_127: ^uint64(0)}, {B0_63: 50}},
				nil),
		},
		{
			info: "d64→d128 diff-scale no nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 2),
					[]types.Decimal64{12345},
					nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 38, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 38, 4), false,
				[]types.Decimal128{{B0_63: 1234500}},
				nil),
		},
		{
			info: "d64→d128 diff-scale with nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 2),
					[]types.Decimal64{12345, 67890},
					[]bool{true, false}),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 38, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 38, 4), false,
				[]types.Decimal128{{}, {B0_63: 6789000}},
				[]bool{true, false}),
		},
		{
			info: "d64→d128 narrowing width",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 4),
					[]types.Decimal64{123456789},
					nil),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 10, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 10, 4), false,
				[]types.Decimal128{{B0_63: 123456789}},
				nil),
		},
		{
			info: "d64→d128 narrowing width with nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(
					types.New(types.T_decimal64, 18, 4),
					[]types.Decimal64{123456789, 987654321},
					[]bool{false, true}),
				NewFunctionTestInput(
					types.New(types.T_decimal128, 10, 4),
					[]types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(
				types.New(types.T_decimal128, 10, 4), false,
				[]types.Decimal128{{B0_63: 123456789}, {}},
				[]bool{false, true}),
		},
	}

	for _, tc := range cases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
