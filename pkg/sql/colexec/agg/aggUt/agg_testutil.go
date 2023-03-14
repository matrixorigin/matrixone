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

package aggut

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	// agg type
	op               int
	isDistinct       bool
	hasDecimalResult bool
	inputTyp         types.Type
	outputTyp        types.Type

	// test data for Fill() and Eval()
	input    any
	inputNsp []uint64
	expected any

	// test data for Merge()
	mergeInput  any
	mergeNsp    []uint64
	mergeExpect any

	// test Marshal() and Unmarshal() or not
	testMarshal bool
}

func RunTest(t *testing.T, testCases []testCase) {
	for _, c := range testCases {
		// update some parameter
		switch c.op {
		case agg.AggregateAvg, agg.AggregateVariance, agg.AggregateStdDevPop, agg.AggregateMedian:
			c.hasDecimalResult = true
		default:
			c.hasDecimalResult = false
		}
		c.outputTyp, _ = agg.ReturnType(c.op, c.inputTyp)

		RunBaseTest(t, &c)
		if c.testMarshal {
			RunBaseMarshalTest(t, &c)
		}
	}
}

func RunBaseTest(t *testing.T, c *testCase) {
	// base test: Grows(), Fill(), Eval() and Merge()
	m := mpool.MustNewZeroNoFixed()

	// Grows(), Fill() and Eval() test
	{
		// New()
		agg0, newErr := agg.New(c.op, c.isDistinct, c.inputTyp)
		require.NoError(t, newErr)

		// Grows()
		growsErr := agg0.Grows(1, m)
		require.NoError(t, growsErr)

		// Fill()
		vec, l := GetVector(c.inputTyp, c.input, c.inputNsp)
		if l > 0 && vec != nil {
			for i := 0; i < l; i++ {
				agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
			}
		}

		// Eval()
		v, err := agg0.Eval(m)
		require.NoError(t, err)
		CompareResult(t, c.outputTyp, c.expected, v, c.hasDecimalResult)
		if vec != nil {
			vec.Free(m)
		}
		v.Free(m)
	}

	// Merge() Test
	if c.mergeInput != nil {
		// New()
		agg0, newErr := agg.New(c.op, c.isDistinct, c.inputTyp)
		require.NoError(t, newErr)

		// Grows()
		growsErr := agg0.Grows(1, m)
		require.NoError(t, growsErr)

		// Fill()
		vec, l := GetVector(c.inputTyp, c.input, c.inputNsp)
		if l > 0 && vec != nil {
			for i := 0; i < l; i++ {
				agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
			}
		}

		// create another agg for merge
		agg1, _ := agg.New(c.op, c.isDistinct, c.inputTyp)
		agg1.Grows(1, m)
		vec2, l2 := GetVector(c.inputTyp, c.mergeInput, c.inputNsp)
		if l2 > 0 && vec2 != nil {
			for i := 0; i < l2; i++ {
				agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
			}
		}

		// Merge()
		agg0.Merge(agg1, 0, 0)

		// Eval()
		v, err := agg0.Eval(m)
		require.NoError(t, err)
		CompareResult(t, c.outputTyp, c.mergeExpect, v, c.hasDecimalResult)

		// release
		if vec != nil {
			vec.Free(m)
		}
		if vec2 != nil {
			vec2.Free(m)
		}
		v.Free(m)
	}
	//require.Equal(t, int64(0), m.Size())
}

func RunBaseMarshalTest(t *testing.T, c *testCase) {
	// base test: Grows(), Fill() and Eval()
	m := mpool.MustNewZeroNoFixed()
	{
		// New()
		agg0, newErr := agg.New(c.op, c.isDistinct, c.inputTyp)
		require.NoError(t, newErr)

		// Grows()
		growsErr := agg0.Grows(1, m)
		require.NoError(t, growsErr)

		// Fill()
		vec, l := GetVector(c.inputTyp, c.input, c.inputNsp)
		mid := l / 2
		if mid > 0 && vec != nil {
			for i := 0; i < mid; i++ {
				agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
			}
		}

		// Marshal and Unmarshal()
		d, marshalErr := agg0.MarshalBinary()
		require.NoError(t, marshalErr)
		agg1, _ := agg.New(c.op, c.isDistinct, c.inputTyp)
		unmarshalErr := agg1.UnmarshalBinary(d)
		require.NoError(t, unmarshalErr)
		agg1.WildAggReAlloc(m)

		// Fill() after marshal and unmarshal
		if l > 0 && vec != nil {
			for i := mid; i < l; i++ {
				agg1.Fill(0, int64(i), 1, []*vector.Vector{vec})
			}
		}

		// Eval() after marshal and unmarshal
		v, err := agg1.Eval(m)
		require.NoError(t, err)
		CompareResult(t, c.outputTyp, c.expected, v, c.hasDecimalResult)
		if vec != nil {
			vec.Free(m)
		}
		v.Free(m)
	}

	// Merge() Test
	if c.mergeInput != nil {
		// create an agg for marshal and unmarshal
		agg0, _ := agg.New(c.op, c.isDistinct, c.inputTyp)
		agg0.Grows(1, m)
		vec, l := GetVector(c.inputTyp, c.input, c.inputNsp)
		if l != 0 && vec != nil {
			for i := 0; i < l; i++ {
				agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
			}
		}

		// create another agg for merge
		agg1, _ := agg.New(c.op, c.isDistinct, c.inputTyp)
		agg1.Grows(1, m)
		vec2, l2 := GetVector(c.inputTyp, c.mergeInput, c.mergeNsp)
		if l2 != 0 && vec2 != nil {
			for i := 0; i < l2; i++ {
				agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
			}
		}

		// Marshal and Unmarshal()
		d, marshalErr := agg0.MarshalBinary()
		require.NoError(t, marshalErr)
		mAgg, _ := agg.New(c.op, c.isDistinct, c.inputTyp)
		unmarshalErr := mAgg.UnmarshalBinary(d)
		require.NoError(t, unmarshalErr)
		mAgg.WildAggReAlloc(m)

		// Merge()
		mAgg.Merge(agg1, 0, 0)

		// Eval()
		v, err := mAgg.Eval(m)
		require.NoError(t, err)
		CompareResult(t, c.outputTyp, c.mergeExpect, v, c.hasDecimalResult)
		if vec != nil {
			vec.Free(m)
		}
		if vec2 != nil {
			vec2.Free(m)
		}
		v.Free(m)
	}
	//require.Equal(t, int64(0), m.Size())
}

func GetVector(typ types.Type, input any, nsp []uint64) (*vector.Vector, int) {
	switch typ.Oid {
	case types.T_bool:
		return testutil.MakeBoolVector(input.([]bool)), len(input.([]bool))
	case types.T_int8:
		return testutil.MakeInt8Vector(input.([]int8), nsp), len(input.([]int8))
	case types.T_int16:
		return testutil.MakeInt16Vector(input.([]int16), nsp), len(input.([]int16))
	case types.T_int32:
		return testutil.MakeInt32Vector(input.([]int32), nsp), len(input.([]int32))
	case types.T_int64:
		return testutil.MakeInt64Vector(input.([]int64), nsp), len(input.([]int64))
	case types.T_uint8:
		return testutil.MakeUint8Vector(input.([]uint8), nsp), len(input.([]uint8))
	case types.T_uint16:
		return testutil.MakeUint16Vector(input.([]uint16), nsp), len(input.([]uint16))
	case types.T_uint32:
		return testutil.MakeUint32Vector(input.([]uint32), nsp), len(input.([]uint32))
	case types.T_uint64:
		return testutil.MakeUint64Vector(input.([]uint64), nsp), len(input.([]uint64))
	case types.T_float32:
		return testutil.MakeFloat32Vector(input.([]float32), nsp), len(input.([]float32))
	case types.T_float64:
		return testutil.MakeFloat64Vector(input.([]float64), nsp), len(input.([]float64))
	case types.T_char:
		return testutil.MakeVarcharVector(input.([]string), nsp), len(input.([]string))
	case types.T_varchar:
		return testutil.MakeVarcharVector(input.([]string), nsp), len(input.([]string))
	case types.T_date:
		return testutil.MakeDateVector(input.([]string), nsp), len(input.([]string))
	case types.T_time:
		return testutil.MakeTimeVector(input.([]string), nsp), len(input.([]string))
	case types.T_datetime:
		return testutil.MakeDateTimeVector(input.([]string), nsp), len(input.([]string))
	case types.T_timestamp:
		return testutil.MakeTimeStampVector(input.([]string), nsp), len(input.([]string))
	case types.T_decimal64:
		return testutil.MakeDecimal64Vector(input.([]int64), nsp, typ), len(input.([]int64))
	case types.T_decimal128:
		return testutil.MakeDecimal128Vector(input.([]int64), nsp, typ), len(input.([]int64))
	case types.T_uuid:
		// Make vector by string.
		// There is another function which can make uuid by uuid directly
		return testutil.MakeUuidVectorByString(input.([]string), nsp), len(input.([]string))
	}

	return nil, 0
}

func CompareResult(t *testing.T, typ types.Type, expected any, vec *vector.Vector, hasDecimalResult bool) bool {
	switch typ.Oid {
	case types.T_bool:
		require.Equal(t, expected.([]bool), vector.MustFixedCol[bool](vec))
	case types.T_int8:
		require.Equal(t, expected.([]int8), vector.MustFixedCol[int8](vec))
	case types.T_int16:
		require.Equal(t, expected.([]int16), vector.MustFixedCol[int16](vec))
	case types.T_int32:
		require.Equal(t, expected.([]int32), vector.MustFixedCol[int32](vec))
	case types.T_int64:
		require.Equal(t, expected.([]int64), vector.MustFixedCol[int64](vec))
	case types.T_uint8:
		require.Equal(t, expected.([]uint8), vector.MustFixedCol[uint8](vec))
	case types.T_uint16:
		require.Equal(t, expected.([]uint16), vector.MustFixedCol[uint16](vec))
	case types.T_uint32:
		require.Equal(t, expected.([]uint32), vector.MustFixedCol[uint32](vec))
	case types.T_uint64:
		require.Equal(t, expected.([]uint64), vector.MustFixedCol[uint64](vec))
	case types.T_float32:
		require.Equal(t, expected.([]float32), vector.MustFixedCol[float32](vec))
	case types.T_float64:
		require.Equal(t, expected.([]float64), vector.MustFixedCol[float64](vec))
	case types.T_char:
		require.Equal(t, expected.([]string), vector.MustStrCol(vec))
	case types.T_varchar:
		require.Equal(t, expected.([]string), vector.MustStrCol(vec))
	case types.T_date:
		require.Equal(t, expected.([]types.Date), vector.MustFixedCol[types.Date](vec))
	case types.T_time:
		require.Equal(t, expected.([]types.Time), vector.MustFixedCol[types.Datetime](vec))
	case types.T_datetime:
		require.Equal(t, expected.([]types.Datetime), vector.MustFixedCol[types.Datetime](vec))
	case types.T_timestamp:
		require.Equal(t, expected.([]types.Timestamp), vector.MustFixedCol[types.Timestamp](vec))
	case types.T_decimal64:
		if hasDecimalResult {
			result := testutil.MakeDecimal64ArrByFloat64Arr(expected.([]float64))
			require.Equal(t, result, vector.MustFixedCol[types.Decimal64](vec))
		} else {
			result := testutil.MakeDecimal64ArrByInt64Arr(expected.([]int64))
			require.Equal(t, result, vector.MustFixedCol[types.Decimal64](vec))
		}
	case types.T_decimal128:
		if hasDecimalResult {
			result := testutil.MakeDecimal128ArrByFloat64Arr(expected.([]float64))
			require.Equal(t, result, vector.MustFixedCol[types.Decimal128](vec))
		} else {
			result := testutil.MakeDecimal128ArrByInt64Arr(expected.([]int64))
			require.Equal(t, result, vector.MustFixedCol[types.Decimal128](vec))
		}
	case types.T_uuid:
		result := vector.MustFixedCol[types.Uuid](testutil.MakeUuidVectorByString(expected.([]string), nil))
		require.Equal(t, result, vector.MustFixedCol[types.Uuid](vec))
	default:
		return false
	}
	return true

}
