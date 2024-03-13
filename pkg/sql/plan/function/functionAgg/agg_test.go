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

package functionAgg

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/assertx"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/stretchr/testify/require"
)

func testUnaryAggSupported(
	newAgg func(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error),
	paramSupported []types.T, getReturnType func(typ []types.Type) types.Type) error {
	for _, t := range paramSupported {
		inputs := []types.Type{t.ToType()}

		_, err := newAgg(0, false, inputs, getReturnType(inputs), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type simpleAggTester[inputT, outputT any] struct {
	source any

	// base methods to calculate the agg result.
	grow  func(int)
	free  func(pool *mpool.MPool)
	fill  func(int64, inputT, outputT, int64, bool, bool) (outputT, bool, error)
	merge func(int64, int64, outputT, outputT, bool, bool, any) (outputT, bool, error)
	eval  func([]outputT) ([]outputT, error)
}

// please make sure the inputValues is not empty. and make sure its length is not less than inputNsp.Length().
// todo: need to handle the batch-fill method next time.
func (tr *simpleAggTester[in, out]) testUnaryAgg(
	inputValues []in, inputNsp *nulls.Nulls,
	check func(results out, isEmpty bool) bool) (err error) {

	// basic completeness check.
	if tr.grow == nil || tr.free == nil || tr.fill == nil || tr.merge == nil || tr.eval == nil {
		return moerr.NewInternalErrorNoCtx("agg test failed. basic completeness check failed.")
	}

	mp := mpool.MustNewZero()
	results := make([]out, 2)
	empties := make([]bool, 2)
	for i := range empties {
		empties[i] = true
	}
	tr.grow(2)

	length := len(inputValues)
	half := length / 2
	// cut the inputValues into two parts. so that we can test the merge method.
	for i := 0; i < half; i++ {
		if inputNsp != nil && inputNsp.Contains(uint64(i)) {
			results[0], empties[0], err = tr.fill(0, inputValues[i], results[0], 1, empties[0], true)
		} else {
			results[0], empties[0], err = tr.fill(0, inputValues[i], results[0], 1, empties[0], false)
		}
		if err != nil {
			return moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("agg test failed. fill failed. err is %s", err))
		}
	}

	for i := half; i < length; i++ {
		if inputNsp != nil && inputNsp.Contains(uint64(i)) {
			results[1], empties[1], err = tr.fill(1, inputValues[i], results[1], 1, empties[1], true)
		} else {
			results[1], empties[1], err = tr.fill(1, inputValues[i], results[1], 1, empties[1], false)
		}
		if err != nil {
			return moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("agg test failed. fill failed. err is %s", err))
		}
	}

	// in fact, it's bad to use only one agg to test the merge method.
	// because some memory will not be released.
	// it requires its eval function should use the results' length. but can't use its own struct.
	results[0], empties[0], err = tr.merge(0, 1, results[0], results[1], empties[0], empties[1], tr.source)
	if err != nil {
		return moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("agg test failed. merge failed. err is %s", err))
	}

	//results = results[:1]
	results, err = tr.eval(results)
	if err != nil {
		return moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("agg test failed. eval failed. err is %s", err))
	}

	checkSucceed := check(results[0], empties[0])
	if !checkSucceed {
		return moerr.NewInternalErrorNoCtx("agg test failed. result check failed. get result is [value: %v, empty status: %v]",
			results[0], empties[0])
	}

	tr.free(mp)
	if mp.CurrNB() != int64(0) {
		return moerr.NewInternalErrorNoCtx("aggregation's memory leak test failed. pool is not 0 after free.")
	}
	return nil
}

func generateBytesList(str ...string) [][]byte {
	list := make([][]byte, len(str))
	for i, s := range str {
		list[i] = []byte(s)
	}
	return list
}

var testMoAllTypes = []types.T{
	types.T_bool,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64,
	types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary,
	types.T_array_float32, types.T_array_float64,
	types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
	types.T_enum,
	types.T_decimal64, types.T_decimal128,
	types.T_uuid,
}

var testMoAllTypesWithoutArray = []types.T{
	types.T_bool,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64,
	types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text, types.T_binary, types.T_varbinary,
	types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
	types.T_enum,
	types.T_decimal64, types.T_decimal128,
	types.T_uuid,
}

func TestAnyValue(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggAnyValue, AggAnyValueSupportedParameters, AggAnyValueReturnType))

	s := &sAggAnyValue[bool]{}
	{
		tr := &simpleAggTester[bool, bool]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]bool{true, true, true, true}, nil, func(result bool, isEmpty bool) bool {
			return result == true && !isEmpty
		})
		require.NoError(t, err)
	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggAnyValue[bool])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestMax(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggMax, AggMaxSupportedParameters, AggMaxReturnType))

	s := &sAggMax[int32]{}
	{
		tr := &simpleAggTester[int32, int32]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int32{1, 2, 3, 4}, nil, func(result int32, isEmpty bool) bool {
			return result == 4 && !isEmpty
		})
		require.NoError(t, err)

		nsp1 := nulls.NewWithSize(2)
		nsp1.Add(0)
		nsp1.Add(1)
		err = tr.testUnaryAgg([]int32{0, 0}, nsp1, func(result int32, isEmpty bool) bool {
			return isEmpty
		})
		require.NoError(t, err)
	}

	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggMax[int32])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestMin(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggMin, AggMinSupportedParameters, AggMinReturnType))

	s := &sAggMin[int32]{}
	{
		tr := &simpleAggTester[int32, int32]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int32{1, 2, 3, 4}, nil, func(result int32, isEmpty bool) bool {
			return result == 1 && !isEmpty
		})
		require.NoError(t, err)

		nsp1 := nulls.NewWithSize(2)
		nsp1.Add(0)
		nsp1.Add(1)
		err = tr.testUnaryAgg([]int32{0, 0}, nsp1, func(result int32, isEmpty bool) bool {
			return isEmpty
		})
		require.NoError(t, err)
	}

	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggMin[int32])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestSum(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggSum, AggSumSupportedParameters, AggSumReturnType))

	s := &sAggSum[int64, int64]{}
	{
		tr := &simpleAggTester[int64, int64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4}, nil, func(result int64, isEmpty bool) bool {
			return result == 10 && !isEmpty
		})
		require.NoError(t, err)

		nsp1 := nulls.NewWithSize(4)
		nsp1.Add(0)
		nsp1.Add(2)
		err = tr.testUnaryAgg([]int64{1, 2, 3, 4}, nsp1, func(results int64, isEmpty bool) bool {
			return results == 6 && !isEmpty
		})
		require.NoError(t, err)
	}

	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggSum[int64, int64])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestAvg(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggAvg, AggAvgSupportedParameters, AggAvgReturnType))

	s := &sAggAvg[int64]{}
	{
		tr := &simpleAggTester[int64, float64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4}, nil, func(result float64, isEmpty bool) bool {
			return result == 2.5 && !isEmpty
		})
		require.NoError(t, err)

		s.cnts = nil
		nsp1 := nulls.NewWithSize(4)
		nsp1.Add(0)
		nsp1.Add(2)
		err = tr.testUnaryAgg([]int64{1, 2, 3, 4}, nsp1, func(result float64, isEmpty bool) bool {
			return result == 3 && !isEmpty
		})
		require.NoError(t, err)
	}

	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggAvg[int64])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, len(s.cnts), len(s2.cnts))
		for i := range s.cnts {
			require.Equal(t, s.cnts[i], s2.cnts[i])
		}
	}
}

func TestBitAnd(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggBitAnd, AggBitAndSupportedParameters, AggBitAndReturnType))

	s := &sAggBitAnd[int64]{}
	{
		tr := &simpleAggTester[int64, uint64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		// 010 & 011 && 111 = 010
		nsp := nulls.NewWithSize(4)
		nsp.Add(3)
		err := tr.testUnaryAgg([]int64{2, 3, 7, 0}, nsp, func(result uint64, isEmpty bool) bool {
			return result == 2 && !isEmpty
		})
		require.NoError(t, err)
	}

	s2 := &sAggBinaryBitAnd{}
	{
		tr := &simpleAggTester[[]byte, []byte]{
			source: s2,
			grow:   s2.Grows,
			free:   s2.Free,
			fill:   s2.Fill,
			merge:  s2.Merge,
			eval:   s2.Eval,
		}
		err := tr.testUnaryAgg(generateBytesList("010", "011", "111"), nil, func(result []byte, isEmpty bool) bool {
			return string(result) == "010" && !isEmpty
		})
		require.NoError(t, err)
	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s3 := new(sAggBitXor[int64])
		err = s3.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestBitOr(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggBitOr, AggBitOrSupportedParameters, AggBitOrReturnType))

	s1 := &sAggBitOr[int64]{}
	{
		tr := &simpleAggTester[int64, uint64]{
			source: s1,
			grow:   s1.Grows,
			free:   s1.Free,
			fill:   s1.Fill,
			merge:  s1.Merge,
			eval:   s1.Eval,
		}
		// 010 | 011 | 111 = 111
		nsp := nulls.NewWithSize(4)
		nsp.Add(3)
		err := tr.testUnaryAgg([]int64{2, 3, 7, 0}, nsp, func(result uint64, isEmpty bool) bool {
			return result == 7 && !isEmpty
		})
		require.NoError(t, err)
	}

	s2 := &sAggBinaryBitOr{}
	{
		tr := &simpleAggTester[[]byte, []byte]{
			source: s2,
			grow:   s2.Grows,
			free:   s2.Free,
			fill:   s2.Fill,
			merge:  s2.Merge,
			eval:   s2.Eval,
		}
		err := tr.testUnaryAgg(generateBytesList("010", "011", "111"), nil, func(result []byte, isEmpty bool) bool {
			return string(result) == "111" && !isEmpty
		})
		require.NoError(t, err)
	}
	{
		data, err := s1.MarshalBinary()
		require.NoError(t, err)
		s3 := new(sAggBitXor[int64])
		err = s3.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestBitXor(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggBitXor, AggBitXorSupportedParameters, AggBitXorReturnType))

	s1 := &sAggBitXor[int64]{}
	{
		tr := &simpleAggTester[int64, uint64]{
			source: s1,
			grow:   s1.Grows,
			free:   s1.Free,
			fill:   s1.Fill,
			merge:  s1.Merge,
			eval:   s1.Eval,
		}
		// 010 ^ 011 ^ 111 = 110
		nsp := nulls.NewWithSize(4)
		nsp.Add(3)
		err := tr.testUnaryAgg([]int64{2, 3, 7, 0}, nsp, func(result uint64, isEmpty bool) bool {
			return result == 6 && !isEmpty
		})
		require.NoError(t, err)
	}
	s2 := &sAggBinaryBitXor{}
	{
		tr := &simpleAggTester[[]byte, []byte]{
			source: s2,
			grow:   s2.Grows,
			free:   s2.Free,
			fill:   s2.Fill,
			merge:  s2.Merge,
			eval:   s2.Eval,
		}
		err := tr.testUnaryAgg(generateBytesList("010", "011", "111"), nil, func(result []byte, isEmpty bool) bool {
			return string(result) == "110" && !isEmpty
		})
		require.NoError(t, err)
	}

	{
		data, err := s1.MarshalBinary()
		require.NoError(t, err)
		s3 := new(sAggBitXor[int64])
		err = s3.UnmarshalBinary(data)
		require.NoError(t, err)
	}
}

func TestCount(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggCount, testMoAllTypes, AggCountReturnType))

	s := &sAggCount[int64]{isCountStar: false}
	{
		tr := &simpleAggTester[int64, int64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		nsp := nulls.NewWithSize(10)
		nsp.Add(0)
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nsp, func(result int64, isEmpty bool) bool {
			return !isEmpty && result == 9
		})
		require.NoError(t, err)
	}

	s2 := &sAggCount[int64]{isCountStar: true}
	{
		tr := &simpleAggTester[int64, int64]{
			source: s2,
			grow:   s2.Grows,
			free:   s2.Free,
			fill:   s2.Fill,
			merge:  s2.Merge,
			eval:   s2.Eval,
		}
		nsp := nulls.NewWithSize(10)
		nsp.Add(0)
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nsp, func(result int64, isEmpty bool) bool {
			return !isEmpty && result == 10
		})
		require.NoError(t, err)
	}

	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s3 := new(sAggCount[int64])
		err = s3.UnmarshalBinary(data)
		require.NoError(t, err)
		require.Equal(t, s.isCountStar, s3.isCountStar)
	}
}

// what's the agg function.
func TestApproxCount(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggApproxCount, testMoAllTypesWithoutArray, AggApproxCountReturnType))

	s := &sAggApproxCountDistinct[int64]{}
	{
		tr := &simpleAggTester[int64, uint64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		nsp := nulls.NewWithSize(10)
		nsp.Add(0)
		err := tr.testUnaryAgg([]int64{1, 2, 2, 3, 3, 4, 4, 5, 5, 6}, nsp, func(result uint64, isEmpty bool) bool {
			return !isEmpty && result == 5
		})
		require.NoError(t, err)
	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggApproxCountDistinct[int64])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, len(s.sk), len(s2.sk))
		for i := range s.sk {
			require.Equal(t, s.sk[i].Estimate(), s2.sk[i].Estimate())
		}
	}
}

func TestMedian(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggMedian, AggMedianSupportedParameters, AggMedianReturnType))

	s := &sAggMedian[int64]{}
	{
		tr := &simpleAggTester[int64, float64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, func(result float64, isEmpty bool) bool {
			return !isEmpty && result == 5
		})
		require.NoError(t, err)

		s.values = nil
		err = tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil, func(result float64, isEmpty bool) bool {
			return !isEmpty && result == 4.5
		})
		require.NoError(t, err)
	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggMedian[int64])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, len(s.values), len(s2.values))
		for i := range s.values {
			require.Equal(t, s.values[i], s2.values[i])
		}
	}
}

func TestVarPop(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggVarPop, AggVarianceSupportedParameters, AggVarianceReturnType))

	s := &sAggVarPop[int64]{}
	{
		tr := &simpleAggTester[int64, float64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, func(result float64, isEmpty bool) bool {
			return !isEmpty && assertx.InEpsilonF64(result, 6.666666666666667)
		})
		require.NoError(t, err)

		s.sum, s.counts = nil, nil
		nsp := nulls.NewWithSize(10)
		nsp.Add(9)
		err = tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}, nsp, func(result float64, isEmpty bool) bool {
			return !isEmpty && assertx.InEpsilonF64(result, 6.666666666666667)
		})
		require.NoError(t, err)
	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggVarPop[int64])
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, len(s.counts), len(s2.counts))
		for i := range s.counts {
			require.Equal(t, s.counts[i], s2.counts[i])
		}
		require.Equal(t, len(s.sum), len(s2.sum))
		for i := range s.sum {
			require.Equal(t, s.sum[i], s2.sum[i])
		}
	}
}

func TestStdDevPop(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggStdDevPop, AggStdDevSupportedParameters, AggStdDevReturnType))

	s := &sAggVarPop[int64]{}
	{
		tr := &simpleAggTester[int64, float64]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.EvalStdDevPop,
		}
		err := tr.testUnaryAgg([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, func(result float64, isEmpty bool) bool {
			return !isEmpty && assertx.InEpsilonF64(result, math.Sqrt(6.666666666666667))
		})
		require.NoError(t, err)
	}
}

func TestGroupConcat(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggGroupConcat, []types.T{types.T_varchar}, AggGroupConcatReturnType))

	// input strings
	var strs = []string{
		"A",
		"B",
		"C",
		"D",
	}

	// pack the input strings into [][]byte
	var packedInput = make([][]byte, len(strs))
	packers := types.NewPackerArray(len(strs), mpool.MustNewZero())
	for i, str := range strs {
		packers[i].EncodeStringType(util.UnsafeStringToBytes(str))
		packedInput[i] = packers[i].GetBuf()
	}

	nsp := nulls.NewWithSize(4)
	nsp.Add(3)
	s := &sAggGroupConcat{
		separator: ",",
	}

	{
		tr := &simpleAggTester[[]byte, []byte]{
			source: s,
			grow:   s.Grows,
			free:   s.Free,
			fill:   s.Fill,
			merge:  s.Merge,
			eval:   s.Eval,
		}
		err := tr.testUnaryAgg(packedInput, nsp, func(result []byte, isEmpty bool) bool {
			return util.UnsafeBytesToString(result) == "A,B,C" && !isEmpty
		})
		require.NoError(t, err)

	}
	{
		data, err := s.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggGroupConcat)
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, s.separator, s2.separator)
		require.Equal(t, len(s.result), len(s2.result))
		for i := range s.result {
			require.Equal(t, s.result[i], s2.result[i])
		}
	}
}

func TestClusterCenters(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggClusterCenters, AggClusterCentersSupportedParameters, AggClusterCentersReturnType))

	s1 := &sAggClusterCenters{
		clusterCnt: 2,
		distType:   kmeans.L2Distance,
		arrType:    types.T_array_float64.ToType(),
		normalize:  true, // <-- Spherical Kmeans UT
	}
	// input vectors/arrays of 7 rows with 6th row as null
	var vecf64Input = [][]byte{
		types.ArrayToBytes[float64]([]float64{1, 2, 3, 4}),
		types.ArrayToBytes[float64]([]float64{1, 2, 4, 5}),
		types.ArrayToBytes[float64]([]float64{1, 2, 4, 5}),
		types.ArrayToBytes[float64]([]float64{10, 2, 4, 5}),
		types.ArrayToBytes[float64]([]float64{10, 3, 4, 5}),
		types.ArrayToBytes[float64]([]float64{0, 0, 0, 0}),
		types.ArrayToBytes[float64]([]float64{10, 5, 4, 5}),
	}
	nsp := nulls.NewWithSize(7)
	nsp.Add(5)
	wantVecf64Output := [][]float64{
		{0.15915269938161652, 0.31830539876323305, 0.5757527355814477, 0.7349054349630643}, // approx {1, 2, 3.6666666666666665, 4.666666666666666},
		{0.8077006350571528, 0.2663717322796547, 0.3230802540228611, 0.4038503175285764},   // approx {10, 3.333333333333333, 4, 5}
	}

	{
		// Test aggFn
		tr := &simpleAggTester[[]byte, []byte]{
			source: s1,
			grow:   s1.Grows,
			free:   s1.Free,
			fill:   s1.Fill,
			merge:  s1.Merge,
			eval:   s1.Eval,
		}
		err := tr.testUnaryAgg(vecf64Input, nsp, func(result []byte, isEmpty bool) bool {
			var vecf64Output [][]float64
			err := json.Unmarshal(result, &vecf64Output)
			//t.Log(vecf64Output)
			return err == nil && !isEmpty && assertx.InEpsilonF64Slices(wantVecf64Output, vecf64Output)
		})

		require.NoError(t, err)
	}

	{
		// Test Marshall and Unmarshall
		data, err := s1.MarshalBinary()
		require.NoError(t, err)
		s2 := new(sAggClusterCenters)
		err = s2.UnmarshalBinary(data)
		require.NoError(t, err)

		require.Equal(t, s1.arrType, s2.arrType)
		require.Equal(t, s1.clusterCnt, s2.clusterCnt)
		require.Equal(t, s1.distType, s2.distType)
		require.Equal(t, s1.initType, s2.initType)
		require.Equal(t, s1.normalize, s2.normalize)
		require.Equal(t, len(s1.groupedData), len(s2.groupedData))
		require.Equal(t, s1.groupedData, s2.groupedData)
	}
}

func TestBitmapOr(t *testing.T) {
	require.NoError(t, testUnaryAggSupported(NewAggBitmapConstruct, AggBitmapConstructSupportedParameters, AggBitmapConstructReturnType))
	s1 := &sAggBitmapConstruct{}
	{
		tr := &simpleAggTester[uint64, []byte]{
			source: s1,
			grow:   s1.Grows,
			free:   s1.Free,
			fill:   s1.Fill,
			merge:  s1.Merge,
			eval:   s1.Eval,
		}
		nsp := nulls.NewWithSize(5)
		nsp.Add(3)
		err := tr.testUnaryAgg([]uint64{1, 3, 6, 0, 9}, nsp, func(result []byte, isEmpty bool) bool {
			bmp := roaring.New()
			_ = bmp.UnmarshalBinary(result)
			return !isEmpty && bmp.GetCardinality() == 4 && bmp.String() == "{1,3,6,9}"
		})
		require.NoError(t, err)
	}

	require.NoError(t, testUnaryAggSupported(NewAggBitmapOr, AggBitmapOrSupportedParameters, AggBitmapOrReturnType))
	s2 := &sAggBitmapOr{}
	{
		tr := &simpleAggTester[[]byte, []byte]{
			source: s2,
			grow:   s2.Grows,
			free:   s2.Free,
			fill:   s2.Fill,
			merge:  s2.Merge,
			eval:   s2.Eval,
		}

		genBytes := func(vals []int) []byte {
			bmp := roaring.New()
			for _, val := range vals {
				bmp.Add(uint32(val))
			}
			bytes, _ := bmp.MarshalBinary()
			return bytes
		}
		bytes0 := genBytes([]int{1, 3, 5, 7, 9})
		bytes1 := genBytes([]int{0, 2, 4, 6, 8})
		bytes2 := genBytes([]int{2, 3, 5, 7, 11})
		err := tr.testUnaryAgg([][]byte{bytes0, bytes1, bytes2}, nil, func(result []byte, isEmpty bool) bool {
			bmp := roaring.New()
			_ = bmp.UnmarshalBinary(result)
			return !isEmpty && bmp.GetCardinality() == 11 && bmp.String() == "{0,1,2,3,4,5,6,7,8,9,11}"
		})
		require.NoError(t, err)
	}
}
