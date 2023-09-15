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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/stretchr/testify/require"
	"testing"
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
	eval  func([]outputT, error) ([]outputT, error)
}

// please make sure the inputValues is not empty. and make sure its length is not less than inputNsp.Length().
func (tr *simpleAggTester[in, out]) testUnaryAgg(
	inputValues []in, inputNsp *nulls.Nulls,
	check func(results out, isEmpty bool) bool) error {

	var err error
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
			return moerr.NewInternalErrorNoCtx("agg test failed. fill failed.")
		}
	}

	for i := half; i < length; i++ {
		if inputNsp != nil && inputNsp.Contains(uint64(i)) {
			results[1], empties[1], err = tr.fill(1, inputValues[i], results[1], 1, empties[1], true)
		} else {
			results[1], empties[1], err = tr.fill(1, inputValues[i], results[1], 1, empties[1], false)
		}
		if err != nil {
			return moerr.NewInternalErrorNoCtx("agg test failed. fill failed.")
		}
	}

	results[0], empties[0], err = tr.merge(0, 1, results[0], results[1], empties[0], empties[1], tr.source)
	if err != nil {
		return moerr.NewInternalErrorNoCtx("agg test failed. merge failed.")
	}

	results = results[:1]
	results, err = tr.eval(results, nil)
	if err != nil {
		return moerr.NewInternalErrorNoCtx("agg test failed. eval failed.")
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
	require.NoError(t, testUnaryAggSupported(NewAggMin, AggMinSupportedParameters, AggMinxReturnType))

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
