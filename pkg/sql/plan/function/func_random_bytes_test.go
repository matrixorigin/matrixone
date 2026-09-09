// Copyright 2026 Matrix Origin
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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRandomBytesAcceptsBoundsAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(
				types.T_int64.ToType(),
				[]int64{1, randomBytesMaxLength, 0},
				[]bool{false, false, true},
			),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil),
		RandomBytes,
	)

	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, RandomBytes(testCase.parameters, testCase.result, proc, testCase.fnLength, nil))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, testCase.fnLength, result.Length())
	require.False(t, result.IsNull(0))
	require.Len(t, result.GetBytesAt(0), 1)
	require.False(t, result.IsNull(1))
	require.Len(t, result.GetBytesAt(1), randomBytesMaxLength)
	require.True(t, result.IsNull(2))
}

func TestRandomBytesRejectsOutOfRangeSignedLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, length := range []int64{-1, 0, randomBytesMaxLength + 1, math.MaxInt64} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), []int64{length}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
				RandomBytes,
			)

			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := RandomBytes(testCase.parameters, testCase.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
			require.EqualError(t, err, "length value is out of range in 'random_bytes'")
		})
	}
}

func TestRandomBytesRejectsOutOfRangeUnsignedLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, length := range []uint64{randomBytesMaxLength + 1, math.MaxUint64} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_uint64.ToType(), []uint64{length}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
				RandomBytes,
			)

			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := RandomBytes(testCase.parameters, testCase.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
			require.EqualError(t, err, "length value is out of range in 'random_bytes'")
		})
	}
}

func TestRandomBytesSkipsMaskedOutOfRangeRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 0}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil),
		RandomBytes,
	)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}

	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, RandomBytes(testCase.parameters, testCase.result, proc, testCase.fnLength, selectList))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 2, result.Length())
	require.False(t, result.IsNull(0))
	require.Len(t, result.GetBytesAt(0), 1)
	require.True(t, result.IsNull(1))
}

func TestRandomBytesReportsEntropySourceFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{16}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
		RandomBytes,
	)

	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := randomBytesWithReader(
		testCase.parameters,
		testCase.result,
		proc,
		1,
		nil,
		func([]byte) (int, error) { return 0, errors.New("entropy source unavailable") },
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
	require.EqualError(t, err, "internal error: random_bytes failed to generate 16 bytes: entropy source unavailable")
}
