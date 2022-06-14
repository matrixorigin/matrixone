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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/stretchr/testify/require"
)

func mockFunctionRegister() [][]Function {
	mockRegister := make([][]Function, 2)
	// function f1
	mockRegister[0] = []Function{
		{
			Index:       0,
			Args:        []types.T{types.T_int64, types.T_int64},
			TypeCheckFn: strictTypeCheck,
		},
		{
			Index:       1,
			Args:        []types.T{types.T_int64, types.T_float64},
			TypeCheckFn: strictTypeCheck,
		},
	}
	// function f2
	mockRegister[1] = []Function{
		{
			Index: 0,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) bool {
				return len(inputTypes) < 3
			},
		},
		{
			Index: 1,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T, _ types.T) bool {
				return len(inputTypes) == 1
			},
		},
	}
	return mockRegister
}

func mockFunctionIdRegister() map[string]int32 {
	mockIds := make(map[string]int32)
	mockIds["f1"] = 0
	mockIds["f2"] = 1
	return mockIds
}

func TestFunctionEqual(t *testing.T) {
	fr := mockFunctionRegister()
	fs1 := fr[0]
	fs2 := fr[1]

	require.Equal(t, true, functionsEqual(fs1[0], fs1[0]))
	require.Equal(t, false, functionsEqual(fs1[0], fs1[1]))
	require.Equal(t, true, functionsEqual(fs2[0], fs2[0]))
	require.Equal(t, false, functionsEqual(fs2[0], fs1[0]))
	require.Equal(t, false, functionsEqual(fs2[0], fs2[1]))
}

func TestFunctionRegister(t *testing.T) {
	const notFound = -1
	functionRegister = mockFunctionRegister()
	functionIdRegister = mockFunctionIdRegister()

	testCases := []struct {
		id    int
		fname string
		args  []types.T
		index int // expected function's index
	}{
		{
			id:    0,
			fname: "f1",
			args:  []types.T{types.T_int64, types.T_int64},
			index: 0,
		},
		{
			id:    1,
			fname: "f2",
			args:  nil,
			index: 0,
		},
		{
			id:    2,
			fname: "f1",
			args:  []types.T{types.T_int64, ScalarNull},
			index: notFound,
		},
		{
			id:    3,
			fname: "f2",
			args:  []types.T{types.T_int64, types.T_int64, types.T_int64},
			index: notFound,
		},
		{
			id:    4,
			fname: "f2",
			args:  []types.T{types.T_int64},
			index: notFound,
		},
		{
			id:    5,
			fname: "f2",
			args:  []types.T{types.T_int64, types.T_int32},
			index: 0,
		},
		{
			id:    6,
			fname: "f3",
			args:  []types.T{types.T_int64, types.T_int64},
			index: notFound,
		},
	}

	for _, tc := range testCases {
		msg := fmt.Sprintf("case id is %d", tc.id)

		f1, fid, _, err := GetFunctionByName(tc.fname, tc.args)
		if tc.index == notFound {
			require.Equal(t, emptyFunction, f1, msg)
		} else {
			require.NoError(t, err)
			f2, err2 := GetFunctionByID(fid)
			require.NoError(t, err2, msg)
			require.Equal(t, true, functionsEqual(f1, f2), msg)
		}
	}

	// test errMsg
	{
		_, _, _, err := GetFunctionByName("testFunctionName", nil)
		require.Equal(t, errors.New(errno.UndefinedFunction, "function 'testFunctionName' doesn't register, get id failed"), err)
	}
	{
		_, _, _, err := GetFunctionByName("f1", []types.T{})
		require.Equal(t, errors.New(errno.UndefinedFunction, "undefined function f1[]"), err)
	}
	{
		errMessage := "too much function matches:\n" +
			"f1[BIGINT BIGINT]\n" +
			"f1[BIGINT DOUBLE]"
		_, _, _, err := GetFunctionByName("f1", []types.T{types.T_int64, ScalarNull})
		require.Equal(t, errors.New(errno.SyntaxError, errMessage), err)
	}
}

func TestFunctionOverloadID(t *testing.T) {
	tcs := []struct {
		fid        int32
		overloadId int32
	}{
		{fid: 0, overloadId: 0},
		{fid: 1, overloadId: 10},
		{fid: 10, overloadId: 15},
		{fid: 400, overloadId: 1165},
		{fid: 3004, overloadId: 12345},
	}
	for _, tc := range tcs {
		f := EncodeOverloadID(tc.fid, tc.overloadId)
		actualF, actualO := DecodeOverloadID(f)
		require.Equal(t, tc.fid, actualF)
		require.Equal(t, tc.overloadId, actualO)
	}
}
