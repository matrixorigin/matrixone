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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func mockFunctionRegister() map[string][]Function {
	mockRegister := make(map[string][]Function)
	mockRegister["f1"] = []Function{
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
	mockRegister["f2"] = []Function{
		{
			Index: 0,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T) bool {
				if len(inputTypes) < 3 {
					return true
				}
				return false
			},
		},
		{
			Index: 1,
			TypeCheckFn: func(inputTypes []types.T, _ []types.T) bool {
				if len(inputTypes) == 1 {
					return true
				}
				return false
			},
		},
	}
	return mockRegister
}

func TestFunctionEqual(t *testing.T) {
	fr := mockFunctionRegister()
	fs1 := fr["f1"]
	fs2 := fr["f2"]

	require.Equal(t, true, functionsEqual(fs1[0], fs1[0]))
	require.Equal(t, false, functionsEqual(fs1[0], fs1[1]))
	require.Equal(t, true, functionsEqual(fs2[0], fs2[0]))
	require.Equal(t, false, functionsEqual(fs2[0], fs1[0]))
	require.Equal(t, false, functionsEqual(fs2[0], fs2[1]))
}

func TestFunctionRegister(t *testing.T) {
	const notFound = -1
	functionRegister = mockFunctionRegister()

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
			args:  []types.T{types.T_int64, NullType},
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

		f1, err := GetFunctionByName(tc.fname, tc.args)
		if tc.index == notFound {
			require.Equal(t, emptyFunction, f1, msg)
		} else {
			require.NoError(t, err)
			f2, err2 := GetFunctionByIndex(tc.fname, f1.Index)
			require.NoError(t, err2, msg)
			require.Equal(t, true, functionsEqual(f1, f2), msg)
		}
	}
}
