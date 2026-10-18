// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestStrictBinaryCapacityCountsBytes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		oid             types.T
		width           int32
		input, expected string
		failure         bool
	}{
		{"multibyte overflow", types.T_varbinary, 3, "😀", "", true},
		{"fixed multibyte overflow", types.T_binary, 3, "😀", "", true},
		{"exact capacity", types.T_varbinary, 4, "😀", "😀", false},
		{"fixed padding", types.T_binary, 8, "😀", "😀\x00\x00\x00\x00", false},
		{"no trailing space exemption", types.T_varbinary, 3, "abc ", "", true},
		{"empty fixed value", types.T_binary, 4, "", "\x00\x00\x00\x00", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			target := types.New(tc.oid, tc.width, 0)
			caseTest := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.input}, nil),
				NewFunctionTestInput(target, []string{""}, nil),
			}, NewFunctionTestResult(target, tc.failure, []string{tc.expected}, nil), NewStrictCast)
			ok, info := caseTest.RunAndFree()
			require.True(t, ok, info)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestStrictBinaryCapacitySkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.New(types.T_varbinary, 3, 0)
	caseTest := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"😀", "x"}, nil),
		NewFunctionTestInput(target, []string{"", ""}, nil),
	}, NewFunctionTestResult(target, false, []string{"", "x"}, []bool{true, false}), NewStrictCast).
		WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	ok, info := caseTest.RunAndFree()
	require.True(t, ok, info)
	require.Zero(t, proc.Mp().CurrNB())
}
