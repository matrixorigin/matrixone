// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type indexSubPartRow struct {
	name       string
	params     string
	nullParams bool
	column     string
	nullColumn bool
	want       int64
	missing    bool
}

// TestInternalIndexSubPartDeclaredPrefixes pins the retained prefix-length
// semantics of mo_catalog.mo_indexes.algo_params through both persisted
// encodings: the length a part declares is numeric, and every identity or
// shape without a retained prefix stays SQL NULL rather than fabricating
// one (#30935).
func TestInternalIndexSubPartDeclaredPrefixes(t *testing.T) {
	declared := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}
	resolved, err := GetFunctionByName(context.Background(), "mo_index_sub_part", declared)
	require.NoError(t, err, "the retained prefix index metadata function must resolve")
	require.Equal(t, types.T_int64.ToType(), resolved.retType)

	for _, tc := range []indexSubPartRow{
		{name: "legacy csv prefix length", params: `{"prefix_lengths":"name:10"}`, column: "name", want: 10},
		{name: "legacy csv keeps per-part values", params: `{"prefix_lengths":"a:10,b:20"}`, column: "b", want: 20},
		{name: "v2 json encodes delimiter-bearing names", params: `{"prefix_lengths_v2":"{\"a:weird\":7}"}`, column: "a:weird", want: 7},
		{name: "mixed metadata keeps other parts null", params: `{"lists":"2","prefix_lengths":"a:10"}`, column: "c", want: 0, missing: true},
		{name: "no prefix metadata stays null", params: `{"lists":"2"}`, column: "a", want: 0, missing: true},
		{name: "empty params stay null", column: "a", want: 0, missing: true},
		{name: "malformed metadata degrades to null", params: `{"prefix_lengths":"a:"}`, column: "a", want: 0, missing: true},
		{name: "metadata without prefixes stays null", params: `{"algo_type":"ann"}`, column: "a", want: 0, missing: true},
		{name: "legacy case mismatch matches rename spelling", params: `{"prefix_lengths":"Name:10"}`, column: "name", want: 10},
		{name: "null params stay null", nullParams: true, column: "a", want: 0, missing: true},
		{name: "null column stays null", params: `{"prefix_lengths":"a:10"}`, nullColumn: true, want: 0, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.params}, []bool{tc.nullParams}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.column}, []bool{tc.nullColumn}),
			}
			result := NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{tc.want}, []bool{tc.missing})
			op := allSupportedFunctions[MO_INDEX_SUB_PART].Overloads[0].newOp()
			testCase := NewFunctionTestCase(testutil.NewProcess(t), inputs, result, op)
			ok, info := testCase.RunAndFree()
			require.True(t, ok, tc.name+": "+info)
		})
	}
}
