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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestJSONSearchRegistrationAndTypeCheck(t *testing.T) {
	ctx := context.Background()
	for _, args := range [][]types.Type{
		{types.T_json.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		{types.T_text.ToType(), types.T_char.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		{types.T_any.ToType(), types.T_any.ToType(), types.T_any.ToType()},
	} {
		resolved, err := GetFunctionByName(ctx, "json_search", args)
		require.NoError(t, err)
		require.Equal(t, int32(JSON_SEARCH), resolved.fid)
		require.Equal(t, types.T_json, resolved.retType.Oid)
	}

	for _, args := range [][]types.Type{
		{types.T_json.ToType(), types.T_varchar.ToType()},
		{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
		{types.T_json.ToType(), types.T_int64.ToType(), types.T_varchar.ToType()},
		{types.T_json.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()},
	} {
		_, err := GetFunctionByName(ctx, "json_search", args)
		require.Error(t, err)
	}

	overload, err := GetFunctionByName(ctx, "json_search", []types.Type{
		types.T_json.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	})
	require.NoError(t, err)
	require.False(t, DeduceNotNullable(overload.GetEncodedOverloadID(), nil))
}

func TestJSONSearchOneAllAndLocations(t *testing.T) {
	proc := testutil.NewProcess(t)
	document := `{"a":["x","x"],"one potato":{"x":"x"},"nested":[{"x":"x"}],"n":1,"bool":true,"z":null}`

	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"one"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.Equal(t, `"$.a[0]"`, jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, document)}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"one"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.Equal(t, `"$.a[0]"`, jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"ALL"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.JSONEq(t, `["$.a[0]","$.a[1]","$.nested[0].x","$.\"one potato\".x"]`,
		jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"all"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.a[*]"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.a[1]"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.JSONEq(t, `["$.a[0]","$.a[1]"]`, jsonVectorRowString(t, vec, 0))
}

func TestJSONSearchCaseNullAndLikeEscapes(t *testing.T) {
	proc := testutil.NewProcess(t)
	document := `["abc","ABC","a_b","a%b","a\\b"]`

	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"all"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"abc"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.JSONEq(t, `["$[0]"]`, jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"all"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a!_b"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"!"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.JSONEq(t, `["$[2]"]`, jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"all"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`a\%b`}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.JSONEq(t, `["$[3]"]`, jsonVectorRowString(t, vec, 0))

	vec = runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{document}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"all"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"no-match"}, nil),
		},
		types.T_json.ToType(), JSONSearch, nil)
	require.True(t, vec.IsNull(0))
}

func TestJSONSearchDocumentAndPathOrdering(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tt := range []struct {
		name         string
		document     string
		documentNull bool
		mode         string
		modeNull     bool
		term         string
		termNull     bool
		path         string
		pathNull     bool
		withPath     bool
		wantError    string
		wantNull     bool
	}{
		{name: "document null suppresses invalid mode", documentNull: true, mode: "invalid", term: "x", wantNull: true},
		{name: "invalid document precedes mode", document: "not json", mode: "invalid", term: "x", wantError: "invalid JSON document"},
		{name: "invalid mode precedes term null", document: `{"a":"x"}`, mode: "invalid", termNull: true, wantError: "Incorrect arguments to json_search"},
		{name: "null term suppresses malformed path", document: `{"a":"x"}`, mode: "one", termNull: true, path: "not-a-path", withPath: true, wantNull: true},
		{name: "null path returns null", document: `{"a":"x"}`, mode: "one", term: "x", pathNull: true, withPath: true, wantNull: true},
		{name: "malformed path errors", document: `{"a":"x"}`, mode: "one", term: "x", path: "not-a-path", withPath: true, wantError: "invalid json path"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.document}, []bool{tt.documentNull}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.mode}, []bool{tt.modeNull}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.term}, []bool{tt.termNull}),
			}
			if tt.withPath {
				inputs = append(inputs,
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tt.path}, []bool{tt.pathNull}),
				)
			}
			fc := NewFunctionTestCase(
				proc,
				inputs,
				NewFunctionTestResult(types.T_json.ToType(), tt.wantError != "", nil, nil),
				JSONSearch,
			)
			defer fc.Free()
			require.NoError(t, fc.result.PreExtendAndReset(1))
			err := fc.fn(fc.parameters, fc.result, proc, 1, nil)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantNull, fc.result.GetResultVector().IsNull(0))
		})
	}
}

func TestJSONSearchPreparedDomainRejectsNonText(t *testing.T) {
	proc := testutil.NewProcess(t)
	document, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`{"a":"x"}`), 1, proc.Mp())
	require.NoError(t, err)
	defer document.Free(proc.Mp())
	mode, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("one"), 1, proc.Mp())
	require.NoError(t, err)
	defer mode.Free(proc.Mp())
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("1"), false, proc.Mp()))
	input.SetPrepareParamKind(vector.PrepareParamInteger)
	input.SetPrepareParamType(types.T_int64)

	result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.ErrorContains(t, JSONSearch(
		[]*vector.Vector{document, mode, input},
		result,
		proc,
		1,
		nil,
	), "json_search argument")
}

func TestJSONSearchPreparedTextAndBinaryProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	document, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`{"a":"x"}`), 1, proc.Mp())
	require.NoError(t, err)
	defer document.Free(proc.Mp())
	mode, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("one"), 1, proc.Mp())
	require.NoError(t, err)
	defer mode.Free(proc.Mp())

	term := vector.NewVec(types.T_varchar.ToType())
	defer term.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(term, []byte("x"), false, proc.Mp()))
	term.SetPrepareParamType(types.T_varchar)

	result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JSONSearch(
		[]*vector.Vector{document, mode, term}, result, proc, 1, nil))
	require.Equal(t, `"$.a"`, jsonVectorRowString(t, result.GetResultVector(), 0))

	term.ResetWithSameType()
	require.NoError(t, vector.AppendBytes(term, []byte("x"), false, proc.Mp()))
	term.SetIsBinaryString(true)
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, JSONSearch(
		[]*vector.Vector{document, mode, term}, result, proc, 1, nil))

	term.ResetWithSameType()
	require.NoError(t, vector.AppendBytes(term, nil, true, proc.Mp()))
	term.SetPrepareParamType(types.T_int64)
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JSONSearch(
		[]*vector.Vector{document, mode, term}, result, proc, 1, nil))
	require.True(t, result.GetResultVector().IsNull(0))
}

func TestJSONSearchErrorDoesNotPoisonReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	document := vector.NewVec(types.T_varchar.ToType())
	mode := vector.NewVec(types.T_varchar.ToType())
	term := vector.NewVec(types.T_varchar.ToType())
	defer document.Free(proc.Mp())
	defer mode.Free(proc.Mp())
	defer term.Free(proc.Mp())

	result := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	defer result.Free()

	run := func(doc, matchMode, search string) error {
		document.ResetWithSameType()
		mode.ResetWithSameType()
		term.ResetWithSameType()
		require.NoError(t, vector.AppendBytes(document, []byte(doc), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(mode, []byte(matchMode), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(term, []byte(search), false, proc.Mp()))
		require.NoError(t, result.PreExtendAndReset(1))
		return JSONSearch(
			[]*vector.Vector{document, mode, term},
			result,
			proc,
			1,
			nil,
		)
	}

	require.Error(t, run(`{"a":"x"}`, "invalid", "x"))
	require.NoError(t, run(`{"a":"x"}`, "one", "x"))
	require.Equal(t, `"$.a"`, jsonVectorRowString(t, result.GetResultVector(), 0))
}

func TestJSONSearchPreservesDocumentDepthError(t *testing.T) {
	proc := testutil.NewProcess(t)
	tooDeep := strings.Repeat(`{"a":`, bytejson.JSONDocumentMaxNestingDepth+1) +
		`"x"` + strings.Repeat(`}`, bytejson.JSONDocumentMaxNestingDepth+1)
	fc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{tooDeep}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"one"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		NewFunctionTestResult(types.T_json.ToType(), true, nil, nil),
		JSONSearch,
	)
	defer fc.Free()
	require.NoError(t, fc.result.PreExtendAndReset(1))
	err := fc.fn(fc.parameters, fc.result, proc, 1, nil)
	require.ErrorContains(t, err, "nesting depth exceeds 100")
}

func TestJSONSearchIgnoreAllRowsSkipsArgumentErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec := runJsonFunctionWithSelectList(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-json"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"invalid-mode"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		types.T_json.ToType(), JSONSearch,
		&FunctionSelectList{AllNull: true},
	)
	require.Equal(t, 1, vec.Length())
	require.True(t, vec.IsNull(0))
}
