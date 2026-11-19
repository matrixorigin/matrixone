// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// #30775: the JSON literal null is a value. Modifying a literal-null document
// keeps it a JSON literal null; it must never surface as SQL NULL from the
// function result.
func TestJsonModifyLiteralNullDocumentStaysValue(t *testing.T) {
	raw := []byte(`null`)
	paths := []*bytejson.Path{makePath(t, "$.x")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(3)}

	for name, fn := range map[string]func([]byte, []*bytejson.Path, []bytejson.ByteJson) (bytejson.ByteJson, error){
		"json_set":     computeStringJsonSet,
		"json_insert":  computeStringJsonInsert,
		"json_replace": computeStringJsonReplace,
	} {
		bj, err := fn(raw, paths, newVal)
		require.NoError(t, err, name)
		require.True(t, bj.IsNull(), "%s: literal null must stay a JSON value, got %#v", name, bj)
		require.Equal(t, bytejson.TpCodeLiteral, bj.Type, name)
	}
}

// #30774: MySQL evaluates a scalar's index zero as the autowrapped scalar
// itself, so SET/REPLACE replace the scalar and INSERT leaves it unchanged
// instead of wrapping everything in an array.
func TestJsonModifyScalarIndexZero(t *testing.T) {
	three := []bytejson.ByteJson{makeByteJsonInt(3)}

	// root scalar 1
	set, err := computeStringJsonSet([]byte(`1`), []*bytejson.Path{makePath(t, "$[0]")}, three)
	require.NoError(t, err)
	require.Equal(t, int64(3), set.GetInt64())
	require.Equal(t, bytejson.TpCodeInt64, set.Type)

	ins, err := computeStringJsonInsert([]byte(`1`), []*bytejson.Path{makePath(t, "$[0]")}, three)
	require.NoError(t, err)
	require.Equal(t, int64(1), ins.GetInt64())

	rep, err := computeStringJsonReplace([]byte(`1`), []*bytejson.Path{makePath(t, "$[0]")}, three)
	require.NoError(t, err)
	require.Equal(t, int64(3), rep.GetInt64())

	// nested scalar {"a":1} via $.a[0]: compare by re-reading the stored value.
	for name, want := range map[string]int64{"set": 3, "insert": 1, "replace": 3} {
		var bj bytejson.ByteJson
		var err error
		switch name {
		case "set":
			bj, err = computeStringJsonSet([]byte(`{"a":1}`), []*bytejson.Path{makePath(t, "$.a[0]")}, three)
		case "insert":
			bj, err = computeStringJsonInsert([]byte(`{"a":1}`), []*bytejson.Path{makePath(t, "$.a[0]")}, three)
		default:
			bj, err = computeStringJsonReplace([]byte(`{"a":1}`), []*bytejson.Path{makePath(t, "$.a[0]")}, three)
		}
		require.NoError(t, err, name)
		rounded, rerr := types.ParseSliceToByteJson([]byte(unmarshalToOutput(bj)))
		require.NoError(t, rerr, name)
		nested := makePath(t, "$.a")
		picked, exists := rounded.QueryWithExists([]*bytejson.Path{nested})
		require.True(t, exists, name)
		require.Equal(t, bytejson.TpCodeInt64, picked.Type, name)
		require.Equal(t, want, picked.GetInt64(), name)
	}

	// pre-existing array [1,2] keeps ordinary array semantics: $[0]=3, $[1]=2.
	aset, err := computeStringJsonSet([]byte(`[1,2]`), []*bytejson.Path{makePath(t, "$[0]")}, three)
	require.NoError(t, err)
	rounded, rerr := types.ParseSliceToByteJson([]byte(unmarshalToOutput(aset)))
	require.NoError(t, rerr)
	require.Equal(t, bytejson.TpCodeArray, rounded.Type)
	require.Equal(t, 2, rounded.GetElemCnt())
	first := makePath(t, "$[0]")
	second := makePath(t, "$[1]")
	p0, e0 := rounded.QueryWithExists([]*bytejson.Path{first})
	require.True(t, e0)
	require.Equal(t, int64(3), p0.GetInt64())
	p1, e1 := rounded.QueryWithExists([]*bytejson.Path{second})
	require.True(t, e1)
	require.Equal(t, int64(2), p1.GetInt64())
}

// #30781: JSON_LENGTH of an existing JSON literal null is 1 (a scalar), for
// both the document root and an existing path; a missing path stays NULL.
func TestJsonLengthLiteralNullIsScalarLength(t *testing.T) {
	root, _ := types.ParseSliceToByteJson([]byte(`{"a":null}`))
	p := makePath(t, "$.a")
	val, exists := root.QueryWithExists([]*bytejson.Path{p})
	require.True(t, exists)
	require.True(t, val.IsNull())
	require.Equal(t, int64(1), jsonValueLength(val))

	nullDoc, _ := types.ParseSliceToByteJson([]byte(`null`))
	require.Equal(t, int64(1), jsonValueLength(nullDoc))

	empty, _ := types.ParseSliceToByteJson([]byte(`{}`))
	pMiss := makePath(t, "$.b")
	_, missExists := empty.QueryWithExists([]*bytejson.Path{pMiss})
	require.False(t, missExists)
}

func unmarshalToOutput(bj bytejson.ByteJson) string {
	// String() renders the JSON text form; Marshal produces the binary
	// bytejson payload, which is not JSON text.
	return bj.String()
}

// #30775, executor level: JSON_SET/INSERT/REPLACE on a literal-null JSON
// document must return the JSON null document as a value, not SQL NULL - the
// projection result column therefore has no null bit set.
func TestJsonModifyExecutorKeepsLiteralNullValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	encoded := makeJSONEncodedFromText(t, []string{`null`}, []bool{false})
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), encoded, []bool{false}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"$.x"}, []bool{false}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{3}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_json.ToType(), false, encoded, []bool{false})

	op := &opBuiltInJsonSet{}
	for _, tc := range []struct {
		name string
		fn   executeLogicOfOverload
	}{
		{"json_set", op.buildJsonSet},
		{"json_insert", op.buildJsonInsert},
		{"json_replace", op.buildJsonReplace},
	} {
		fcTC := NewFunctionTestCase(proc, inputs, expect, tc.fn)
		succeed, info := fcTC.RunAndFree()
		require.True(t, succeed, tc.name+": "+info)
	}
}
