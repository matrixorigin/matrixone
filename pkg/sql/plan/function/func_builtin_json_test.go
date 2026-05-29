// Copyright 2024 Matrix Origin
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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// newTypedByteJson
// ============================================================================

func TestNewTypedByteJson(t *testing.T) {
	tests := []struct {
		name string
		tp   bytejson.TpCode
		s    string
	}{
		{"date", bytejson.TpCodeDate, "2024-01-15"},
		{"time", bytejson.TpCodeTime, "14:30:00"},
		{"datetime", bytejson.TpCodeDatetime, "2024-01-15 14:30:00"},
		{"decimal", bytejson.TpCodeDecimal, "123.456"},
		{"blob", bytejson.TpCodeBlob, "hello"},
		{"empty_string", bytejson.TpCodeString, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj := newTypedByteJson(tt.tp, tt.s)
			require.Equal(t, tt.tp, bj.Type)

			// Verify uvarint-prefixed length encoding
			l, n := binary.Uvarint(bj.Data)
			require.Equal(t, int(l), len(tt.s), "uvarint length mismatch")
			require.Equal(t, tt.s, string(bj.Data[n:]), "data mismatch")
		})
	}
}

// ============================================================================
// computeString / computeStringSimple (use raw JSON text, not internal encoding)
// ============================================================================

func makePath(t *testing.T, s string) *bytejson.Path {
	p, err := types.ParseStringToPath(s)
	require.NoError(t, err)
	return &p
}

func TestComputeString_ValidJson(t *testing.T) {
	raw := []byte(`{"a": 1, "b": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

func TestComputeString_InvalidJson(t *testing.T) {
	raw := []byte(`not json`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.Error(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeString_EmptyArray(t *testing.T) {
	raw := []byte(`[]`)
	paths := []*bytejson.Path{makePath(t, "$[0]")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeString_StringValue(t *testing.T) {
	raw := []byte(`{"a": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeString(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeString, bj.Type)
}

func TestComputeStringSimple_ValidJson(t *testing.T) {
	raw := []byte(`{"a": 1, "b": "hello"}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeStringSimple(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

func TestComputeStringSimple_InvalidJson(t *testing.T) {
	raw := []byte(`not json`)
	paths := []*bytejson.Path{makePath(t, "$.a")}

	bj, err := computeStringSimple(raw, paths)
	require.Error(t, err)
	require.Equal(t, bytejson.Null, bj)
}

func TestComputeStringSimple_SimplePath(t *testing.T) {
	raw := []byte(`{"a": {"b": 99}}`)
	paths := []*bytejson.Path{makePath(t, "$.a.b")}

	bj, err := computeStringSimple(raw, paths)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
	require.Equal(t, bytejson.TpCodeInt64, bj.Type)
}

// ============================================================================
// computeStringJsonSet / computeStringJsonInsert / computeStringJsonReplace
// ============================================================================

func makeByteJsonInt(v int64) bytejson.ByteJson {
	bj, _ := types.ParseSliceToByteJson([]byte(itoa(v)))
	return bj
}

func itoa(v int64) string {
	if v < 0 {
		return "-" + itoa(-v)
	}
	if v < 10 {
		return string([]byte{byte('0' + v)})
	}
	return itoa(v/10) + itoa(v%10)
}

func TestComputeStringJsonSet_NewKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.b")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(42)}

	bj, err := computeStringJsonSet(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonSet_OverwriteKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	bj, err := computeStringJsonSet(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonInsert_NewKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.b")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(42)}

	bj, err := computeStringJsonInsert(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonInsert_ExistingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	// INSERT should NOT overwrite existing key
	bj, err := computeStringJsonInsert(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonReplace_ExistingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.a")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	bj, err := computeStringJsonReplace(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

func TestComputeStringJsonReplace_MissingKey(t *testing.T) {
	raw := []byte(`{"a": 1}`)
	paths := []*bytejson.Path{makePath(t, "$.missing")}
	newVal := []bytejson.ByteJson{makeByteJsonInt(99)}

	// REPLACE does nothing for missing key
	bj, err := computeStringJsonReplace(raw, paths, newVal)
	require.NoError(t, err)
	require.NotEqual(t, bytejson.Null, bj)
}

// ============================================================================
// getPaths
// ============================================================================

func TestGetPaths_AllConst(t *testing.T) {
	paths := []*bytejson.Path{
		makePath(t, "$.a"),
		makePath(t, "$.b"),
	}
	op := &opBuiltInJsonExtract{
		allConst: true,
		npath:    2,
		paths:    paths,
	}

	// allConst=true returns full paths slice regardless of index
	result := op.getPaths(0)
	require.Equal(t, paths, result)

	result = op.getPaths(999)
	require.Equal(t, paths, result)
}

func TestGetPaths_NonConst(t *testing.T) {
	p0 := makePath(t, "$.a")
	p1 := makePath(t, "$.b")
	p2 := makePath(t, "$.c")
	p3 := makePath(t, "$.d")

	paths := []*bytejson.Path{p0, p1, p2, p3}
	op := &opBuiltInJsonExtract{
		allConst: false,
		npath:    2,
		paths:    paths,
	}

	// Row 0: paths[0:2]
	result := op.getPaths(0)
	require.Equal(t, []*bytejson.Path{p0, p1}, result)

	// Row 1: paths[2:4]
	result = op.getPaths(1)
	require.Equal(t, []*bytejson.Path{p2, p3}, result)
}
