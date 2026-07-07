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

package bytejson

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Modify(t *testing.T) {
	path, _ := ParseJsonPath("$.a")
	type args struct {
		pathList    []*Path
		valList     []ByteJson
		modifyType  JsonModifyType
		expectedErr bool
	}
	tests := []args{
		// path is empty
		{
			pathList:    []*Path{},
			valList:     []ByteJson{},
			modifyType:  JsonModifyReplace,
			expectedErr: false,
		},
		// path length is not equal to val length
		{
			pathList:    []*Path{},
			valList:     []ByteJson{Null},
			modifyType:  JsonModifyReplace,
			expectedErr: true,
		},
		// modifyType is not valid
		{

			pathList:    []*Path{&path},
			valList:     []ByteJson{Null},
			modifyType:  JsonModifyType(100),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		bj := Null
		_, err := bj.Modify(test.pathList, test.valList, test.modifyType)
		if test.expectedErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestRemove(t *testing.T) {
	tests := []struct {
		name  string
		input string
		paths []string
		want  string
	}{
		{
			name:  "object key",
			input: `{"a":1,"b":2}`,
			paths: []string{"$.a"},
			want:  `{"b": 2}`,
		},
		{
			name:  "array element compacts",
			input: `["a",["b","c"],"d"]`,
			paths: []string{"$[1]"},
			want:  `["a", "d"]`,
		},
		{
			name:  "nested object key",
			input: `{"a":{"b":1,"c":2},"d":3}`,
			paths: []string{"$.a.b"},
			want:  `{"a": {"c": 2}, "d": 3}`,
		},
		{
			name:  "missing path is noop",
			input: `{"a":1}`,
			paths: []string{"$.b", "$.a.b", "$[1]"},
			want:  `{"a": 1}`,
		},
		{
			name:  "paths are applied left to right",
			input: `["a","b","c","d"]`,
			paths: []string{"$[1]", "$[1]"},
			want:  `["a", "d"]`,
		},
		{
			name:  "scalar array index zero autowrap removes target",
			input: `{"a":1,"b":2}`,
			paths: []string{"$.a[0]"},
			want:  `{"b": 2}`,
		},
		{
			name:  "root object array index zero autowrap removes member",
			input: `{"a":1,"b":2}`,
			paths: []string{"$[0].a"},
			want:  `{"b": 2}`,
		},
		{
			name:  "root scalar array index zero autowrap is noop",
			input: `1`,
			paths: []string{"$[0]"},
			want:  `1`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj := mustParseByteJson(t, tt.input)
			paths := make([]*Path, 0, len(tt.paths))
			for _, pathStr := range tt.paths {
				path, err := ParseJsonPath(pathStr)
				require.NoError(t, err)
				paths = append(paths, &path)
			}

			got, err := bj.Remove(paths)
			require.NoError(t, err)
			require.Equal(t, tt.want, mustMarshalByteJson(t, got))
		})
	}
}

func TestRemoveRejectsNonSimplePath(t *testing.T) {
	bj := mustParseByteJson(t, `{"a":1}`)
	path, err := ParseJsonPath("$.*")
	require.NoError(t, err)

	_, err = bj.Remove([]*Path{&path})
	require.Error(t, err)
}

func TestAppendBinaryJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		wantType TpCode
		wantErr  bool
	}{
		{
			name:     "nil value",
			input:    nil,
			wantType: TpCodeLiteral,
			wantErr:  false,
		},
		{
			name:     "bool true",
			input:    true,
			wantType: TpCodeLiteral,
			wantErr:  false,
		},
		{
			name:     "bool false",
			input:    false,
			wantType: TpCodeLiteral,
			wantErr:  false,
		},
		{
			name:     "int64",
			input:    int64(123),
			wantType: TpCodeInt64,
			wantErr:  false,
		},
		{
			name:     "uint64",
			input:    uint64(123),
			wantType: TpCodeUint64,
			wantErr:  false,
		},
		{
			name:     "float64",
			input:    float64(123.45),
			wantType: TpCodeFloat64,
			wantErr:  false,
		},
		{
			name:     "string",
			input:    "test",
			wantType: TpCodeString,
			wantErr:  false,
		},
		{
			name:     "array",
			input:    []any{int64(1), int64(2), true},
			wantType: TpCodeArray,
			wantErr:  false,
		},
		{
			name:     "object",
			input:    map[string]any{"key": "value"},
			wantType: TpCodeObject,
			wantErr:  false,
		},
		{
			name:     "invalid type",
			input:    struct{}{},
			wantType: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotBuf, err := appendBinaryJSON(nil, tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantType, gotType)

			require.NotEmpty(t, gotBuf)

			switch tt.input.(type) {
			case nil:
				require.Equal(t, []byte{LiteralNull}, gotBuf)
			case bool:
				if tt.input.(bool) {
					require.Equal(t, []byte{LiteralTrue}, gotBuf)
				} else {
					require.Equal(t, []byte{LiteralFalse}, gotBuf)
				}
			}
		})
	}
}

func mustParseByteJson(t *testing.T, input string) ByteJson {
	t.Helper()
	var v any
	decoder := json.NewDecoder(bytes.NewBufferString(input))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&v))
	bj, err := CreateByteJSON(v)
	require.NoError(t, err)
	return bj
}

func mustMarshalByteJson(t *testing.T, bj ByteJson) string {
	t.Helper()
	out, err := bj.MarshalJSON()
	require.NoError(t, err)
	return string(out)
}

func TestAppendBinaryNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    json.Number
		wantType TpCode
		wantErr  bool
	}{
		{
			name:     "int64",
			input:    json.Number("123"),
			wantType: TpCodeInt64,
			wantErr:  false,
		},
		{
			name:     "uint64",
			input:    json.Number("18446744073709551615"),
			wantType: TpCodeUint64,
			wantErr:  false,
		},
		{
			name:     "float64",
			input:    json.Number("123.45"),
			wantType: TpCodeFloat64,
			wantErr:  false,
		},
		{
			name:     "invalid number",
			input:    json.Number("invalid"),
			wantType: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotBuf, err := appendBinaryNumber(nil, tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantType, gotType)
			require.NotEmpty(t, gotBuf)
		})
	}
}

func TestAppendBinaryString(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "simple string",
			input: "test",
		},
		{
			name:  "unicode string",
			input: "测试",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appendBinaryString(nil, tt.input)
			require.NotEmpty(t, got)

			strLen, lenLen := calStrLen(got)
			require.Equal(t, len(tt.input), strLen)
			require.True(t, lenLen > 0)

			require.Equal(t, tt.input, string(got[lenLen:]))
		})
	}
}

func Test_appendBinaryValElem(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		wantType TpCode
		wantErr  bool
	}{
		{
			name:     "invalid type",
			input:    struct{}{},
			wantType: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBuf, err := appendBinaryValElem(make([]byte, 0), 0, 0, tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, gotBuf)
		})
	}
}
