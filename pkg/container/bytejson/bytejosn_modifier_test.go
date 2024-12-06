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
