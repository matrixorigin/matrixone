// Copyright 2022 Matrix Origin
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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestMarshalBinarySubtypesRemainLegacyReadable(t *testing.T) {
	opaque := ByteJson{Type: TpCodeOpaque, Data: appendBinaryString(nil, string([]byte{0x01, 0x02}))}
	bit := ByteJson{Type: TpCodeBit, Data: appendBinaryString(nil, string([]byte{0x01}))}
	array, err := CreateByteJSON([]any{opaque, bit})
	require.NoError(t, err)
	object, err := CreateByteJSON(map[string]any{"bit": bit, "opaque": opaque})
	require.NoError(t, err)

	tests := []struct {
		name  string
		value ByteJson
		check func(*testing.T, ByteJson)
	}{
		{
			name:  "root opaque",
			value: opaque,
			check: func(t *testing.T, got ByteJson) {
				require.Equal(t, TpCodeBlob, got.Type)
				require.Equal(t, `"AQI="`, got.String())
				length, ok := BinaryJSONPayloadLen(got)
				require.True(t, ok)
				require.Equal(t, 2, length)
			},
		},
		{
			name:  "root bit",
			value: bit,
			check: func(t *testing.T, got ByteJson) {
				require.Equal(t, TpCodeBlob, got.Type)
				require.Equal(t, "BIT", got.TYPE())
				require.Equal(t, `"AQ=="`, got.String())
			},
		},
		{
			name:  "array",
			value: array,
			check: func(t *testing.T, got ByteJson) {
				require.Equal(t, TpCodeBlob, got.GetArrayElem(0).Type)
				require.Equal(t, TpCodeBlob, got.GetArrayElem(1).Type)
				require.Equal(t, "BIT", got.GetArrayElem(1).TYPE())
				require.Equal(t, `["AQI=", "AQ=="]`, got.String())
			},
		},
		{
			name:  "object",
			value: object,
			check: func(t *testing.T, got ByteJson) {
				require.Equal(t, TpCodeBlob, got.GetObjectVal(0).Type)
				require.Equal(t, "BIT", got.GetObjectVal(0).TYPE())
				require.Equal(t, TpCodeBlob, got.GetObjectVal(1).Type)
				require.Equal(t, `{"bit": "AQ==", "opaque": "AQI="}`, got.String())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stored, err := tc.value.Marshal()
			require.NoError(t, err)
			requireLegacyJSONReadable(t, stored)

			var got ByteJson
			require.NoError(t, got.Unmarshal(stored))
			tc.check(t, got)
		})
	}
}

func TestMySQLOpaqueTaggedValue(t *testing.T) {
	payload := []byte{0x00, 0xff, 0x41}
	for _, tc := range []struct {
		name      string
		fieldType uint8
		want      string
	}{
		{name: "varbinary", fieldType: 15, want: "base64:type15:AP9B"},
		{name: "bit", fieldType: 16, want: "base64:type16:AP9B"},
		{name: "blob", fieldType: 252, want: "base64:type252:AP9B"},
		{name: "binary", fieldType: 254, want: "base64:type254:AP9B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := NewMySQLOpaque(defines.MORPCLatestVersion, tc.fieldType, payload)
			require.NoError(t, err)
			require.Equal(t, TpCodeBlob, value.Type)
			wantType := "BLOB"
			if tc.fieldType == 16 {
				wantType = "BIT"
			}
			require.Equal(t, wantType, value.TYPE())
			require.Equal(t, strconv.Quote(tc.want), value.String())
			require.Equal(t, tc.want, mustUnquote(t, value))
			length, ok := BinaryJSONPayloadLen(value)
			require.True(t, ok)
			require.Equal(t, len(payload), length)

			stored, err := value.Marshal()
			require.NoError(t, err)
			requireLegacyJSONReadable(t, stored)
			var restored ByteJson
			require.NoError(t, restored.Unmarshal(stored))
			require.Equal(t, value.String(), restored.String())
		})
	}
}

func TestMySQLOpaqueRequiresRollingUpgradeAdmission(t *testing.T) {
	require.Equal(t, int64(defines.MORPCVersion52), MySQLOpaqueProtocolVersion)

	_, err := NewMySQLOpaque(defines.MORPCVersion49, 252, []byte{0x00})
	require.ErrorContains(t, err, "MORPC protocol version "+strconv.FormatInt(MySQLOpaqueProtocolVersion, 10))

	value, err := NewMySQLOpaque(MySQLOpaqueProtocolVersion, 252, []byte{0x00})
	require.NoError(t, err)
	require.Equal(t, `"base64:type252:AA=="`, value.String())
}

func TestMySQLOpaqueTypeDoesNotAllocatePayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0x01}, 1<<20)
	value, err := NewMySQLOpaque(defines.MORPCLatestVersion, 16, payload)
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(10, func() {
		if typ := value.TYPE(); typ != "BIT" {
			t.Fatalf("unexpected type: %s", typ)
		}
	})
	require.Zero(t, allocs, "tagged BIT TYPE should not materialize the payload")
}

func TestUint64TypeRemainsInteger(t *testing.T) {
	value, err := CreateByteJSON(uint64(2024))
	require.NoError(t, err)
	require.Equal(t, "INTEGER", value.TYPE())
}

func TestBinaryJSONPayloadLenLegacyBlobLargePayloadAllocations(t *testing.T) {
	payload := bytes.Repeat([]byte{0xef}, 1<<20)
	legacy := makeBinaryJson(TpCodeBlob, []byte(base64.StdEncoding.EncodeToString(payload)))

	allocs := testing.AllocsPerRun(10, func() {
		length, ok := BinaryJSONPayloadLen(legacy)
		if !ok || length != len(payload) {
			t.Fatalf("unexpected payload length: length=%d ok=%v", length, ok)
		}
	})
	require.Less(t, allocs, float64(1), "payload length should not allocate decoded payload buffers")
}

func TestBinaryJSONPayloadLenLegacyBlobPreservesBase64Newlines(t *testing.T) {
	payload := bytes.Repeat([]byte{0xef}, 16*1024)
	encoded := base64.StdEncoding.EncodeToString(payload)
	legacyWithNewlines := makeBinaryJson(TpCodeBlob, []byte(encoded[:4095]+"\r\n"+encoded[4095:]))

	length, ok := BinaryJSONPayloadLen(legacyWithNewlines)
	require.True(t, ok)
	require.Equal(t, len(payload), length)
}

func TestBinaryJSONPayloadLenLegacyBitPreservesBase64Newlines(t *testing.T) {
	payload := bytes.Repeat([]byte{0x01}, 16*1024)
	encoded := base64.StdEncoding.EncodeToString(payload)
	legacyWithNewlines := makeBinaryJson(TpCodeBlob, []byte(persistedBitPrefix+encoded[:4095]+"\r\n"+encoded[4095:]))

	length, ok := BinaryJSONPayloadLen(legacyWithNewlines)
	require.True(t, ok)
	require.Equal(t, len(payload), length)
}

// requireLegacyJSONReadable models the pre-TpCodeOpaque/TpCodeBit reader. It
// intentionally rejects type codes newer than TpCodeBlob and recursively
// validates every value entry and offset that the old reader would follow.
func requireLegacyJSONReadable(t *testing.T, stored []byte) {
	t.Helper()
	require.NotEmpty(t, stored)
	requireLegacyJSONValueReadable(t, TpCode(stored[0]), stored[1:])
}

func requireLegacyJSONValueReadable(t *testing.T, tp TpCode, data []byte) {
	t.Helper()
	switch tp {
	case TpCodeLiteral:
		require.NotEmpty(t, data)
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		require.GreaterOrEqual(t, len(data), numberSize)
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob:
		length, prefixLen := binary.Uvarint(data)
		require.Greater(t, prefixLen, 0)
		require.LessOrEqual(t, uint64(prefixLen)+length, uint64(len(data)))
	case TpCodeArray, TpCodeObject:
		require.GreaterOrEqual(t, len(data), headerSize)
		count := int(endian.Uint32(data))
		docSize := int(endian.Uint32(data[docSizeOff:]))
		require.LessOrEqual(t, headerSize, docSize)
		require.LessOrEqual(t, docSize, len(data))
		keyTableSize := 0
		if tp == TpCodeObject {
			keyTableSize = count * keyEntrySize
			require.LessOrEqual(t, headerSize+keyTableSize+count*valEntrySize, docSize)
			for i := 0; i < count; i++ {
				off := headerSize + i*keyEntrySize
				keyOff := int(endian.Uint32(data[off:]))
				keyLen := int(endian.Uint16(data[off+keyOriginOff:]))
				require.LessOrEqual(t, keyOff+keyLen, docSize)
			}
		} else {
			require.LessOrEqual(t, headerSize+count*valEntrySize, docSize)
		}
		valTableOff := headerSize + keyTableSize
		for i := 0; i < count; i++ {
			off := valTableOff + i*valEntrySize
			childType := TpCode(data[off])
			if childType == TpCodeLiteral {
				requireLegacyJSONValueReadable(t, childType, data[off+valTypeSize:off+valEntrySize])
				continue
			}
			valueOff := int(endian.Uint32(data[off+valTypeSize:]))
			require.Less(t, valueOff, docSize)
			requireLegacyJSONValueReadable(t, childType, data[valueOff:docSize])
		}
	default:
		t.Fatalf("legacy reader does not recognize JSON type code %#x", tp)
	}
}

func TestLiteral(t *testing.T) {
	j := []string{"true", "false", "null"}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.Equal(t, x, bj.String())
	}
}

func TestEmptyJSONInputUsesStableError(t *testing.T) {
	_, err := ParseFromString("")
	require.ErrorContains(t, err, "json text is empty")

	_, err = ParseFromByteSlice(nil)
	require.ErrorContains(t, err, "json text is empty")
}

func TestParserFreesCompletedRootWhenTokenizerRejectsSuffix(t *testing.T) {
	p := parser{src: []byte(`{}x`)}
	_, err := p.do()
	require.Error(t, err)
	require.Empty(t, p.stack)
	require.Nil(t, p.top.V)
}

func TestParserFreesWideCompletedRootWhenTokenizerRejectsSuffix(t *testing.T) {
	p := parser{src: []byte(`{"values":[` + strings.Repeat(`0,`, 1024) + `0]}x`)}
	_, err := p.do()
	require.Error(t, err)
	require.Empty(t, p.stack)
	require.Nil(t, p.top.V)
}

func TestNumber(t *testing.T) {
	// generate max int64
	j := []string{
		"9223372036854775807",
		"-9223372036854775808",
		"1",
		"-1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to int64
		now, err := strconv.ParseInt(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetInt64())
	}

	// generate max uint64
	j = []string{
		"18446744073709551615",
		"0",
		"1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to uint64
		now, err := strconv.ParseUint(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetUint64())
	}

	//generate max float64
	j = []string{
		"1.7976931348623157e+308",
		"-1.7976931348623157e+308",
		"1.797693134862315708145274237317043567981e+308",
		"4.940656458412465441765687928682213723651e-324",
		"0.112131431",
		"1.13353411",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to float64
		now, err := strconv.ParseFloat(x, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetFloat64())
	}
}

func TestObject(t *testing.T) {
	j := []string{
		`{"a":1}`,
		`{"a": 1, "b": 2, "c": true, "d": false, "e": null, "f": "string", "g": [1, 2, 3], "h": {"a": 1, "b": 2}, "i": 1.1, "j": 1.1e+10, "k": 1.1e-10}`,
		`{"a":{}}`,
		`{"a":{"b":{"c":{"d":[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456]}}}}`,
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.NoError(t, err)
		require.JSONEq(t, x, bj.String())
	}
	t.Run("last win", func(t *testing.T) {
		s := `{"x": 17, "x": "red", "x": [3, 5, 7]}`
		bj, err := ParseFromString(s)
		require.NoError(t, err)
		require.JSONEq(t, `{"x":[3,5,7]}`, bj.String())
	})
	t.Run("sort key", func(t *testing.T) {
		s := `{"c":1,"a":2,"b":3}`
		bj, err := ParseFromString(s)
		require.NoError(t, err)
		require.Equal(t, `{"a": 2, "b": 3, "c": 1}`, bj.String())
	})
	t.Run("unexpected EOF", func(t *testing.T) {
		s := `{"c":1,"a":2,"b":3`
		_, err := ParseNodeString(s)
		require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
	})
}

func TestArray(t *testing.T) {
	j := []string{
		`[`,
		`[{]`,
		`[{}]`,
		`["1"]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456,1.1e+10,1.1e-10]`,
	}
	for i, x := range j {
		bj, err := ParseFromString(x)
		if i > 1 {
			require.Nil(t, err)
			require.JSONEq(t, x, bj.String())
		} else {
			require.NotNil(t, err)
		}
	}
}

func TestQuery(t *testing.T) {
	kases := []struct {
		jsonStr string
		pathStr string
		outStr  string
	}{
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.a",
			outStr:  "\"1\"",
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.b",
			outStr:  "\"2\"",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[0]",
			outStr:  "1",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[2]",
			outStr:  "3",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[*]",
			outStr:  "[1,2,3]",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}]}`,
			pathStr: "$.a[3].b",
			outStr:  "4",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}]}`,
			pathStr: "$.a[3].c",
			outStr:  "null",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}],"c":5}`,
			pathStr: "$.*",
			outStr:  `[[1,2,3,{"b":4}],5]`,
		},
		{
			jsonStr: `{"a":[1,2,3,{"a":4}]}`,
			pathStr: "$**.a",
			outStr:  `[[1,2,3,{"a":4}],4]`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[0]",
			outStr:  `{"a":1}`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[0].a",
			outStr:  `1`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[1]",
			outStr:  `null`,
		},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		path, err := ParseJsonPath(kase.pathStr)
		require.Nil(t, err)
		out := bj.Query([]*Path{&path})
		require.JSONEq(t, kase.outStr, out.String())

		if path.IsSimple() {
			out2 := bj.QuerySimple([]*Path{&path})
			require.JSONEq(t, kase.outStr, out2.String())
		}
	}
}

func TestQueryWithExistsPreservesJSONNull(t *testing.T) {
	bj, err := ParseFromString(`{"a":null,"b":1,"items":[null,2]}`)
	require.NoError(t, err)

	parsePaths := func(pathStrings ...string) []*Path {
		paths := make([]*Path, len(pathStrings))
		for i, pathString := range pathStrings {
			path, parseErr := ParseJsonPath(pathString)
			require.NoError(t, parseErr)
			paths[i] = &path
		}
		return paths
	}

	tests := []struct {
		name   string
		paths  []string
		result string
		exists bool
	}{
		{name: "existing null", paths: []string{"$.a"}, result: "null", exists: true},
		{name: "missing", paths: []string{"$.missing"}, result: "null", exists: false},
		{name: "null and value", paths: []string{"$.a", "$.b"}, result: "[null,1]", exists: true},
		{name: "all null", paths: []string{"$.a", "$.a"}, result: "[null,null]", exists: true},
		{name: "null and missing", paths: []string{"$.a", "$.missing"}, result: "[null]", exists: true},
		{name: "wildcard", paths: []string{"$.items[*]"}, result: "[null,2]", exists: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			paths := parsePaths(test.paths...)
			result, exists := bj.QueryWithExists(paths)
			require.Equal(t, test.exists, exists)
			require.JSONEq(t, test.result, result.String())

			allSimple := true
			for _, path := range paths {
				allSimple = allSimple && path.IsSimple()
			}
			if allSimple {
				simpleResult, simpleExists := bj.QuerySimpleWithExists(paths)
				require.Equal(t, test.exists, simpleExists)
				require.JSONEq(t, test.result, simpleResult.String())
			}
		})
	}
}

func TestQueryWithExistsAutowrapsScalarIndexZero(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		path    string
		expects string
	}{
		{name: "root null", json: `null`, path: `$[0]`, expects: `null`},
		{name: "nested null", json: `{"a":null}`, path: `$.a[0]`, expects: `null`},
		{name: "root scalar range", json: `1`, path: `$[0 to 0]`, expects: `[1]`},
		{name: "array wildcard", json: `[null]`, path: `$[*]`, expects: `[null]`},
		{name: "array range", json: `[null]`, path: `$[0 to 0]`, expects: `[null]`},
		{name: "object wildcard", json: `{"a":null}`, path: `$.*`, expects: `[null]`},
		{name: "recursive descent", json: `{"a":null}`, path: `$**.a`, expects: `[null]`},
		{name: "empty object range", json: `{}`, path: `$[0 to 0]`, expects: `[{}]`},
		{name: "object last range", json: `{"a":1,"b":2}`, path: `$[last to last]`, expects: `[{"a":1,"b":2}]`},
		{name: "object last range then key", json: `{"a":null,"b":2}`, path: `$[last to last].a`, expects: `[null]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bj, err := ParseFromString(test.json)
			require.NoError(t, err)
			path, err := ParseJsonPath(test.path)
			require.NoError(t, err)

			result, exists := bj.QueryWithExists([]*Path{&path})
			require.True(t, exists)
			require.JSONEq(t, test.expects, result.String())

			if path.IsSimple() {
				simpleResult, simpleExists := bj.QuerySimpleWithExists([]*Path{&path})
				require.True(t, simpleExists)
				require.JSONEq(t, test.expects, simpleResult.String())
			}
		})
	}
}

func TestQueryWithExistsEmptyArrayRangeDoesNotMatch(t *testing.T) {
	tests := []struct {
		name string
		json string
		path string
	}{
		{name: "root numeric range", json: `[]`, path: `$[0 to 0]`},
		{name: "root last range", json: `[]`, path: `$[last to last]`},
		{name: "nested numeric range", json: `{"a":[]}`, path: `$.a[0 to 0]`},
		{name: "nested last range", json: `{"a":[]}`, path: `$.a[last to last]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bj, err := ParseFromString(test.json)
			require.NoError(t, err)
			path, err := ParseJsonPath(test.path)
			require.NoError(t, err)

			_, exists := bj.QueryWithExists([]*Path{&path})
			require.False(t, exists)
		})
	}
}

func TestQueryWithExistsArrayRangeOverlap(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		path    string
		exists  bool
		expects string
	}{
		{name: "json null right of array", json: `[null]`, path: `$[1 to 1]`},
		{name: "right of array", json: `[0,1,2]`, path: `$[5 to 6]`},
		{name: "left of array", json: `[0,1,2]`, path: `$[last-8 to last-7]`},
		{name: "overlap right edge", json: `[0,1,2]`, path: `$[2 to 6]`, exists: true, expects: `[2]`},
		{name: "overlap left edge", json: `[0,1,2]`, path: `$[last-8 to last-2]`, exists: true, expects: `[0]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bj, err := ParseFromString(test.json)
			require.NoError(t, err)
			path, err := ParseJsonPath(test.path)
			require.NoError(t, err)

			result, exists := bj.QueryWithExists([]*Path{&path})
			require.Equal(t, test.exists, exists)
			if test.exists {
				require.JSONEq(t, test.expects, result.String())
			}
		})
	}
}

func TestQuerySimpleContainPath(t *testing.T) {
	kases := []struct {
		name    string
		jsonStr string
		pathStr string
		outStr  string
		exists  bool
	}{
		{
			name:    "root scalar autowrap",
			jsonStr: `1`,
			pathStr: `$[0]`,
			outStr:  `1`,
			exists:  true,
		},
		{
			name:    "root scalar nonzero index misses",
			jsonStr: `1`,
			pathStr: `$[1]`,
			outStr:  `null`,
			exists:  false,
		},
		{
			name:    "object scalar child autowrap",
			jsonStr: `{"a":1}`,
			pathStr: `$.a[0]`,
			outStr:  `1`,
			exists:  true,
		},
		{
			name:    "nested scalar child autowrap",
			jsonStr: `{"a":[{"b":1}]}`,
			pathStr: `$.a[0].b[0]`,
			outStr:  `1`,
			exists:  true,
		},
		{
			name:    "string scalar child autowrap",
			jsonStr: `{"a":"x"}`,
			pathStr: `$.a[0]`,
			outStr:  `"x"`,
			exists:  true,
		},
		{
			name:    "normal array path still works",
			jsonStr: `[1,2,3]`,
			pathStr: `$[1]`,
			outStr:  `2`,
			exists:  true,
		},
	}

	for _, kase := range kases {
		t.Run(kase.name, func(t *testing.T) {
			bj, err := ParseFromString(kase.jsonStr)
			require.NoError(t, err)
			path, err := ParseJsonPath(kase.pathStr)
			require.NoError(t, err)

			out, exists := bj.QuerySimpleContainPath(&path)
			require.Equal(t, kase.exists, exists)
			require.JSONEq(t, kase.outStr, out.String())
		})
	}
}

func TestPathExists(t *testing.T) {
	kases := []struct {
		name    string
		jsonStr string
		pathStr string
		exists  bool
	}{
		{
			name:    "json null is an existing path",
			jsonStr: `{"a":null}`,
			pathStr: `$.a`,
			exists:  true,
		},
		{
			name:    "missing key is not an existing path",
			jsonStr: `{}`,
			pathStr: `$.a`,
			exists:  false,
		},
		{
			name:    "scalar index zero autowraps",
			jsonStr: `{"a":1}`,
			pathStr: `$.a[0]`,
			exists:  true,
		},
		{
			name:    "scalar nonzero index misses",
			jsonStr: `{"a":1}`,
			pathStr: `$.a[1]`,
			exists:  false,
		},
		{
			name:    "object wildcard finds a value",
			jsonStr: `{"a":1}`,
			pathStr: `$.*`,
			exists:  true,
		},
		{
			name:    "array wildcard on an empty array misses",
			jsonStr: `[]`,
			pathStr: `$[*]`,
			exists:  false,
		},
		{
			name:    "array wildcard finds an element",
			jsonStr: `[1,2,3]`,
			pathStr: `$[*]`,
			exists:  true,
		},
		{
			name:    "recursive descent finds a json null",
			jsonStr: `{"a":{"b":null}}`,
			pathStr: `$**.b`,
			exists:  true,
		},
		{
			name:    "recursive descent finds an array index",
			jsonStr: `{"a":true,"b":[1,2,{"c":[4,5,{"d":[6,7,8,9,10]}]}]}`,
			pathStr: `$**[4]`,
			exists:  true,
		},
		{
			name:    "recursive descent reports a missing array index",
			jsonStr: `{"a":true,"b":[1,2,{"c":[4,5,{"d":[6,7,8,9,10]}]}]}`,
			pathStr: `$**.c[3]`,
			exists:  false,
		},
		{
			name:    "recursive descent and wildcard find an array value",
			jsonStr: `[1,2,3]`,
			pathStr: `$**[*]`,
			exists:  true,
		},
		{
			name:    "array range with no elements misses",
			jsonStr: `[1]`,
			pathStr: `$[1 to 2]`,
			exists:  false,
		},
	}

	for _, kase := range kases {
		t.Run(kase.name, func(t *testing.T) {
			bj, err := ParseFromString(kase.jsonStr)
			require.NoError(t, err)
			path, err := ParseJsonPath(kase.pathStr)
			require.NoError(t, err)
			require.Equal(t, kase.exists, bj.PathExists(&path))
		})
	}
}

func TestUnnest(t *testing.T) {
	kases := []struct {
		jsonStr   string
		pathStr   string
		mode      string
		recursive bool
		outer     bool
		outStr    []string
		valid     bool
	}{
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "other",
			valid:   false,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "both",
			pathStr: "$",
			outStr: []string{
				`key: a, path: $.a, value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.a",
			mode:    "both",
			valid:   true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "object",
			outStr: []string{
				`key: a, path: $.a, value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "array",
			valid:   true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "array",
			outStr: []string{
				`path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "object",
			valid:   true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "both",
			outStr: []string{
				`path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			outer:   true,
			outStr: []string{
				`path: $, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "array",
			recursive: true,
			pathStr:   "$.a",
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			pathStr: "$.b",
			valid:   true,
			outer:   true,
			outStr: []string{
				`path: $.b, this: {"c": 4, "d": [5, 6, 7]}`,
			},
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			pathStr: "$.b.d",
			outStr: []string{
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			pathStr:   "$.b",
			recursive: true,
			outStr: []string{
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$.*",
			recursive: true,
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			pathStr: "$.a**.a",
			outStr: []string{
				`key: b, path: $.a[0].a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			pathStr: "$**.a",
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.b.a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$**.a",
			recursive: true,
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.a[3].b, value: 4, this: {"b": 4}`,
				`key: b, path: $.b.a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
	}
	filterMap := map[string]struct{}{
		"index": {},
		"this":  {},
		"value": {},
		"path":  {},
		"key":   {},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		var path Path
		if len(kase.pathStr) > 0 {
			path, err = ParseJsonPath(kase.pathStr)
			require.Nil(t, err)
		}
		out, _, err := bj.Unnest(&path, kase.outer, kase.recursive, kase.mode, filterMap)
		if !kase.valid {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		for i, o := range out {
			require.Equal(t, kase.outStr[i], o.String())
		}
	}

}

func TestByteJson_Unquote(t *testing.T) {
	kases := []struct {
		jsonStr string
		outStr  string
		valid   bool
	}{
		{
			jsonStr: `"a"`,
			outStr:  "a",
			valid:   true,
		},
		{
			jsonStr: `"a\"b"`,
			outStr:  `a"b`,
			valid:   true,
		},
		{
			jsonStr: `"\"x\""`,
			outStr:  `"x"`,
			valid:   true,
		},
		{
			jsonStr: `"\"\""`,
			outStr:  `""`,
			valid:   true,
		},
		{
			jsonStr: `"a\b"`,
			outStr:  "a\b",
			valid:   true,
		},
		{
			jsonStr: `"a\r"`,
			outStr:  "a\r",
			valid:   true,
		},
		{
			jsonStr: `"a\t"`,
			outStr:  `a	`,
			valid:   true,
		},
		{
			jsonStr: `"a\n"`,
			outStr: `a
`,
			valid: true,
		},
		{
			jsonStr: `"\u554a\u554a\u5361\u5361"`,
			outStr:  `啊啊卡卡`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\uff0c\u006d\u006f"`,
			outStr:  `你好，mo`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\uff0cmo"`,
			outStr:  `你好，mo`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\ufc"`,
			valid:   false,
		},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		if !kase.valid {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		out, err := bj.Unquote()
		require.Nil(t, err)
		require.Equal(t, kase.outStr, out)
	}
}

func TestByteJson_UnquotePreservesPayloadBoundaryQuotes(t *testing.T) {
	values := []string{
		"plain",
		"a\"b",
		"\"leading",
		"trailing\"",
		"\"both\"",
		"\"",
		"\"\"",
		"\"你好\"",
	}
	for _, want := range values {
		encoded, err := json.Marshal(want)
		require.NoError(t, err)
		bj, err := ParseFromString(string(encoded))
		require.NoError(t, err)
		got, err := bj.Unquote()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func BenchmarkParseJsonByteFromString(b *testing.B) {
	s := `{"a":{"b":{"c":{"d":[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456]}}}}`
	for i := 0; i < b.N; i++ {
		ParseJsonByteFromString(s)
	}
}

func FuzzParseJsonByteFromString(f *testing.F) {
	f.Add(`{"a":{"b":{"c":{"d":[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456]}}}}`)
	f.Add("0A00")
	f.Add("1E1000")
	f.Add("{\"\":")
	f.Add("{\"\":0}")
	f.Add("null")
	f.Add("true")
	f.Add("false")
	f.Add("\"\xec\"")
	f.Add("\"\\ud800\\ud800\\udC00\"")
	f.Add("[]0")
	f.Add("")
	f.Add("\n")
	f.Add("0000")
	f.Add(":")
	f.Add("[0[],")
	f.Add("[]0")
	f.Add("{\"\"}")
	f.Add("{0:0}")
	f.Fuzz(func(t *testing.T, s string) {
		valid := true
		var v any
		err := json.Unmarshal([]byte(s), &v)
		if err != nil {
			valid = false
		}
		data, err := ParseJsonByteFromString(s)
		if valid {
			require.NoError(t, err)

			var bj ByteJson
			bj.Unmarshal(data)

			require.JSONEq(t, s, bj.String())
			return
		}
		require.NotNil(t, err)
	})
}

func TestNormalizeToIntString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "0", want: "0"},
		{input: "0.0", want: "0"},
		{input: "-0", want: "0"},
		{input: "-0.0", want: "0"},
		{input: "-1.0e0", want: "-1"},
		{input: "1.0e-000", want: "1"},
		{input: "1.00000", want: "1"},
		{input: "1.0000000001"},
		{input: "0e0", want: "0"},
		{input: "1E1", want: "10"},
		{input: "-100.00e-02", want: "-1"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			part, ok := ParseNumberParts([]byte(tc.input))
			require.True(t, ok)

			got, ok := NormalizeToIntString(part)
			if tc.want != "" {
				require.True(t, ok)
				require.Equal(t, tc.want, got)
			} else {
				require.False(t, ok)
			}
		})
	}
}
