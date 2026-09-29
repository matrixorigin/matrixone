// Copyright 2021 Matrix Origin
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

package vector

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func jsonAdmissionValue(t testing.TB, text string) []byte {
	t.Helper()
	value, err := bytejson.ParseFromString(text)
	require.NoError(t, err)
	raw, err := value.Marshal()
	require.NoError(t, err)
	return raw
}

func TestJSONRawAdmissionRejectsMalformedBeforePublication(t *testing.T) {
	good := jsonAdmissionValue(t, `[1,0]`)
	bad := jsonAdmissionValue(t, `[0,0]`)
	bad[1+8+5] = 0xfd
	inputs := map[string][]byte{"late child": bad, "short integer": {bytejson.TpCodeInt64, 1}, "empty": {}, "oversized literal": {bytejson.TpCodeLiteral, bytejson.LiteralNull, 0xff}, "nonminimal string": {bytejson.TpCodeString, 0x80, 0}, "empty decimal": {bytejson.TpCodeDecimal, 0}}
	inputs["aliased descendants"] = nestedJSONAdmissionValue(t, true)
	for name, raw := range inputs {
		t.Run(name, func(t *testing.T) {
			for _, operation := range []string{"constant", "append", "bulk", "list", "string list", "replace", "set constant", "set encoded constant", "set typed constant", "writer", "bytejson", "encoder"} {
				t.Run(operation, func(t *testing.T) {
					mp := mpool.MustNewZero()
					v := NewVec(types.T_json.ToType())
					defer func() { v.Free(mp); require.Zero(t, mp.CurrNB()) }()
					require.NoError(t, AppendBytes(v, good, false, mp))
					before, err := v.MarshalBinary()
					require.NoError(t, err)
					switch operation {
					case "constant":
						var result *Vector
						result, err = NewConstBytes(types.T_json.ToType(), raw, 1, mp)
						if result != nil {
							result.Free(mp)
						}
					case "append":
						err = AppendBytes(v, raw, false, mp)
					case "bulk":
						err = AppendMultiBytes(v, raw, false, 2, mp)
					case "list":
						err = AppendBytesList(v, [][]byte{good, raw}, nil, mp)
					case "string list":
						err = AppendStringList(v, []string{string(good), string(raw)}, nil, mp)
					case "replace":
						err = SetBytesAt(v, 0, raw, mp)
					case "set constant":
						err = SetConstBytes(v, raw, 2, mp)
					case "set encoded constant":
						err = SetConstByteJsonEncoded(v, invalidAdmissionEncoder{raw}, 2, mp)
					case "set typed constant":
						value := bytejson.ByteJson{}
						if len(raw) > 0 {
							value.Type, value.Data = raw[0], raw[1:]
						}
						err = SetConstByteJson(v, value, 2, mp)
					case "writer":
						err = AppendBytesWithWriter(v, len(raw), mp, func(dst []byte) error { copy(dst, raw); return nil })
					case "bytejson":
						value := bytejson.ByteJson{}
						if len(raw) > 0 {
							value.Type = raw[0]
							value.Data = raw[1:]
						}
						err = AppendByteJson(v, value, false, mp)
					case "encoder":
						err = AppendByteJsonEncoded(v, invalidAdmissionEncoder{raw}, mp)
					}
					require.Error(t, err)
					after, err := v.MarshalBinary()
					require.NoError(t, err)
					require.Equal(t, before, after, "failed admission must preserve visible state")
					require.NoError(t, AppendBytes(v, good, false, mp), "failed admission must allow a subsequent valid append")
					require.Equal(t, 2, v.Length())
					require.Equal(t, good, v.GetBytesAt(0))
					require.Equal(t, good, v.GetBytesAt(1))
				})
			}
		})
	}
}

// Model a corrupt wire/storage document, not an encoding produced by SQL.
func nestedJSONAdmissionValue(t *testing.T, alias bool) []byte {
	t.Helper()
	child := jsonAdmissionValue(t, `[null,null]`)[1:]
	for i := 0; i < 18; i++ {
		parent := make([]byte, 18+len(child))
		binary.LittleEndian.PutUint32(parent, 2)
		binary.LittleEndian.PutUint32(parent[4:], uint32(len(parent)))
		parent[8], parent[13] = bytejson.TpCodeArray, bytejson.TpCodeArray
		binary.LittleEndian.PutUint32(parent[9:], 18)
		binary.LittleEndian.PutUint32(parent[14:], 18)
		if !alias {
			parent[13], parent[14] = bytejson.TpCodeLiteral, bytejson.LiteralNull
		}
		copy(parent[18:], child)
		child = parent
	}
	return append([]byte{bytejson.TpCodeArray}, child...)
}

type invalidAdmissionEncoder struct{ raw []byte }

func (e invalidAdmissionEncoder) TypeCode() byte {
	if len(e.raw) == 0 {
		return 0
	}
	return e.raw[0]
}
func (e invalidAdmissionEncoder) DataSize() uint32 {
	if len(e.raw) == 0 {
		return 0
	}
	return uint32(len(e.raw) - 1)
}
func (e invalidAdmissionEncoder) EncodeDataInto(dst []byte) (int, error) {
	if len(e.raw) == 0 {
		return 0, nil
	}
	return copy(dst, e.raw[1:]), nil
}

func TestJSONCheckedDecodeRejectsMalformedPayload(t *testing.T) {
	t.Run("late child", func(t *testing.T) {
		good := jsonAdmissionValue(t, `[0,0]`)
		bad := bytes.Clone(good)
		bad[1+8+5] = 0xfd
		testJSONCheckedDecodeRejectsPayload(t, good, bad)
	})
	t.Run("aliased descendants", func(t *testing.T) {
		testJSONCheckedDecodeRejectsPayload(t, nestedJSONAdmissionValue(t, false), nestedJSONAdmissionValue(t, true))
	})
}

func testJSONCheckedDecodeRejectsPayload(t *testing.T, good, bad []byte) {
	t.Helper()
	mp := mpool.MustNewZero()
	source := NewVec(types.T_json.ToType())
	defer func() { source.Free(mp); require.Zero(t, mp.CurrNB()) }()
	require.Equal(t, len(good), len(bad))
	require.NoError(t, AppendBytes(source, good, false, mp))
	// Simulate damaged storage after valid admission. Do not use an unchecked
	// constructor as evidence that ordinary SQL creates malformed JSON.
	copy(source.GetBytesAt(0), bad)
	binary, err := source.MarshalBinary()
	require.NoError(t, err)
	var legacy bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithBufferV1(&legacy))
	var selected bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&selected, []int32{0}))
	copy(source.GetBytesAt(0), good)
	validBinary, err := source.MarshalBinary()
	require.NoError(t, err)
	var validLegacy, validSelected bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithBufferV1(&validLegacy))
	require.NoError(t, source.MarshalSelectedRowsTo(&validSelected, []int32{0}))
	for _, mode := range []string{"binary", "copy", "reader", "legacy", "selected", "raw copy"} {
		t.Run(mode, func(t *testing.T) {
			target := NewVec(types.T_json.ToType())
			defer target.Free(mp)
			switch mode {
			case "binary":
				err = target.UnmarshalBinary(binary)
			case "copy":
				err = target.UnmarshalBinaryWithCopy(binary, mp)
			case "reader":
				err = target.UnmarshalWithReader(bytes.NewReader(binary), mp)
			case "legacy":
				err = target.UnmarshalBinaryV1(legacy.Bytes())
			case "selected":
				err = target.UnmarshalSelectedRowsFrom(bytes.NewReader(selected.Bytes()), 1, mp)
			case "raw copy":
				var copied *Vector
				copy(source.GetBytesAt(0), bad)
				copied, err = NewVecWithDataCopy(*source.GetType(), source.Length(), source.GetData(), source.GetArea(), mp)
				copy(source.GetBytesAt(0), good)
				if copied != nil {
					copied.Free(mp)
				}
				require.Nil(t, copied, "failed constructor must not return a published vector")
			}
			require.Error(t, err)
			require.Zero(t, target.Length(), "decode error must not publish corrupted rows")
			switch mode {
			case "binary":
				err = target.UnmarshalBinary(validBinary)
			case "copy":
				err = target.UnmarshalBinaryWithCopy(validBinary, mp)
			case "reader":
				err = target.UnmarshalWithReader(bytes.NewReader(validBinary), mp)
			case "legacy":
				err = target.UnmarshalBinaryV1(validLegacy.Bytes())
			case "selected":
				err = target.UnmarshalSelectedRowsFrom(bytes.NewReader(validSelected.Bytes()), 1, mp)
			case "raw copy":
				copied, copyErr := NewVecWithDataCopy(*source.GetType(), source.Length(), source.GetData(), source.GetArea(), mp)
				require.NoError(t, copyErr)
				defer copied.Free(mp)
				require.Equal(t, 1, copied.Length())
				require.Equal(t, good, copied.GetBytesAt(0))
				return
			}
			require.NoError(t, err, "failed decode must allow retry with valid input")
			require.Equal(t, 1, target.Length())
			require.Equal(t, good, target.GetBytesAt(0))
		})
	}
}

func TestJSONAdmissionPreservesNullsAndValidatedCopies(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	values := [][]byte{jsonAdmissionValue(t, `[0,0]`), nil, jsonAdmissionValue(t, `false`)}
	require.NoError(t, AppendBytesList(source, values, []bool{false, true, false}, mp))
	for _, mode := range []string{"union one", "union batch", "union all", "copy", "trusted decode"} {
		t.Run(mode, func(t *testing.T) {
			dst := NewVec(types.T_json.ToType())
			defer dst.Free(mp)
			switch mode {
			case "union one":
				for i := 0; i < 3; i++ {
					require.NoError(t, dst.UnionOne(source, int64(i), mp))
				}
			case "union batch":
				require.NoError(t, dst.UnionBatch(source, 0, 3, nil, mp))
			case "union all":
				require.NoError(t, GetUnionAllFunction(types.T_json.ToType(), mp)(dst, source))
			case "copy":
				require.NoError(t, AppendBytesList(dst, values, []bool{false, true, false}, mp))
				require.NoError(t, dst.Copy(source, 0, 0, mp))
			case "trusted decode":
				wire, err := source.MarshalBinary()
				require.NoError(t, err)
				checked := NewVec(types.T_json.ToType())
				defer checked.Free(mp)
				require.NoError(t, checked.UnmarshalBinary(wire))
				// The exact checked wire bytes remain immutable until both views die.
				require.NoError(t, dst.UnmarshalBinaryTrusted(wire))
			}
			require.Equal(t, 3, dst.Length())
			require.True(t, dst.IsNull(1))
			require.Equal(t, values[0], dst.GetBytesAt(0))
			require.Equal(t, values[2], dst.GetBytesAt(2))
		})
	}
}

func BenchmarkJSONRawAdmission(b *testing.B) {
	for _, tc := range []struct{ name, text string }{
		{"small", `[0,0]`}, {"wide", `[0` + strings.Repeat(`,0`, 2048) + `]`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_json.ToType())
			defer v.Free(mp)
			raw := jsonAdmissionValue(b, tc.text)
			require.NoError(b, v.PreExtendWithArea(1, len(raw), mp))
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				v.CleanOnlyData()
				if err := AppendBytes(v, raw, false, mp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestJSONLegacyAdmissionRejectsTruncatedWire(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_json.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, jsonAdmissionValue(t, `[0,0]`), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	var wire bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithBufferV1(&wire))
	for end := 0; end < wire.Len(); end++ {
		dst := NewVec(types.T_json.ToType())
		require.Error(t, dst.UnmarshalBinaryV1(wire.Bytes()[:end]))
		require.Zero(t, dst.Length())
		dst.Free(mp)
	}
	dst := NewVec(types.T_json.ToType())
	defer dst.Free(mp)
	require.NoError(t, dst.UnmarshalBinaryV1(wire.Bytes()))
	require.True(t, dst.IsNull(1))
}
