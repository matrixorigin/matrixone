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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestLegacyClassDecodeNormalization(t *testing.T) {
	type encodedClass struct {
		name    string
		tag     byte
		invalid bool
	}
	for _, kind := range []string{"fixed", "varlen", "json", "constant", "constant_null"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			var source *Vector
			var err error
			t.Cleanup(func() {
				if source != nil {
					source.Free(mp)
				}
			})
			switch kind {
			case "constant":
				source, err = NewConstFixed(types.T_int64.ToType(), int64(11), 3, mp)
				require.NoError(t, err)
			case "constant_null":
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			case "fixed":
				source = NewVec(types.T_int64.ToType(), mp)
				for row, value := range []int64{11, 22, 33} {
					require.NoError(t, AppendFixed(source, value, row == 1, mp))
				}
			default:
				typ := types.T_varchar.ToType()
				values := [][]byte{[]byte("short"), nil, []byte("a value longer than the inline string representation")}
				if kind == "json" {
					typ = types.T_json.ToType()
					values = [][]byte{jsonAdmissionValue(t, `[1]`), nil, jsonAdmissionValue(t, `{"value":"longer than inline storage"}`)}
				}
				source = NewVec(typ, mp)
				for row, value := range values {
					require.NoError(t, AppendBytes(source, value, row == 1, mp))
				}
			}
			wire, err := source.MarshalBinary()
			require.NoError(t, err)
			var legacy bytes.Buffer
			require.NoError(t, source.MarshalBinaryWithBufferV1(&legacy))
			classes := []encodedClass{{name: "original", tag: byte(source.class)}}
			if !source.IsConst() {
				classes = append(classes, encodedClass{name: "legacy", tag: LegacyRowBufferClass})
			}
			if kind == "fixed" {
				classes = append(classes, encodedClass{name: "unknown", tag: LegacyRowBufferClass + 1, invalid: true})
			}
			for _, class := range classes {
				t.Run(class.name, func(t *testing.T) {
					for _, mode := range []string{"binary", "trusted", "copy", "legacy_v1", "reader"} {
						t.Run(mode, func(t *testing.T) {
							encoded := bytes.Clone(wire)
							if mode == "legacy_v1" {
								encoded = bytes.Clone(legacy.Bytes())
							}
							encoded[0] = class.tag
							before := bytes.Clone(encoded)
							target := NewVec(types.T_any.ToType(), mp)
							t.Cleanup(func() { target.Free(mp) })
							var decodeErr error
							switch mode {
							case "binary":
								decodeErr = target.UnmarshalBinary(encoded)
							case "trusted":
								if !class.invalid {
									checked := NewVec(types.T_any.ToType(), mp)
									t.Cleanup(func() { checked.Free(mp) })
									require.NoError(t, checked.UnmarshalBinary(encoded))
								}
								decodeErr = target.UnmarshalBinaryTrusted(encoded)
							case "copy":
								decodeErr = target.UnmarshalBinaryWithCopy(encoded, mp)
							case "legacy_v1":
								decodeErr = target.UnmarshalBinaryV1(encoded)
							case "reader":
								decodeErr = target.UnmarshalWithReader(bytes.NewReader(encoded), mp)
							}
							require.Equal(t, before, encoded, "decoding must not rewrite shared input bytes")
							if class.invalid {
								require.Error(t, decodeErr)
								return
							}
							require.NoError(t, decodeErr)
							require.Equal(t, source.class, target.class)
							require.Equal(t, *source.GetType(), *target.GetType())
							require.Equal(t, source.Length(), target.Length())
							for row := 0; row < source.Length(); row++ {
								require.Equal(t, source.IsNull(uint64(row)), target.IsNull(uint64(row)))
								if !source.IsNull(uint64(row)) {
									if source.GetType().IsVarlen() {
										require.Equal(t, source.GetBytesAt(row), target.GetBytesAt(row))
									} else {
										require.Equal(t, source.GetRawBytesAt(row), target.GetRawBytesAt(row))
									}
								}
							}
						})
					}
				})
			}
		})
	}
}
