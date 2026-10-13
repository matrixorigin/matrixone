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

package vector

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAppendBytesRejectsInvalidNativeUnicodeValue(t *testing.T) {
	mp := mpool.MustNewZero()
	unicode := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	vec := NewVec(unicode)
	defer vec.Free(mp)

	// This byte sequence is a valid UCA weight for U+FFFF, but is not valid
	// UTF-8 text. It must not be admitted as a native Unicode value where it
	// could alias the real U+FFFF value in a later hash/sort path.
	err := AppendBytes(vec, []byte{0x30, 0xfb, 0xc1, 0x30, 0xff, 0xff, 0x20}, false, mp)
	require.Error(t, err)
	require.Zero(t, vec.Length())

	binaryVec := NewVec(types.T_binary.ToType())
	defer binaryVec.Free(mp)
	require.NoError(t, AppendBytes(binaryVec, []byte{0xff}, false, mp))
}

func TestBuildVarlenaFromVarlenaCollationAdmission(t *testing.T) {
	legacy := types.T_varchar.ToType()
	mb3 := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB3UnicodeCI)
	mb4 := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	binary := types.NewWithCharset(types.T_varbinary, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	for _, tc := range []struct {
		name  string
		typ   types.Type
		value []byte
		bad   bool
	}{
		{"legacy_empty", legacy, nil, false},
		{"legacy_inline", legacy, []byte{0xff, 0x00, 0x80}, false},
		{"legacy_area", legacy, bytes.Repeat([]byte{0xff}, 49), false},
		{"unicode_inline", mb4, []byte("😀"), false},
		{"unicode_area", mb4, bytes.Repeat([]byte("😀"), 8), false},
		{"mb3_bmp", mb3, []byte("中文"), false},
		{"mb3_reject_non_bmp_inline", mb3, []byte("😀"), true},
		{"mb3_reject_non_bmp_area", mb3, bytes.Repeat([]byte("😀"), 8), true},
		{"unicode_reject_invalid_inline", mb4, []byte{0xff}, true},
		{"unicode_reject_invalid_area", mb4, bytes.Repeat([]byte{0xff}, 49), true},
		{"binary_with_unicode_metadata", binary, []byte{0xff}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			vec := NewVec(tc.typ)
			t.Cleanup(func() { vec.Free(mp) })
			// Construct a structurally valid descriptor directly so invalid text
			// reaches this admission boundary instead of failing in AppendBytes.
			var source types.Varlena
			var area []byte
			if len(tc.value) <= types.VarlenaInlineSize {
				source[0] = byte(len(tc.value))
				copy(source[1:], tc.value)
			} else {
				area = bytes.Clone(tc.value)
				source.SetOffsetLen(0, uint32(len(area)))
			}
			output := types.Varlena{1, 'x'}
			before := output
			err := BuildVarlenaFromVarlena(vec, &output, &source, &area, mp)
			if tc.bad {
				require.Error(t, err)
				require.Equal(t, before, output)
				require.Empty(t, vec.area)
				require.Zero(t, mp.CurrNB())
				return
			}
			require.NoError(t, err)
			require.True(t, bytes.Equal(tc.value, output.GetByteSlice(vec.area)))
			// The copied value must not borrow the source's inline or area bytes.
			if len(tc.value) > 0 {
				source.GetByteSlice(area)[0] ^= 0xff
				require.True(t, bytes.Equal(tc.value, output.GetByteSlice(vec.area)))
			}
		})
	}
}

func BenchmarkUnionOneVarlenaCopy(b *testing.B) {
	for _, charset := range []struct {
		name string
		id   uint8
	}{
		{"legacy", types.T_varchar.ToType().Charset},
		{"unicode", types.CharsetUTF8MB4UnicodeCI},
	} {
		for _, size := range []struct {
			name string
			n    int
		}{{"inline", 20}, {"area", 49}} {
			b.Run(charset.name+"/"+size.name, func(b *testing.B) {
				const rows = 8192
				mp := mpool.MustNewZero()
				b.Cleanup(func() { require.Zero(b, mp.CurrNB()) })
				typ := types.NewWithCharset(types.T_varchar, 64, 0, charset.id)
				source := NewVec(typ)
				b.Cleanup(func() { source.Free(mp) })
				destination := NewVec(typ)
				b.Cleanup(func() { destination.Free(mp) })
				value := bytes.Repeat([]byte{'v'}, size.n)
				for row := range rows {
					require.NoError(b, AppendBytes(source, value, false, mp))
					require.NoError(b, destination.UnionOne(source, int64(row), mp))
				}
				b.ReportAllocs()
				b.SetBytes(int64(rows * len(value)))
				b.ResetTimer()
				for range b.N {
					destination.ResetWithSameType()
					for row := range rows {
						if err := destination.UnionOne(source, int64(row), mp); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				require.Equal(b, rows, destination.Length())
				for row := range rows {
					require.Equal(b, value, destination.GetBytesAt(row))
				}
			})
		}
	}
}
