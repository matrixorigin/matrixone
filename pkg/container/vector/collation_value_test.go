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

func TestCopyVarlenaPreservesCollationValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		charset uint8
		value   []byte
		invalid bool
	}{
		{"legacy opaque", 0, []byte{0xff}, false},
		{"unicode invalid", types.CharsetUTF8MB4UnicodeCI, []byte{0xff}, true},
		{"mb3 supplementary", types.CharsetUTF8MB3UnicodeCI, []byte("😀"), true},
		{"mb4 supplementary", types.CharsetUTF8MB4UnicodeCI, []byte("😀"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, prefix := range []string{"", "external-varlena-padding-"} {
				func() {
					mp := mpool.MustNewZero()
					defer func() { require.Zero(t, mp.CurrNB()) }()
					src := NewVec(types.T_varbinary.ToType())
					defer src.Free(mp)
					value := append([]byte(prefix), tc.value...)
					require.NoError(t, AppendBytes(src, value, false, mp))
					dst := NewVec(types.NewWithCharset(types.T_varchar, 64, 0, tc.charset))
					defer dst.Free(mp)
					var out types.Varlena
					before := out
					in := MustFixedColNoTypeCheck[types.Varlena](src)[0]
					area := src.GetArea()
					err := BuildVarlenaFromVarlena(dst, &out, &in, &area, mp)
					if tc.invalid {
						require.Error(t, err)
						require.Equal(t, before, out)
						require.Empty(t, dst.GetArea())
					} else {
						require.NoError(t, err)
						require.True(t, bytes.Equal(value, out.GetByteSlice(dst.GetArea())))
					}
				}()
			}
		})
	}
}

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
