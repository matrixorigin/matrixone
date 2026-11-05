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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalBinaryReuseEmptyPayload(t *testing.T) {
	for _, tt := range []struct {
		name       string
		varlen     bool
		nullBitmap bool
		next       string
	}{
		{name: "fixed_to_null", next: "null"},
		{name: "null_bitmap_to_const_null", nullBitmap: true, next: "null"},
		{name: "fixed_to_empty", next: "empty"},
		{name: "varlen_to_null", varlen: true, next: "null"},
		{name: "varlen_to_empty", varlen: true, next: "empty"},
		{name: "varlen_to_inline", varlen: true, next: "inline"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_int64.ToType()
			if tt.varlen {
				typ = types.T_varchar.ToType()
			}
			before := NewVec(typ)
			t.Cleanup(func() { before.Free(mp) })
			longValue := strings.Repeat("x", 40)
			if tt.varlen {
				require.NoError(t, AppendBytes(before, []byte(longValue), false, mp))
				require.NotEmpty(t, before.GetArea())
			} else {
				require.NoError(t, AppendFixed(before, int64(42), false, mp))
				if tt.nullBitmap {
					require.NoError(t, AppendFixed(before, int64(0), true, mp))
				}
			}

			var after *Vector
			if tt.next == "null" {
				after = NewConstNull(typ, 2, mp)
			} else {
				after = NewVec(typ)
			}
			t.Cleanup(func() { after.Free(mp) })
			if tt.next == "inline" {
				require.NoError(t, AppendBytes(after, []byte("ok"), false, mp))
			}
			beforePayload, err := before.MarshalBinary()
			require.NoError(t, err)
			afterPayload, err := after.MarshalBinary()
			require.NoError(t, err)
			beforeSnapshot, afterSnapshot := bytes.Clone(beforePayload), bytes.Clone(afterPayload)
			// 反序列化及 Free 都不得改写借用的编码缓冲区。
			t.Cleanup(func() {
				require.Equal(t, beforeSnapshot, beforePayload)
				require.Equal(t, afterSnapshot, afterPayload)
			})
			decoded := NewVecFromReuse()
			t.Cleanup(func() { decoded.Free(mp) })

			require.NoError(t, decoded.UnmarshalBinary(beforePayload))
			require.NotEmpty(t, decoded.GetData())
			require.NoError(t, decoded.UnmarshalBinary(afterPayload))
			require.Equal(t, typ, *decoded.GetType())
			require.Equal(t, after.Length(), decoded.Length())
			require.True(t, decoded.NeedDup())
			if tt.next == "null" {
				require.True(t, decoded.IsConstNull())
				require.True(t, decoded.IsNull(1))
			} else {
				require.False(t, decoded.IsConst())
			}
			if tt.next == "inline" {
				require.Equal(t, "ok", string(decoded.GetBytesAt(0)))
			} else {
				require.Empty(t, decoded.GetData())
			}
			require.Empty(t, decoded.GetArea())

			// 再次读取非空向量，覆盖空值之后的重用及原有 NULL 位图语义。
			require.NoError(t, decoded.UnmarshalBinary(beforePayload))
			require.Equal(t, before.Length(), decoded.Length())
			require.False(t, decoded.IsConst())
			if tt.varlen {
				require.Equal(t, longValue, string(decoded.GetBytesAt(0)))
			} else {
				require.Equal(t, int64(42), GetFixedAtWithTypeCheck[int64](decoded, 0))
				if tt.nullBitmap {
					require.True(t, decoded.IsNull(1))
				}
			}
		})
	}
}
