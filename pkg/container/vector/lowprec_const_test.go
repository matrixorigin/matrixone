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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Low-precision float columns share storage width with uint8/uint16 but must
// be read through their own element types; typecheck builds reject the raw
// integer view. Const-set and union of a constant source exercise both paths.
func TestLowPrecisionConstSetAndUnion(t *testing.T) {
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4, types.T_uint8, types.T_uint16} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := oid.ToType()
			src := NewVec(typ, mp)
			raw := make([]byte, typ.TypeSize()*2)
			raw[0], raw[typ.TypeSize()] = 0x3f, 0x40
			require.NoError(t, src.PreExtend(2, mp))
			src.SetLength(2)
			copy(src.GetData(), raw)
			src.SetNull(0)

			dst := NewVec(typ, mp)
			set := GetConstSetFunction(typ, mp)
			require.NoError(t, set(dst, src, 1, 3))
			require.True(t, dst.IsConst())
			require.Equal(t, 3, dst.Length())
			require.Equal(t, src.GetRawBytesAt(1), dst.GetRawBytesAt(0))
			require.NoError(t, set(dst, src, 0, 2))
			require.True(t, dst.IsConstNull())

			value := NewVec(typ, mp)
			require.NoError(t, set(value, src, 1, 2))
			union := NewVec(typ, mp)
			require.NoError(t, GetUnionAllFunction(typ, mp)(union, value))
			require.Equal(t, 2, union.Length())
			require.Equal(t, src.GetRawBytesAt(1), union.GetRawBytesAt(1))

			for _, v := range []*Vector{src, dst, value, union} {
				v.Free(mp)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}
