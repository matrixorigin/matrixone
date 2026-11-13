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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTryUnmarshalBinaryVarlena(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	for _, constant := range []bool{false, true} {
		name := "flat"
		if constant {
			name = "constant"
		}
		t.Run(name, func(t *testing.T) {
			value := []byte("a varlena value stored in the external area")
			var src *Vector
			if constant {
				var err error
				src, err = NewConstBytes(types.T_varchar.ToType(), value, 4, mp)
				require.NoError(t, err)
			} else {
				src = NewVec(types.T_varchar.ToType())
				require.NoError(t, AppendBytes(src, value, false, mp))
			}
			t.Cleanup(func() { src.Free(mp) })
			payload, err := src.MarshalBinary()
			require.NoError(t, err)
			decoded := TryUnmarshalBinaryVarlena(payload)
			require.NotNil(t, decoded)
			t.Cleanup(func() { decoded.Free(mp) })
			require.Equal(t, src.Length(), decoded.Length())
			require.Equal(t, src.IsConst(), decoded.IsConst())
			require.Equal(t, value, decoded.GetBytesAt(0))
			for n := 0; n < len(payload); n++ {
				require.Nil(t, TryUnmarshalBinaryVarlena(payload[:n]), "truncated payload at %d bytes", n)
			}
		})
	}
	t.Run("short-physical-data", func(t *testing.T) {
		// Complete serialization, but its declared varlena row has only four bytes.
		src := NewVec(types.T_int32.ToType())
		t.Cleanup(func() { src.Free(mp) })
		require.NoError(t, AppendFixed(src, int32(7), false, mp))
		payload, err := src.MarshalBinary()
		require.NoError(t, err)
		require.Nil(t, TryUnmarshalBinaryVarlena(payload), "fixed-width vectors are not varlena")
		typ := types.T_varchar.ToType()
		copy(payload[1:1+types.TSize], types.EncodeType(&typ))
		require.Nil(t, TryUnmarshalBinaryVarlena(payload))
	})
}
