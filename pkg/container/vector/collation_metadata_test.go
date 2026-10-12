// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataVectorReaderRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, domain := range []struct{ id, revision uint8 }{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}, {7, 1}, {12, 1}} {
		typ := types.NewWithCharset(types.T_varchar, 32, 0, domain.id)
		typ.CollationVersion = domain.revision
		src := NewVec(typ)
		require.NoError(t, AppendBytes(src, []byte("😀"), false, mp))
		proto, err := VectorToProtoVector(src)
		require.NoError(t, err)
		fromProto, err := ProtoVectorToVector(proto)
		require.NoError(t, err)
		require.Equal(t, typ, *fromProto.GetType())
		require.Equal(t, "😀", fromProto.GetStringAt(0))
		fromProto.Free(mp)
		wire, err := src.MarshalBinary()
		require.NoError(t, err)
		for _, copyData := range []bool{false, true} {
			restored := NewVec(types.T_any.ToType())
			if copyData {
				err = restored.UnmarshalBinaryWithCopy(wire, mp)
			} else {
				err = restored.UnmarshalBinary(wire)
			}
			require.NoError(t, err)
			require.Equal(t, typ, *restored.GetType())
			require.Equal(t, "😀", restored.GetStringAt(0))
			restored.Free(mp)
		}
		var legacy bytes.Buffer
		require.NoError(t, src.MarshalBinaryWithBufferV1(&legacy))
		restored := NewVec(types.T_any.ToType())
		require.NoError(t, restored.UnmarshalBinaryV1(legacy.Bytes()))
		require.Equal(t, typ, *restored.GetType())
		restored.Free(mp)
		proto.Type.CollationVersion = 257
		bad, err := ProtoVectorToVector(proto)
		require.Error(t, err)
		require.Nil(t, bad)
		// Byte 3 within the native Type is its revision, after the vector class.
		wire[1+3] = 2
		for _, decode := range []func(*Vector) error{
			func(v *Vector) error { return v.UnmarshalBinary(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryTrusted(wire) },
			func(v *Vector) error { return v.UnmarshalBinaryWithCopy(wire, mp) },
			func(v *Vector) error { legacy.Bytes()[1+3] = 2; return v.UnmarshalBinaryV1(legacy.Bytes()) },
		} {
			v := NewVec(types.T_any.ToType())
			require.Error(t, decode(v))
			v.Free(mp)
		}
		src.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}
