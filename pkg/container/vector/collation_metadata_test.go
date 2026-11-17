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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMySQLSpecialPhysicalVectorCollation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, tc := range []struct {
		name string
		typ  plan.Type
	}{
		{name: "ENUM", typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "Red,GREEN", Charset: 4, CollationVersion: 1}},
		{name: "SET", typ: plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b", Charset: 4, CollationVersion: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, err := types.TypeFromPlan(tc.typ)
			require.NoError(t, err)
			checkPhysical := func(t *testing.T, vec *Vector) {
				t.Helper()
				require.Equal(t, storage.Oid, vec.GetType().Oid)
				require.Zero(t, vec.GetType().Charset)
				require.Zero(t, vec.GetType().CollationVersion)
			}

			for _, tc := range []struct {
				name string
				new  func() *Vector
			}{
				{name: "new", new: func() *Vector { return NewVec(storage) }},
				{name: "constant null", new: func() *Vector { return NewConstNull(storage, 1, mp) }},
				{name: "reset", new: func() *Vector {
					vec := NewVec(types.T_int32.ToType())
					vec.Reset(storage)
					return vec
				}},
				{name: "reset with new type", new: func() *Vector {
					vec := NewVec(types.T_int32.ToType())
					vec.ResetWithNewType(&storage)
					return vec
				}},
				{name: "set type", new: func() *Vector {
					vec := NewVec(types.T_int32.ToType())
					vec.SetType(storage)
					return vec
				}},
				{name: "set type and fix data", new: func() *Vector {
					vec := NewVec(types.T_int32.ToType())
					require.NoError(t, vec.SetTypeAndFixData(storage, mp))
					return vec
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					vec := tc.new()
					defer vec.Free(mp)
					checkPhysical(t, vec)
				})
			}

			src := NewVec(storage)
			defer src.Free(mp)
			proto, err := VectorToProtoVector(src)
			require.NoError(t, err)
			require.Zero(t, proto.Type.Charset)
			require.Zero(t, proto.Type.CollationVersion)
			fromProto, err := ProtoVectorToVector(proto)
			require.NoError(t, err)
			defer fromProto.Free(mp)
			checkPhysical(t, fromProto)

			logicalProto := proto
			logicalProto.Type = tc.typ
			fromProto, err = ProtoVectorToVector(logicalProto)
			require.NoError(t, err)
			defer fromProto.Free(mp)
			checkPhysical(t, fromProto)

			// Validation happens before physical normalization. In particular, an
			// ordinary uint64 cannot acquire SET metadata by crossing this codec.
			invalid := proto
			invalid.Type = plan.Type{Id: int32(types.T_uint64), Charset: 4, CollationVersion: 1}
			fromProto, err = ProtoVectorToVector(invalid)
			require.Error(t, err)
			require.Nil(t, fromProto)
		})
	}
}

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
