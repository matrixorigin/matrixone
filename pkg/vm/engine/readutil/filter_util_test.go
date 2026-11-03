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

package readutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestEncodePrimaryKeyVectorOwnershipAndConstants(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packer := types.NewPacker()
	t.Cleanup(packer.Close)

	fixed := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { fixed.Free(mp) })
	values := []int64{-1, 42, 42, 1000}
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(fixed, value, false, mp))
	}
	keys := EncodePrimaryKeyVector(fixed, packer)
	want := make([][]byte, 0, len(values))
	for _, value := range values {
		packer.Reset()
		packer.EncodeInt64(value)
		want = append(want, packer.Bytes())
	}
	packer.Reset()
	packer.EncodeStringType([]byte("reuse the packer buffer"))
	require.Equal(t, want, keys, "retained keys must not alias the packer buffer")
	keys[1][0] ^= 0xff
	require.Equal(t, want[2], keys[2], "duplicate input rows own independent ordinary payloads")

	constFixed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
	require.NoError(t, err)
	t.Cleanup(func() { constFixed.Free(mp) })
	constantKeys := EncodePrimaryKeyVector(constFixed, packer)
	packer.Reset()
	packer.EncodeInt64(7)
	constantKey := packer.Bytes()
	require.Equal(t, [][]byte{constantKey, constantKey, constantKey}, constantKeys)
	require.Same(t, &constantKeys[0][0], &constantKeys[1][0], "fixed constant expansion retains its aliasing")

	constNull := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	t.Cleanup(func() { constNull.Free(mp) })
	require.Equal(t, [][]byte{nil, nil, nil}, EncodePrimaryKeyVector(constNull, packer))
	empty := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { empty.Free(mp) })
	require.Nil(t, EncodePrimaryKeyVector(empty, packer))

	variable := vector.NewVec(types.T_varbinary.ToType())
	t.Cleanup(func() { variable.Free(mp) })
	valuesBytes := [][]byte{[]byte("long retained value"), {0, 1, 0}, []byte("other")}
	for _, value := range valuesBytes {
		require.NoError(t, vector.AppendBytes(variable, value, false, mp))
	}
	variableKeys := EncodePrimaryKeyVector(variable, packer)
	want = want[:0]
	for _, value := range valuesBytes {
		packer.Reset()
		packer.EncodeStringType(value)
		want = append(want, packer.Bytes())
	}
	packer.Reset()
	packer.EncodeInt64(123456)
	require.Equal(t, want, variableKeys)
}

func TestEncodePrimaryKeyVectorVariableConstantOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packer := types.NewPacker()
	t.Cleanup(packer.Close)
	vec, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte{0, 1, 0}, 3, mp)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(mp) })
	keys := EncodePrimaryKeyVector(vec, packer)
	require.Len(t, keys, 3)
	want := append([]byte(nil), keys[1]...)
	keys[0][0] ^= 0xff
	packer.Reset()
	packer.EncodeInt64(100)
	require.Equal(t, want, keys[1])
	require.Equal(t, want, keys[2])
}

func TestEncodePrimaryKeyVectorShortenedFixedConstant(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	p := types.NewPacker()
	t.Cleanup(p.Close)
	v, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(mp) })
	v.SetLength(0)
	require.Panics(t, func() { EncodePrimaryKeyVector(v, p) })
}

func TestEncodePrimaryKeyVectorTypeFamilies(t *testing.T) {
	for _, tc := range []struct {
		oid   types.T
		value any
	}{
		{types.T_bool, true}, {types.T_bit, uint64(17)},
		{types.T_int8, int8(-7)}, {types.T_int16, int16(-129)}, {types.T_int32, int32(-65537)}, {types.T_int64, int64(-9007199254740993)},
		{types.T_uint8, uint8(255)}, {types.T_uint16, uint16(256)}, {types.T_uint32, uint32(65536)}, {types.T_uint64, uint64(1 << 63)},
		{types.T_float32, float32(-1.5)}, {types.T_float64, float64(-1.5)},
		{types.T_date, types.Date(17)}, {types.T_time, types.Time(-17)}, {types.T_datetime, types.Datetime(17)}, {types.T_timestamp, types.Timestamp(17)},
		{types.T_decimal64, types.Decimal64(17)}, {types.T_decimal128, types.Decimal128{}}, {types.T_decimal256, types.Decimal256{}},
		{types.T_uuid, types.Uuid{}}, {types.T_enum, types.Enum(17)}, {types.T_year, types.MoYear(2024)},
	} {
		t.Run(tc.oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			p := types.NewPacker()
			t.Cleanup(p.Close)
			v := vector.NewVec(tc.oid.ToType())
			t.Cleanup(func() { v.Free(mp) })
			require.NoError(t, vector.AppendAny(v, tc.value, false, mp))
			require.NoError(t, vector.AppendAny(v, tc.value, false, mp))
			want := EncodePrimaryKey(tc.value, p)
			got := EncodePrimaryKeyVector(v, p)
			p.Reset()
			p.EncodeStringType([]byte("reuse"))
			require.Equal(t, [][]byte{want, want}, got)
			got[0][0] ^= 0xff
			require.Equal(t, want, got[1])
		})
	}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	p := types.NewPacker()
	t.Cleanup(p.Close)
	null := vector.NewConstNull(types.T_int64.ToType(), 0, mp)
	t.Cleanup(func() { null.Free(mp) })
	got := EncodePrimaryKeyVector(null, p)
	require.NotNil(t, got)
	require.Empty(t, got)
	unsupported := vector.NewVec(types.T_any.ToType())
	t.Cleanup(func() { unsupported.Free(mp) })
	require.Panics(t, func() { EncodePrimaryKeyVector(unsupported, p) })
}
