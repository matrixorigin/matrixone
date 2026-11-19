// Copyright 2026 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestGetLowPrecisionFloatAt(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := NewVec(oid.ToType())
		var err error
		switch oid {
		case types.T_bf16:
			err = AppendFixed(vec, types.BF16FromFloat32(-0.5), false, mp)
		case types.T_float16:
			err = AppendFixed(vec, types.Float16FromFloat32(-0.5), false, mp)
		case types.T_float8:
			err = AppendFixed(vec, types.Float8FromFloat32(-0.5), false, mp)
		case types.T_float4:
			err = AppendFixed(vec, types.Float4FromFloat32(-0.5), false, mp)
		}
		require.NoError(t, err)
		f, ok := GetLowPrecisionFloatAt(vec, 0)
		require.True(t, ok)
		require.Equal(t, float32(-0.5), f, oid.String())
		vec.Free(mp)
	}
	vec := NewVec(types.T_float32.ToType())
	require.NoError(t, AppendFixed(vec, float32(1), false, mp))
	_, ok := GetLowPrecisionFloatAt(vec, 0)
	require.False(t, ok)
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// TestLowPrecisionFloatMarshalRoundTrip checks that bf16, float16, float8 and float4
// vectors decode after MarshalBinary; the decoder's type allowlist gates WAL replay and
// batches sent between services.
func TestLowPrecisionFloatMarshalRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := NewVec(oid.ToType())
		for i, v := range []float32{1.5, -0.5, 0} {
			var err error
			switch oid {
			case types.T_bf16:
				err = AppendFixed(vec, types.BF16FromFloat32(v), i == 2, mp)
			case types.T_float16:
				err = AppendFixed(vec, types.Float16FromFloat32(v), i == 2, mp)
			case types.T_float8:
				err = AppendFixed(vec, types.Float8FromFloat32(v), i == 2, mp)
			case types.T_float4:
				err = AppendFixed(vec, types.Float4FromFloat32(v), i == 2, mp)
			}
			require.NoError(t, err)
		}
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		got := NewVec(oid.ToType())
		require.NoError(t, got.UnmarshalBinary(data), oid.String())
		require.Equal(t, 3, got.Length())
		f, _ := GetLowPrecisionFloatAt(got, 1)
		require.Equal(t, float32(-0.5), f, oid.String())
		require.True(t, got.IsNull(2))
		vec.Free(mp)
	}
}

func testLowPrecisionFloatRowStrings[T types.LowPrecFloat](t *testing.T, oid types.T, stored []T, want []float32) {
	t.Helper()
	t.Run(oid.String(), func(t *testing.T) {
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
		vec := NewVec(oid.ToType())
		t.Cleanup(func() { vec.Free(mp) })
		require.NoError(t, AppendFixedList(vec, stored, nil, mp))
		var zero T
		require.NoError(t, AppendFixed(vec, zero, true, mp))
		for i, value := range stored {
			text := fmt.Sprintf("%v", want[i])
			require.Equal(t, text, vec.RowToString(i))
			constant, err := NewConstFixed(oid.ToType(), value, 10, mp)
			require.NoError(t, err)
			t.Cleanup(func() { constant.Free(mp) })
			require.Equal(t, text, constant.RowToString(7))
		}
		require.Equal(t, "null", vec.RowToString(len(stored)))
		null := NewConstNull(oid.ToType(), 10, mp)
		t.Cleanup(func() { null.Free(mp) })
		require.Equal(t, "null", null.RowToString(7))
	})
}
