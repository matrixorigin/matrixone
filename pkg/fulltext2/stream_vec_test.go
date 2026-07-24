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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestColumnBufferRoundTrip pins the box-free typed streaming codec: a pk encoded into a
// ColumnBuffer (producer, appendAnyPk) must decode back to the identical value in a vector
// of that type (consumer, AppendColumnBuffer), across the fixed-width and varlena paths.
func TestColumnBufferRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()

	build := func(pkType types.T, vals []any) *vectorindex.ColumnBuffer {
		w, fixed := fixedPkByteWidth(int32(pkType))
		if !fixed {
			w = 0
		}
		k := &vectorindex.ColumnBuffer{Type: pkType}
		for _, v := range vals {
			appendAnyPk(k, w, v)
		}
		require.Equal(t, len(vals), k.N)
		return k
	}
	toVec := func(pkType types.T, vals []any) *vector.Vector {
		vec := vector.NewVec(pkType.ToType())
		require.NoError(t, AppendColumnBuffer(build(pkType, vals), vec, mp))
		require.Equal(t, len(vals), vec.Length())
		return vec
	}

	// fixed-width paths
	require.Equal(t, []int64{1, -5, 1 << 40},
		vector.MustFixedColWithTypeCheck[int64](toVec(types.T_int64, []any{int64(1), int64(-5), int64(1 << 40)})))
	require.Equal(t, []uint64{0, 1, 1<<64 - 1},
		vector.MustFixedColWithTypeCheck[uint64](toVec(types.T_uint64, []any{uint64(0), uint64(1), uint64(1<<64 - 1)})))
	require.Equal(t, []uint64{7, 255},
		vector.MustFixedColWithTypeCheck[uint64](toVec(types.T_bit, []any{uint64(7), uint64(255)})))
	require.Equal(t, []int32{-3, 100000},
		vector.MustFixedColWithTypeCheck[int32](toVec(types.T_int32, []any{int32(-3), int32(100000)})))
	require.Equal(t, []int8{-1, 2},
		vector.MustFixedColWithTypeCheck[int8](toVec(types.T_int8, []any{int8(-1), int8(2)})))
	require.Equal(t, []types.Datetime{types.Datetime(123456789), types.Datetime(-42)},
		vector.MustFixedColWithTypeCheck[types.Datetime](toVec(types.T_datetime, []any{types.Datetime(123456789), types.Datetime(-42)})))
	require.Equal(t, []types.Decimal128{{B0_63: 5, B64_127: 9}},
		vector.MustFixedColWithTypeCheck[types.Decimal128](toVec(types.T_decimal128, []any{types.Decimal128{B0_63: 5, B64_127: 9}})))

	// varlena paths
	vvar := toVec(types.T_varchar, []any{[]byte("abc"), []byte(""), []byte("a longer varchar pk")})
	require.Equal(t, "abc", vvar.GetStringAt(0))
	require.Equal(t, "", vvar.GetStringAt(1))
	require.Equal(t, "a longer varchar pk", vvar.GetStringAt(2))

	u1, _ := types.ParseUuid("00000000-0000-0000-0000-000000000001")
	u2, _ := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")
	require.Equal(t, []types.Uuid{u1, u2},
		vector.MustFixedColWithTypeCheck[types.Uuid](toVec(types.T_uuid, []any{u1, u2})))
}
