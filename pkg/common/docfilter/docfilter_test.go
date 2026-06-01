// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSupportsBitset(t *testing.T) {
	for _, oid := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	} {
		require.True(t, SupportsBitset(oid.ToType()), oid.String())
	}
	for _, oid := range []types.T{types.T_varchar, types.T_char, types.T_decimal64, types.T_float64} {
		require.False(t, SupportsBitset(oid.ToType()), oid.String())
	}
}

// buildIntVec builds a fixed integer vector; rows whose value is in nullRows are null.
func buildIntVec[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](
	t *testing.T, mp *mpool.MPool, typ types.Type, vals []T, nullRows map[int]bool,
) *vector.Vector {
	v := vector.NewVec(typ)
	for i, val := range vals {
		require.NoError(t, vector.AppendFixed(v, val, nullRows[i], mp))
	}
	return v
}

func TestRoaringDocFilterInt64(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 1 << 40, -7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	bm := BuildBitset(v)
	payload, err := MarshalBitset(bm)
	require.NoError(t, err)

	// round-trip deserialize
	f, err := NewRoaringDocFilter(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())

	// every present value tests true via raw bytes
	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d should be present", present[i])
	}

	// absent values test false
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 4, 999, 1 << 41}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)))
	}

	// TestVector parallels Test and reports nulls as non-existent
	probe := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 100, 0}, map[int]bool{3: true})
	defer probe.Free(mp)
	got := f.TestVector(probe, nil)
	require.Equal(t, []uint8{1, 0, 1, 0}, got)

	// callback receives matching (exist, isnull, row)
	var seen []string
	f.TestVector(probe, func(exist, isnull bool, row int) {
		if isnull {
			seen = append(seen, "null")
		} else if exist {
			seen = append(seen, "hit")
		} else {
			seen = append(seen, "miss")
		}
	})
	require.Equal(t, []string{"hit", "miss", "hit", "null"}, seen)
}

func TestRoaringDocFilterWidths(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("int32", func(t *testing.T) {
		v := buildIntVec(t, mp, types.T_int32.ToType(), []int32{5, 6, 7}, nil)
		defer v.Free(mp)
		f, err := NewRoaringDocFilter(must(MarshalBitset(BuildBitset(v))))
		require.NoError(t, err)
		require.True(t, f.Test(v.GetRawBytesAt(0)))
		require.True(t, f.Test(v.GetRawBytesAt(2)))
	})

	t.Run("uint64", func(t *testing.T) {
		v := buildIntVec(t, mp, types.T_uint64.ToType(), []uint64{0, 1 << 63, 42}, nil)
		defer v.Free(mp)
		f, err := NewRoaringDocFilter(must(MarshalBitset(BuildBitset(v))))
		require.NoError(t, err)
		for i := 0; i < v.Length(); i++ {
			require.True(t, f.Test(v.GetRawBytesAt(i)))
		}
	})

	t.Run("uint8", func(t *testing.T) {
		v := buildIntVec(t, mp, types.T_uint8.ToType(), []uint8{0, 200, 255}, nil)
		defer v.Free(mp)
		f, err := NewRoaringDocFilter(must(MarshalBitset(BuildBitset(v))))
		require.NoError(t, err)
		require.True(t, f.Test(v.GetRawBytesAt(2)))
	})
}

// TestShareIndependentFree confirms one share's Free does not break another.
func TestShareIndependentFree(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{11, 22}, nil)
	defer v.Free(mp)

	f, err := NewRoaringDocFilter(must(MarshalBitset(BuildBitset(v))))
	require.NoError(t, err)

	a := f.Share()
	b := f.Share()
	require.True(t, a.Valid() && b.Valid())

	a.Free()
	require.False(t, a.Valid())
	// b (and the original) remain usable after a is freed
	require.True(t, b.Valid())
	require.True(t, b.Test(v.GetRawBytesAt(0)))
	require.True(t, f.Test(v.GetRawBytesAt(1)))
}

func TestNilFilterSafe(t *testing.T) {
	var f *RoaringDocFilter
	require.False(t, f.Valid())
	require.False(t, f.Test([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	require.Nil(t, f.TestVector(nil, nil))
	f.Free() // must not panic
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
