// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// makeVec creates a vector with explicit NULLs for testing.
func makeVec[T canUseRoaring](t *testing.T, mp *mpool.MPool, typ types.Type, vals []T, isNull []bool) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(vec, vals, isNull, mp))
	return vec
}

func TestRoaringAddFuncSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	tests := []struct {
		name      string
		typ       types.T
		vals      []int32
		isNull    []bool
		wantCount uint64
	}{
		{
			name:      "no nulls",
			typ:       types.T_int32,
			vals:      []int32{1, 2, 3},
			isNull:    []bool{false, false, false},
			wantCount: 3,
		},
		{
			name:      "all nulls",
			typ:       types.T_int32,
			vals:      []int32{0, 0, 0},
			isNull:    []bool{true, true, true},
			wantCount: 0,
		},
		{
			name:      "mixed nulls and values",
			typ:       types.T_int32,
			vals:      []int32{10, 0, 20, 0, 30},
			isNull:    []bool{false, true, false, true, false},
			wantCount: 3,
		},
		{
			name:      "null at zero value - zero should not be added",
			typ:       types.T_int32,
			vals:      []int32{0},
			isNull:    []bool{true},
			wantCount: 0,
		},
		{
			name:      "real zero vs null zero",
			typ:       types.T_int32,
			vals:      []int32{0, 0},
			isNull:    []bool{false, true},
			wantCount: 1, // only real zero
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRoaringFilter(tt.typ)
			vec := makeVec(t, mp, tt.typ.ToType(), tt.vals, tt.isNull)
			defer vec.Free(mp)
			f.addFunc(f, vec)
			require.Equal(t, tt.wantCount, f.b.GetCardinality(), "unexpected bitmap cardinality")
		})
	}
}

func TestRoaringTestFuncSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	// Build a filter containing value 5.
	f := newRoaringFilter(types.T_int32)
	{
		buildVec := makeVec(t, mp, types.T_int32.ToType(), []int32{5}, []bool{false})
		defer buildVec.Free(mp)
		f.addFunc(f, buildVec)
	}

	tests := []struct {
		name    string
		vals    []int32
		isNull  []bool
		wantIdx int
		wantVal any
	}{
		{
			name:    "probe with matching value",
			vals:    []int32{5},
			isNull:  []bool{false},
			wantIdx: 0,
			wantVal: int32(5),
		},
		{
			name:    "probe with null only - no match",
			vals:    []int32{5},
			isNull:  []bool{true},
			wantIdx: -1,
			wantVal: nil,
		},
		{
			name:    "null before match - match still found",
			vals:    []int32{0, 5},
			isNull:  []bool{true, false},
			wantIdx: 1,
			wantVal: int32(5),
		},
		{
			name:    "null with zero-value that matches bitmap entry",
			vals:    []int32{5},
			isNull:  []bool{true},
			wantIdx: -1,
			wantVal: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probeVec := makeVec(t, mp, types.T_int32.ToType(), tt.vals, tt.isNull)
			defer probeVec.Free(mp)
			idx, val := f.testFunc(f, probeVec)
			require.Equal(t, tt.wantIdx, idx, "unexpected index")
			require.Equal(t, tt.wantVal, val, "unexpected value")
		})
	}
}

func TestRoaringTestAndAddFuncSkipsNulls(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	tests := []struct {
		name         string
		vals         []int32
		isNull       []bool
		wantIdx      int
		wantVal      any
		wantBitCount uint64
	}{
		{
			name:         "no duplicates, no nulls",
			vals:         []int32{1, 2, 3},
			isNull:       []bool{false, false, false},
			wantIdx:      -1,
			wantVal:      nil,
			wantBitCount: 3,
		},
		{
			name:         "duplicate detected",
			vals:         []int32{1, 2, 1},
			isNull:       []bool{false, false, false},
			wantIdx:      2,
			wantVal:      int32(1),
			wantBitCount: 2, // 1 and 2 added before dup detected
		},
		{
			name:         "multiple nulls - no false duplicate",
			vals:         []int32{0, 0, 0},
			isNull:       []bool{true, true, true},
			wantIdx:      -1,
			wantVal:      nil,
			wantBitCount: 0,
		},
		{
			name:         "nulls interleaved - only real values checked",
			vals:         []int32{10, 0, 20, 0, 30},
			isNull:       []bool{false, true, false, true, false},
			wantIdx:      -1,
			wantVal:      nil,
			wantBitCount: 3,
		},
		{
			name:         "null then duplicate real value",
			vals:         []int32{0, 42, 42},
			isNull:       []bool{true, false, false},
			wantIdx:      2,
			wantVal:      int32(42),
			wantBitCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRoaringFilter(types.T_int32)
			vec := makeVec(t, mp, types.T_int32.ToType(), tt.vals, tt.isNull)
			defer vec.Free(mp)
			idx, val := f.testAndAddFunc(f, vec)
			require.Equal(t, tt.wantIdx, idx, "unexpected index")
			require.Equal(t, tt.wantVal, val, "unexpected value")
			require.Equal(t, tt.wantBitCount, f.b.GetCardinality(), "unexpected bitmap cardinality")
		})
	}
}

func TestRoaringMultipleTypes(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	t.Run("int8", func(t *testing.T) {
		f := newRoaringFilter(types.T_int8)
		vec := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []int8{1, 0, 3}, []bool{false, true, false}, mp))
		defer vec.Free(mp)
		f.addFunc(f, vec)
		require.Equal(t, uint64(2), f.b.GetCardinality())
	})

	t.Run("int16", func(t *testing.T) {
		f := newRoaringFilter(types.T_int16)
		vec := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []int16{100, 0, 200}, []bool{false, true, false}, mp))
		defer vec.Free(mp)
		f.addFunc(f, vec)
		require.Equal(t, uint64(2), f.b.GetCardinality())
	})

	t.Run("uint8", func(t *testing.T) {
		f := newRoaringFilter(types.T_uint8)
		vec := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []uint8{10, 0, 20}, []bool{false, true, false}, mp))
		defer vec.Free(mp)
		f.addFunc(f, vec)
		require.Equal(t, uint64(2), f.b.GetCardinality())
	})

	t.Run("uint16", func(t *testing.T) {
		f := newRoaringFilter(types.T_uint16)
		vec := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []uint16{1000, 0, 2000}, []bool{false, true, false}, mp))
		defer vec.Free(mp)
		f.addFunc(f, vec)
		require.Equal(t, uint64(2), f.b.GetCardinality())
	})

	t.Run("uint32", func(t *testing.T) {
		f := newRoaringFilter(types.T_uint32)
		vec := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixedList(vec, []uint32{50000, 0, 60000}, []bool{false, true, false}, mp))
		defer vec.Free(mp)
		f.addFunc(f, vec)
		require.Equal(t, uint64(2), f.b.GetCardinality())
	})
}

func TestRoaringTestAndAddMultipleBatches(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	f := newRoaringFilter(types.T_int32)

	// Batch 1: values 1, NULL, 3
	v1 := makeVec(t, mp, types.T_int32.ToType(), []int32{1, 0, 3}, []bool{false, true, false})
	defer v1.Free(mp)
	idx, _ := f.testAndAddFunc(f, v1)
	require.Equal(t, -1, idx, "batch 1 should have no duplicates")

	// Batch 2: values NULL, 2, NULL — no duplicate (NULLs skipped)
	v2 := makeVec(t, mp, types.T_int32.ToType(), []int32{0, 2, 0}, []bool{true, false, true})
	defer v2.Free(mp)
	idx, _ = f.testAndAddFunc(f, v2)
	require.Equal(t, -1, idx, "batch 2 NULLs should not trigger duplicate")

	// Batch 3: values 1 — duplicate of batch 1
	v3 := makeVec(t, mp, types.T_int32.ToType(), []int32{1}, []bool{false})
	defer v3.Free(mp)
	idx, val := f.testAndAddFunc(f, v3)
	require.Equal(t, 0, idx, "batch 3 should detect duplicate")
	require.Equal(t, int32(1), val)
}

func TestValueToString(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"int8", int8(42), "42"},
		{"int16", int16(-100), "-100"},
		{"int32", int32(999), "999"},
		{"uint8", uint8(255), "255"},
		{"uint16", uint16(65535), "65535"},
		{"uint32", uint32(4294967295), "4294967295"},
		{"nil", nil, ""},
		{"string", "hello", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, valueToString(tt.val))
		})
	}
}

func TestIfCanUseRoaringFilter(t *testing.T) {
	roaringTypes := []types.T{
		types.T_int8, types.T_int16, types.T_int32,
		types.T_uint8, types.T_uint16, types.T_uint32,
	}
	for _, typ := range roaringTypes {
		require.True(t, IfCanUseRoaringFilter(typ), "expected true for %v", typ)
	}

	nonRoaringTypes := []types.T{
		types.T_int64, types.T_uint64, types.T_float32, types.T_float64,
		types.T_varchar, types.T_date, types.T_datetime,
	}
	for _, typ := range nonRoaringTypes {
		require.False(t, IfCanUseRoaringFilter(typ), "expected false for %v", typ)
	}
}

func TestNewRoaringFilterAllTypes(t *testing.T) {
	for _, typ := range []types.T{
		types.T_int8, types.T_int16, types.T_int32,
		types.T_uint8, types.T_uint16, types.T_uint32,
	} {
		f := newRoaringFilter(typ)
		require.NotNil(t, f)
		require.NotNil(t, f.b)
		require.NotNil(t, f.addFunc)
		require.NotNil(t, f.testFunc)
		require.NotNil(t, f.testAndAddFunc)
	}
}
