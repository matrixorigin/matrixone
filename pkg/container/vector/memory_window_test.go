// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package vector

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestWindowHasBitmapBitsSelectedRange(t *testing.T) {
	var sparse, dense nulls.Nulls
	sparse.Add(0, 63, 64, 129, 8191, 20000)
	dense.AddRange(0, 8191)
	for _, tc := range []struct {
		name       string
		bitmap     *nulls.Nulls
		start, end int
		want       bool
	}{
		{"nil", nil, 1, 8192, false},
		{"empty", &sparse, 64, 64, false},
		{"before-word", &sparse, 1, 63, false},
		{"word-end", &sparse, 62, 64, true},
		{"word-start", &sparse, 64, 65, true},
		{"between-words", &sparse, 65, 129, false},
		{"nonzero-window", &sparse, 128, 130, true},
		{"tail", &sparse, 8191, 8192, true},
		{"stale-outside-window", &sparse, 8192, 20000, false},
		{"dense-prefix-tail-hole", &dense, 8191, 8192, false},
		{"dense-tail", &dense, 8190, 8191, true},
		{"past-backing", &dense, 20000, 20001, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, windowHasBitmapBits(tc.bitmap, tc.start, tc.end))
		})
	}
}

var benchmarkWindowBitmapFound bool

func BenchmarkWindowHasBitmapBitsSingleRow(b *testing.B) {
	for _, rows := range []int{128, 8192} {
		b.Run(fmt.Sprintf("dense-%d", rows), func(b *testing.B) {
			var bitmap nulls.Nulls
			bitmap.AddRange(0, uint64(rows))
			b.ResetTimer()
			var found bool
			for i := 0; i < b.N; i++ {
				row := i % rows
				found = windowHasBitmapBits(&bitmap, row, row+1)
			}
			benchmarkWindowBitmapFound = found
		})
	}
}

func TestMemoryWindowReferencedArea(t *testing.T) {
	m := mpool.MustNew("window-memory")
	defer mpool.DeleteMPool(m)
	src := NewVec(types.T_varchar.ToType())
	defer src.Free(m)
	values := [][]byte{[]byte(strings.Repeat("x", 4096)), []byte("inline"), []byte(strings.Repeat("y", 32)), nil}
	require.NoError(t, AppendBytesList(src, values, []bool{false, false, false, true}, m))
	window, err := src.Window(1, 4)
	require.NoError(t, err)
	defer window.Free(m)
	require.Equal(t, int64(3*types.VarlenaSize+32), window.WindowSize(0, 3))
	require.Greater(t, window.Size(), int(window.WindowSize(0, 3)), "shared area is not the copied window payload")
	for _, mode := range []string{"union-all", "union-window"} {
		t.Run(mode, func(t *testing.T) {
			dst := NewVec(types.T_varchar.ToType())
			defer dst.Free(m)
			peak := dst.AppendWindowAllocation(window, 0, 3)
			before := m.AllocatedBytes()
			if mode == "union-all" {
				require.NoError(t, GetUnionAllFunction(*dst.GetType(), m)(dst, window))
			} else {
				require.NoError(t, dst.UnionBatch(window, 0, window.Length(), nil, m))
			}
			require.GreaterOrEqual(t, peak, m.AllocatedBytes()-before)
			require.Equal(t, 3, dst.Length())
			require.Equal(t, 32, len(dst.GetArea()), "discard area belonging only to unselected rows")
			require.Equal(t, []byte(strings.Repeat("y", 32)), dst.GetBytesAt(1))
			require.Zero(t, dst.AppendAllocation(0, 0))
		})
	}

}

func TestUnionWindowPreservesSharedOffsetsAndSelectedBits(t *testing.T) {
	m := mpool.MustNew("shared-window-memory")
	defer mpool.DeleteMPool(m)
	constant, err := NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("s", 256)), 130, m)
	require.NoError(t, err)
	defer constant.Free(m)
	shared := NewVec(types.T_varchar.ToType())
	defer shared.Free(m)
	require.NoError(t, shared.UnionBatch(constant, 0, 130, nil, m))
	shared.GetNulls().Add(2, 129, 300)
	shared.GetGrouping().Add(5, 128, 300)
	for _, mode := range []string{"union-window", "union-all"} {
		t.Run(mode, func(t *testing.T) {
			dst := NewVec(types.T_varchar.ToType())
			defer dst.Free(m)
			require.NoError(t, AppendBytes(dst, []byte(strings.Repeat("p", 32)), false, m))
			bound := dst.AppendWindowAllocation(shared, 1, 129)
			before := int64(dst.Allocated()) + int64(nulls.Allocated(dst.GetNulls())+nulls.Allocated(dst.GetGrouping()))
			if mode == "union-window" {
				require.NoError(t, dst.UnionBatch(shared, 1, 128, nil, m))
			} else {
				window, err := shared.Window(1, 129)
				require.NoError(t, err)
				defer window.Free(m)
				require.NoError(t, GetUnionAllFunction(*dst.GetType(), m)(dst, window))
			}
			after := int64(dst.Allocated()) + int64(nulls.Allocated(dst.GetNulls())+nulls.Allocated(dst.GetGrouping()))
			require.LessOrEqual(t, after-before, bound)
			require.Equal(t, 32+256, len(dst.GetArea()), "repeated references share one copied physical area")
			require.Equal(t, 129, dst.Length())
			require.Equal(t, strings.Repeat("p", 32), string(dst.GetBytesAt(0)))
			require.Equal(t, strings.Repeat("s", 256), string(dst.GetBytesAt(128)))
			for row := range 129 {
				require.Equal(t, row == 2, dst.GetNulls().Contains(uint64(row)))
				require.Equal(t, row == 5 || row == 128, dst.GetGrouping().Contains(uint64(row)))
			}
			require.False(t, dst.GetNulls().Contains(129))
			require.False(t, dst.GetGrouping().Contains(300))
		})
	}
}

func TestWindowPreservesConstantGroupingRange(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	value, err := NewConstFixed(types.T_int64.ToType(), int64(7), 6, mp)
	require.NoError(t, err)
	defer value.Free(mp)
	value.GetGrouping().Add(0, 2, 4, 8)
	rollup := NewRollupConst(types.T_varchar.ToType(), 6, mp)
	defer rollup.Free(mp)
	for _, src := range []*Vector{value, rollup} {
		window, err := src.Window(1, 5)
		require.NoError(t, err)
		require.Equal(t, 4, window.Length())
		for row := range 4 {
			require.Equal(t, src.GetGrouping().Contains(uint64(row+1)), window.GetGrouping().Contains(uint64(row)))
		}
		require.False(t, window.GetGrouping().Contains(7), "stale source bits are outside the window")
		window.Free(mp)
		require.True(t, src.GetGrouping().Contains(4), "window owns its bitmap independently")
	}
}

func TestUnionConstantGroupingUsesSelectedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fixed, err := NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
	require.NoError(t, err)
	variable, err := NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("v", 32)), 3, mp)
	require.NoError(t, err)
	null := NewConstNull(types.T_varchar.ToType(), 3, mp)
	for _, src := range []*Vector{fixed, variable, null} {
		defer src.Free(mp)
		// The count equals length but one bit is stale. It does not mean
		// every logical row belongs to a grouping set.
		src.GetGrouping().Add(0, 2, 5)
		for _, selection := range []struct {
			offset int64
			count  int
			flags  []uint8
			group  []bool
		}{
			{0, 3, nil, []bool{true, false, true}},
			{1, 2, nil, []bool{false, true}},
			{0, 3, []uint8{1, 0, 1}, []bool{true, true}},
			{1, 2, []uint8{1, 1}, []bool{false, true}},
		} {
			dst := NewVec(*src.GetType())
			require.NoError(t, dst.UnionBatch(src, selection.offset, selection.count, selection.flags, mp))
			require.Equal(t, len(selection.group), dst.Length())
			for row, grouped := range selection.group {
				require.Equal(t, grouped, dst.GetGrouping().Contains(uint64(row)))
				require.Equal(t, src.IsConstNull(), dst.GetNulls().Contains(uint64(row)))
			}
			require.False(t, dst.GetGrouping().Contains(5))
			dst.Free(mp)
		}
	}
}

func TestAppendWindowReusesCapacityWithoutCopiedBitmapBits(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	src := NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	require.NoError(t, AppendFixed(src, int64(7), false, mp))
	src.GetNulls().Add(10)
	src.GetGrouping().Add(10)
	dst := NewVec(types.T_int64.ToType())
	defer dst.Free(mp)
	require.NoError(t, dst.PreExtend(8, mp))
	require.Zero(t, dst.AppendWindowAllocation(src, 0, 1), "reused data and no selected bitmap bits require no new allocation")
	before := mp.AllocatedBytes()
	require.NoError(t, dst.UnionBatch(src, 0, 1, nil, mp))
	require.Equal(t, before, mp.AllocatedBytes())
	require.Zero(t, nulls.Allocated(dst.GetNulls()))
	require.Zero(t, nulls.Allocated(dst.GetGrouping()))
}

func TestMemoryWindowConstantExpansion(t *testing.T) {
	m := mpool.MustNew("constant-memory")
	defer mpool.DeleteMPool(m)
	v, err := NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("a", 64)), 100, m)
	require.NoError(t, err)
	defer v.Free(m)
	require.Equal(t, int64(7*(types.VarlenaSize+64)), v.WindowSize(3, 10))
	n := NewConstNull(types.T_varchar.ToType(), 100, m)
	defer n.Free(m)
	require.Equal(t, int64(7*types.VarlenaSize), n.WindowSize(3, 10))
}

func TestAppendAllocationIncludesFullNewStorage(t *testing.T) {
	m := mpool.MustNew("growth-memory")
	defer mpool.DeleteMPool(m)
	v := NewVec(types.T_varchar.ToType())
	defer v.Free(m)
	require.NoError(t, v.PreExtendWithArea(4, 4096, m))
	old := v.Allocated()
	peak := v.AppendAllocation(900, 10000)
	require.Greater(t, peak, int64(old))
	before := m.AllocatedBytes()
	require.NoError(t, v.PreExtendWithArea(900, 10000, m))
	// Both old allocations existed before either replacement was installed.
	require.Equal(t, int64(v.Allocated()), peak)
	require.Equal(t, int64(v.Allocated()-old), m.AllocatedBytes()-before)
}
