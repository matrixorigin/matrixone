// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

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
			require.Equal(t, []byte(strings.Repeat("y", 32)), dst.GetBytesAt(1))
			require.Zero(t, dst.AppendAllocation(0, 0))
		})
	}

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
