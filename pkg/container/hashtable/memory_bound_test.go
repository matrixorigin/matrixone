// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashtable

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestBuildMemoryBoundMatchesCellPolicy(t *testing.T) {
	for _, rows := range []uint64{0, 512, 513} {
		retained, peak, ok := BuildMemoryBound(rows, false)
		require.True(t, ok)
		pool := mpool.MustNewZero()
		table := &Int64HashMap{}
		require.NoError(t, table.Init(pool))
		require.NoError(t, table.ResizeOnDemand(int(rows)))
		require.GreaterOrEqual(t, retained, table.Size())
		require.GreaterOrEqual(t, peak, retained)
		if rows == 513 {
			require.Greater(t, peak, retained, "old block survives complete replacement allocation")
		}
		table.Free()
		require.Zero(t, pool.CurrNB())
	}
	_, _, ok := BuildMemoryBound(math.MaxUint64, true)
	require.False(t, ok)
}
