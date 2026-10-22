// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package bitmap

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBitmapAllocationAndGrowth(t *testing.T) {
	n := &Bitmap{}
	require.Equal(t, int64(8), n.GrowthAllocation(1))
	n.TryExpandWithSize(64)
	require.Equal(t, 8, n.Allocated())
	require.Zero(t, n.GrowthAllocation(64))
	require.Equal(t, int64(16), n.GrowthAllocation(65), "new allocation includes old words, not only the extra word")
	n.TryExpandWithSize(65)
	require.Equal(t, 16, n.Allocated())
	n.data = n.data[:1]
	n.len = 1
	require.Equal(t, 8, n.Size())
	require.Equal(t, 16, n.Allocated())
	require.Zero(t, n.GrowthAllocation(128))
	n.Reset()
	require.Zero(t, n.Allocated())
}
