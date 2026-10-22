// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package nulls

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNullsAllocationAndGrowth(t *testing.T) {
	n := &Nulls{}
	require.Equal(t, int64(8), GrowthAllocation(n, 1))
	n.Add(0)
	require.Equal(t, 8, Allocated(n))
	require.Zero(t, GrowthAllocation(n, 64))
	require.Equal(t, int64(16), GrowthAllocation(n, 65))
	n.Add(64)
	require.Equal(t, 16, Allocated(n))
	n.Reset()
	require.Zero(t, Allocated(n))
	require.Zero(t, Allocated(nil))
	require.Equal(t, int64(16), GrowthAllocation(nil, 65))
}
