// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package pSpool

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpoolMetadataBoundIncludesEveryReceiverQueue(t *testing.T) {
	for _, receivers := range []uint32{1, 2, 3, 16, 17, 16384, 16385} {
		bound, ok := MemoryMetadataBound(receivers)
		require.True(t, ok)
		queue := nextPowerOfTwo(max(uint32(2), receivers))
		require.GreaterOrEqual(t, bound, int64(receivers)*int64(queue)*4)
	}
	before, ok := MemoryMetadataBound(16384)
	require.True(t, ok)
	after, ok := MemoryMetadataBound(16385)
	require.True(t, ok)
	require.Greater(t, after, before, "the queue capacity doubles just above a power-of-two fanout")
	_, ok = MemoryMetadataBound(math.MaxUint32)
	require.False(t, ok, "uint32 queue-capacity wrap is not an executable layout")
}
