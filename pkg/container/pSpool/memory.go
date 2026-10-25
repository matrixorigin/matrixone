// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package pSpool

import (
	"math"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// MemoryMetadataBound covers InitMyPipelineSpool's fixed objects/arrays and
// receiver queues without creating any of them. The receiver queues are C
// separate allocations of nextPowerOfTwo(max(2,C)) uint32 entries: their total
// is quadratic in fanout, not one batch buffer per receiver. Batch data/areas,
// dynamic Vecs/Attrs and per-cache byte-slice lists are charged separately.
func MemoryMetadataBound(receivers uint32) (int64, bool) {
	if receivers == 0 {
		return 0, false
	}
	slots := max(uint64(2), uint64(receivers))
	queue := uint64(1) << bits.Len64(slots-1)
	// newReceivers uses uint32 queue arithmetic. A wrapped queue cannot be
	// executed safely; this is its representation boundary, not a fanout policy.
	if queue > math.MaxUint32 {
		return 0, false
	}
	allocation := func(n, width uint64, scanned bool) (int64, bool) {
		if n != 0 && width > math.MaxUint64/n {
			return 0, false
		}
		size := n * width
		if size == 0 {
			return 0, true
		}
		if scanned {
			if size > math.MaxUint64-8 {
				return 0, false
			}
			size += 8
		}
		capacity, ok := malloc.ClassAllocationSize(size)
		return int64(capacity), ok && capacity <= math.MaxInt64
	}
	var total int64
	add := func(n, width uint64, scanned bool) bool {
		bytes, ok := allocation(n, width, scanned)
		if !ok || bytes > math.MaxInt64-total {
			return false
		}
		total += bytes
		return true
	}
	for _, object := range []uintptr{unsafe.Sizeof(PipelineSpool{}), unsafe.Sizeof(cachedBatch{}), unsafe.Sizeof(spoolBuffer{})} {
		if !add(1, uint64(object), true) {
			return 0, false
		}
	}
	for _, array := range []struct {
		n, width uint64
		scanned  bool
	}{
		{slots, uint64(unsafe.Sizeof(pipelineSpoolMessage{})), true},
		{slots, uint64(unsafe.Sizeof(bool(false))), false}, // doRefCheck.
		{slots, uint64(unsafe.Sizeof(bool(false))), false}, // occupied, first Send.
		{uint64(receivers), uint64(unsafe.Sizeof(receiver{})), true},
		{slots, uint64(unsafe.Sizeof(readyToUseItem{})), true},
		{slots, uint64(unsafe.Sizeof(oneBatchMemoryCache{})), true},
	} {
		if !add(array.n, array.width, array.scanned) {
			return 0, false
		}
	}
	if receivers > 1 && !add(slots, uint64(unsafe.Sizeof(atomic.Int32{})), false) {
		return 0, false
	}
	// Batch objects and queues are allocated separately inside initialization,
	// so round one allocation before multiplying by their instance count.
	for _, item := range []struct {
		count, n, width uint64
		scanned         bool
	}{
		{slots, 1, uint64(unsafe.Sizeof(batch.Batch{})), true},
		{uint64(receivers), queue, 4, false},
	} {
		bytes, ok := allocation(item.n, item.width, item.scanned)
		if !ok || uint64(bytes) > uint64(math.MaxInt64)/item.count {
			return 0, false
		}
		if int64(item.count)*bytes > math.MaxInt64-total {
			return 0, false
		}
		total += int64(item.count) * bytes
	}
	// The uint32 channel allocates its header and payload together. Round the
	// combined request: rounding payload and header separately undercounts
	// the page-class transition when a power-of-two payload gains its header.
	// 256 bounds hchan plus alignment on supported 64-bit Go platforms.
	if !add(1, 256+slots*4, false) || !add(1, 256, false) {
		return 0, false
	}
	return total, true
}
