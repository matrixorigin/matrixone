// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package ioutil

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/pierrec/lz4/v4"
)

func byteWindowEnd(data *batch.Batch, start, maxRows int, base, limit int64) int {
	hi := min(data.RowCount(), start+maxRows)
	if limit <= 0 {
		return hi
	}
	size := func(end int) int64 {
		n := base
		for _, v := range data.Vecs {
			n += v.WindowSize(start, end)
		}
		return n
	}
	lo := start + 1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if size(mid) <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// SinkerAppendAllocation predicts full new vector backing stores, taking the
// actual staged tail and reusable batches into account. Write preextends each
// selected window using precisely these same vector growth rules.
func SinkerAppendAllocation(data, tail *batch.Batch, buffers containers.IBatchBuffer, limit int64) int64 {
	var total int64
	offset, reused := 0, 0
	for offset < data.RowCount() {
		curr := tail
		tail = nil
		var first int64
		for _, v := range data.Vecs {
			first += v.WindowSize(offset, offset+1)
		}
		if curr == nil || curr.RowCount() >= objectio.BlockMaxRows || limit > 0 && curr.RowCount() > 0 && int64(curr.Size())+first > limit {
			curr = nil
			if p, ok := buffers.(interface{ Peek(int) *batch.Batch }); ok {
				curr = p.Peek(reused)
				reused++
			}
		}
		rows, base := 0, int64(0)
		if curr != nil {
			rows = curr.RowCount()
			base = int64(curr.Size())
		}
		end := byteWindowEnd(data, offset, objectio.BlockMaxRows-rows, base, limit)
		for col, src := range data.Vecs {
			dst := (*vector.Vector)(nil)
			if curr != nil {
				dst = curr.Vecs[col]
			} else {
				dst = vector.NewOffHeapVecWithType(*src.GetType())
			}
			growth := dst.AppendWindowAllocation(src, offset, end)
			if growth > math.MaxInt64-total {
				return math.MaxInt64
			}
			total += growth

		}
		offset = end
	}
	return total
}

func roundedPowerOfTwo(n int64) int64 {
	if n <= 1 {
		return 1
	}
	if n > 1<<62 {
		return math.MaxInt64
	}
	n--
	for shift := uint(1); shift < 64; shift <<= 1 {
		n |= n >> shift
	}
	return n + 1
}

func ioEncodingBound(payload, columns int64) (encoded, compressed, metadata int64) {
	metadata = columns*int64(objectio.ZoneMapSize+128) + int64(objectio.BlockMaxRows)*128
	encoded = payload + metadata
	compressed = int64(lz4.CompressBlockBound(int(encoded)))
	return
}

// warmArenaBytes covers all three retained arena components: data and LZ4
// power-of-two buffers, plus bytes.Buffer serialization capacity. IOEnvelope
// has a distinct warm-checkout term so historical capacity cannot consume the
// allocation/serialization/fallback growth promises of this operation.
func warmArenaBytes(payload, columns int64) int64 {
	_, compressed, _ := ioEncodingBound(payload, columns)
	return 4 * roundedPowerOfTwo(compressed)
}

// IOEnvelope is the sum of simultaneously live spill allocations. Borrowed
// source vectors belong to I/D. Capacity rounding follows Grow/WriteArena and
// compression uses LZ4's own bound. Every source window and merge output uses
// the same byte unit; a legal singleton can exceed that target.
func IOEnvelope(projections [][]types.Type, unit, maxRowBytes int64) (int64, error) {
	if unit <= 0 || maxRowBytes < 0 {
		return 0, fmt.Errorf("invalid IO memory envelope")
	}
	payload := max(unit, maxRowBytes)
	var columns int64
	for _, schema := range projections {
		columns = max(columns, int64(len(schema)))
	}
	if payload > int64(math.MaxInt)/4 || columns > math.MaxInt64/(objectio.BlockMaxRows*128) {
		return 0, fmt.Errorf("IO memory envelope overflow")
	}
	// Per-frame column headers, null masks and block/bloom/hash metadata. The
	// bloom builder's row-index/hash arrays are bounded by one object block.
	encoded, compressed, metadata := ioEncodingBound(payload, columns)
	// Clone/merge vector old+new, serialization bytes.Buffer old+new, native
	// compressed scratch, arena old+new/fallback, and bloom builder scratch.
	parts := []int64{3 * mpool.GrowCapacity(0, payload), 3 * encoded, roundedPowerOfTwo(compressed), 3 * roundedPowerOfTwo(compressed), warmArenaBytes(payload, columns), metadata}
	var total int64
	for _, part := range parts {
		if part < 0 || total > math.MaxInt64-part {
			return 0, fmt.Errorf("IO memory envelope overflow")
		}
		total += part
	}
	return total, nil
}
