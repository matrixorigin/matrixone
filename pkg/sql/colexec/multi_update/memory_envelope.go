// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package multi_update

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

// The fixed part of one committed-block entry includes its batch, two vector
// objects, bitmap object and slice backing, plus map buckets (including growth
// slack) and the temporary sorted block-id list. Each bucket allowance has
// eight padded key/value slots plus its control/overflow words. Two buckets per
// entry cover old+new map growth even at the least occupied boundary; the map
// header and sorted block-id array are included separately. Vector data/area
// and bitmap backing are accounted separately.
const deleteBlockMapBytes = int64(2*(8*(unsafe.Sizeof(types.Blockid{})+2*unsafe.Sizeof(uintptr(0)))+16) + 64 + unsafe.Sizeof(types.Blockid{}))
const deleteBlockObjectsBytes = int64(unsafe.Sizeof(deleteBlockData{})+unsafe.Sizeof(batch.Batch{})+
	2*unsafe.Sizeof(vector.Vector{})+unsafe.Sizeof(nulls.Nulls{})+2*unsafe.Sizeof(uintptr(0))) + deleteBlockMapBytes

func addMemoryBytes(a, b int64) int64 {
	if b < 0 || a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// deletePreparationBytes is a linear future-growth promise. In the worst case
// every row creates a distinct committed block. One rounded double-payload
// allowance covers live split-vector capacity; a second covers replacement
// backing during split append or BatchSet compaction. Those growth phases are
// sequential. Original retained source vectors remain charged separately.
// Per-row rounding covers independently allocated data/area stores, while
// bitmap words cover both columns' null/grouping old+new backing. No old vector
// is rescanned or recharged when a small source window is appended.
func deletePreparationBytes(bat *batch.Batch, start, end int, cols []int) int64 {
	rowids := bat.Vecs[cols[0]]
	if rowids.GetNulls().Count() == bat.RowCount() {
		return 0
	}
	var size int64
	for _, col := range cols {
		payload := bat.Vecs[col].WindowSize(start, end)
		if payload > math.MaxInt64/2 {
			return math.MaxInt64
		}
		capacity := mpool.GrowCapacity(0, min(2*payload, int64(mpool.CapLimit)))
		size = addMemoryBytes(size, 2*capacity)
	}
	rows := int64(end - start)
	// Two vectors × (data+area) × (old+new) × minimum eight-byte rounding.
	const allocationRounding = 2 * 2 * 2 * 8
	// Two vectors × (null+grouping) × (old+new) × one word per row.
	const bitmapWords = 2 * 2 * 2 * 8
	perRow := int64(options.DefaultBlockMaxRows/8) + deleteBlockObjectsBytes + allocationRounding + bitmapWords
	if rows > math.MaxInt64/perRow {
		return math.MaxInt64
	}
	return addMemoryBytes(size, rows*perRow)
}

func (writer *s3WriterDelegate) windowFootprint(bat *batch.Batch, start, end int) int64 {
	size := writer.projectionBytes(bat, start, end)
	for _, ctx := range writer.updateCtxs {
		if len(ctx.DeleteCols) > 0 {
			size = addMemoryBytes(size, deletePreparationBytes(bat, start, end, ctx.DeleteCols))
		}
	}
	return size
}

// A window has at most BlockMaxRows rows, so Extend can replace only its tail
// and create one additional batch. Match BatchSet's full-input fast path too.
func (writer *s3WriterDelegate) deleteAppendAllocation(idx int, bat *batch.Batch, cols []int) int64 {
	var tail *batch.Batch
	if idx < len(writer.deleteBatches) && writer.deleteBatches[idx] != nil {
		bs := writer.deleteBatches[idx]
		if bs.Length() > 0 {
			tail = bs.Get(bs.Length() - 1)
		}
	}
	if bat.RowCount() == objectio.BlockMaxRows || tail != nil && tail.RowCount() == objectio.BlockMaxRows {
		tail = nil
	}
	var size int64
	for start := 0; start < bat.RowCount(); {
		rows := 0
		if tail != nil {
			rows = tail.RowCount()
		}
		end := min(bat.RowCount(), start+objectio.BlockMaxRows-rows)
		for col, input := range cols {
			var dst *vector.Vector
			if tail != nil {
				dst = tail.Vecs[col]
			} else {
				dst = vector.NewOffHeapVecWithType(*bat.Vecs[input].GetType())
			}
			size = addMemoryBytes(size, dst.AppendWindowAllocation(bat.Vecs[input], start, end))
		}
		start = end
		tail = nil
	}
	return size
}

// InsertOnlyOutput proves that every frame contains fixed object metadata.
// A main-table actionInsert alone is insufficient: an index context can still
// carry raw RowID/PK deletes. Unknown/empty context lists keep the conservative
// raw-delete contract.
func InsertOnlyOutput(contexts []*MultiUpdateCtx) bool {
	if len(contexts) == 0 {
		return false
	}
	for _, ctx := range contexts {
		if ctx == nil || len(ctx.DeleteCols) > 0 {
			return false
		}
	}
	return true
}

// OutputEnvelope preserves the conservative raw-delete protocol, whose nested
// RowID/PK frame can contain a legal wide singleton. Pure INSERT callers use
// colexec.InsertMetadataOutputEnvelope instead of including unrelated raw rows.
func OutputEnvelope(unit, maxRowBytes int64, tableCount int) (int64, error) {
	if unit <= 0 || maxRowBytes < 0 || tableCount < 0 {
		return 0, fmt.Errorf("invalid import output envelope")
	}
	q := max(unit, maxRowBytes)
	if q > (math.MaxInt64-(1<<20))/4 {
		return 0, fmt.Errorf("import output envelope overflow")
	}
	metadata, err := colexec.InsertMetadataOutputEnvelope(unit, tableCount)
	if err != nil {
		return 0, err
	}
	return max(4*q+(1<<20), metadata), nil
}
