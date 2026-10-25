// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package mergeblock

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// MetadataMemoryBound applies only to the modern two-column metadata protocol
// (BlockInfo, ObjectStats). Legacy TableIdx/raw-batch frames and frames missing
// ObjectStats have independent decoding/object-loading owners and do not carry
// this capability. The compiler must prove the producer's protocol first.
type MetadataMemoryBound struct {
	Rows           int
	BatchBytes     int64
	RowBytes       int64
	ColumnBytes    [2]int64
	ColumnRowBytes [2]int64
	ScratchBytes   int64
}

// MetadataOutputBound derives the consumer promise from its upstream INSERT
// metadata protocol frame limit, rather than a larger raw-data output lease or
// unrelated source-row schema.
// Split retains the private destination vectors across frames. Each column can
// have its own historical maximum even when their maxima occur in different
// frames. Old+new stores coexist during AppendBytes replacement; a per-column
// doubled payload capacity and two generations cover that complete growth.
func MetadataOutputBound(frameLimit int64) (MetadataMemoryBound, bool) {
	var result MetadataMemoryBound
	entry := int64(types.VarlenaSize + objectio.BlockInfoSize)
	if frameLimit < entry {
		return result, false
	}
	rows := frameLimit / entry
	if rows > int64(math.MaxInt) {
		return result, false
	}
	result.Rows = int(rows)
	result.BatchBytes = frameLimit
	for col, width := range [2]int64{int64(objectio.BlockInfoSize), int64(objectio.ObjectStatsLen)} {
		if rows > math.MaxInt64/(width+types.VarlenaSize) {
			return MetadataMemoryBound{}, false
		}
		result.ColumnRowBytes[col] = width + types.VarlenaSize
		result.ColumnBytes[col] = min(frameLimit, rows*(width+types.VarlenaSize))
		result.RowBytes += result.ColumnRowBytes[col]
		data, area := rows*types.VarlenaSize, rows*width
		if data > int64(mpool.CapLimit) || area > int64(mpool.CapLimit) {
			return MetadataMemoryBound{}, false
		}
		data = mpool.GrowCapacity(0, min(2*data, int64(mpool.CapLimit)))
		area = mpool.GrowCapacity(0, min(2*area, int64(mpool.CapLimit)))
		growth := 2 * (data + area)
		if result.ScratchBytes > math.MaxInt64-growth {
			return MetadataMemoryBound{}, false
		}
		result.ScratchBytes += growth
	}
	// splitObjectStats uses BuildObjectMeta(uint16(blockCount)) while both
	// destination columns are live. Its versioned constructors expose the
	// exact fixed header/column lengths without allocating data-dependent IO.
	meta := int64(len(objectio.BuildBlockHeader())) + min(rows, int64(math.MaxUint16))*int64(len(objectio.BuildObjectColumnMeta()))
	// One map entry and batch/two-vector/slice objects. Both maps have only
	// main-table index zero under the modern protocol; mp2 remains empty.
	objects := int64(unsafe.Sizeof(batch.Batch{}) + 2*unsafe.Sizeof(vector.Vector{}) + 2*unsafe.Sizeof(uintptr(0)))
	mapBytes := int64(2 * (64 + 8*(unsafe.Sizeof(int(0))+unsafe.Sizeof(uintptr(0))) + 16))
	// Initial GetMetaLocBat appends two types: its one-element old backing
	// coexists with two-element new backing. Each Go class is <=2x request.
	typeAppend := int64(2 * (1 + 2) * unsafe.Sizeof(types.Type{}))
	extra := 2*meta + objects + mapBytes + typeAppend // Go allocation-class rounding for meta.
	if result.ScratchBytes > math.MaxInt64-extra {
		return MetadataMemoryBound{}, false
	}
	result.ScratchBytes += extra
	return result, true
}
