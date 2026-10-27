// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi_update

import (
	"math"
	"strconv"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// MetadataMemoryBound describes the modern WriteS3 -> FlushS3Info protocol:
// one row with action, tableID, affectedRows, name and encoded batch. The
// compiler must prove this provenance; legacy multirow/actionUpdate frames do
// not carry this capability. Nested modern batches contain at most two vectors.
type MetadataMemoryBound struct {
	Rows           int
	BatchBytes     int64
	RowBytes       int64
	ColumnBytes    [5]int64
	ColumnRowBytes [5]int64
	ScratchBytes   int64
}

// metadataGoObjectBytes includes the scanned-object header before rounding to
// a power-of-two class. This also bounds Go's intermediate allocation classes.
// All requests here are fixed small structures or checked table-map buckets.
func metadataGoObjectBytes(size uintptr) int64 {
	class, ok := malloc.ClassAllocationSize(uint64(size + unsafe.Sizeof(uintptr(0))))
	if !ok || class > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(class)
}

// MetadataOutputBound takes the upstream protocol frame limit. Pure
// INSERT derives that limit from colexec.InsertMetadataFrameLimit, independently
// of a larger shared raw-data output lease. spoolFrame creates a fresh outer
// batch and appends exactly once. Its Insert/Delete payload and masks are decoded with ReadNoCopy, so
// FlushS3Info borrows that frame rather than making another payload clone.
// Local batch/vector headers and copied attribute names live until Call's
// deferred cleanup; the table-ID source map lives until operator Free.
func MetadataOutputBound(frameLimit int64, tableCount, totalColumns int) (MetadataMemoryBound, bool) {
	var result MetadataMemoryBound
	const fixed = 1 + 8 + 8 + 2*types.VarlenaSize
	if frameLimit < fixed || tableCount <= 0 || totalColumns < tableCount {
		return result, false
	}
	nameBytes := max(len(objectio.PhysicalAddr_Attr), types.ObjectidSize+1+len(strconv.Itoa(deletion.FlushDeltaLoc)))
	result.Rows, result.BatchBytes, result.RowBytes = 1, frameLimit, frameLimit
	result.ColumnBytes = [5]int64{1, 8, 8, int64(types.VarlenaSize + nameBytes), frameLimit}
	result.ColumnRowBytes = result.ColumnBytes

	batchObjects := metadataGoObjectBytes(unsafe.Sizeof(batch.Batch{})) +
		2*metadataGoObjectBytes(unsafe.Sizeof(vector.Vector{})) +
		metadataGoObjectBytes(2*unsafe.Sizeof(uintptr(0))) + // Vecs backing.
		metadataGoObjectBytes(2*unsafe.Sizeof(string(""))) // Attrs backing.
	// These are the only nested schemas emitted by the modern writer:
	// inserted BlockInfo/ObjectStats, tombstone ObjectStats, or RowID/PK.
	attrs := max(metadataGoObjectBytes(uintptr(len(catalog.BlockMeta_BlockInfo)))+metadataGoObjectBytes(uintptr(len(catalog.ObjectMeta_ObjectStats))),
		metadataGoObjectBytes(uintptr(len(catalog.Row_ID)))+metadataGoObjectBytes(uintptr(len("pk"))))
	// A single decoded frame has one action. Sources cache at most one
	// relation per statically planned table. Two padded buckets per table
	// cover old/new map growth; the two map headers are separate owners.
	mapHeader := metadataGoObjectBytes(64)
	actionBucket := metadataGoObjectBytes(8*(2*unsafe.Sizeof(uintptr(0))) + 2*unsafe.Sizeof(uintptr(0)))
	sourceBucket := metadataGoObjectBytes(8*(unsafe.Sizeof(uint64(0))+2*unsafe.Sizeof(uintptr(0))) + 2*unsafe.Sizeof(uintptr(0)))
	// Prepare retains context info/attribute headers and two buffer-pointer
	// arrays, and briefly creates an S3 delegate to order contexts. That
	// delegate's arrays, empty block maps and free lists are then destroyed.
	ctxObject := metadataGoObjectBytes(unsafe.Sizeof(updateCtxInfo{}))
	freeList := metadataGoObjectBytes(unsafe.Sizeof(containers.BatchFreeList{}))
	blockBucket := metadataGoObjectBytes(8*(unsafe.Sizeof(types.Blockid{})+2*unsafe.Sizeof(uintptr(0))) + 2*unsafe.Sizeof(uintptr(0)))
	perTable := 4*sourceBucket + ctxObject + freeList + mapHeader + blockBucket
	if int64(tableCount) > math.MaxInt64/perTable {
		return MetadataMemoryBound{}, false
	}
	static := batchObjects + attrs + 3*mapHeader + actionBucket +
		metadataGoObjectBytes(unsafe.Sizeof(s3WriterDelegate{})) + metadataGoObjectBytes(unsafe.Sizeof(mpool.MPool{})) +
		2*mapHeader + metadataGoObjectBytes(2*unsafe.Sizeof(uint64(0))) // Empty pool's ptr/xpoolFree maps and allocation counter.
	mapBytes := int64(tableCount) * perTable
	if mapBytes > math.MaxInt64-static {
		return MetadataMemoryBound{}, false
	}
	result.ScratchBytes = static + mapBytes
	// Nine pointer arrays: seven delegate arrays plus insert/delete buffers.
	// The remaining arrays hold sequence-slice headers, sort/PK indexes,
	// schema versions, cluster flags, row counts and deferred-delete credit.
	ptr := unsafe.Sizeof(uintptr(0))
	for _, width := range []uintptr{ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr,
		unsafe.Sizeof([]uint16{}), unsafe.Sizeof(int(0)), unsafe.Sizeof(int(0)),
		unsafe.Sizeof(uint32(0)), unsafe.Sizeof(bool(false)), unsafe.Sizeof(uint64(0)), unsafe.Sizeof(int64(0))} {
		if int64(tableCount) > math.MaxInt64/int64(width) {
			return MetadataMemoryBound{}, false
		}
		bytes := metadataGoObjectBytes(uintptr(tableCount) * width)
		if result.ScratchBytes > math.MaxInt64-bytes {
			return MetadataMemoryBound{}, false
		}
		result.ScratchBytes += bytes
	}
	// insertAttrs plus GetSequms' seqnums, types and attribute arrays append
	// from empty slices. Five N elements cover complete new class-rounded
	// backing (<=4N) while old backing (<N) still exists.
	columnBytes := int64(5 * (2*unsafe.Sizeof(string("")) + unsafe.Sizeof(types.Type{}) + unsafe.Sizeof(uint16(0))))
	if int64(totalColumns) > (math.MaxInt64-result.ScratchBytes)/columnBytes {
		return MetadataMemoryBound{}, false
	}
	result.ScratchBytes += int64(totalColumns) * columnBytes
	return result, true
}
