// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package publication

import (
	"context"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// readSingleBlockToBatch reads a single block from object content into a CN batch
func readSingleBlockToBatch(
	ctx context.Context,
	objectContent []byte,
	dataMeta objectio.ObjectDataMeta,
	blkIdx uint32,
	cols []uint16,
	typs []types.Type,
	maxSeqnum uint16,
	allocator fileservice.CacheDataAllocator,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	blkMeta := dataMeta.GetBlockMeta(blkIdx)

	// Create vectors for this block
	vecs := make([]*vector.Vector, len(cols))
	for i, typ := range typs {
		vecs[i] = vector.NewVec(typ)
	}

	for i, seqnum := range cols {
		var colMeta objectio.ColumnMeta
		var ext objectio.Extent

		if seqnum > maxSeqnum || blkMeta.ColumnMeta(seqnum).DataType() == 0 {
			// Generate null values for missing columns
			length := int(blkMeta.GetRows())
			for j := 0; j < length; j++ {
				if err := vector.AppendAny(vecs[i], nil, true, mp); err != nil {
					// Clean up on error
					for k := 0; k <= i; k++ {
						vecs[k].Free(mp)
					}
					return nil, err
				}
			}
			continue
		}
		colMeta = blkMeta.ColumnMeta(seqnum)
		ext = colMeta.Location()

		// Read column data from objectContent bytes
		if int(ext.Offset()+ext.Length()) > len(objectContent) {
			for k := 0; k <= i; k++ {
				vecs[k].Free(mp)
			}
			return nil, moerr.NewInternalErrorf(ctx, "object content too small for column extent at seqnum %d, block %d", seqnum, blkIdx)
		}
		colData := objectContent[ext.Offset() : ext.Offset()+ext.Length()]

		// Decompress if needed
		var decompressedData []byte
		var decompressedBuf fscache.Data

		if ext.Alg() == compress.None {
			decompressedData = append([]byte(nil), colData...)
		} else {
			decompressedBuf = allocator.AllocateCacheDataWithHint(ctx, int(ext.OriginSize()), malloc.NoClear)
			bs, err := compress.Decompress(colData, decompressedBuf.Bytes(), compress.Lz4)
			if err != nil {
				if decompressedBuf != nil {
					decompressedBuf.Release()
				}
				for k := 0; k <= i; k++ {
					vecs[k].Free(mp)
				}
				return nil, moerr.NewInternalErrorf(ctx, "failed to decompress column data: %v", err)
			}
			decompressedData = decompressedBuf.Bytes()[:len(bs)]
			decompressedData = append([]byte(nil), decompressedData...)
			if decompressedBuf != nil {
				decompressedBuf.Release()
			}
		}

		// Decode to vector.Vector
		obj, err := objectio.Decode(decompressedData)
		if err != nil {
			for k := 0; k <= i; k++ {
				vecs[k].Free(mp)
			}
			return nil, moerr.NewInternalErrorf(ctx, "failed to decode column data: %v", err)
		}
		decodedVec := obj.(*vector.Vector)

		// Copy data from decoded vector to result vector
		if err := vecs[i].UnionBatch(decodedVec, 0, decodedVec.Length(), nil, mp); err != nil {
			for k := 0; k <= i; k++ {
				vecs[k].Free(mp)
			}
			return nil, moerr.NewInternalErrorf(ctx, "failed to union vector: %v", err)
		}
	}

	// Create batch
	bat := batch.NewWithSize(len(cols))
	bat.Vecs = vecs
	attrs := make([]string, len(cols))
	for i := range cols {
		attrs[i] = fmt.Sprintf("col_%d", i)
	}
	bat.SetAttributes(attrs)
	bat.SetRowCount(vecs[0].Length())

	return bat, nil
}

// rewriteTombstoneRowidsBatch rewrites delete rowids in CN batch using aobjectMap
func rewriteTombstoneRowidsBatch(
	ctx context.Context,
	bat *batch.Batch,
	aobjectMap AObjectMap,
	mp *mpool.MPool,
) error {
	if bat == nil || bat.RowCount() == 0 || aobjectMap == nil {
		return nil
	}

	// Tombstone schema: first column is delete rowid (TombstoneAttr_Rowid_Attr)
	rowidVec := bat.Vecs[0]
	if rowidVec == nil || rowidVec.Length() == 0 {
		return nil
	}

	// Verify the column type is Rowid
	if rowidVec.GetType().Oid != types.T_Rowid {
		return moerr.NewInternalErrorf(ctx, "first column of tombstone should be rowid, got %s", rowidVec.GetType().String())
	}

	// Get rowid values from the vector
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)

	// Iterate through each rowid and rewrite if mapping exists
	for i := range rowids {
		// Extract object ID from rowid
		upstreamObjID := rowids[i].BorrowObjectID()
		upstreamIDStr := upstreamObjID.String()

		// Check if this object ID has a mapping in aobjectMap
		if mapping, exists := aobjectMap.Get(upstreamIDStr); exists {
			// Get downstream object ID from mapping
			downstreamObjID := mapping.DownstreamStats.ObjectName().ObjectId()

			// Replace the segment ID in rowid with downstream object's segment ID
			rowids[i].SetSegment(types.Segmentid(*downstreamObjID.Segment()))
			rowids[i].SetObjOffset(downstreamObjID.Offset())

			// Rewrite row offset using RowOffsetMap if available
			if mapping.RowOffsetMap != nil {
				oldRowOffset := rowids[i].GetRowOffset()
				if newRowOffset, ok := mapping.RowOffsetMap[oldRowOffset]; ok {
					rowids[i].SetRowOffset(newRowOffset)
				}
			}
		}
	}

	return nil
}

// convertObjectToBatch converts object file content to batch directly from memory
// This function is specifically for appendable objects (aobj)
// Steps:
// 1. Read object meta from objectContent bytes using stats.Extent()
// 2. Get column information from block meta
// 3. Read column data directly from objectContent bytes using column extents
// 4. Decode and create vectors
// 5. Create batch with columns
func convertObjectToBatch(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
	snapshotTS types.TS,
	localFS fileservice.FileService,
	mp *mpool.MPool,
) (*containers.Batch, error) {
	// Step 1: Read object meta from objectContent bytes
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return nil, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
	}
	metaBytes := objectContent[metaExtent.Offset() : metaExtent.Offset()+metaExtent.Length()]

	// Check if meta needs decompression (same as ReadExtent does)
	var decompressedMetaBytes []byte
	var decompressedMetaBuf fscache.Data
	if metaExtent.Alg() == compress.None {
		decompressedMetaBytes = metaBytes
	} else {
		// Allocate buffer for decompressed data
		allocator := fileservice.DefaultCacheDataAllocator()
		decompressedMetaBuf = allocator.AllocateCacheDataWithHint(ctx, int(metaExtent.OriginSize()), malloc.NoClear)
		bs, err := compress.Decompress(metaBytes, decompressedMetaBuf.Bytes(), compress.Lz4)
		if err != nil {
			if decompressedMetaBuf != nil {
				decompressedMetaBuf.Release()
			}
			return nil, moerr.NewInternalErrorf(ctx, "failed to decompress meta data: %v", err)
		}
		decompressedMetaBytes = decompressedMetaBuf.Bytes()[:len(bs)]
		// Clone the data to ensure meta doesn't hold reference to buffer
		decompressedMetaBytes = append([]byte(nil), decompressedMetaBytes...)
		if decompressedMetaBuf != nil {
			decompressedMetaBuf.Release()
		}
	}

	meta := objectio.MustObjectMeta(decompressedMetaBytes)

	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blkCnt := dataMeta.BlockCount()
	if blkCnt == 0 {
		return containers.NewBatch(), nil
	}

	// Step 2: Get column information from first block meta
	firstBlkMeta := dataMeta.GetBlockMeta(0)
	maxSeqnum := firstBlkMeta.GetMaxSeqnum()

	// Step 3: Prepare columns and types
	// For appendable objects, we need to include commit TS column
	cols := make([]uint16, 0, maxSeqnum+2)
	typs := make([]types.Type, 0, maxSeqnum+2)

	// Add data columns
	for seqnum := uint16(0); seqnum <= maxSeqnum; seqnum++ {
		colMeta := firstBlkMeta.ColumnMeta(seqnum)
		if colMeta.DataType() == 0 {
			continue // Skip invalid columns
		}
		cols = append(cols, seqnum)
		typ := types.T(colMeta.DataType()).ToType()
		typs = append(typs, typ)
	}

	// Add commit TS column for appendable objects
	cols = append(cols, objectio.SEQNUM_COMMITTS)
	typs = append(typs, objectio.TSType)

	// Step 4: Read column data from ALL blocks and merge
	// Initialize vectors for each column
	vecs := make([]containers.Vector, len(cols))
	for i, typ := range typs {
		vecs[i] = containers.MakeVector(typ, mp)
	}
	allocator := fileservice.DefaultCacheDataAllocator()

	// Iterate through all blocks
	for blkIdx := uint32(0); blkIdx < blkCnt; blkIdx++ {
		blkMeta := dataMeta.GetBlockMeta(blkIdx)

		for i, seqnum := range cols {
			var colMeta objectio.ColumnMeta
			var ext objectio.Extent

			// Handle special columns (commit TS)
			if seqnum >= objectio.SEQNUM_UPPER {
				if seqnum == objectio.SEQNUM_COMMITTS {
					metaColCnt := blkMeta.GetMetaColumnCount()
					colMeta = blkMeta.ColumnMeta(metaColCnt - 1)
				} else {
					return nil, moerr.NewInternalErrorf(ctx, "unsupported special column: %d", seqnum)
				}
			} else {
				// Normal column
				if seqnum > maxSeqnum || blkMeta.ColumnMeta(seqnum).DataType() == 0 {
					// Generate null values for missing columns
					length := int(blkMeta.GetRows())
					for j := 0; j < length; j++ {
						vecs[i].Append(nil, true)
					}
					continue
				}
				colMeta = blkMeta.ColumnMeta(seqnum)
			}

			ext = colMeta.Location()

			// Read column data from objectContent bytes
			if int(ext.Offset()+ext.Length()) > len(objectContent) {
				return nil, moerr.NewInternalErrorf(ctx, "object content too small for column extent at seqnum %d, block %d", seqnum, blkIdx)
			}
			colData := objectContent[ext.Offset() : ext.Offset()+ext.Length()]

			// Decompress if needed
			var decompressedData []byte
			var decompressedBuf fscache.Data

			if ext.Alg() == compress.None { // Clone non-compressed data to avoid buffer sharing with objectContent
				// objectContent may be reused/pooled, and UnmarshalBinary doesn't copy data
				decompressedData = append([]byte(nil), colData...)
			} else {
				// Allocate buffer for decompressed data
				decompressedBuf = allocator.AllocateCacheDataWithHint(ctx, int(ext.OriginSize()), malloc.NoClear)
				bs, err := compress.Decompress(colData, decompressedBuf.Bytes(), compress.Lz4)
				if err != nil {
					if decompressedBuf != nil {
						decompressedBuf.Release()
					}
					return nil, moerr.NewInternalErrorf(ctx, "failed to decompress column data: %v", err)
				}
				decompressedData = decompressedBuf.Bytes()[:len(bs)]
				// Clone the data to ensure decoded vector doesn't hold reference to buffer
				decompressedData = append([]byte(nil), decompressedData...)
				// Release buffer immediately after cloning
				if decompressedBuf != nil {
					decompressedBuf.Release()
				}
			}

			// Decode to vector.Vector
			obj, err := objectio.Decode(decompressedData)
			if err != nil {
				return nil, moerr.NewInternalErrorf(ctx, "failed to decode column data: %v", err)
			}
			vec := obj.(*vector.Vector)

			// Extend result vector with this block's data
			if err := vecs[i].ExtendVec(vec); err != nil {
				return nil, moerr.NewInternalErrorf(ctx, "failed to extend vector: %v", err)
			}
		}
	}

	// Step 5: Create batch with columns
	bat := containers.NewBatch()
	for i, vec := range vecs {
		var attr string
		if cols[i] == objectio.SEQNUM_COMMITTS {
			attr = objectio.TombstoneAttr_CommitTs_Attr
		} else {
			attr = fmt.Sprintf("tmp_%d", i)
		}
		bat.AddVector(attr, vec)
	}

	return bat, nil
}

// filterBatchBySnapshotTS filters batch rows by snapshot TS
// For appendable objects, rows with commit TS >snapshot TS should be filtered out
func filterBatchBySnapshotTS(
	ctx context.Context,
	bat *containers.Batch,
	snapshotTS types.TS,
	mp *mpool.MPool,
) (*containers.Batch, error) {
	if bat == nil {
		return nil, nil
	}

	// Find the commit TS column
	commitTSVec := bat.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr)
	if commitTSVec == nil {
		return nil, moerr.NewInternalErrorf(ctx, "commit TS column not found in batch")
	}

	// Verify the column type is TS
	if commitTSVec.GetType().Oid != types.T_TS {
		return nil, moerr.NewInternalErrorf(ctx, "commit TS column type mismatch: expected TS, got %s", commitTSVec.GetType().String())
	}

	// Get commit TS values
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec.GetDownstreamVector())

	// Build bitmap of rows to delete (commit TS < snapshot TS)
	deletes := roaring.New()
	for i, ts := range commitTSs {
		if ts.GT(&snapshotTS) {
			deletes.Add(uint32(i))
		}
	}

	// If no rows to delete, return original batch
	if deletes.IsEmpty() {
		return bat, nil
	}

	// Compact all vectors to remove deleted rows
	for _, vec := range bat.Vecs {
		vec.Compact(deletes)
	}

	return bat, nil
}

// createObjectFromBatch sorts batch by primary key, removes commit TS column,
// writes to object file, and returns objectio.ObjectStats
// isTombstone: true for tombstone objects, false for data objects
// sortKeySeqnum: the seqnum of the sortkey column in the original object
// keepOriginalName: if true, use original object name instead of generating new one
// Also returns rowOffsetMap: maps original rowoffset to new rowoffset after sorting
func createObjectFromBatch(
	ctx context.Context,
	bat *containers.Batch,
	originalStats *objectio.ObjectStats,
	snapshotTS types.TS,
	isTombstone bool,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	sortKeySeqnum uint16,
	keepOriginalName bool,
) (objectio.ObjectStats, map[uint32]uint32, error) {
	if bat == nil || bat.Length() == 0 {
		return objectio.ObjectStats{}, nil, nil
	}

	// Step 1: Convert to CN batch for sorting
	cnBat := containers.ToCNBatch(bat)
	defer cnBat.Clean(mp)

	// Step 2: Sort by primary key (first column, seqnum 0)
	// Primary key is typically the first column
	if len(cnBat.Vecs) == 0 {
		return objectio.ObjectStats{}, nil, moerr.NewInternalErrorf(ctx, "batch has no columns")
	}
	pkIdx := 0 // Primary key is the first column
	sortedIdx := make([]int64, cnBat.Vecs[0].Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, cnBat.Vecs[pkIdx])

	// Build rowOffsetMap: maps original rowoffset to new rowoffset after sorting
	// sortedIdx[newIdx] = oldIdx, so we need: rowOffsetMap[oldIdx] = newIdx
	rowOffsetMap := make(map[uint32]uint32, len(sortedIdx))
	for newIdx, oldIdx := range sortedIdx {
		rowOffsetMap[uint32(oldIdx)] = uint32(newIdx)
	}

	for i := 0; i < len(cnBat.Vecs); i++ {
		if err := cnBat.Vecs[i].Shuffle(sortedIdx, mp); err != nil {
			return objectio.ObjectStats{}, nil, moerr.NewInternalErrorf(ctx, "failed to shuffle vector: %v", err)
		}
	}

	// Step 3: Remove commit TS column
	// Find commit TS column index
	commitTSIdx := -1
	for i, attr := range cnBat.Attrs {
		if attr == objectio.TombstoneAttr_CommitTs_Attr {
			commitTSIdx = i
			break
		}
	}
	if commitTSIdx == -1 {
		return objectio.ObjectStats{}, nil, moerr.NewInternalErrorf(ctx, "commit TS column not found")
	}

	// Create new batch without commit TS column
	newBat := &batch.Batch{
		Vecs:  make([]*vector.Vector, 0, len(cnBat.Vecs)-1),
		Attrs: make([]string, 0, len(cnBat.Attrs)-1),
	}
	for i, vec := range cnBat.Vecs {
		if i != commitTSIdx {
			newBat.Attrs = append(newBat.Attrs, cnBat.Attrs[i])
			newBat.Vecs = append(newBat.Vecs, vec)
		}
	}
	newBat.SetRowCount(cnBat.Vecs[0].Length())

	// Step 4: Write to object file
	// Get seqnums from original stats to determine column seqnums
	// For appendable objects, we need to exclude commit TS seqnum
	seqnums := make([]uint16, 0, len(newBat.Vecs))
	for i := uint16(0); i < uint16(len(newBat.Vecs)); i++ {
		seqnums = append(seqnums, i)
	}

	// Map sortkey seqnum to position in new batch
	// Since commit TS is removed but data columns keep their original positions,
	// the sortkey position is the same as its seqnum (assuming sortkey is a data column, not commit TS)
	sortKeyPos := 0
	if sortKeySeqnum != math.MaxUint16 {
		// Convert seqnum to position in new batch
		sortKeyPos = int(sortKeySeqnum)
		// If sortkey position is invalid (out of range), fallback to 0
		if sortKeyPos >= len(newBat.Vecs) {
			sortKeyPos = 0
		}
	}
	// If sortKeySeqnum is math.MaxUint16, it means no sortkey was set, use 0 as default

	// Create block writer - use data schema for data objects, tombstone schema for tombstone objects
	var writer *ioutil.BlockWriter
	if keepOriginalName {
		// Use original object name (for non-appendable tombstone)
		segid := originalStats.ObjectName().SegmentId()
		num := originalStats.ObjectName().Num()
		writer = ioutil.ConstructWriterWithSegmentID(
			&segid,
			num,
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			isTombstone,
			localFS,
			nil, // arena
		)
	} else if isTombstone {
		// Use tombstone schema with new object name
		writer = ioutil.ConstructWriter(
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			true,       // isTombstone
			localFS,
		)
	} else {
		// Use data schema with new object name
		writer = ioutil.ConstructWriter(
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			false,      // isTombstone
			localFS,
		)
	}

	// Write batch to appropriate schema
	// WriteBatch will use isTombstone flag to write to correct schema (SchemaData or SchemaTombstone)
	// and build objMetaBuilder and update zonemap
	_, err := writer.WriteBatch(newBat)
	if err != nil {
		return objectio.ObjectStats{}, nil, moerr.NewInternalErrorf(ctx, "failed to write batch: %v", err)
	}

	// Sync writer to flush data
	// Sync will call WriteObjectMeta which sets colmeta (or tombstonesColmeta for tombstone),
	// and then DescribeObject which uses colmeta[sortKeySeqnum] to set zonemap
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return objectio.ObjectStats{}, nil, moerr.NewInternalErrorf(ctx, "failed to sync writer: %v", err)
	}

	// Step 5: Get and return objectio.ObjectStats
	objStats := writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	return objStats, rowOffsetMap, nil
}
