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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// FilterObject filters an object based on snapshot TS
// Input: object stats (as bytes), snapshot TS, and whether it's an aobj (checked from object stats)
// For aobj: gets object file, converts to batch, filters by snapshot TS, creates new object
// The mapping between new UUID and upstream aobj is recorded in iterationCtx.ActiveAObj
// For nobj: writes directly to fileservice
func FilterObject(
	ctx context.Context,
	objectStatsBytes []byte,
	snapshotTS types.TS,
	iterationCtx *IterationContext,
	localFS fileservice.FileService,
	mp *mpool.MPool,
) error {
	if len(objectStatsBytes) != objectio.ObjectStatsLen {
		return moerr.NewInternalErrorf(ctx, "invalid object stats length: expected %d, got %d", objectio.ObjectStatsLen, len(objectStatsBytes))
	}

	// Parse ObjectStats from bytes
	var stats objectio.ObjectStats
	stats.UnMarshal(objectStatsBytes)

	// Check if it's an appendable object
	isAObj := stats.GetAppendable()

	if isAObj {
		// Handle appendable object
		return filterAppendableObject(ctx, &stats, snapshotTS, iterationCtx, localFS, mp)
	} else {
		// Handle non-appendable object - write directly to fileservice
		return filterNonAppendableObject(ctx, &stats, iterationCtx, localFS)
	}
}

// filterAppendableObject handles appendable objects
// Gets object file from upstream, converts to batch, filters by snapshot TS, creates new object
// Records the mapping between new UUID and upstream aobj in iterationCtx.ActiveAObj
func filterAppendableObject(
	ctx context.Context,
	stats *objectio.ObjectStats,
	snapshotTS types.TS,
	iterationCtx *IterationContext,
	localFS fileservice.FileService,
	mp *mpool.MPool,
) error {
	// Get object name from stats (upstream aobj UUID)
	upstreamAObjUUID := stats.ObjectName().String()

	// Get object file from upstream using GETOBJECT
	objectContent, err := getObjectFromUpstream(ctx, iterationCtx, upstreamAObjUUID)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
	}

	// Convert object file to batch
	bat, err := convertObjectToBatch(ctx, objectContent, stats, snapshotTS, localFS, mp)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to convert object to batch: %v", err)
	}
	defer bat.Close()

	// Filter batch by snapshot TS
	filteredBat, err := filterBatchBySnapshotTS(ctx, bat, snapshotTS, mp)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to filter batch by snapshot TS: %v", err)
	}
	defer filteredBat.Close()

	// Sort batch by primary key, remove commit TS column, write to file, and record ObjectStats
	objStats, err := createObjectFromBatch(ctx, filteredBat, stats, snapshotTS, localFS, mp)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create object from batch: %v", err)
	}

	// Record mapping in iteration context
	// Map from upstream aobj UUID to both current and previous object stats
	if iterationCtx.ActiveAObj == nil {
		iterationCtx.ActiveAObj = make(map[string]AObjMapping)
	}

	// Get previous stats if exists, otherwise use zero value
	mapping := iterationCtx.ActiveAObj[upstreamAObjUUID]
	mapping.Previous = mapping.Current // Save previous stats
	mapping.Current = objStats         // Set new current stats
	iterationCtx.ActiveAObj[upstreamAObjUUID] = mapping

	return nil
}

// filterNonAppendableObject handles non-appendable objects
// Writes directly to fileservice
func filterNonAppendableObject(
	ctx context.Context,
	stats *objectio.ObjectStats,
	iterationCtx *IterationContext,
	localFS fileservice.FileService,
) error {
	// Get object name from stats
	objectName := stats.ObjectName().String()

	// Get object file from upstream
	objectContent, err := getObjectFromUpstream(ctx, iterationCtx, objectName)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
	}

	// Write directly to local fileservice
	err = localFS.Write(ctx, fileservice.IOVector{
		FilePath: objectName,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(objectContent)),
				Data:   objectContent,
			},
		},
	})
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to write object to fileservice: %v", err)
	}

	return nil
}

// getObjectFromUpstream gets object file from upstream using GETOBJECT SQL
func getObjectFromUpstream(
	ctx context.Context,
	iterationCtx *IterationContext,
	objectName string,
) ([]byte, error) {
	if iterationCtx.UpstreamExecutor == nil {
		return nil, moerr.NewInternalError(ctx, "upstream executor is nil")
	}

	// Build GETOBJECT SQL
	getObjectSQL := PublicationSQLBuilder.GetObjectSQL(objectName)

	// Execute SQL through upstream executor
	result, err := iterationCtx.UpstreamExecutor.ExecSQL(ctx, getObjectSQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to execute GETOBJECT query: %v", err)
	}
	defer result.Close()

	// Read result - GETOBJECT returns object content as blob
	var objectContent []byte
	if result.Next() {
		if err := result.Scan(&objectContent); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan object content: %v", err)
		}
	} else {
		return nil, moerr.NewInternalErrorf(ctx, "no object content returned for %s", objectName)
	}

	return objectContent, nil
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
	meta := objectio.MustObjectMeta(metaBytes)

	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blkCnt := dataMeta.BlockCount()
	if blkCnt == 0 {
		return containers.NewBatch(), nil
	}

	// Step 2: Get column information from first block meta
	blkMeta := dataMeta.GetBlockMeta(0)
	maxSeqnum := blkMeta.GetMaxSeqnum()

	// Step 3: Prepare columns and types
	// For appendable objects, we need to include commit TS column
	cols := make([]uint16, 0, maxSeqnum+2)
	typs := make([]types.Type, 0, maxSeqnum+2)

	// Add data columns
	for seqnum := uint16(0); seqnum <= maxSeqnum; seqnum++ {
		colMeta := blkMeta.ColumnMeta(seqnum)
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

	// Step 4: Read column data directly from objectContent bytes
	vecs := make([]containers.Vector, 0, len(cols))
	allocator := fileservice.DefaultCacheDataAllocator()

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
				// Generate null vector for missing columns
				length := int(blkMeta.GetRows())
				nullVec := containers.MakeVector(typs[i], mp)
				for j := 0; j < length; j++ {
					nullVec.Append(nil, false)
				}
				vecs = append(vecs, nullVec)
				continue
			}
			colMeta = blkMeta.ColumnMeta(seqnum)
		}

		ext = colMeta.Location()

		// Read column data from objectContent bytes
		if int(ext.Offset()+ext.Length()) > len(objectContent) {
			return nil, moerr.NewInternalErrorf(ctx, "object content too small for column extent at seqnum %d", seqnum)
		}
		colData := objectContent[ext.Offset() : ext.Offset()+ext.Length()]

		// Decompress if needed
		var decompressedData []byte
		if ext.Alg() == compress.None {
			decompressedData = colData
		} else {
			// Allocate buffer for decompressed data
			decompressedBuf := allocator.AllocateCacheDataWithHint(ctx, int(ext.OriginSize()), malloc.NoClear)
			bs, err := compress.Decompress(colData, decompressedBuf.Bytes(), compress.Lz4)
			if err != nil {
				return nil, moerr.NewInternalErrorf(ctx, "failed to decompress column data: %v", err)
			}
			decompressedData = decompressedBuf.Bytes()[:len(bs)]
		}

		// Decode to vector.Vector
		obj, err := objectio.Decode(decompressedData)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to decode column data: %v", err)
		}
		vec := obj.(*vector.Vector)

		// Convert to containers.Vector
		tnVec := containers.ToTNVector(vec, mp)
		vecs = append(vecs, tnVec)
	}

	// Step 5: Create batch with columns
	bat := containers.NewBatch()
	for i, vec := range vecs {
		var attr string
		if cols[i] == objectio.SEQNUM_COMMITTS {
			attr = objectio.TombstoneAttr_CommitTs_Attr
		} else {
			attr = types.T(typs[i].Oid).String()
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
func createObjectFromBatch(
	ctx context.Context,
	bat *containers.Batch,
	originalStats *objectio.ObjectStats,
	snapshotTS types.TS,
	localFS fileservice.FileService,
	mp *mpool.MPool,
) (objectio.ObjectStats, error) {
	if bat == nil || bat.Length() == 0 {
		return objectio.ObjectStats{}, nil
	}

	// Step 1: Convert to CN batch for sorting
	cnBat := containers.ToCNBatch(bat)
	defer cnBat.Clean(mp)

	// Step 2: Sort by primary key (first column, seqnum 0)
	// Primary key is typically the first column
	if len(cnBat.Vecs) == 0 {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "batch has no columns")
	}
	pkIdx := 0 // Primary key is the first column
	sortedIdx := make([]int64, cnBat.Vecs[0].Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, cnBat.Vecs[pkIdx])
	for i := 0; i < len(cnBat.Vecs); i++ {
		if err := cnBat.Vecs[i].Shuffle(sortedIdx, mp); err != nil {
			return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to shuffle vector: %v", err)
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
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "commit TS column not found")
	}

	// Create new batch without commit TS column
	newBat := batch.NewWithSize(len(cnBat.Vecs) - 1)
	newBat.Attrs = make([]string, 0, len(cnBat.Attrs)-1)
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

	// Create block writer
	writer := ioutil.ConstructWriter(
		0, // version
		seqnums,
		0,     // sortkeyPos (primary key is first column)
		true,  // sortkeyIsPK
		false, // isTombstone
		localFS,
	)

	// Write batch
	_, err := writer.WriteBatch(newBat)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to write batch: %v", err)
	}

	// Sync writer to flush data
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to sync writer: %v", err)
	}

	// Step 5: Get and return objectio.ObjectStats
	objStats := writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	return objStats, nil
}
