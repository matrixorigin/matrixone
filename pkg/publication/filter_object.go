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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
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
	isTombstone bool,
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
		return filterAppendableObject(ctx, &stats, snapshotTS, iterationCtx, localFS, isTombstone, mp)
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
	isTombstone bool,
	mp *mpool.MPool,
) error {
	// Get object name from stats (upstream aobj UUID)
	upstreamAObjUUID := stats.ObjectName().ObjectId()

	// Record mapping in iteration context
	// Map from upstream aobj UUID to both current and previous object stats
	if iterationCtx.ActiveAObj == nil {
		iterationCtx.ActiveAObj = make(map[objectio.ObjectId]AObjMapping)
	}

	// Get previous stats if exists, otherwise use zero value
	mapping := iterationCtx.ActiveAObj[*upstreamAObjUUID]

	// Get object file from upstream using GETOBJECT
	objectContent, err := getObjectFromUpstream(ctx, iterationCtx, stats.ObjectName().String())
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to get object from upstream: %v", err)
	}

	// Extract sortkey from original object metadata
	sortKeySeqnum, err := extractSortKeyFromObject(ctx, objectContent, stats)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to extract sortkey from object: %v", err)
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
	// This is data object (not tombstone), so use SchemaData
	objStats, err := createObjectFromBatch(ctx, filteredBat, stats, snapshotTS, isTombstone, localFS, mp, sortKeySeqnum)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to create object from batch: %v", err)
	}

	// Update mapping
	mapping.Previous = mapping.Current // Save previous stats
	mapping.Current = objStats         // Set new current stats
	iterationCtx.ActiveAObj[*upstreamAObjUUID] = mapping

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
		// Check if the error is due to file already exists
		if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			// Log warning instead of returning error
			logutil.Warn("file already exists, deleting and rewriting",
				zap.String("file", objectName),
				logutil.ErrorField(err))

			// Delete the existing file
			deleteErr := localFS.Delete(ctx, objectName)
			if deleteErr != nil {
				logutil.Warn("failed to delete existing file, ignoring",
					zap.String("file", objectName),
					logutil.ErrorField(deleteErr))
			}

			// Retry writing after deletion
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
				return moerr.NewInternalErrorf(ctx, "failed to write object to fileservice after deletion: %v", err)
			}
		} else {
			return moerr.NewInternalErrorf(ctx, "failed to write object to fileservice: %v", err)
		}
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

// extractSortKeyFromObject extracts sortkey seqnum from object metadata
func extractSortKeyFromObject(
	ctx context.Context,
	objectContent []byte,
	stats *objectio.ObjectStats,
) (uint16, error) {
	// Read object meta from objectContent bytes
	metaExtent := stats.Extent()
	if int(metaExtent.Offset()+metaExtent.Length()) > len(objectContent) {
		return 0, moerr.NewInternalErrorf(ctx, "object content too small for meta extent")
	}
	metaBytes := objectContent[metaExtent.Offset() : metaExtent.Offset()+metaExtent.Length()]

	// Check if meta needs decompression
	var decompressedMetaBytes []byte
	var decompressedBuf fscache.Data
	if metaExtent.Alg() == compress.None {
		decompressedMetaBytes = metaBytes
	} else {
		// Allocate buffer for decompressed data
		allocator := fileservice.DefaultCacheDataAllocator()
		decompressedBuf = allocator.AllocateCacheDataWithHint(ctx, int(metaExtent.OriginSize()), malloc.NoClear)
		bs, err := compress.Decompress(metaBytes, decompressedBuf.Bytes(), compress.Lz4)
		if err != nil {
			if decompressedBuf != nil {
				decompressedBuf.Release()
			}
			return 0, moerr.NewInternalErrorf(ctx, "failed to decompress meta data: %v", err)
		}
		decompressedMetaBytes = decompressedBuf.Bytes()[:len(bs)]
		// Clone the data to ensure meta doesn't hold reference to buffer
		decompressedMetaBytes = append([]byte(nil), decompressedMetaBytes...)
		if decompressedBuf != nil {
			decompressedBuf.Release()
		}
	}

	meta := objectio.MustObjectMeta(decompressedMetaBytes)
	dataMeta := meta.MustGetMeta(objectio.SchemaData)

	// Get sortkey seqnum from block header
	sortKeySeqnum := dataMeta.BlockHeader().SortKey()
	return sortKeySeqnum, nil
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
		var decompressedBuf fscache.Data
		if ext.Alg() == compress.None {
			decompressedData = colData
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
func createObjectFromBatch(
	ctx context.Context,
	bat *containers.Batch,
	originalStats *objectio.ObjectStats,
	snapshotTS types.TS,
	isTombstone bool,
	localFS fileservice.FileService,
	mp *mpool.MPool,
	sortKeySeqnum uint16,
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
	if isTombstone {
		// Use tombstone schema
		writer = ioutil.ConstructWriter(
			0, // version
			seqnums,
			sortKeyPos, // sortkeyPos from original object metadata
			true,       // sortkeyIsPK
			true,       // isTombstone
			localFS,
		)
	} else {
		// Use data schema
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
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to write batch: %v", err)
	}

	// Sync writer to flush data
	// Sync will call WriteObjectMeta which sets colmeta (or tombstonesColmeta for tombstone),
	// and then DescribeObject which uses colmeta[sortKeySeqnum] to set zonemap
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return objectio.ObjectStats{}, moerr.NewInternalErrorf(ctx, "failed to sync writer: %v", err)
	}

	// Step 5: Get and return objectio.ObjectStats
	objStats := writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	return objStats, nil
}
