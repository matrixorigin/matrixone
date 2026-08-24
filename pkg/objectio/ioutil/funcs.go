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

package ioutil

import (
	"context"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// TombstoneCommitTSColumn is the validated logical-row access contract for the
// persisted commitTS special column. Const vectors have one physical value but
// can represent many logical rows, so callers must use At rather than indexing
// the backing slice.
type TombstoneCommitTSColumn struct {
	vec *vector.Vector
}

func (c TombstoneCommitTSColumn) IsPresent() bool {
	return c.vec != nil
}

func (c TombstoneCommitTSColumn) At(row int) types.TS {
	return vector.GetFixedAtNoTypeCheck[types.TS](c.vec, row)
}

// TombstoneAbortColumn is the validated logical-row access contract for the
// persisted abort special column. Const vectors have one physical value but
// can represent many logical rows, so callers must use At rather than indexing
// the backing slice.
type TombstoneAbortColumn struct {
	vec *vector.Vector
}

func (c TombstoneAbortColumn) IsPresent() bool { return c.vec != nil }

func (c TombstoneAbortColumn) At(row int) bool {
	return vector.GetFixedAtNoTypeCheck[bool](c.vec, row)
}

// ValidateTombstoneRowIDColumn validates the dense rowid column that defines
// the logical cardinality of a tombstone batch. Unlike commitTS and abort,
// rowids cannot be broadcast: every logical tombstone row must own a distinct
// physical rowid value.
func ValidateTombstoneRowIDColumn(expectedRows int, rowIDVec *vector.Vector) ([]types.Rowid, error) {
	if expectedRows < 0 {
		return nil, moerr.NewInvalidInputNoCtxf("negative tombstone row count %d", expectedRows)
	}
	if rowIDVec == nil {
		return nil, moerr.NewInvalidInputNoCtx("tombstone rowid column is missing")
	}
	if rowIDVec.GetType().Oid != types.T_Rowid {
		return nil, moerr.NewInvalidInputNoCtxf(
			"tombstone rowid column has type %s, expected ROWID", rowIDVec.GetType().String())
	}
	if rowIDVec.IsConst() || rowIDVec.Length() != expectedRows || rowIDVec.GetNulls().Any() {
		return nil, moerr.NewInvalidInputNoCtx("tombstone rowid column is unavailable")
	}
	typeSize := int(rowIDVec.GetType().TypeSize())
	if typeSize <= 0 || expectedRows > math.MaxInt/typeSize ||
		len(rowIDVec.GetData()) < expectedRows*typeSize {
		return nil, moerr.NewInvalidInputNoCtx("tombstone rowid backing data is invalid")
	}
	return vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec), nil
}

func ValidateTombstoneAbortColumn(expectedRows int, abortVec *vector.Vector) (TombstoneAbortColumn, error) {
	if expectedRows < 0 {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtxf("negative tombstone row count %d", expectedRows)
	}
	if abortVec == nil {
		return TombstoneAbortColumn{}, nil
	}
	if abortVec.GetType().Oid != types.T_bool {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtxf(
			"tombstone abort column has type %s, expected BOOL", abortVec.GetType().String())
	}
	if abortVec.IsConstNull() {
		if abortVec.Length() == expectedRows {
			return TombstoneAbortColumn{}, nil
		}
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtxf(
			"tombstone abort const-null column has %d rows, expected %d",
			abortVec.Length(), expectedRows,
		)
	}
	if abortVec.Length() == 0 && expectedRows != 0 {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtx("tombstone abort column is empty")
	}
	if abortVec.Length() != expectedRows {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtxf(
			"tombstone abort column has %d rows, expected %d",
			abortVec.Length(), expectedRows,
		)
	}
	if abortVec.GetNulls().Any() {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtx("tombstone abort column contains null rows")
	}
	typeSize := int(abortVec.GetType().TypeSize())
	backingRows := expectedRows
	if abortVec.IsConst() && backingRows > 0 {
		backingRows = 1
	}
	if typeSize <= 0 || backingRows > math.MaxInt/typeSize ||
		len(abortVec.GetData()) < backingRows*typeSize {
		return TombstoneAbortColumn{}, moerr.NewInvalidInputNoCtx("tombstone abort backing data is invalid")
	}
	return TombstoneAbortColumn{vec: abortVec}, nil
}

// ResolveLegacyBackupTombstoneCommitTS recognizes the legacy non-appendable
// backup layout [rowid, primary-key, commitTS], where commitTS was persisted
// as an ordinary trailing column instead of a special column.
func ResolveLegacyBackupTombstoneCommitTS(block objectio.BlockObject) (uint16, bool) {
	const legacyCommitTSPosition uint16 = 2
	if block.BlockHeader().Appendable() ||
		block.GetColumnCount() != 3 ||
		block.GetMetaColumnCount() != 3 ||
		block.GetMaxSeqnum() != legacyCommitTSPosition ||
		block.ColumnMeta(0).DataType() != uint8(types.T_Rowid) ||
		block.ColumnMeta(1).DataType() == uint8(types.T_any) ||
		block.ColumnMeta(legacyCommitTSPosition).DataType() != uint8(types.T_TS) {
		return 0, false
	}
	return legacyCommitTSPosition, true
}

func ValidateTombstoneCommitTSColumn(expectedRows int, commitTSVec *vector.Vector) (TombstoneCommitTSColumn, error) {
	if expectedRows < 0 {
		return TombstoneCommitTSColumn{}, moerr.NewInvalidInputNoCtxf("negative tombstone row count %d", expectedRows)
	}
	if commitTSVec == nil {
		return TombstoneCommitTSColumn{}, moerr.NewInvalidInputNoCtx("tombstone commit-ts column is missing")
	}
	if commitTSVec.GetType().Oid != types.T_TS {
		return TombstoneCommitTSColumn{}, moerr.NewInvalidInputNoCtxf(
			"tombstone commit-ts column has type %s, expected TS", commitTSVec.GetType().String())
	}
	if commitTSVec.IsConstNull() || commitTSVec.Length() != expectedRows || commitTSVec.GetNulls().Any() {
		return TombstoneCommitTSColumn{}, moerr.NewInvalidInputNoCtx("tombstone commit-ts column is unavailable")
	}
	typeSize := int(commitTSVec.GetType().TypeSize())
	backingRows := expectedRows
	if commitTSVec.IsConst() && backingRows > 0 {
		backingRows = 1
	}
	if typeSize <= 0 || backingRows > math.MaxInt/typeSize ||
		len(commitTSVec.GetData()) < backingRows*typeSize {
		return TombstoneCommitTSColumn{}, moerr.NewInvalidInputNoCtx("tombstone commit-ts backing data is invalid")
	}
	return TombstoneCommitTSColumn{vec: commitTSVec}, nil
}

func ListTSRangeFiles(
	ctx context.Context,
	dir string,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, dir),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

func ListTSRangeFilesInGCDir(
	ctx context.Context,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, GetGCDir()),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

func IsRowDeleted(
	ctx context.Context,
	ts *types.TS,
	row *types.Rowid,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	fs fileservice.FileService,
) (bool, error) {
	var isDeleted bool
	loadedBlkCnt := 0
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		if isDeleted {
			return false, nil
		}
		var err error
		var location objectio.ObjectLocation
		tombstoneObject.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, location[:])
		deleted, err := IsRowDeletedByLocation(
			ctx, ts, row, location[:], fs, tombstoneObject.GetCNCreated(),
		)
		if err != nil {
			return false, err
		}
		loadedBlkCnt++
		// if deleted, stop searching
		if deleted {
			isDeleted = true
			return false, nil
		}
		return true, nil
	}

	tombstoneObjectCnt, skipObjectCnt, totalBlkCnt, err := CheckTombstoneFile(
		ctx, row[:], getTombstoneFileFn, onBlockSelectedFn, fs,
	)
	if err != nil {
		return false, err
	}

	v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loadedBlkCnt))
	v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(tombstoneObjectCnt))
	if tombstoneObjectCnt > 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(skipObjectCnt) / float64(tombstoneObjectCnt))
	}
	if totalBlkCnt > 0 {
		v2.TxnReaderTombstoneBLSelectivityHistogram.Observe(float64(loadedBlkCnt) / float64(totalBlkCnt))
	}

	return isDeleted, nil
}

func GetTombstonesByBlockId(
	ctx context.Context,
	ts *types.TS,
	blockId *objectio.Blockid,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	deletedMask *objectio.Bitmap,
	fs fileservice.FileService,
) (err error) {
	loadedBlkCnt := 0
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		var (
			err2     error
			mask     objectio.Bitmap
			location objectio.ObjectLocation
		)
		tombstoneObject.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, location[:])
		if mask, err2 = FillBlockDeleteMask(
			ctx, ts, blockId, location[:], fs, tombstoneObject.GetCNCreated(),
		); err2 != nil {
			return false, err2
		} else {
			deletedMask.Or(mask)
		}
		loadedBlkCnt++
		mask.Release()
		return true, nil
	}

	var (
		tombstoneObjectCnt int
		skipObjectCnt      int
		totalBlkCnt        int
	)

	if tombstoneObjectCnt, skipObjectCnt, totalBlkCnt, err = CheckTombstoneFile(
		ctx, blockId[:], getTombstoneFileFn, onBlockSelectedFn, fs,
	); err != nil {
		return
	}

	if loadedBlkCnt > 0 {
		v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loadedBlkCnt))
	}

	if tombstoneObjectCnt > 0 {
		v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(tombstoneObjectCnt))
	}

	if tombstoneObjectCnt > 0 && skipObjectCnt > 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(skipObjectCnt) / float64(tombstoneObjectCnt))
	}
	if totalBlkCnt > 0 && loadedBlkCnt > 0 {
		v2.TxnReaderTombstoneBLSelectivityHistogram.Observe(float64(loadedBlkCnt) / float64(totalBlkCnt))
	}

	return
}

/*
func FindTombstonesOfBlock(
	ctx context.Context,
	blockId objectio.Blockid,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	return findTombstoneOfXXX(ctx, blockId[:], tombstoneObjects, fs)
}
*/

func FindTombstonesOfObject(
	ctx context.Context,
	objectId *objectio.ObjectId,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	return findTombstoneOfXXX(ctx, objectId[:], tombstoneObjects, fs)
}

func findTombstoneOfXXX(
	ctx context.Context,
	pattern []byte,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	sels.InitWithSize(int64(len(tombstoneObjects)))
	var curr int
	getTombstoneFile := func() (*objectio.ObjectStats, error) {
		if curr >= len(tombstoneObjects) {
			return nil, nil
		}
		i := curr
		curr++
		return &tombstoneObjects[i], nil
	}
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		sels.Add(uint64(curr - 1))
		return false, nil
	}
	_, _, _, err = CheckTombstoneFile(
		ctx, pattern, getTombstoneFile, onBlockSelectedFn, fs,
	)
	return
}

func CheckTombstoneFile(
	ctx context.Context,
	prefixPattern []byte,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	onBlockSelectedFn func(*objectio.ObjectStats, int) (bool, error),
	fs fileservice.FileService,
) (
	tombstoneObjectCnt int,
	skipObjectCnt int,
	totalBlkCnt int,
	err error,
) {
	if ctx == nil || getTombstoneFileFn == nil || onBlockSelectedFn == nil {
		err = moerr.NewInvalidInputNoCtx(
			"tombstone file check requires context and callbacks",
		)
		return
	}
	switch len(prefixPattern) {
	case types.SegmentidSize, types.ObjectidSize, types.BlockidSize, types.RowidSize:
	default:
		err = moerr.NewInvalidInputNoCtxf(
			"invalid tombstone rowid prefix length %d", len(prefixPattern),
		)
		return
	}
	validateRowIDZoneMap := func(zm objectio.ZoneMap, scope string) error {
		if !zm.IsInited() || zm.GetType() != types.T_Rowid ||
			len(zm.GetMinBuf()) != types.RowidSize ||
			len(zm.GetMaxBuf()) != types.RowidSize {
			return moerr.NewInvalidInputNoCtxf(
				"%s has an invalid rowid zone map", scope,
			)
		}
		return nil
	}
	getBlockZoneMap := func(
		dataMeta objectio.ObjectDataMeta,
		blockID uint32,
	) (zm objectio.ZoneMap, blockErr error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				zm = nil
				blockErr = moerr.NewInvalidInputNoCtxf(
					"tombstone block %d metadata is malformed: %v", blockID, recovered,
				)
			}
		}()
		zm = dataMeta.GetBlockMeta(blockID).MustGetColumn(0).ZoneMap()
		blockErr = validateRowIDZoneMap(zm, "tombstone block")
		return
	}
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		default:
		}
		var tombstoneObject *objectio.ObjectStats
		tombstoneObject, err = getTombstoneFileFn()
		if err != nil || tombstoneObject == nil {
			return
		}
		tombstoneObjectCnt++
		tombstoneZM := tombstoneObject.SortKeyZoneMap()
		if err = validateRowIDZoneMap(tombstoneZM, "tombstone object"); err != nil {
			return
		}
		if !tombstoneZM.RowidPrefixEq(prefixPattern) {
			skipObjectCnt++
			continue
		}
		var objMeta objectio.ObjectMeta
		location := tombstoneObject.ObjectLocation()
		if fs == nil {
			err = moerr.NewInvalidInputNoCtx(
				"tombstone file check requires file service for a matching object",
			)
			return
		}

		if objMeta, err = objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		); err != nil {
			return
		}
		dataMeta, metaErr := GetDataMetaForLocation(objMeta, location)
		if metaErr != nil {
			err = metaErr
			return
		}

		blockCount := dataMeta.BlockCount()
		startID := uint32(dataMeta.BlockHeader().StartID())
		if blockCount > 0 && uint64(startID)+uint64(blockCount)-1 > math.MaxUint32 {
			err = moerr.NewInvalidInputNoCtx("tombstone metadata block range overflows")
			return
		}
		blkCnt := int(blockCount)
		if blkCnt > math.MaxInt-totalBlkCnt {
			err = moerr.NewInvalidInputNoCtx("tombstone block count overflows")
			return
		}
		totalBlkCnt += blkCnt

		// Use an explicit lower-bound search so malformed metadata and
		// cancellation can be returned instead of panicking inside sort.Search.
		low, high := 0, blkCnt
		for low < high {
			select {
			case <-ctx.Done():
				err = context.Cause(ctx)
				return
			default:
			}
			middle := int(uint(low+high) >> 1)
			var columnZoneMap objectio.ZoneMap
			columnZoneMap, err = getBlockZoneMap(dataMeta, startID+uint32(middle))
			if err != nil {
				return
			}
			if columnZoneMap.AnyGEByValue(prefixPattern) {
				high = middle
			} else {
				low = middle + 1
			}
		}
		startIdx := low

		for pos := startIdx; pos < blkCnt; pos++ {
			select {
			case <-ctx.Done():
				err = context.Cause(ctx)
				return
			default:
			}
			columnZoneMap, zoneMapErr := getBlockZoneMap(
				dataMeta, startID+uint32(pos),
			)
			if zoneMapErr != nil {
				err = zoneMapErr
				return
			}
			// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
			// !PrefixEq means there is no rowid of this block in this zonemap, so skip
			if columnZoneMap.RowidPrefixEq(prefixPattern) {
				var goOn bool
				goOn, err = onBlockSelectedFn(tombstoneObject, pos)
				if err != nil {
					return
				}
				if !goOn {
					break
				}
			} else if columnZoneMap.RowidPrefixGT(prefixPattern) {
				// all zone maps are sorted by the rowid
				// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
				break
			}
		}
	}
}

// CoarseFilterTombstoneObject It is used to filter out tombstone objects that do not contain any deleted data objects.
// This is a coarse filter using ZM, so false positives may occur
func CoarseFilterTombstoneObject(
	ctx context.Context,
	nextDeletedDataObject func() *objectio.ObjectId,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (filtered []objectio.ObjectStats, err error) {
	var bm, b bitmap.Bitmap
	bm.InitWithSize(int64(len(tombstoneObjects)))
	var objid *objectio.ObjectId
	for objid = nextDeletedDataObject(); objid != nil; objid = nextDeletedDataObject() {
		b, err = FindTombstonesOfObject(ctx, objid, tombstoneObjects, fs)
		if err != nil {
			return
		}
		bm.Or(&b)
	}
	filtered = make([]objectio.ObjectStats, 0, bm.Count())
	itr := bm.Iterator()
	for itr.HasNext() {
		filtered = append(filtered, tombstoneObjects[itr.Next()])
	}

	return
}

func IsRowDeletedByLocation(
	ctx context.Context,
	snapshotTS *types.TS,
	row *objectio.Rowid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleted bool, err error) {
	if ctx == nil || row == nil || fs == nil || (!createdByCN && snapshotTS == nil) {
		return false, moerr.NewInvalidInputNoCtx(
			"tombstone row lookup requires context, row, file service, and snapshot timestamp",
		)
	}
	var hidden objectio.HiddenColumnSelection
	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS | objectio.HiddenColumnSelection_Abort
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	data := containers.NewVectors(len(attrs))
	if !createdByCN {
		// ReadDeletes returns rowid, commitTS and (when present) abort; the
		// copied PK attribute is not requested by this helper.
		data = containers.NewVectors(3)
	}
	_, release, err := ReadDeletes(ctx, location, fs, createdByCN, data, nil)
	if err != nil {
		return
	}
	if release != nil {
		defer release()
	}
	if data.Rows() == 0 {
		return
	}
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](&data[0])
	idx := sort.Search(len(rowids), func(i int) bool {
		return rowids[i].GE(row)
	})
	if createdByCN {
		deleted = (idx < len(rowids)) && (rowids[idx].EQ(row))
	} else {
		commitTSs, validateErr := ValidateTombstoneCommitTSColumn(len(rowids), &data[1])
		if validateErr != nil {
			return false, validateErr
		}
		aborts, validateErr := ValidateTombstoneAbortColumn(len(rowids), &data[2])
		if validateErr != nil {
			return false, validateErr
		}
		for i := idx; i < len(rowids); i++ {
			if !rowids[i].EQ(row) {
				break
			}
			commitTS := commitTSs.At(i)
			if (!aborts.IsPresent() || !aborts.At(i)) &&
				commitTS != types.MaxTs() && commitTS.LE(snapshotTS) {
				deleted = true
				break
			}
		}
	}
	return
}

func FillBlockDeleteMask(
	ctx context.Context,
	snapshotTS *types.TS,
	blockId *types.Blockid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleteMask objectio.Bitmap, err error) {
	if location.IsEmpty() {
		return
	}
	if ctx == nil || blockId == nil || fs == nil ||
		(!createdByCN && snapshotTS == nil) {
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx(
			"tombstone mask read requires context, block id, file service, and a snapshot for TN tombstones",
		)
	}

	var (
		release func()
		meta    objectio.ObjectDataMeta
		hidden  objectio.HiddenColumnSelection
	)

	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	persistedDeletes := containers.NewVectors(len(attrs))

	if meta, release, err = ReadDeletes(
		ctx, location, fs, createdByCN, persistedDeletes, nil,
	); err != nil {
		return
	}
	if release != nil {
		defer release()
	}

	if createdByCN {
		deleteMask, err = EvalDeleteMaskFromCNCreatedTombstones(blockId, &persistedDeletes[0])
	} else {
		deleteMask, err = EvalDeleteMaskFromDNCreatedTombstones(
			&persistedDeletes[0],
			&persistedDeletes[1],
			&persistedDeletes[2],
			meta.GetBlockMeta(uint32(location.ID())),
			snapshotTS,
			blockId,
		)
	}

	return
}

// ReadDeletes will read the pk column if pk type not nil
func ReadDeletes(
	ctx context.Context,
	deltaLoc objectio.Location,
	fs fileservice.FileService,
	isPersistedByCN bool,
	cacheVectors containers.Vectors,
	pkType *types.Type,
) (meta objectio.ObjectDataMeta, release func(), err error) {

	var cols []uint16
	var typs []types.Type

	if isPersistedByCN {
		cols = []uint16{objectio.TombstoneAttr_Rowid_SeqNum}
		typs = []types.Type{objectio.RowidType}
	} else {
		cols = []uint16{
			objectio.TombstoneAttr_Rowid_SeqNum,
			objectio.TombstoneAttr_CommitTs_SeqNum,
			objectio.TombstoneAttr_Abort_SeqNum,
		}
		typs = []types.Type{objectio.RowidType, objectio.TSType, types.T_bool.ToType()}
	}

	if pkType != nil {
		cols = append(cols[:1], append([]uint16{objectio.TombstoneAttr_PK_SeqNum}, cols[1:]...)...)
		typs = append(typs[:1], append([]types.Type{*pkType}, typs[1:]...)...)
	}
	if len(cacheVectors) < len(cols) {
		err = moerr.NewInvalidInputNoCtxf(
			"tombstone column cache has %d slots, needs %d", len(cacheVectors), len(cols),
		)
		return
	}

	meta, release, err = LoadTombstoneColumns(
		ctx, cols, typs, fs, deltaLoc, cacheVectors, nil, fileservice.Policy(0),
	)
	cleanup := func() {
		if release != nil {
			release()
			release = nil
		}
	}
	if err != nil {
		cleanup()
		return
	}
	// The location row count is only a block-size estimate when the caller
	// constructs locations from ObjectStats without loading block metadata.
	// Tombstone objects may contain multiple short blocks, so use the loaded
	// dense rowid column as the block's logical cardinality instead.
	rowCount := cacheVectors[0].Length()
	if _, err = ValidateTombstoneRowIDColumn(rowCount, &cacheVectors[0]); err != nil {
		cleanup()
		return
	}
	commitIdx, abortIdx := -1, -1
	if !isPersistedByCN {
		commitIdx = len(cols) - 2
		abortIdx = len(cols) - 1
	}
	validateLoadedRows := func() error {
		for pos := range cols {
			// Keep the dedicated validators authoritative for the two DN-only
			// columns.  Besides checking cardinality, they validate the const
			// encoding and preserve the precise diagnostics callers rely on.
			if pos == commitIdx || pos == abortIdx {
				continue
			}
			if cacheVectors[pos].Length() != rowCount {
				return moerr.NewInvalidInputNoCtxf(
					"tombstone column %d has %d rows, expected %d",
					pos, cacheVectors[pos].Length(), rowCount,
				)
			}
		}
		return nil
	}
	if err = validateLoadedRows(); err != nil {
		cleanup()
		return
	}
	if isPersistedByCN {
		return
	}

	if _, err = ValidateTombstoneCommitTSColumn(
		rowCount, &cacheVectors[commitIdx],
	); err != nil {
		if cacheVectors[commitIdx].IsConstNull() {
			if physicalCommitTS, ok := ResolveLegacyBackupTombstoneCommitTS(
				meta.GetBlockMeta(uint32(deltaLoc.ID())),
			); ok {
				cleanup()
				cols[commitIdx] = physicalCommitTS
				meta, release, err = LoadTombstoneColumns(
					ctx, cols, typs, fs, deltaLoc, cacheVectors, nil, fileservice.Policy(0),
				)
				if err != nil {
					cleanup()
					return
				}
				rowCount = cacheVectors[0].Length()
				if _, err = ValidateTombstoneRowIDColumn(
					rowCount, &cacheVectors[0],
				); err != nil {
					cleanup()
					return
				}
				if err = validateLoadedRows(); err != nil {
					cleanup()
					return
				}
				_, err = ValidateTombstoneCommitTSColumn(
					rowCount, &cacheVectors[commitIdx],
				)
			}
		}
		if err != nil {
			cleanup()
			return
		}
	}
	if _, err = ValidateTombstoneAbortColumn(rowCount, &cacheVectors[abortIdx]); err != nil {
		cleanup()
	}
	return
}

func EvalDeleteMaskFromDNCreatedTombstones(
	deletedRows *vector.Vector,
	commitTSVec *vector.Vector,
	abortVec *vector.Vector,
	meta objectio.BlockObject,
	ts *types.TS,
	blockid *types.Blockid,
) (rows objectio.Bitmap, err error) {
	if deletedRows == nil {
		return
	}
	if ts == nil || blockid == nil {
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx(
			"DN tombstone evaluation requires snapshot timestamp and block id",
		)
	}
	rowids, err := ValidateTombstoneRowIDColumn(deletedRows.Length(), deletedRows)
	if err != nil {
		return objectio.NullBitmap, err
	}
	commitTSs, err := ValidateTombstoneCommitTSColumn(len(rowids), commitTSVec)
	if err != nil {
		return objectio.NullBitmap, err
	}
	aborts, err := ValidateTombstoneAbortColumn(len(rowids), abortVec)
	if err != nil {
		return objectio.NullBitmap, err
	}
	start, end := FindStartEndOfBlockFromSortedRowids(rowids, blockid)
	if start >= end {
		return
	}

	noTSCheck := false
	if end-start > 10 && !aborts.IsPresent() && *ts != types.MaxTs() {
		// fast path is true if the maxTS is less than the snapshotTS
		// this means that all the rows between start and end are visible
		layout := objectio.ResolveSpecialColumnLayout(meta)
		if idx, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
			// A zonemap is only an optimization. Malformed metadata must fall
			// back to the validated per-row timestamps, never panic or make a
			// false-positive visibility decision.
			func() {
				defer func() {
					if recover() != nil {
						noTSCheck = false
					}
				}()
				zm := meta.MustGetColumn(idx).ZoneMap()
				if !zm.IsInited() || zm.GetType() != types.T_TS ||
					len(zm.GetMinBuf()) != types.TxnTsSize ||
					len(zm.GetMaxBuf()) != types.TxnTsSize {
					return
				}
				noTSCheck = zm.FastLEValue(ts[:], 0)
			}()
		}
	}
	rows = objectio.GetReusableBitmap()
	if noTSCheck {
		for i := end - 1; i >= start; i-- {
			row := rowids[i].GetRowOffset()
			rows.Add(uint64(row))
		}
	} else {
		for i := end - 1; i >= start; i-- {
			commitTS := commitTSs.At(i)
			if (aborts.IsPresent() && aborts.At(i)) ||
				commitTS == types.MaxTs() || commitTS.GT(ts) {
				continue
			}
			row := rowids[i].GetRowOffset()
			rows.Add(uint64(row))
		}
	}

	return
}

func EvalDeleteMaskFromCNCreatedTombstones(
	bid *types.Blockid,
	deletedRows *vector.Vector,
) (rows objectio.Bitmap, err error) {
	if deletedRows == nil {
		return
	}
	if bid == nil {
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx(
			"CN tombstone evaluation requires a block id",
		)
	}
	rowids, err := ValidateTombstoneRowIDColumn(deletedRows.Length(), deletedRows)
	if err != nil {
		return objectio.NullBitmap, err
	}

	start, end := FindStartEndOfBlockFromSortedRowids(rowids, bid)
	if start < end {
		rows = objectio.GetReusableBitmap()
	}
	for i := end - 1; i >= start; i-- {
		row := rowids[i].GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

func FindStartEndOfBlockFromSortedRowids(rowids []types.Rowid, id *types.Blockid) (start int, end int) {
	lowRowid := objectio.NewRowid(id, 0)
	highRowid := objectio.NewRowid(id, math.MaxUint32)
	i, j := 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value >= lowRowid
		if !rowids[m].LT(&lowRowid) {
			j = m
		} else {
			i = m + 1
		}
	}
	start = i

	i, j = 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value > highRowid
		if highRowid.LT(&rowids[m]) {
			j = m
		} else {
			i = m + 1
		}
	}
	end = i
	return
}
