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

package blockio

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

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
	deletedMask *nulls.Nulls,
	fs fileservice.FileService,
) (err error) {
	loadedBlkCnt := 0
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		var location objectio.ObjectLocation
		tombstoneObject.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, location[:])
		if mask, err := FillBlockDeleteMask(
			ctx, ts, blockId, location[:], fs, tombstoneObject.GetCNCreated(),
		); err != nil {
			return false, err
		} else {
			deletedMask.Or(mask)
		}
		loadedBlkCnt++
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

	v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loadedBlkCnt))
	v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(tombstoneObjectCnt))
	if tombstoneObjectCnt > 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(skipObjectCnt) / float64(tombstoneObjectCnt))
	}
	if totalBlkCnt > 0 {
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
	if getTombstoneFileFn == nil {
		return
	}
	var tombstoneObject *objectio.ObjectStats
	for tombstoneObject, err = getTombstoneFileFn(); err == nil && tombstoneObject != nil; tombstoneObject, err = getTombstoneFileFn() {
		tombstoneObjectCnt++
		tombstoneZM := tombstoneObject.SortKeyZoneMap()
		if !tombstoneZM.RowidPrefixEq(prefixPattern) {
			skipObjectCnt++
			continue
		}
		var objMeta objectio.ObjectMeta
		location := tombstoneObject.ObjectLocation()

		if objMeta, err = objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		); err != nil {
			return
		}
		dataMeta := objMeta.MustDataMeta()

		blkCnt := int(dataMeta.BlockCount())
		totalBlkCnt += blkCnt

		startIdx := sort.Search(blkCnt, func(i int) bool {
			return dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(prefixPattern)
		})

		for pos := startIdx; pos < blkCnt; pos++ {
			blkMeta := dataMeta.GetBlockMeta(uint32(pos))
			columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
			// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
			// !PrefixEq means there is no rowid of this block in this zonemap, so skip
			if !columnZonemap.RowidPrefixEq(prefixPattern) {
				if columnZonemap.RowidPrefixGT(prefixPattern) {
					// all zone maps are sorted by the rowid
					// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
					break
				}
				continue
			}
			var goOn bool
			if goOn, err = onBlockSelectedFn(tombstoneObject, pos); err != nil || !goOn {
				break
			}
		}
	}
	return
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
