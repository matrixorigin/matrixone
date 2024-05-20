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

package logtail

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type fileData struct {
	data          map[uint16]*blockData
	name          objectio.ObjectName
	obj           *objData
	isDeleteBatch bool
	isChange      bool
	isABlock      bool
}

type objData struct {
	stats     *objectio.ObjectStats
	data      []*batch.Batch
	sortKey   uint16
	infoRow   []int
	infoDel   []int
	infoTNRow []int
	tid       uint64
	delete    bool
	isABlock  bool
}

type blockData struct {
	num       uint16
	deleteRow []int
	insertRow []int
	blockType objectio.DataMetaType
	location  objectio.Location
	data      *batch.Batch
	sortKey   uint16
	isABlock  bool
	blockId   types.Blockid
	tid       uint64
	tombstone *blockData
}

type iBlocks struct {
	insertBlocks []*insertBlock
}

type iObjects struct {
	rowObjects []*insertObjects
}

type insertBlock struct {
	blockId   objectio.Blockid
	location  objectio.Location
	deleteRow int
	apply     bool
	data      *blockData
}

type insertObjects struct {
	location objectio.Location
	apply    bool
	obj      *objData
}

type tableOffset struct {
	offset int
	end    int
}

func getCheckpointData(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
) (*CheckpointData, error) {
	data := NewCheckpointData(common.CheckpointAllocator)
	reader, err := blockio.NewObjectReader(fs, location)
	if err != nil {
		return nil, err
	}
	err = data.readMetaBatch(ctx, version, reader, nil)
	if err != nil {
		return nil, err
	}
	err = data.readAll(ctx, version, fs)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func addObjectToObjectData(
	stats *objectio.ObjectStats,
	isABlk, isDelete bool, isTN bool,
	row int, tid uint64,
	objectsData *map[string]*fileData,
) {
	name := stats.ObjectName().String()
	if (*objectsData)[name] == nil {
		object := &fileData{
			name:          stats.ObjectName(),
			obj:           &objData{},
			isDeleteBatch: isDelete,
			isChange:      false,
			isABlock:      isABlk,
		}
		object.obj.stats = stats
		object.obj.tid = tid
		object.obj.delete = isDelete
		object.obj.isABlock = isABlk
		if isABlk {
			object.data = make(map[uint16]*blockData)
			object.data[0] = &blockData{
				num:       0,
				location:  stats.ObjectLocation(),
				blockType: objectio.SchemaData,
				isABlock:  true,
				tid:       tid,
				sortKey:   uint16(math.MaxUint16),
			}
		}
		(*objectsData)[name] = object
		if !isTN {
			if isDelete {
				(*objectsData)[name].obj.infoDel = []int{row}
			} else {
				(*objectsData)[name].obj.infoRow = []int{row}
			}
		} else {
			(*objectsData)[name].obj.infoTNRow = []int{row}
		}
		return
	}

	if !isTN {
		if isDelete {
			(*objectsData)[name].obj.infoDel = append((*objectsData)[name].obj.infoDel, row)
		} else {
			(*objectsData)[name].obj.infoRow = append((*objectsData)[name].obj.infoRow, row)
		}
	} else {
		(*objectsData)[name].obj.infoTNRow = append((*objectsData)[name].obj.infoTNRow, row)
	}

}

func addBlockToObjectData(
	location objectio.Location,
	isABlk, isCnBatch bool,
	row int, tid uint64,
	blockID types.Blockid,
	blockType objectio.DataMetaType,
	objectsData *map[string]*fileData,
) {
	name := location.Name().String()
	if (*objectsData)[name] == nil {
		object := &fileData{
			name:          location.Name(),
			data:          make(map[uint16]*blockData),
			isChange:      false,
			isDeleteBatch: isCnBatch,
			isABlock:      isABlk,
		}
		(*objectsData)[name] = object
	}
	if (*objectsData)[name].data == nil {
		(*objectsData)[name].data = make(map[uint16]*blockData)
	}
	if (*objectsData)[name].data[location.ID()] == nil {
		(*objectsData)[name].data[location.ID()] = &blockData{
			num:       location.ID(),
			location:  location,
			blockType: blockType,
			isABlock:  isABlk,
			tid:       tid,
			blockId:   blockID,
			sortKey:   uint16(math.MaxUint16),
		}
		if isCnBatch {
			(*objectsData)[name].data[location.ID()].deleteRow = []int{row}
		} else {
			(*objectsData)[name].data[location.ID()].insertRow = []int{row}
		}
	} else {
		if isCnBatch {
			(*objectsData)[name].data[location.ID()].deleteRow = append((*objectsData)[name].data[location.ID()].deleteRow, row)
		} else {
			(*objectsData)[name].data[location.ID()].insertRow = append((*objectsData)[name].data[location.ID()].insertRow, row)
		}
	}
}

func trimObjectsData(
	ctx context.Context,
	fs fileservice.FileService,
	ts types.TS,
	objectsData *map[string]*fileData,
) (bool, error) {
	isCkpChange := false
	for name := range *objectsData {
		isChange := false
		if (*objectsData)[name].obj != nil && (*objectsData)[name].obj.isABlock {
			if !(*objectsData)[name].obj.delete {
				panic(fmt.Sprintf("object %s is not a delete batch", name))
			}
			if len((*objectsData)[name].data) == 0 {
				var bat *batch.Batch
				var err error
				commitTs := types.TS{}
				// As long as there is an aBlk to be deleted, isCkpChange must be set to true.
				isCkpChange = true
				obj := (*objectsData)[name].obj
				location := obj.stats.ObjectLocation()
				meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
				if err != nil {
					return isCkpChange, err
				}
				sortKey := uint16(math.MaxUint16)
				if meta.MustDataMeta().BlockHeader().Appendable() {
					sortKey = meta.MustDataMeta().BlockHeader().SortKey()
				}
				bat, err = blockio.LoadOneBlock(ctx, fs, location, objectio.SchemaData)
				if err != nil {
					return isCkpChange, err
				}
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(&ts) {
						windowCNBatch(bat, 0, uint64(v))
						logutil.Debugf("blkCommitTs %v ts %v , block is %v",
							commitTs.ToString(), ts.ToString(), location.String())
						isChange = true
						break
					}
				}
				(*objectsData)[name].obj.sortKey = sortKey
				(*objectsData)[name].obj.data = make([]*batch.Batch, 0)
				bat = formatData(bat)
				(*objectsData)[name].obj.data = append((*objectsData)[name].obj.data, bat)
				(*objectsData)[name].isChange = isChange
				continue
			}
		}

		for id, block := range (*objectsData)[name].data {
			if !block.isABlock && block.blockType == objectio.SchemaData {
				continue
			}
			var bat *batch.Batch
			var err error
			commitTs := types.TS{}
			if block.blockType == objectio.SchemaTombstone {
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaTombstone)
				if err != nil {
					return isCkpChange, err
				}
				deleteRow := make([]int64, 0)
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-3].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(&ts) {
						logutil.Debugf("delete row %v, commitTs %v, location %v",
							v, commitTs.ToString(), block.location.String())
						isChange = true
						isCkpChange = true
					} else {
						deleteRow = append(deleteRow, int64(v))
					}
				}
				if len(deleteRow) != bat.Vecs[0].Length() {
					bat.Shrink(deleteRow, false)
				}
			} else {
				// As long as there is an aBlk to be deleted, isCkpChange must be set to true.
				isCkpChange = true
				meta, err := objectio.FastLoadObjectMeta(ctx, &block.location, false, fs)
				if err != nil {
					return isCkpChange, err
				}
				sortKey := uint16(math.MaxUint16)
				if meta.MustDataMeta().BlockHeader().Appendable() {
					sortKey = meta.MustDataMeta().BlockHeader().SortKey()
				}
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaData)
				if err != nil {
					return isCkpChange, err
				}
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(&ts) {
						windowCNBatch(bat, 0, uint64(v))
						logutil.Debugf("blkCommitTs %v ts %v , block is %v",
							commitTs.ToString(), ts.ToString(), block.location.String())
						isChange = true
						break
					}
				}
				(*objectsData)[name].data[id].sortKey = sortKey
			}
			bat = formatData(bat)
			(*objectsData)[name].data[id].data = bat
		}
		(*objectsData)[name].isChange = isChange
	}
	return isCkpChange, nil
}

func applyDelete(dataBatch *batch.Batch, deleteBatch *batch.Batch, id string) error {
	if deleteBatch == nil {
		return nil
	}
	deleteRow := make([]int64, 0)
	rows := make(map[int64]bool)
	for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
		row := deleteBatch.Vecs[0].GetRawBytesAt(i)
		rowId := objectio.HackBytes2Rowid(row)
		blockId, ro := rowId.Decode()
		if blockId.String() != id {
			continue
		}
		rows[int64(ro)] = true
	}
	for i := 0; i < dataBatch.Vecs[0].Length(); i++ {
		if rows[int64(i)] {
			deleteRow = append(deleteRow, int64(i))
		}
	}
	dataBatch.Shrink(deleteRow, true)
	return nil
}

func updateBlockMeta(blkMeta, blkMetaTxn *containers.Batch, row int, blockID types.Blockid, location objectio.Location, sort bool) {
	blkMeta.GetVectorByName(catalog2.AttrRowID).Update(
		row,
		objectio.HackBlockid2Rowid(&blockID),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_ID).Update(
		row,
		blockID,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_EntryState).Update(
		row,
		false,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_Sorted).Update(
		row,
		sort,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_SegmentID).Update(
		row,
		*blockID.Segment(),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
		row,
		[]byte(location),
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
		row,
		nil,
		true)
	blkMetaTxn.GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
		row,
		[]byte(location),
		false)
	blkMetaTxn.GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
		row,
		nil,
		true)

	if !sort {
		logutil.Infof("block %v is not sorted", blockID.String())
	}
}

func appendValToBatch(src, dst *containers.Batch, row int) {
	for v, vec := range src.Vecs {
		val := vec.Get(row)
		if val == nil {
			dst.Vecs[v].Append(val, true)
		} else {
			dst.Vecs[v].Append(val, false)
		}
	}
}

// Need to format the loaded batch, otherwise panic may occur when WriteBatch.
func formatData(data *batch.Batch) *batch.Batch {
	if data.Vecs[0].Length() > 0 {
		data.Attrs = make([]string, 0)
		for i := range data.Vecs {
			att := fmt.Sprintf("col_%d", i)
			data.Attrs = append(data.Attrs, att)
		}
		tmp := containers.ToTNBatch(data, common.CheckpointAllocator)
		data = containers.ToCNBatch(tmp)
	}
	return data
}

func LoadCheckpointEntriesFromKey(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
	softDeletes *map[string]bool,
	baseTS *types.TS,
) ([]*objectio.BackupObject, *CheckpointData, error) {
	locations := make([]*objectio.BackupObject, 0)
	data, err := getCheckpointData(ctx, fs, location, version)
	if err != nil {
		return nil, nil, err
	}

	locations = append(locations, &objectio.BackupObject{
		Location: location,
		NeedCopy: true,
	})

	for _, location = range data.locations {
		locations = append(locations, &objectio.BackupObject{
			Location: location,
			NeedCopy: true,
		})
	}
	for i := 0; i < data.bats[ObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[ObjectInfoIDX].GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deletedAt := data.bats[ObjectInfoIDX].GetVectorByName(EntryNode_DeleteAt).Get(i).(types.TS)
		createAt := data.bats[ObjectInfoIDX].GetVectorByName(EntryNode_CreateAt).Get(i).(types.TS)
		commitAt := data.bats[ObjectInfoIDX].GetVectorByName(txnbase.SnapshotAttr_CommitTS).Get(i).(types.TS)
		isAblk := data.bats[ObjectInfoIDX].GetVectorByName(ObjectAttr_State).Get(i).(bool)
		if objectStats.Extent().End() == 0 {
			panic(fmt.Sprintf("object %v Extent not empty", objectStats.ObjectName().String()))
		}

		if deletedAt.IsEmpty() && isAblk {
			panic(fmt.Sprintf("object %v is not deleted", objectStats.ObjectName().String()))
		}
		bo := &objectio.BackupObject{
			Location: objectStats.ObjectLocation(),
			CrateTS:  createAt,
			DropTS:   deletedAt,
		}
		if baseTS.IsEmpty() || (!baseTS.IsEmpty() &&
			(createAt.GreaterEq(baseTS) || commitAt.GreaterEq(baseTS))) {
			bo.NeedCopy = true
		}
		locations = append(locations, bo)
		if !deletedAt.IsEmpty() {
			if softDeletes != nil {
				if !(*softDeletes)[objectStats.ObjectName().String()] {
					(*softDeletes)[objectStats.ObjectName().String()] = true
				}
			}
		}
	}

	for i := 0; i < data.bats[TNObjectInfoIDX].Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := data.bats[TNObjectInfoIDX].GetVectorByName(ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deletedAt := data.bats[TNObjectInfoIDX].GetVectorByName(EntryNode_DeleteAt).Get(i).(types.TS)
		if objectStats.Extent().End() > 0 {
			panic(any(fmt.Sprintf("extent end is not 0: %v, name is %v", objectStats.Extent().End(), objectStats.ObjectName().String())))
		}
		if !deletedAt.IsEmpty() {
			panic(any(fmt.Sprintf("deleteAt is not empty: %v, name is %v", deletedAt.ToString(), objectStats.ObjectName().String())))
		}
		//locations = append(locations, objectStats.ObjectName())
	}

	for i := 0; i < data.bats[BLKMetaInsertIDX].Length(); i++ {
		deltaLoc := objectio.Location(
			data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		commitTS := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_CommitTs).Get(i).(types.TS)
		if deltaLoc.IsEmpty() {
			metaLoc := objectio.Location(
				data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte))
			panic(fmt.Sprintf("block %v deltaLoc is empty", metaLoc.String()))
		}
		bo := &objectio.BackupObject{
			Location: deltaLoc,
			CrateTS:  commitTS,
		}
		if baseTS.IsEmpty() ||
			(!baseTS.IsEmpty() && commitTS.GreaterEq(baseTS)) {
			bo.NeedCopy = true
		}
		locations = append(locations, bo)
	}
	for i := 0; i < data.bats[BLKCNMetaInsertIDX].Length(); i++ {
		metaLoc := objectio.Location(
			data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte))
		commitTS := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_CommitTs).Get(i).(types.TS)
		if !metaLoc.IsEmpty() {
			if softDeletes != nil {
				if !(*softDeletes)[metaLoc.Name().String()] {
					(*softDeletes)[metaLoc.Name().String()] = true
					//Fixme:The objectlist has updated this object to the cropped object,
					// and the expired object in the soft-deleted blocklist has not been processed.
					logutil.Warnf("block %v metaLoc is not deleted", metaLoc.String())
					//panic(fmt.Sprintf("block111 %v metaLoc is not deleted", metaLoc.String()))
				}
			}
		}
		deltaLoc := objectio.Location(
			data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		if deltaLoc.IsEmpty() {
			panic(fmt.Sprintf("block %v deltaLoc is empty", deltaLoc.String()))
		}
		bo := &objectio.BackupObject{
			Location: deltaLoc,
			CrateTS:  commitTS,
		}
		if baseTS.IsEmpty() ||
			(!baseTS.IsEmpty() && commitTS.GreaterEq(baseTS)) {
			bo.NeedCopy = true
		}
		locations = append(locations, bo)
	}
	return locations, data, nil
}

func ReWriteCheckpointAndBlockFromKey(
	ctx context.Context,
	fs, dstFs fileservice.FileService,
	loc, tnLocation objectio.Location,
	version uint32, ts types.TS,
	softDeletes map[string]bool,
) (objectio.Location, objectio.Location, []string, error) {
	logutil.Info("[Start]", common.OperationField("ReWrite Checkpoint"),
		common.OperandField(loc.String()),
		common.OperandField(ts.ToString()))
	phaseNumber := 0
	var err error
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField("ReWrite Checkpoint"),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()
	objectsData := make(map[string]*fileData, 0)

	defer func() {
		for i := range objectsData {
			if objectsData[i].obj != nil && objectsData[i].obj.data != nil {
				for z := range objectsData[i].obj.data {
					for y := range objectsData[i].obj.data[z].Vecs {
						objectsData[i].obj.data[z].Vecs[y].Free(common.DebugAllocator)
					}
				}
			}
			for j := range objectsData[i].data {
				if objectsData[i].data[j].data == nil {
					continue
				}
				for z := range objectsData[i].data[j].data.Vecs {
					objectsData[i].data[j].data.Vecs[z].Free(common.CheckpointAllocator)
				}
			}
		}
	}()
	phaseNumber = 1
	// Load checkpoint
	data, err := getCheckpointData(ctx, fs, loc, version)
	if err != nil {
		return nil, nil, nil, err
	}
	data.FormatData(common.CheckpointAllocator)
	defer data.Close()

	phaseNumber = 2
	// Analyze checkpoint to get the object file
	var files []string
	isCkpChange := false
	blkCNMetaInsert := data.bats[BLKCNMetaInsertIDX]
	blkMetaInsTxnBat := data.bats[BLKMetaInsertTxnIDX]
	blkMetaInsTxnBatTid := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID)

	blkMetaInsert := data.bats[BLKMetaInsertIDX]
	blkMetaInsertMetaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc)
	blkMetaInsertDeltaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc)
	blkMetaInsertEntryState := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_EntryState)
	blkMetaInsertBlkID := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_ID)

	objInfoData := data.bats[ObjectInfoIDX]
	objInfoStats := objInfoData.GetVectorByName(ObjectAttr_ObjectStats)
	objInfoState := objInfoData.GetVectorByName(ObjectAttr_State)
	objInfoTid := objInfoData.GetVectorByName(SnapshotAttr_TID)
	objInfoDelete := objInfoData.GetVectorByName(EntryNode_DeleteAt)
	objInfoCommit := objInfoData.GetVectorByName(txnbase.SnapshotAttr_CommitTS)

	for i := 0; i < objInfoData.Length(); i++ {
		stats := objectio.NewObjectStats()
		stats.UnMarshal(objInfoStats.Get(i).([]byte))
		isABlk := objInfoState.Get(i).(bool)
		deleteAt := objInfoDelete.Get(i).(types.TS)
		commitTS := objInfoCommit.Get(i).(types.TS)
		tid := objInfoTid.Get(i).(uint64)
		if commitTS.Less(&ts) {
			panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commitTS.ToString(), ts.ToString())))
		}

		if isABlk && deleteAt.IsEmpty() {
			panic(any(fmt.Sprintf("block %v deleteAt is empty", stats.ObjectName().String())))
		}
		addObjectToObjectData(stats, isABlk, !deleteAt.IsEmpty(), false, i, tid, &objectsData)
	}

	tnObjInfoData := data.bats[TNObjectInfoIDX]
	tnObjInfoStats := tnObjInfoData.GetVectorByName(ObjectAttr_ObjectStats)
	tnObjInfoState := tnObjInfoData.GetVectorByName(ObjectAttr_State)
	tnObjInfoTid := tnObjInfoData.GetVectorByName(SnapshotAttr_TID)
	tnObjInfoDelete := tnObjInfoData.GetVectorByName(EntryNode_DeleteAt)
	tnObjInfoCommit := tnObjInfoData.GetVectorByName(txnbase.SnapshotAttr_CommitTS)
	for i := 0; i < tnObjInfoData.Length(); i++ {
		stats := objectio.NewObjectStats()
		stats.UnMarshal(tnObjInfoStats.Get(i).([]byte))
		isABlk := tnObjInfoState.Get(i).(bool)
		deleteAt := tnObjInfoDelete.Get(i).(types.TS)
		tid := tnObjInfoTid.Get(i).(uint64)
		commitTS := tnObjInfoCommit.Get(i).(types.TS)

		if commitTS.Less(&ts) {
			panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commitTS.ToString(), ts.ToString())))
		}

		if stats.Extent().End() > 0 {
			panic(any(fmt.Sprintf("extent end is not 0: %v, name is %v", stats.Extent().End(), stats.ObjectName().String())))
		}
		if !deleteAt.IsEmpty() {
			panic(any(fmt.Sprintf("deleteAt is not empty: %v, name is %v", deleteAt.ToString(), stats.ObjectName().String())))
		}
		addObjectToObjectData(stats, isABlk, !deleteAt.IsEmpty(), true, i, tid, &objectsData)
	}

	if blkCNMetaInsert.Length() > 0 {
		panic(any("blkCNMetaInsert is not empty"))
	}

	for i := 0; i < blkMetaInsert.Length(); i++ {
		metaLoc := objectio.Location(blkMetaInsertMetaLoc.Get(i).([]byte))
		deltaLoc := objectio.Location(blkMetaInsertDeltaLoc.Get(i).([]byte))
		blkID := blkMetaInsertBlkID.Get(i).(types.Blockid)
		isABlk := blkMetaInsertEntryState.Get(i).(bool)
		if deltaLoc.IsEmpty() || !metaLoc.IsEmpty() {
			panic(any(fmt.Sprintf("deltaLoc is empty: %v-%v", deltaLoc.String(), metaLoc.String())))
		}
		name := objectio.BuildObjectName(blkID.Segment(), blkID.Sequence())
		if isABlk {
			if objectsData[name.String()] == nil {
				continue
			}
			if !objectsData[name.String()].isDeleteBatch {
				panic(any(fmt.Sprintf("object %v is not deleteBatch", name.String())))
			}
			addBlockToObjectData(deltaLoc, isABlk, true, i,
				blkMetaInsTxnBatTid.Get(i).(uint64), blkID, objectio.SchemaTombstone, &objectsData)
			objectsData[name.String()].data[blkID.Sequence()].blockId = blkID
			objectsData[name.String()].data[blkID.Sequence()].tombstone = objectsData[deltaLoc.Name().String()].data[deltaLoc.ID()]
			if len(objectsData[name.String()].data[blkID.Sequence()].deleteRow) > 0 {
				objectsData[name.String()].data[blkID.Sequence()].deleteRow = append(objectsData[name.String()].data[blkID.Sequence()].deleteRow, i)
			} else {
				objectsData[name.String()].data[blkID.Sequence()].deleteRow = []int{i}
			}
		} else {
			if objectsData[name.String()] != nil {
				if objectsData[name.String()].isDeleteBatch {
					addBlockToObjectData(deltaLoc, isABlk, true, i,
						blkMetaInsTxnBatTid.Get(i).(uint64), blkID, objectio.SchemaTombstone, &objectsData)
					continue
				}
			}
			addBlockToObjectData(deltaLoc, isABlk, false, i,
				blkMetaInsTxnBatTid.Get(i).(uint64), blkID, objectio.SchemaTombstone, &objectsData)
		}
	}

	phaseNumber = 3
	// Trim object files based on timestamp
	isCkpChange, err = trimObjectsData(ctx, fs, ts, &objectsData)
	if err != nil {
		return nil, nil, nil, err
	}
	if !isCkpChange {
		return loc, tnLocation, files, nil
	}

	backupPool := dbutils.MakeDefaultSmallPool("backup-vector-pool")
	defer backupPool.Destory()

	insertBatch := make(map[uint64]*iBlocks)
	insertObjBatch := make(map[uint64]*iObjects)

	phaseNumber = 4
	// Rewrite object file
	for fileName, objectData := range objectsData {
		if !objectData.isChange && !objectData.isDeleteBatch {
			continue
		}
		dataBlocks := make([]*blockData, 0)
		var blocks []objectio.BlockObject
		var extent objectio.Extent
		for _, block := range objectData.data {
			dataBlocks = append(dataBlocks, block)
		}
		sort.Slice(dataBlocks, func(i, j int) bool {
			return dataBlocks[i].num < dataBlocks[j].num
		})

		if objectData.isChange &&
			(!objectData.isDeleteBatch || (objectData.data[0] != nil &&
				objectData.data[0].blockType == objectio.SchemaTombstone)) {
			// Rewrite the insert block/delete block file.
			objectData.isDeleteBatch = false
			writer, err := blockio.NewBlockWriter(dstFs, fileName)
			if err != nil {
				return nil, nil, nil, err
			}
			for _, block := range dataBlocks {
				if block.sortKey != math.MaxUint16 {
					writer.SetPrimaryKey(block.sortKey)
				}
				if block.blockType == objectio.SchemaData {
					// TODO: maybe remove
					_, err = writer.WriteBatch(block.data)
					if err != nil {
						return nil, nil, nil, err
					}
				} else if block.blockType == objectio.SchemaTombstone {
					_, err = writer.WriteTombstoneBatch(block.data)
					if err != nil {
						return nil, nil, nil, err
					}
				}
			}

			blocks, extent, err = writer.Sync(ctx)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
					return nil, nil, nil, err
				}
				err = fs.Delete(ctx, fileName)
				if err != nil {
					return nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}

		if objectData.isDeleteBatch &&
			objectData.data[0] != nil &&
			objectData.data[0].blockType != objectio.SchemaTombstone {
			var blockLocation objectio.Location
			if !objectData.isABlock {
				// Case of merge nBlock
				for _, dt := range dataBlocks {
					if insertBatch[dataBlocks[0].tid] == nil {
						insertBatch[dataBlocks[0].tid] = &iBlocks{
							insertBlocks: make([]*insertBlock, 0),
						}
					}
					ib := &insertBlock{
						apply:     false,
						deleteRow: dt.deleteRow[len(dt.deleteRow)-1],
						data:      dt,
					}
					insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)
				}
			} else {
				// For the aBlock that needs to be retained,
				// the corresponding NBlock is generated and inserted into the corresponding batch.
				if len(dataBlocks) > 2 {
					panic(any(fmt.Sprintf("dataBlocks len > 2: %v - %d", dataBlocks[0].location.String(), len(dataBlocks))))
				}
				if objectData.data[0].tombstone != nil {
					applyDelete(dataBlocks[0].data, objectData.data[0].tombstone.data, dataBlocks[0].blockId.String())
				}
				sortData := containers.ToTNBatch(dataBlocks[0].data, common.CheckpointAllocator)
				if dataBlocks[0].sortKey != math.MaxUint16 {
					_, err = mergesort.SortBlockColumns(sortData.Vecs, int(dataBlocks[0].sortKey), backupPool)
					if err != nil {
						return nil, nil, nil, err
					}
				}
				dataBlocks[0].data = containers.ToCNBatch(sortData)
				result := batch.NewWithSize(len(dataBlocks[0].data.Vecs) - 3)
				for i := range result.Vecs {
					result.Vecs[i] = dataBlocks[0].data.Vecs[i]
				}
				dataBlocks[0].data = result
				fileNum := uint16(1000) + dataBlocks[0].location.Name().Num()
				segment := dataBlocks[0].location.Name().SegmentId()
				name := objectio.BuildObjectName(&segment, fileNum)

				writer, err := blockio.NewBlockWriter(dstFs, name.String())
				if err != nil {
					return nil, nil, nil, err
				}
				if dataBlocks[0].sortKey != math.MaxUint16 {
					writer.SetPrimaryKey(dataBlocks[0].sortKey)
				}
				_, err = writer.WriteBatch(dataBlocks[0].data)
				if err != nil {
					return nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					panic("sync error")
				}
				files = append(files, name.String())
				blockLocation = objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
				if insertBatch[dataBlocks[0].tid] == nil {
					insertBatch[dataBlocks[0].tid] = &iBlocks{
						insertBlocks: make([]*insertBlock, 0),
					}
				}
				ib := &insertBlock{
					location: blockLocation,
					blockId:  *objectio.BuildObjectBlockid(name, blocks[0].GetID()),
					apply:    false,
				}
				if len(dataBlocks[0].deleteRow) > 0 {
					ib.deleteRow = dataBlocks[0].deleteRow[0]
				}
				insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)

				if objectData.obj != nil {
					objectData.obj.stats = &writer.GetObjectStats()[objectio.SchemaData]
				}
			}
			if objectData.obj != nil {
				obj := objectData.obj
				if insertObjBatch[obj.tid] == nil {
					insertObjBatch[obj.tid] = &iObjects{
						rowObjects: make([]*insertObjects, 0),
					}
				}
				io := &insertObjects{
					location: blockLocation,
					apply:    false,
					obj:      obj,
				}
				insertObjBatch[obj.tid].rowObjects = append(insertObjBatch[obj.tid].rowObjects, io)
			}
		} else {
			if objectData.isDeleteBatch && objectData.data[0] == nil {
				if !objectData.isABlock {
					// Case of merge nBlock
					if insertObjBatch[objectData.obj.tid] == nil {
						insertObjBatch[objectData.obj.tid] = &iObjects{
							rowObjects: make([]*insertObjects, 0),
						}
					}
					io := &insertObjects{
						apply: false,
						obj:   objectData.obj,
					}
					insertObjBatch[objectData.obj.tid].rowObjects = append(insertObjBatch[objectData.obj.tid].rowObjects, io)
				} else {
					sortData := containers.ToTNBatch(objectData.obj.data[0], common.CheckpointAllocator)
					if objectData.obj.sortKey != math.MaxUint16 {
						_, err = mergesort.SortBlockColumns(sortData.Vecs, int(objectData.obj.sortKey), backupPool)
						if err != nil {
							return nil, nil, nil, err
						}
					}
					objectData.obj.data[0] = containers.ToCNBatch(sortData)
					result := batch.NewWithSize(len(objectData.obj.data[0].Vecs) - 3)
					for i := range result.Vecs {
						result.Vecs[i] = objectData.obj.data[0].Vecs[i]
					}
					objectData.obj.data[0] = result
					fileNum := uint16(1000) + objectData.obj.stats.ObjectName().Num()
					segment := objectData.obj.stats.ObjectName().SegmentId()
					name := objectio.BuildObjectName(&segment, fileNum)

					writer, err := blockio.NewBlockWriter(dstFs, name.String())
					if err != nil {
						return nil, nil, nil, err
					}
					if objectData.obj.sortKey != math.MaxUint16 {
						writer.SetPrimaryKey(objectData.obj.sortKey)
					}
					_, err = writer.WriteBatch(objectData.obj.data[0])
					if err != nil {
						return nil, nil, nil, err
					}
					blocks, extent, err = writer.Sync(ctx)
					if err != nil {
						panic("sync error")
					}
					files = append(files, name.String())
					blockLocation := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
					obj := objectData.obj
					if insertObjBatch[obj.tid] == nil {
						insertObjBatch[obj.tid] = &iObjects{
							rowObjects: make([]*insertObjects, 0),
						}
					}
					obj.stats = &writer.GetObjectStats()[objectio.SchemaData]
					objectio.SetObjectStatsObjectName(obj.stats, blockLocation.Name())
					io := &insertObjects{
						location: blockLocation,
						apply:    false,
						obj:      obj,
					}
					insertObjBatch[obj.tid].rowObjects = append(insertObjBatch[obj.tid].rowObjects, io)
				}
			}

			for i := range dataBlocks {
				blockLocation := dataBlocks[i].location
				if objectData.isChange {
					blockLocation = objectio.BuildLocation(objectData.name, extent, blocks[uint16(i)].GetRows(), dataBlocks[i].num)
				}
				for _, insertRow := range dataBlocks[i].insertRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							insertRow,
							[]byte(blockLocation),
							false)
						data.bats[BLKMetaInsertTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							insertRow,
							[]byte(blockLocation),
							false)
					}
				}
				for _, deleteRow := range dataBlocks[uint16(i)].deleteRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							deleteRow,
							[]byte(blockLocation),
							false)
						data.bats[BLKMetaInsertTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							deleteRow,
							[]byte(blockLocation),
							false)
					}
				}
			}
		}
	}

	phaseNumber = 5
	// Transfer the object file that needs to be deleted to insert
	if len(insertBatch) > 0 {
		blkMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertIDX], common.CheckpointAllocator)
		blkMetaTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX], common.CheckpointAllocator)
		for i := 0; i < blkMetaInsert.Length(); i++ {
			tid := data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			appendValToBatch(data.bats[BLKMetaInsertIDX], blkMeta, i)
			appendValToBatch(data.bats[BLKMetaInsertTxnIDX], blkMetaTxn, i)
			if insertBatch[tid] != nil {
				for b, blk := range insertBatch[tid].insertBlocks {
					if blk.apply {
						continue
					}
					if insertBatch[tid].insertBlocks[b].data == nil {

					} else {
						insertBatch[tid].insertBlocks[b].apply = true

						row := blkMeta.Vecs[0].Length() - 1
						if !blk.location.IsEmpty() {
							sort := true
							if insertBatch[tid].insertBlocks[b].data != nil &&
								insertBatch[tid].insertBlocks[b].data.isABlock &&
								insertBatch[tid].insertBlocks[b].data.sortKey == math.MaxUint16 {
								sort = false
							}
							updateBlockMeta(blkMeta, blkMetaTxn, row,
								insertBatch[tid].insertBlocks[b].blockId,
								insertBatch[tid].insertBlocks[b].location,
								sort)
						}
					}
				}
			}
		}

		for tid := range insertBatch {
			for b := range insertBatch[tid].insertBlocks {
				if insertBatch[tid].insertBlocks[b].apply {
					continue
				}
				if insertBatch[tid] != nil && !insertBatch[tid].insertBlocks[b].apply {
					insertBatch[tid].insertBlocks[b].apply = true
					if insertBatch[tid].insertBlocks[b].data == nil {

					} else {
						i := blkMeta.Vecs[0].Length() - 1
						if !insertBatch[tid].insertBlocks[b].location.IsEmpty() {
							sort := true
							if insertBatch[tid].insertBlocks[b].data != nil &&
								insertBatch[tid].insertBlocks[b].data.isABlock &&
								insertBatch[tid].insertBlocks[b].data.sortKey == math.MaxUint16 {
								sort = false
							}
							updateBlockMeta(blkMeta, blkMetaTxn, i,
								insertBatch[tid].insertBlocks[b].blockId,
								insertBatch[tid].insertBlocks[b].location,
								sort)
						}
					}
				}
			}
		}

		for i := range insertBatch {
			for _, block := range insertBatch[i].insertBlocks {
				if block.data != nil {
					for _, cnRow := range block.data.deleteRow {
						if block.data.isABlock {
							data.bats[BLKMetaInsertIDX].Delete(cnRow)
							data.bats[BLKMetaInsertTxnIDX].Delete(cnRow)
						}
					}
				}
			}
		}

		data.bats[BLKMetaInsertIDX].Compact()
		data.bats[BLKMetaInsertTxnIDX].Compact()
		tableInsertOff := make(map[uint64]*tableOffset)
		for i := 0; i < blkMetaTxn.Vecs[0].Length(); i++ {
			tid := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			if tableInsertOff[tid] == nil {
				tableInsertOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableInsertOff[tid].end += 1
		}

		for tid, table := range tableInsertOff {
			data.UpdateBlockInsertBlkMeta(tid, int32(table.offset), int32(table.end))
		}
		data.bats[BLKMetaInsertIDX].Close()
		data.bats[BLKMetaInsertTxnIDX].Close()
		data.bats[BLKMetaInsertIDX] = blkMeta
		data.bats[BLKMetaInsertTxnIDX] = blkMetaTxn
	}

	phaseNumber = 6
	if len(insertObjBatch) > 0 {
		deleteRow := make([]int, 0)
		objectInfoMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[ObjectInfoIDX], common.CheckpointAllocator)
		infoInsert := make(map[int]*objData, 0)
		infoDelete := make(map[int]bool, 0)
		for tid := range insertObjBatch {
			for i := range insertObjBatch[tid].rowObjects {
				if insertObjBatch[tid].rowObjects[i].apply {
					continue
				}
				if !insertObjBatch[tid].rowObjects[i].location.IsEmpty() {
					obj := insertObjBatch[tid].rowObjects[i].obj
					if infoInsert[obj.infoDel[0]] != nil {
						panic("should not have info insert")
					}
					objectio.SetObjectStatsExtent(insertObjBatch[tid].rowObjects[i].obj.stats, insertObjBatch[tid].rowObjects[i].location.Extent())
					objectio.SetObjectStatsObjectName(insertObjBatch[tid].rowObjects[i].obj.stats, insertObjBatch[tid].rowObjects[i].location.Name())
					infoInsert[obj.infoDel[0]] = insertObjBatch[tid].rowObjects[i].obj
					if len(obj.infoTNRow) > 0 {
						data.bats[TNObjectInfoIDX].Delete(obj.infoTNRow[0])
					}
				} else {
					if infoDelete[insertObjBatch[tid].rowObjects[i].obj.infoDel[0]] {
						panic("should not have info delete")
					}
					infoDelete[insertObjBatch[tid].rowObjects[i].obj.infoDel[0]] = true
				}
			}

		}
		for i := 0; i < objInfoData.Length(); i++ {
			appendValToBatch(objInfoData, objectInfoMeta, i)
			if infoInsert[i] != nil && infoDelete[i] {
				panic("info should not have info delete")
			}
			if infoInsert[i] != nil {
				appendValToBatch(objInfoData, objectInfoMeta, i)
				row := objectInfoMeta.Length() - 1
				objectInfoMeta.GetVectorByName(ObjectAttr_ObjectStats).Update(row, infoInsert[i].stats[:], false)
				objectInfoMeta.GetVectorByName(ObjectAttr_State).Update(row, false, false)
				objectInfoMeta.GetVectorByName(EntryNode_DeleteAt).Update(row, types.TS{}, false)
			}

			if infoDelete[i] {
				deleteRow = append(deleteRow, objectInfoMeta.Length()-1)
			}
		}
		for i := range deleteRow {
			objectInfoMeta.Delete(deleteRow[i])
		}
		data.bats[TNObjectInfoIDX].Compact()
		objectInfoMeta.Compact()
		data.bats[ObjectInfoIDX].Close()
		data.bats[ObjectInfoIDX] = objectInfoMeta
		tableInsertOff := make(map[uint64]*tableOffset)
		for i := 0; i < objectInfoMeta.Vecs[0].Length(); i++ {
			tid := objectInfoMeta.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			if tableInsertOff[tid] == nil {
				tableInsertOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableInsertOff[tid].end += 1
		}

		for tid, table := range tableInsertOff {
			data.UpdateObjectInsertMeta(tid, int32(table.offset), int32(table.end))
		}
	}
	cnLocation, dnLocation, checkpointFiles, err := data.WriteTo(dstFs, DefaultCheckpointBlockRows, DefaultCheckpointSize)
	if err != nil {
		return nil, nil, nil, err
	}
	logutil.Info("[Done]",
		common.AnyField("checkpoint", cnLocation.String()),
		common.OperationField("ReWrite Checkpoint"),
		common.AnyField("new object", checkpointFiles))
	loc = cnLocation
	tnLocation = dnLocation
	files = append(files, checkpointFiles...)
	files = append(files, cnLocation.Name().String())
	return loc, tnLocation, files, nil
}
