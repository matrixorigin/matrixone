package logtail

import (
	"context"
	"fmt"
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
	"sort"
)

type fileData struct {
	data      map[uint16]*blockData
	name      objectio.ObjectName
	isCnBatch bool
	isDnBatch bool
	isChange  bool
	isAblk    bool
}

type blockData struct {
	num       uint16
	cnRow     []int
	dnRow     []int
	blockType objectio.DataMetaType
	location  objectio.Location
	data      *batch.Batch
	pk        int32
	isAblk    bool
	commitTs  types.TS
	blockId   types.Blockid
	tid       uint64
	tombstone *blockData
}

type iBlocks struct {
	fileNum      int
	insertBlocks []*insertBlock
}

type insertBlock struct {
	blockId  objectio.Blockid
	location objectio.Location
	commitTs types.TS
	cnRow    int
	apply    bool
	data     *blockData
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
	data := NewCheckpointData()
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
			name:      location.Name(),
			data:      make(map[uint16]*blockData),
			isChange:  false,
			isCnBatch: isCnBatch,
			isAblk:    isABlk,
		}
		(*objectsData)[name] = object
	}
	if (*objectsData)[name].data[location.ID()] == nil {
		(*objectsData)[name].data[location.ID()] = &blockData{
			num:       location.ID(),
			location:  location,
			blockType: blockType,
			isAblk:    isABlk,
			tid:       tid,
			blockId:   blockID,
		}
		if isCnBatch {
			(*objectsData)[name].data[location.ID()].cnRow = []int{row}
		} else {
			(*objectsData)[name].data[location.ID()].dnRow = []int{row}
		}
	} else {
		if isCnBatch {
			(*objectsData)[name].data[location.ID()].cnRow = append((*objectsData)[name].data[location.ID()].cnRow, row)
		} else {
			(*objectsData)[name].data[location.ID()].dnRow = append((*objectsData)[name].data[location.ID()].dnRow, row)
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
	for name, objectData := range *objectsData {
		isChange := false
		for id, block := range objectData.data {
			if !block.isAblk && block.blockType == objectio.SchemaData {
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
					if commitTs.Greater(ts) {
						logutil.Debugf("delete row %v, commitTs %v, location %v",
							v, commitTs.ToString(), block.location.String())
						isChange = true
						isCkpChange = true
					} else {
						deleteRow = append(deleteRow, int64(v))
					}
				}
				if len(deleteRow) != bat.Vecs[0].Length() {
					bat.Shrink(deleteRow)
				}
			} else {
				pk := int32(-1)
				meta, err := objectio.FastLoadObjectMeta(ctx, &block.location, false, fs)
				if err != nil {
					return isCkpChange, err
				}
				pk = meta.MustDataMeta().BlockHeader().PkIdxID()
				bat, err = blockio.LoadOneBlock(ctx, fs, block.location, objectio.SchemaData)
				if err != nil {
					return isCkpChange, err
				}
				for v := 0; v < bat.Vecs[0].Length(); v++ {
					err = commitTs.Unmarshal(bat.Vecs[len(bat.Vecs)-2].GetRawBytesAt(v))
					if err != nil {
						return isCkpChange, err
					}
					if commitTs.Greater(ts) {
						windowCNBatch(bat, 0, uint64(v))
						logutil.Debugf("blkCommitTs %v ts %v , block is %v",
							commitTs.ToString(), ts.ToString(), block.location.String())
						isChange = true
						isCkpChange = true
						break
					}
				}
				objectData.data[id].pk = pk
			}
			objectData.data[id].data = bat
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
	rowss := make(map[int64]bool)
	for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
		row := deleteBatch.Vecs[0].GetRawBytesAt(i)
		rowId := objectio.HackBytes2Rowid(row)
		blockId, ro := rowId.Decode()
		if blockId.String() != id {
			continue
		}
		rowss[int64(ro)] = true
	}
	for i := 0; i < dataBatch.Vecs[0].Length(); i++ {
		if rowss[int64(i)] {
			deleteRow = append(deleteRow, int64(i))
		}
	}
	dataBatch.AntiShrink(deleteRow)
	return nil
}

func updateBlockMeta(blkMeta, blkMetaTxn *containers.Batch, row int, blockID types.Blockid, location objectio.Location) {
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
		true,
		false)
	blkMeta.GetVectorByName(catalog.BlockMeta_SegmentID).Update(
		row,
		blockID.Segment(),
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
}

func ReWriteCheckpointAndBlockFromKey(
	ctx context.Context,
	fs, dstFs fileservice.FileService,
	loc, tnLocation objectio.Location,
	version uint32, ts types.TS,
	softDeletes map[string]map[uint16]bool,
) (objectio.Location, objectio.Location, *CheckpointData, []string, error) {
	objectsData := make(map[string]*fileData, 0)
	data, err := getCheckpointData(ctx, fs, loc, version)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	data.FormatData(common.DefaultAllocator)

	var files []string
	isCkpChange := false
	blkCNMetaInsert := data.bats[BLKCNMetaInsertIDX]
	blkCNMetaInsertMetaLoc := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc)
	blkCNMetaInsertDeltaLoc := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc)
	blkCNMetaInsertEntryState := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_EntryState)
	blkCNMetaInsertBlkID := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_ID)

	blkMetaDelTxnBat := data.bats[BLKMetaDeleteTxnIDX]
	blkMetaDelTxnTid := blkMetaDelTxnBat.GetVectorByName(SnapshotAttr_TID)

	blkMetaInsTxnBat := data.bats[BLKMetaInsertTxnIDX]
	blkMetaInsTxnBatTid := blkMetaInsTxnBat.GetVectorByName(SnapshotAttr_TID)

	blkMetaInsert := data.bats[BLKMetaInsertIDX]
	blkMetaInsertMetaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc)
	blkMetaInsertDeltaLoc := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc)
	blkMetaInsertEntryState := data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_EntryState)
	blkCNMetaInsertCommitTs := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_CommitTs)

	for i := 0; i < blkCNMetaInsert.Length(); i++ {
		metaLoc := objectio.Location(blkCNMetaInsertMetaLoc.Get(i).([]byte))
		deltaLoc := objectio.Location(blkCNMetaInsertDeltaLoc.Get(i).([]byte))
		isABlk := blkCNMetaInsertEntryState.Get(i).(bool)
		commits := blkCNMetaInsertCommitTs.Get(i).(types.TS)
		blkID := blkCNMetaInsertBlkID.Get(i).(types.Blockid)
		if commits.Less(ts) {
			panic(any(fmt.Sprintf("commitTs less than ts: %v-%v", commits.ToString(), ts.ToString())))
		}
		if !metaLoc.IsEmpty() && softDeletes[metaLoc.Name().String()] != nil &&
			softDeletes[metaLoc.Name().String()][metaLoc.ID()] {
			// It has been soft deleted by the previous checkpoint, so it will be skipped and not collected.
			continue
		}

		if !deltaLoc.IsEmpty() {
			addBlockToObjectData(deltaLoc, isABlk, true, i,
				blkMetaDelTxnTid.Get(i).(uint64), blkID, objectio.SchemaTombstone, &objectsData)
		}

		if !metaLoc.IsEmpty() && !isABlk {
			addBlockToObjectData(metaLoc, isABlk, true, i,
				blkMetaDelTxnTid.Get(i).(uint64), blkID, objectio.SchemaData, &objectsData)
			name := metaLoc.Name().String()

			if isABlk && !deltaLoc.IsEmpty() && !deltaLoc.Name().Equal(metaLoc.Name()) {
				objectsData[name].data[metaLoc.ID()].tombstone = objectsData[deltaLoc.Name().String()].data[metaLoc.ID()]
			}
		}
	}

	for i := 0; i < blkMetaInsert.Length(); i++ {
		metaLoc := objectio.Location(blkMetaInsertMetaLoc.Get(i).([]byte))
		deltaLoc := objectio.Location(blkMetaInsertDeltaLoc.Get(i).([]byte))
		blkID := types.Blockid{}
		err = blkID.Unmarshal(blkMetaInsertDeltaLoc.Get(i).([]byte))
		if err != nil {
			return nil, nil, nil, nil, err
		}
		isABlk := blkMetaInsertEntryState.Get(i).(bool)
		if isABlk {
			panic(any(fmt.Sprintf("The inserted block is an ablock: %v-%d", metaLoc.String(), i)))
		}
		if !metaLoc.IsEmpty() {
			addBlockToObjectData(metaLoc, isABlk, false, i,
				blkMetaInsTxnBatTid.Get(i).(uint64), blkID, objectio.SchemaData, &objectsData)
		}

		if !deltaLoc.IsEmpty() {
			addBlockToObjectData(deltaLoc, isABlk, false, i,
				blkMetaInsTxnBatTid.Get(i).(uint64), blkID, objectio.SchemaTombstone, &objectsData)
		}
	}

	isCkpChange, err = trimObjectsData(ctx, fs, ts, &objectsData)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	logutil.Infof("checkpoint11 %v has %v objects", ts.ToString(), len(objectsData))
	if !isCkpChange {
		return loc, tnLocation, data, files, nil
	}

	logutil.Infof("checkpoint %v has %v objects", ts.ToString(), len(objectsData))
	backupPool := dbutils.MakeDefaultSmallPool("backup-vector-pool")
	defer backupPool.Destory()

	insertBatch := make(map[uint64]*iBlocks)

	for fileName, objectData := range objectsData {
		if !objectData.isChange && !objectData.isCnBatch {
			continue
		}
		logutil.Infof("object %v, isChange %v, isCnBatch %v", fileName, objectData.isChange, objectData.isCnBatch)
		dataBlocks := make([]*blockData, 0)
		var blocks []objectio.BlockObject
		var extent objectio.Extent
		for _, block := range objectData.data {
			logutil.Infof("object %v, id is %d", fileName, block.num)
			dataBlocks = append(dataBlocks, block)
		}
		sort.Slice(dataBlocks, func(i, j int) bool {
			return dataBlocks[i].num < dataBlocks[j].num
		})

		if objectData.isChange &&
			(!objectData.isCnBatch || (objectData.data[0] != nil &&
				objectData.data[0].blockType == objectio.SchemaTombstone)) {
			// Rewrite the insert block/delete block file.
			objectData.isCnBatch = false
			writer, err := blockio.NewBlockWriter(dstFs, fileName)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			for _, block := range dataBlocks {
				logutil.Infof("write object %v, id is %d", fileName, block.num)
				if block.pk > -1 {
					writer.SetPrimaryKey(uint16(block.pk))
				}
				if block.blockType == objectio.SchemaData {
					_, err = writer.WriteBatch(block.data)
					if err != nil {
						return nil, nil, nil, nil, err
					}
				} else if block.blockType == objectio.SchemaTombstone {
					_, err = writer.WriteTombstoneBatch(block.data)
					if err != nil {
						return nil, nil, nil, nil, err
					}
				}
			}

			blocks, extent, err = writer.Sync(ctx)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
					return nil, nil, nil, nil, err
				}
				err = fs.Delete(ctx, fileName)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					return nil, nil, nil, nil, err
				}
			}
			logutil.Infof("write object %v, blocks is %v", fileName, blocks)
		}

		if objectData.isCnBatch && objectData.data[0] != nil && objectData.data[0].blockType != objectio.SchemaTombstone {
			if !objectData.isAblk {
				for _, dt := range dataBlocks {
					if insertBatch[dataBlocks[0].tid] == nil {
						logutil.Infof("tid is %d, file is %v", dataBlocks[0].location.String())
						insertBatch[dataBlocks[0].tid] = &iBlocks{
							insertBlocks: make([]*insertBlock, 0),
						}
					}
					ib := &insertBlock{
						apply: false,
						cnRow: dt.cnRow[len(dt.cnRow)-1],
						data:  dt,
					}
					insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)
				}
			} else {
				// For the ABlock that needs to be retained,
				// the corresponding NBlock is generated and inserted into the corresponding batch.
				if len(dataBlocks) > 2 {
					panic(any(fmt.Sprintf("dataBlocks len > 2: %v - %d", dataBlocks[0].location.String(), len(dataBlocks))))
				}
				if objectData.data[0].tombstone != nil {
					applyDelete(dataBlocks[0].data, objectData.data[0].tombstone.data, dataBlocks[0].blockId.String())
				}
				dataBlocks[0].data.Attrs = make([]string, 0)
				for i := range dataBlocks[0].data.Vecs {
					att := fmt.Sprintf("col_%d", i)
					dataBlocks[0].data.Attrs = append(dataBlocks[0].data.Attrs, att)
				}
				sortData := containers.ToTNBatch(dataBlocks[0].data)
				if dataBlocks[0].pk > -1 {
					_, err = mergesort.SortBlockColumns(sortData.Vecs, int(dataBlocks[0].pk), backupPool)
					if err != nil {
						return nil, nil, nil, nil, err
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
					return nil, nil, nil, nil, err
				}
				if dataBlocks[0].pk > -1 {
					writer.SetPrimaryKey(uint16(dataBlocks[0].pk))
				}
				_, err = writer.WriteBatch(dataBlocks[0].data)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				blocks, extent, err = writer.Sync(ctx)
				if err != nil {
					panic("sync error")
				}
				files = append(files, name.String())
				blockLocation := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
				if insertBatch[dataBlocks[0].tid] == nil {
					logutil.Infof("tid is %d, file is %v", dataBlocks[0].tid, blockLocation.String())
					insertBatch[dataBlocks[0].tid] = &iBlocks{
						insertBlocks: make([]*insertBlock, 0),
					}
				}
				ib := &insertBlock{
					location: blockLocation,
					blockId:  *objectio.BuildObjectBlockid(name, blocks[0].GetID()),
					apply:    false,
					cnRow:    dataBlocks[0].cnRow[0],
				}
				insertBatch[dataBlocks[0].tid].insertBlocks = append(insertBatch[dataBlocks[0].tid].insertBlocks, ib)
			}
		} else {
			for i := range dataBlocks {
				blockLocation := dataBlocks[i].location
				if objectData.isChange {
					blockLocation = objectio.BuildLocation(objectData.name, extent, blocks[uint16(i)].GetRows(), dataBlocks[i].num)
				}
				for _, dnRow := range dataBlocks[i].dnRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaData {
						data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
							dnRow,
							[]byte(blockLocation),
							false)
						data.bats[BLKMetaInsertTxnIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
							dnRow,
							[]byte(blockLocation),
							false)
					}
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							dnRow,
							[]byte(blockLocation),
							false)
						data.bats[BLKMetaInsertTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							dnRow,
							[]byte(blockLocation),
							false)
					}
				}
				for _, cnRow := range dataBlocks[uint16(i)].cnRow {
					if dataBlocks[uint16(i)].blockType == objectio.SchemaData {
						if dataBlocks[uint16(i)].isAblk {
							data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
								cnRow,
								[]byte(blockLocation),
								false)
							data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Update(
								cnRow,
								[]byte(blockLocation),
								false)
						}
					}
					if dataBlocks[uint16(i)].blockType == objectio.SchemaTombstone {
						data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							cnRow,
							[]byte(blockLocation),
							false)
						data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Update(
							cnRow,
							[]byte(blockLocation),
							false)
					}
				}
			}
		}
	}
	if len(insertBatch) > 0 {
		blkMeta := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertIDX])
		blkMetaTxn := makeRespBatchFromSchema(checkpointDataSchemas_Curr[BLKMetaInsertTxnIDX])
		for i := 0; i < blkMetaInsert.Length(); i++ {
			tid := data.bats[BLKMetaInsertTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			for v, vec := range data.bats[BLKMetaInsertIDX].Vecs {
				val := vec.Get(i)
				if val == nil {
					blkMeta.Vecs[v].Append(val, true)
				} else {
					blkMeta.Vecs[v].Append(val, false)
				}
			}
			for v, vec := range data.bats[BLKMetaInsertTxnIDX].Vecs {
				val := vec.Get(i)
				if val == nil {
					blkMetaTxn.Vecs[v].Append(val, true)
				} else {
					blkMetaTxn.Vecs[v].Append(val, false)
				}
			}
			if insertBatch[tid] != nil {
				for b, blk := range insertBatch[tid].insertBlocks {
					if blk.apply {
						continue
					}
					cnRow := insertBatch[tid].insertBlocks[b].cnRow
					insertBatch[tid].insertBlocks[b].apply = true
					logutil.Infof("rewrite BLKCNMetaInsertIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
					for v, vec := range data.bats[BLKCNMetaInsertIDX].Vecs {
						if !blk.location.IsEmpty() && data.bats[BLKCNMetaInsertIDX].Attrs[v] == catalog.BlockMeta_DeltaLoc {
							blkMeta.Vecs[v].Append(nil, true)
							continue
						}
						val := vec.Get(cnRow)
						if val == nil {
							blkMeta.Vecs[v].Append(val, true)
						} else {
							blkMeta.Vecs[v].Append(val, false)
						}
					}
					logutil.Infof("rewrite BLKMetaDeleteTxnIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
					for v, vec := range data.bats[BLKMetaDeleteTxnIDX].Vecs {
						if !blk.location.IsEmpty() && data.bats[BLKMetaDeleteTxnIDX].Attrs[v] == catalog.BlockMeta_DeltaLoc {
							blkMetaTxn.Vecs[v].Append(nil, true)
							continue
						}
						val := vec.Get(cnRow)
						if val == nil {
							blkMetaTxn.Vecs[v].Append(val, true)
						} else {
							blkMetaTxn.Vecs[v].Append(val, false)
						}
					}

					row := blkMeta.Vecs[0].Length() - 1
					ti := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(row)
					t2 := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(cnRow)
					logutil.Infof("rewrite update  BLKMetaInsertIDX %s, row is %d, t2 is %d, leng %d", insertBatch[tid].insertBlocks[b].location.String(), ti, t2, row)
					if !blk.location.IsEmpty() {
						updateBlockMeta(blkMeta, blkMetaTxn, row,
							insertBatch[tid].insertBlocks[b].blockId,
							insertBatch[tid].insertBlocks[b].location)
					}
				}
			}
		}

		for tid := range insertBatch {
			for b := range insertBatch[tid].insertBlocks {
				logutil.Infof("insertBatch is %d, apply is %v,file is %v", tid, insertBatch[tid].insertBlocks[b].apply, insertBatch[tid].insertBlocks[b].location.String())
				if insertBatch[tid].insertBlocks[b].apply {
					continue
				}
				if insertBatch[tid] != nil && !insertBatch[tid].insertBlocks[b].apply {
					cnRow := insertBatch[tid].insertBlocks[b].cnRow
					insertBatch[tid].insertBlocks[b].apply = true
					logutil.Infof("rewrite1 BLKCNMetaInsertIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
					for v, vec := range data.bats[BLKCNMetaInsertIDX].Vecs {
						if !insertBatch[tid].insertBlocks[b].location.IsEmpty() && data.bats[BLKCNMetaInsertIDX].Attrs[v] == catalog.BlockMeta_DeltaLoc {
							blkMeta.Vecs[v].Append(nil, true)
							continue
						}
						val := vec.Get(cnRow)
						if val == nil {
							blkMeta.Vecs[v].Append(val, true)
						} else {
							blkMeta.Vecs[v].Append(val, false)
						}
					}
					logutil.Infof("rewrite1 BLKMetaDeleteTxnIDX %s, row is %d", insertBatch[tid].insertBlocks[b].location.String(), cnRow)
					for v, vec := range data.bats[BLKMetaDeleteTxnIDX].Vecs {
						if !insertBatch[tid].insertBlocks[b].location.IsEmpty() && data.bats[BLKMetaDeleteTxnIDX].Attrs[v] == catalog.BlockMeta_DeltaLoc {
							blkMetaTxn.Vecs[v].Append(nil, true)
							continue
						}
						val := vec.Get(cnRow)
						if val == nil {
							blkMetaTxn.Vecs[v].Append(val, true)
						} else {
							blkMetaTxn.Vecs[v].Append(val, false)
						}
					}
					i := blkMeta.Vecs[0].Length() - 1

					ti := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i)
					t2 := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(cnRow)
					logutil.Infof("rewrite111 update  BLKMetaInsertIDX %s, row is %d, t2 is %d", insertBatch[tid].insertBlocks[b].location.String(), ti, t2)
					if !insertBatch[tid].insertBlocks[b].location.IsEmpty() {
						updateBlockMeta(blkMeta, blkMetaTxn, i,
							insertBatch[tid].insertBlocks[b].blockId,
							insertBatch[tid].insertBlocks[b].location)
					}
				}
			}
		}

		for i := range insertBatch {
			for _, block := range insertBatch[i].insertBlocks {
				if block.data != nil {
					for _, cnRow := range block.data.cnRow {
						data.bats[BLKCNMetaInsertIDX].Delete(cnRow)
						data.bats[BLKMetaDeleteTxnIDX].Delete(cnRow)
						data.bats[BLKMetaDeleteIDX].Delete(cnRow)
					}
				}
			}
		}

		data.bats[BLKCNMetaInsertIDX].Compact()
		data.bats[BLKMetaDeleteTxnIDX].Compact()
		data.bats[BLKMetaDeleteIDX].Compact()
		tableOff := make(map[uint64]*tableOffset)
		for i := 0; i < blkMetaTxn.Vecs[0].Length(); i++ {
			tid := blkMetaTxn.GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			loca := blkMeta.GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte)
			del := blkMeta.GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte)
			if tableOff[tid] == nil {
				tableOff[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableOff[tid].end += 1
			logutil.Infof("tableOff tid  %d, loc %v, del %v, start %d, end %d, row is %d", tid, objectio.Location(loca).String(), objectio.Location(del).String(), tableOff[tid].offset, tableOff[tid].end, i)
		}
		tableOff1 := make(map[uint64]*tableOffset)
		for i := 0; i < data.bats[BLKMetaDeleteTxnIDX].Vecs[0].Length(); i++ {
			rid := data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog2.AttrRowID)
			tid := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(SnapshotAttr_TID).Get(i).(uint64)
			loca := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte)
			del := data.bats[BLKMetaDeleteTxnIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte)
			if tableOff1[tid] == nil {
				tableOff1[tid] = &tableOffset{
					offset: i,
					end:    i,
				}
			}
			tableOff1[tid].end += 1
			logutil.Infof("tableOff11 tid  %d, loc %v, del %v, start %d, end %d, row is %d, rid is %v", tid, objectio.Location(loca).String(), objectio.Location(del).String(), tableOff1[tid].offset, tableOff1[tid].end, i, rid.String())
		}

		for tid, tabl := range tableOff {
			data.UpdateBlockInsertBlkMeta(tid, int32(tabl.offset), int32(tabl.end))
		}
		for tid, tabl := range tableOff1 {
			data.UpdateBlockDeleteBlkMeta(tid, int32(tabl.offset), int32(tabl.end))
		}
		data.bats[BLKMetaInsertIDX].Close()
		data.bats[BLKMetaInsertTxnIDX].Close()
		data.bats[BLKMetaInsertIDX] = blkMeta
		data.bats[BLKMetaInsertTxnIDX] = blkMetaTxn
	}
	cnLocation, dnLocation, checkpointFiles, err := data.WriteTo(dstFs, DefaultCheckpointBlockRows, DefaultCheckpointSize)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	logutil.Infof("checkpoint cnLocation %s, dnLocation %s, checkpointFiles %s", cnLocation.String(), dnLocation.String(), checkpointFiles)
	loc = cnLocation
	tnLocation = dnLocation
	files = append(files, checkpointFiles...)
	files = append(files, cnLocation.Name().String())
	return loc, tnLocation, data, files, nil
}

func LoadCheckpointEntriesFromKey(
	ctx context.Context,
	fs fileservice.FileService,
	location objectio.Location,
	version uint32,
	softDeletes *map[string]map[uint16]bool,
) ([]objectio.Location, *CheckpointData, error) {
	locations := make([]objectio.Location, 0)
	locations = append(locations, location)
	data, err := getCheckpointData(ctx, fs, location, version)
	if err != nil {
		return nil, nil, err
	}

	for _, location = range data.locations {
		locations = append(locations, location)
	}
	for i := 0; i < data.bats[BLKMetaInsertIDX].Length(); i++ {
		deltaLoc := objectio.Location(
			data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		metaLoc := objectio.Location(
			data.bats[BLKMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte))
		if !metaLoc.IsEmpty() {
			locations = append(locations, metaLoc)
		}
		if !deltaLoc.IsEmpty() {
			locations = append(locations, deltaLoc)
		}
	}
	for i := 0; i < data.bats[BLKCNMetaInsertIDX].Length(); i++ {
		deltaLoc := objectio.Location(
			data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_DeltaLoc).Get(i).([]byte))
		metaLoc := objectio.Location(
			data.bats[BLKCNMetaInsertIDX].GetVectorByName(catalog.BlockMeta_MetaLoc).Get(i).([]byte))
		if !metaLoc.IsEmpty() {
			locations = append(locations, metaLoc)
			if len((*softDeletes)[metaLoc.Name().String()]) == 0 {
				(*softDeletes)[metaLoc.Name().String()] = make(map[uint16]bool)
			}
			(*softDeletes)[metaLoc.Name().String()][metaLoc.ID()] = true
		}
		if !deltaLoc.IsEmpty() {
			locations = append(locations, deltaLoc)
		}
	}
	return locations, data, nil
}
