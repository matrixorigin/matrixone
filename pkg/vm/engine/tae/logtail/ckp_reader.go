// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"strconv"
	"strings"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type CKPReader_V2 struct {
	version     uint32
	location    objectio.Location
	mp          *mpool.MPool
	fs          fileservice.FileService
	withTableID bool
	tid         uint64

	skipFn func(tid uint64) (skip bool)

	ckpDataObjectStats []objectio.ObjectStats
	dataRanges         []ckputil.TableRange         // read by tid
	tombstoneRanges    []ckputil.TableRange         // read by tid
	dataLocations      map[string]objectio.Location // for version 12
	tombstoneLocations map[string]objectio.Location // for version 12
}

// with opt
func NewCKPReader_V2(
	version uint32,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *CKPReader_V2 {
	return &CKPReader_V2{
		version:  version,
		location: location,
		mp:       mp,
		fs:       fs,
	}
}

func NewCKPReaderWithTableID_V2(
	version uint32,
	location objectio.Location,
	tableID uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *CKPReader_V2 {
	return &CKPReader_V2{
		version:     version,
		location:    location,
		withTableID: true,
		tid:         tableID,
		mp:          mp,
		fs:          fs,
	}
}

func (reader *CKPReader_V2) ReadMeta(
	ctx context.Context,
) (err error) {
	if reader.version <= CheckpointVersion12 {
		if reader.withTableID {
			reader.dataLocations, reader.tombstoneLocations, err = readMetaForV12WithTableID(
				ctx, reader.location, reader.tid, reader.mp, reader.fs,
			)
		} else {
			reader.dataLocations, reader.tombstoneLocations, err = readMetaForV12(
				ctx, reader.location, reader.mp, reader.fs,
			)
		}
	} else {
		if reader.withTableID {
			reader.dataRanges, reader.tombstoneRanges, err = readMetaWithTableID(
				ctx, reader.location, reader.tid, reader.mp, reader.fs,
			)
		} else {
			reader.ckpDataObjectStats, err = readMeta(
				ctx, reader.location, reader.mp, reader.fs,
			)
		}
	}
	return
}

func readMetaForV12(
	ctx context.Context,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (data, tombstone map[string]objectio.Location, err error) {
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(fs, location); err != nil {
		return
	}
	attrs := append(BaseAttr, MetaSchemaAttr...)
	typs := append(BaseTypes, MetaShcemaTypes...)
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		CheckpointVersion12, ctx, typs, attrs, MetaIDX, reader, mp,
	); err != nil {
		return
	}
	metaBatch := bats[0]
	data = make(map[string]objectio.Location)
	tombstone = make(map[string]objectio.Location)
	dataLocationsVec := metaBatch.Vecs[MetaSchema_DataObject_Idx+2]
	tombstoneLocationsVec := metaBatch.Vecs[MetaSchema_TombstoneObject_Idx+2]
	for i := 0; i < dataLocationsVec.Length(); i++ {
		dataLocations := BlockLocations(dataLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it := dataLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				data[str] = loc
			}
		}
		tombstoneLocations := BlockLocations(tombstoneLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it = tombstoneLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				tombstone[str] = loc
			}
		}
	}
	return
}

func readMetaForV12WithTableID(
	ctx context.Context,
	location objectio.Location,
	tableID uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (data, tombstone map[string]objectio.Location, err error) {
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(fs, location); err != nil {
		return
	}
	attrs := append(BaseAttr, MetaSchemaAttr...)
	typs := append(BaseTypes, MetaShcemaTypes...)
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		CheckpointVersion12, ctx, typs, attrs, MetaIDX, reader, mp,
	); err != nil {
		return
	}
	metaBatch := bats[0]

	data = make(map[string]objectio.Location)
	tombstone = make(map[string]objectio.Location)
	tids := vector.MustFixedColNoTypeCheck[uint64](metaBatch.Vecs[MetaSchema_Tid_Idx+2].GetDownstreamVector())
	dataLocationsVec := metaBatch.Vecs[MetaSchema_DataObject_Idx+2]
	tombstoneLocationsVec := metaBatch.Vecs[MetaSchema_TombstoneObject_Idx+2]
	start := vector.OrderedFindFirstIndexInSortedSlice(tableID, tids)
	if start == -1 {
		return
	}
	for i := start; i < dataLocationsVec.Length(); i++ {
		if tids[i] != tableID {
			break
		}
		dataLocations := BlockLocations(dataLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it := dataLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				data[str] = loc
			}
		}
		tombstoneLocations := BlockLocations(tombstoneLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it = tombstoneLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				tombstone[str] = loc
			}
		}
	}
	return
}

func readMetaBatch(
	ctx context.Context,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (metaBatch *batch.Batch, release func(), err error) {
	metaVecs := containers.NewVectors(len(ckputil.MetaAttrs))
	if _, release, err = ioutil.LoadColumnsData(
		ctx,
		ckputil.MetaSeqnums,
		ckputil.MetaTypes,
		fs,
		location,
		metaVecs,
		mp,
		0,
	); err != nil {
		return
	}
	metaBatch = batch.New(ckputil.MetaAttrs)
	for i, vec := range metaVecs {
		metaBatch.Vecs[i] = &vec
	}
	metaBatch.SetRowCount(metaVecs.Rows())
	return
}

func readMeta(
	ctx context.Context,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (objects []objectio.ObjectStats, err error) {
	var metaBatch *batch.Batch
	var release func()
	if metaBatch, release, err = readMetaBatch(ctx, location, mp, fs); err != nil {
		return
	}
	defer release()
	return ckputil.ScanObjectStats(metaBatch), nil
}

func readMetaWithTableID(
	ctx context.Context,
	location objectio.Location,
	tableID uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (dataRanges, tombstoneRanges []ckputil.TableRange, err error) {
	var metaBatch *batch.Batch
	var release func()
	if metaBatch, release, err = readMetaBatch(ctx, location, mp, fs); err != nil {
		return
	}
	defer release()
	dataRanges = ckputil.ExportToTableRanges(metaBatch, tableID, ckputil.ObjectType_Data)
	tombstoneRanges = ckputil.ExportToTableRanges(metaBatch, tableID, ckputil.ObjectType_Data)
	return
}

func (reader *CKPReader_V2) GetLocation() []objectio.Location {
	if reader.version <= CheckpointVersion12 {
		locations := make([]objectio.Location, 0, len(reader.dataLocations)+len(reader.dataLocations))
		for _, loc := range reader.dataLocations {
			locations = append(locations, loc)
		}
		for _, loc := range reader.tombstoneLocations {
			locations = append(locations, loc)
		}
		return locations
	} else {
		if reader.withTableID {
			locations := make([]objectio.Location, 0, len(reader.dataRanges)+len(reader.tombstoneRanges))
			for _, tableRange := range reader.dataRanges {
				obj := tableRange.ObjectStats
				for i := 0; i < int(obj.BlkCnt()); i++ {
					loc := obj.ObjectLocation()
					loc.SetID(uint16(i))
					locations = append(locations, loc)
				}
			}
			for _, tableRange := range reader.tombstoneRanges {
				obj := tableRange.ObjectStats
				for i := 0; i < int(obj.BlkCnt()); i++ {
					loc := obj.ObjectLocation()
					loc.SetID(uint16(i))
					locations = append(locations, loc)
				}
			}
			return locations
		} else {
			locations := make([]objectio.Location, 0, len(reader.ckpDataObjectStats))
			for _, obj := range reader.ckpDataObjectStats {
				for i := 0; i < int(obj.BlkCnt()); i++ {
					loc := obj.ObjectLocation()
					loc.SetID(uint16(i))
					locations = append(locations, loc)
				}
			}
			return locations
		}
	}
}

func (reader *CKPReader_V2) PrefetchData(sid string) {
	locations := reader.GetLocation()
	for _, loc := range locations {
		ioutil.Prefetch(sid, reader.fs, loc)
	}
}

func (reader *CKPReader_V2) GetCheckpointData(ctx context.Context) (bat *batch.Batch, err error) {
	if reader.withTableID {
		panic("not support")
	}
	bat = ckputil.NewObjectListBatch()
	if reader.version <= CheckpointVersion12 {
		var dataBatch, tombstoneBatch *containers.Batch
		if dataBatch, tombstoneBatch, err = getCKPDataForV12(
			ctx, reader.dataLocations, reader.tombstoneLocations, reader.mp, reader.fs,
		); err != nil {
			return
		}
		if err = compatibilityForV12(dataBatch, tombstoneBatch, bat, reader.mp); err != nil {
			return
		}
		return
	} else {
		if err = getCKPData(ctx, reader.ckpDataObjectStats, bat, reader.mp, reader.fs); err != nil {
			return
		}
		return
	}
}

func getCKPData(
	ctx context.Context,
	objects []objectio.ObjectStats,
	ckpData *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if ckpData == nil {
		panic("invalid batch")
	}
	for _, obj := range objects {
		reader := ckputil.NewDataReader(
			ctx,
			fs,
			obj,
			readutil.WithColumns(
				ckputil.TableObjectsSeqnums,
				ckputil.TableObjectsTypes,
			),
		)
		var (
			end bool
		)
		tmpBat := ckputil.NewObjectListBatch()
		defer tmpBat.Clean(mp)
		for {
			tmpBat.CleanOnlyData()
			if end, err = reader.Read(
				ctx, tmpBat.Attrs, nil, mp, tmpBat,
			); err != nil {
				return
			}
			if end {
				break
			}
			if _, err = ckpData.Append(ctx, mp, tmpBat); err != nil {
				return
			}
		}
	}
	return
}

func getCKPDataForV12(
	ctx context.Context,
	dataLocations, tombstoneLocations map[string]objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (dataBatch, tombstoneBatch *containers.Batch, err error) {
	readFn := func(locs map[string]objectio.Location, batchIdx uint16, destBatch *containers.Batch) {
		for _, loc := range locs {
			var reader *ioutil.BlockReader
			if reader, err = ioutil.NewObjectReader(fs, loc); err != nil {
				return
			}
			typs := make([]types.Type, len(destBatch.Vecs))
			for i, vec := range destBatch.Vecs {
				typs[i] = *vec.GetType()
			}
			var bats []*containers.Batch
			if bats, err = LoadBlkColumnsByMeta(
				CheckpointVersion12, ctx, typs, destBatch.Attrs, batchIdx, reader, mp,
			); err != nil {
				return
			}
			for _, bat := range bats {
				if err = destBatch.Append(bat); err != nil {
					return
				}
				bat.Close()
			}
		}
	}
	dataBatch = makeRespBatchFromSchema(ObjectInfoSchema, mp)
	readFn(dataLocations, ObjectInfoIDX, dataBatch)
	tombstoneBatch = makeRespBatchFromSchema(ObjectInfoSchema, mp)
	readFn(tombstoneLocations, TombstoneObjectInfoIDX, tombstoneBatch)
	return
}

func compatibilityForV12(
	dataBatch, tombstoneBatch *containers.Batch,
	ckpData *batch.Batch,
	mp *mpool.MPool,
) (err error) {
	if ckpData == nil {
		panic("invalid batch")
	}
	compatibilityFn := func(src *containers.Batch, dataType int8) {
		vector.AppendMultiFixed(
			ckpData.Vecs[ckputil.TableObjectsAttr_Accout_Idx],
			0,
			true,
			src.Length(),
			mp,
		)
		ckpData.Vecs[ckputil.TableObjectsAttr_DB_Idx] = src.Vecs[ObjectInfo_DBID_Idx+2].GetDownstreamVector()
		src.Vecs[ObjectInfo_DBID_Idx+2] = nil
		ckpData.Vecs[ckputil.TableObjectsAttr_Table_Idx] = src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector()
		src.Vecs[ObjectInfo_TID_Idx+2] = nil
		vector.AppendMultiFixed(
			ckpData.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx],
			dataType,
			true,
			src.Length(),
			mp,
		)
		ckpData.Vecs[ckputil.TableObjectsAttr_ID_Idx] = src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector()
		src.Vecs[ObjectInfo_ObjectStats_Idx+2] = nil
		ckpData.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx] = src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector()
		src.Vecs[ObjectInfo_CreateAt_Idx+2] = nil
		ckpData.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx] = src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector()
		src.Vecs[ObjectInfo_DeleteAt_Idx+2] = nil
	}

	compatibilityFn(dataBatch, ckputil.ObjectType_Data)
	compatibilityFn(tombstoneBatch, ckputil.ObjectType_Tombstone)
	ckpData.SetRowCount(ckpData.Vecs[0].Length())
	return
}

func (reader *CKPReader_V2) ForEachRow(
	ctx context.Context,
	forEachRow func(
		account uint32,
		dbid, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		create, delete types.TS,
		rowID types.Rowid,
	) error,
) (err error) {
	if reader.version <= CheckpointVersion12 {
		var dataBatch, tombstoneBatch *containers.Batch
		if dataBatch, tombstoneBatch, err = getCKPDataForV12(
			ctx, reader.dataLocations, reader.tombstoneLocations, reader.mp, reader.fs,
		); err != nil {
			return
		}
		if dataBatch != nil {
			defer dataBatch.Close()
		}
		if tombstoneBatch != nil {
			defer tombstoneBatch.Close()
		}
		if err = forEachRowForV12(dataBatch, tombstoneBatch, forEachRow); err != nil {
			return
		}
		return
	} else {
		return forEachRowInCKPData(
			ctx, reader.ckpDataObjectStats, forEachRow, reader.mp, reader.fs,
		)
	}
}

func forEachRowInCKPData(
	ctx context.Context,
	objectstats []objectio.ObjectStats,
	forEachRow func(
		account uint32,
		dbid, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		create, delete types.TS,
		rowID types.Rowid,
	) error,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	for _, obj := range objectstats {
		if err = ckputil.ForEachFile(
			ctx,
			obj,
			forEachRow,
			func() error {
				return nil
			},
			mp,
			fs,
		); err != nil {
			return
		}
	}
	return
}

func forEachRowForV12(
	dataBatch, tombstoneBatch *containers.Batch,
	forEachRow func(
		account uint32,
		dbid, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		create, delete types.TS,
		rowID types.Rowid,
	) error,
) (err error) {
	scanFn := func(src *containers.Batch, objectType int8) {
		dbids := vector.MustFixedColNoTypeCheck[uint64](src.Vecs[ObjectInfo_DBID_Idx+2].GetDownstreamVector())
		tids := vector.MustFixedColNoTypeCheck[uint64](src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector())
		objectstatsVec := src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector()
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector())
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector())
		for i := 0; i < src.Length(); i++ {
			rowID := types.Rowid{}
			rowID.SetRowOffset(uint32(i))
			objStats := objectio.ObjectStats(objectstatsVec.GetBytesAt(i))
			forEachRow(0, dbids[i], tids[i], objectType, objStats, createTSs[i], deleteTSs[i], rowID)
		}
	}
	if dataBatch != nil {
		scanFn(dataBatch, ckputil.ObjectType_Data)
	}
	if tombstoneBatch != nil {
		scanFn(tombstoneBatch, ckputil.ObjectType_Tombstone)
	}
	return
}

func (reader *CKPReader_V2) ConsumeCheckpointWithTableID(
	ctx context.Context,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
) (err error) {
	if !reader.withTableID {
		panic("not support")
	}
	if reader.version <= CheckpointVersion12 {
		var dataBatch, tombstoneBatch *containers.Batch
		if dataBatch, tombstoneBatch, err = getCKPDataForV12(
			ctx, reader.dataLocations, reader.tombstoneLocations, reader.mp, reader.fs,
		); err != nil {
			return
		}
		if dataBatch != nil {
			defer dataBatch.Close()
		}
		if tombstoneBatch != nil {
			defer tombstoneBatch.Close()
		}
		return consumeCheckpointWithTableIDForV12(
			ctx, forEachObject, dataBatch, tombstoneBatch, reader.tid,
		)
	} else {
		return consumeCheckpointWithTableID(
			ctx, forEachObject, reader.dataRanges, reader.tombstoneRanges, reader.tid, reader.mp, reader.fs,
		)
	}
}

func consumeCheckpointWithTableID(
	ctx context.Context,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
	dataRanges, tombstoneRanges []ckputil.TableRange,
	tableID uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if len(dataRanges) != 0 {
		iter := ckputil.NewObjectIter(ctx, dataRanges, mp, fs)
		for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
			entry := iter.Entry()
			if err := forEachObject(ctx, entry, false); err != nil {
				return err
			}
		}
		iter.Close()
	}
	if tombstoneRanges != nil {
		iter := ckputil.NewObjectIter(ctx, tombstoneRanges, mp, fs)
		for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
			entry := iter.Entry()
			if err := forEachObject(ctx, entry, true); err != nil {
				return err
			}
		}
		iter.Close()
	}
	return
}

func consumeCheckpointWithTableIDForV12(
	ctx context.Context,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
	dataBatch, tombstoneBatch *containers.Batch,
	tableID uint64,
) (err error) {
	replayFn := func(src *containers.Batch, isTombstone bool) {
		if src == nil || src.Length() == 0 {
			return
		}
		tids := vector.MustFixedColNoTypeCheck[uint64](src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector())
		objectstatsVec := src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector()
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector())
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector())
		for i := 0; i < src.Length(); i++ {
			if tids[i] != tableID {
				continue
			}
			obj := objectio.ObjectEntry{
				ObjectStats: objectio.ObjectStats(objectstatsVec.GetBytesAt(i)),
				CreateTime:  createTSs[i],
				DeleteTime:  deleteTSs[i],
			}
			if err := forEachObject(ctx, obj, isTombstone); err != nil {
				return
			}
		}

	}
	replayFn(dataBatch, false)
	replayFn(tombstoneBatch, true)
	return
}

func ReplayCheckpoint(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	reader *CKPReader_V2,
) (err error) {
	return reader.ForEachRow(
		ctx,
		func(
			account uint32,
			dbid, tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			create, delete types.TS,
			rowID types.Rowid,
		) error {
			if forSys == pkgcatalog.IsSystemTable(tid) {
				c.OnReplayObjectBatch_V2(
					dbid, tid, objectType, objectStats, create, delete,
				)
			}
			return nil
		},
	)
}

func GetCheckpointMetaInfo(
	ctx context.Context,
	id uint64,
	reader *CKPReader_V2,
) (res *ObjectInfoJson, err error) {
	tombstone := make(map[string]struct{})
	tombstoneInfo := make(map[uint64]*tableinfo)

	files := make(map[uint64]*tableinfo)
	objBatchLength := 0
	if err = reader.ForEachRow(
		ctx,
		func(
			accout uint32,
			dbid, tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			createTs, deleteTs types.TS,
			rowID types.Rowid,
		) error {
			if objectType == ckputil.ObjectType_Data {
				objBatchLength++
				if files[tid] == nil {
					files[tid] = &tableinfo{
						tid: tid,
					}
				}
				if deleteTs.IsEmpty() {
					files[tid].add++
				} else {
					files[tid].delete++
				}
			}
			return nil
		},
	); err != nil {
		return
	}
	return getMetaInfo(
		files, id, objBatchLength, tombstoneInfo, tombstone,
	)
}

func GetTableIDsFromCheckpoint(
	ctx context.Context,
	reader *CKPReader_V2,
) (result []uint64, err error) {
	seen := make(map[uint64]struct{})

	if err = reader.ForEachRow(
		ctx,
		func(
			accout uint32,
			dbid, tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			createTs, deleteTs types.TS,
			rowID types.Rowid,
		) error {
			if objectType == ckputil.ObjectType_Data {
				if _, ok := seen[tid]; !ok {
					result = append(result, tid)
					seen[tid] = struct{}{}
				}
			}
			return nil
		},
	); err != nil {
		return
	}
	return
}

// only need to read meta
func GetObjectsFromCKPMeta(
	ctx context.Context,
	reader *CKPReader_V2,
	pinned map[string]bool,
) (err error) {

	locations := reader.GetLocation()
	for _, loc := range locations {
		pinned[loc.Name().String()] = true
	}
	return
}

// metaLoc records locaion of checkpoints.
// In v1, for each checkpoint, it records location and version
// separated by ';'.
// e.g. ckp1-location;ckp1-version;ckp2-location;ckp2-version;
// In v2, it first records the version of the whole metaloc,
// then follows the same format as v1 for each ckp.
// e.g. 2;ckp1-location;ckp1-version;ckp2-location;ckp2-version;
func ConsumeCheckpointEntries(
	ctx context.Context,
	sid string,
	metaLoc string,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
	mp *mpool.MPool,
	fs fileservice.FileService) (err error) {
	if metaLoc == "" {
		return nil
	}
	v2.LogtailLoadCheckpointCounter.Inc()
	now := time.Now()
	defer func() {
		v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
	}()
	locationsAndVersions := strings.Split(metaLoc, ";")

	// If the length of locationsAndVersions is even, the protocal version is v1.
	// If it's odd, the first value is version.
	if len(locationsAndVersions)%2 == 1 {
		locationsAndVersions = locationsAndVersions[1:]
	}

	readers := make([]*CKPReader_V2, 0)
	for i := 0; i < len(locationsAndVersions); i += 2 {
		key := locationsAndVersions[i]
		var version uint64
		if version, err = strconv.ParseUint(
			locationsAndVersions[i+1], 10, 32,
		); err != nil {
			logutil.Error(
				"Parse-CKP-Name-Error",
				zap.String("loc", metaLoc),
				zap.Int("i", i),
				zap.Error(err),
			)
			return err
		}
		var location objectio.Location
		if location, err = objectio.StringToLocation(
			key,
		); err != nil {
			logutil.Error(
				"Parse-CKP-Name-Error",
				zap.String("loc", metaLoc),
				zap.Int("i", i),
				zap.Error(err),
			)
			return err
		}
		reader := NewCKPReaderWithTableID_V2(uint32(version), location, tableID, mp, fs)
		readers = append(readers, reader)
	}

	for _, reader := range readers {
		ioutil.Prefetch(sid, fs, reader.location)
	}

	for _, reader := range readers {
		if err := reader.ReadMeta(ctx); err != nil {
			return err
		}
		reader.PrefetchData(sid)
	}

	for _, reader := range readers {
		if err := reader.ConsumeCheckpointWithTableID(
			ctx, forEachObject,
		); err != nil {
			return err
		}
	}
	return
}
