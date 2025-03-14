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
	"fmt"
	"sort"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type ckpDataReader interface {
	Read(ctx context.Context, bat *batch.Batch, mp *mpool.MPool) (end bool, err error)
	Reset(ctx context.Context)
}

type ckpObjectReader struct {
	objects   []objectio.ObjectStats
	objectIdx int
	reader    engine.Reader
	fs        fileservice.FileService
}

func newObjectReader(
	ctx context.Context,
	objects []objectio.ObjectStats,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *ckpObjectReader {
	var reader engine.Reader
	if len(objects) != 0 {
		object := objects[0]
		reader = ckputil.NewDataReader(
			ctx,
			fs,
			object,
			readutil.WithColumns(
				ckputil.DataScan_TableIDSeqnums,
				ckputil.DataScan_TableIDTypes,
			),
		)
	}
	return &ckpObjectReader{
		objects: objects,
		reader:  reader,
		fs:      fs,
	}
}

func (r *ckpObjectReader) Read(
	ctx context.Context,
	bat *batch.Batch,
	mp *mpool.MPool,
) (end bool, err error) {
	if r.objectIdx >= len(r.objects) {
		return true, nil
	}
	for {
		if end, err = r.reader.Read(ctx, bat.Attrs, nil, mp, bat); err != nil {
			return
		}
		if !end {
			return
		}
		r.objectIdx++
		if r.objectIdx >= len(r.objects) {
			return true, nil
		}
		object := r.objects[r.objectIdx]
		r.reader = ckputil.NewDataReader(
			ctx,
			r.fs,
			object,
			readutil.WithColumns(
				ckputil.DataScan_TableIDSeqnums,
				ckputil.DataScan_TableIDTypes,
			),
		)
	}
}

func (r *ckpObjectReader) Reset(ctx context.Context) {
	r.objectIdx = 0
	if len(r.objects) != 0 {
		object := r.objects[0]
		r.reader = ckputil.NewDataReader(
			ctx,
			r.fs,
			object,
			readutil.WithColumns(
				ckputil.DataScan_TableIDSeqnums,
				ckputil.DataScan_TableIDTypes,
			),
		)
	}
}

type ckpObjectReaderForV12 struct {
	dataLocations      []objectio.Location
	tombstoneLocations []objectio.Location
	objectIndex        int
	fs                 fileservice.FileService
}

func newCKPObjectReaderForV12(
	dataLocations []objectio.Location,
	tombstoneLocations []objectio.Location,
	fs fileservice.FileService,
) *ckpObjectReaderForV12 {
	return &ckpObjectReaderForV12{
		dataLocations:      dataLocations,
		tombstoneLocations: tombstoneLocations,
		fs:                 fs,
	}
}

func (r *ckpObjectReaderForV12) Read(
	ctx context.Context,
	destBatch *batch.Batch,
	mp *mpool.MPool,
) (end bool, err error) {
	if r.objectIndex >= len(r.dataLocations)+len(r.tombstoneLocations) {
		return true, nil
	}
	var location objectio.Location
	var isTombstone bool
	var batchIndex uint16
	if r.objectIndex >= len(r.dataLocations) {
		isTombstone = true
		batchIndex = TombstoneObjectInfoIDX
		location = r.tombstoneLocations[r.objectIndex-len(r.dataLocations)]
	} else {
		isTombstone = false
		batchIndex = ObjectInfoIDX
		location = r.dataLocations[r.objectIndex]
	}

	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(r.fs, location); err != nil {
		return
	}
	typs := make([]types.Type, len(destBatch.Vecs))
	for i, vec := range destBatch.Vecs {
		typs[i] = *vec.GetType()
	}
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		CheckpointVersion12, ctx, typs, destBatch.Attrs, batchIndex, reader, mp,
	); err != nil {
		return
	}
	objID := location.ObjectId()
	for i, bat := range bats {
		defer bat.Close()
		blkID := objectio.NewBlockidWithObjectID(&objID, uint16(i))
		if isTombstone {
			if err = compatibilityForV12(&blkID, nil, bat, destBatch, mp); err != nil {
				return
			}
		} else {
			if err = compatibilityForV12(&blkID, bat, nil, destBatch, mp); err != nil {
				return
			}
		}
	}
	r.objectIndex++
	return
}

func (r *ckpObjectReaderForV12) Reset(ctx context.Context) {
	r.objectIndex = 0
}

type CKPReader struct {
	version     uint32
	location    objectio.Location
	mp          *mpool.MPool
	fs          fileservice.FileService
	withTableID bool
	tid         uint64

	ckpDataObjectStats []objectio.ObjectStats
	dataRanges         []ckputil.TableRange         // read by tid
	tombstoneRanges    []ckputil.TableRange         // read by tid
	dataLocations      map[string]objectio.Location // for version 12
	tombstoneLocations map[string]objectio.Location // for version 12

	ckpDataReader
}

func NewCKPReader(
	version uint32,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *CKPReader {
	return &CKPReader{
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
) *CKPReader {
	return &CKPReader{
		version:     version,
		location:    location,
		withTableID: true,
		tid:         tableID,
		mp:          mp,
		fs:          fs,
	}
}

func (reader *CKPReader) ReadMeta(
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
		dataLocations := make([]objectio.Location, 0, len(reader.dataLocations))
		for _, loc := range reader.dataLocations {
			dataLocations = append(dataLocations, loc)
		}
		tombstoneLocations := make([]objectio.Location, 0, len(reader.tombstoneLocations))
		for _, loc := range reader.tombstoneLocations {
			tombstoneLocations = append(tombstoneLocations, loc)
		}
		reader.ckpDataReader = newCKPObjectReaderForV12(
			dataLocations,
			tombstoneLocations,
			reader.fs,
		)
	} else {
		if reader.withTableID {
			reader.dataRanges, reader.tombstoneRanges, err = readMetaWithTableID(
				ctx, reader.location, reader.tid, reader.mp, reader.fs,
			)
			// TODO new reader
		} else {
			reader.ckpDataObjectStats, err = readMeta(
				ctx, reader.location, reader.mp, reader.fs,
			)
			reader.ckpDataReader = newObjectReader(
				ctx,
				reader.ckpDataObjectStats,
				reader.mp,
				reader.fs,
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
	defer metaBatch.Close()
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
	defer metaBatch.Close()

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
	tombstoneRanges = ckputil.ExportToTableRanges(metaBatch, tableID, ckputil.ObjectType_Tombstone)
	return
}

func (reader *CKPReader) GetLocations() []objectio.Location {
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

func (reader *CKPReader) PrefetchData(sid string) {
	locations := reader.GetLocations()
	for _, loc := range locations {
		ioutil.Prefetch(sid, reader.fs, loc)
	}
}

func (reader *CKPReader) GetCheckpointData(ctx context.Context) (ckpData *batch.Batch, err error) {
	if reader.withTableID {
		panic("not support")
	}
	ckpData = ckputil.MakeDataScanTableIDBatch()
	tmpBatch := ckputil.MakeDataScanTableIDBatch()
	defer tmpBatch.Clean(reader.mp)
	for {
		tmpBatch.CleanOnlyData()
		var end bool
		if end, err = reader.ckpDataReader.Read(ctx, tmpBatch, reader.mp); err != nil {
			return
		}
		if end {
			return
		}
		if _, err = ckpData.Append(ctx, reader.mp, tmpBatch); err != nil {
			return
		}
	}
}

func (reader *CKPReader) LoadBatchData(
	ctx context.Context,
	_ []string,
	_ *plan.Expr,
	_ *mpool.MPool,
	data *batch.Batch,
) (end bool, err error) {
	if data == nil {
		panic("invalid input")
	}
	return reader.ckpDataReader.Read(ctx, data, reader.mp)
}

func compatibilityForV12(
	blockID *objectio.Blockid,
	dataBatch, tombstoneBatch *containers.Batch,
	ckpData *batch.Batch,
	mp *mpool.MPool,
) (err error) {
	if ckpData == nil {
		panic("invalid batch")
	}
	compatibilityFn := func(src *containers.Batch, dataType int8) {
		sels := make([]int64, src.Length())
		for i := 0; i < src.Length(); i++ {
			sels[i] = int64(i)
		}
		vector.AppendMultiFixed(
			ckpData.Vecs[ckputil.TableObjectsAttr_Accout_Idx],
			0,
			true,
			src.Length(),
			mp,
		)
		ckpData.Vecs[ckputil.TableObjectsAttr_DB_Idx].Union(src.Vecs[ObjectInfo_DBID_Idx+2].GetDownstreamVector(), sels, mp)
		ckpData.Vecs[ckputil.TableObjectsAttr_Table_Idx].Union(src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector(), sels, mp)
		vector.AppendMultiFixed(
			ckpData.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx],
			dataType,
			false,
			src.Length(),
			mp,
		)
		ckpData.Vecs[ckputil.TableObjectsAttr_ID_Idx].Union(src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector(), sels, mp)
		ckpData.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx].Union(src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector(), sels, mp)
		ckpData.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx].Union(src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector(), sels, mp)
		for i := 0; i < src.Length(); i++ {
			rowID := types.NewRowid(blockID, uint32(i))
			vector.AppendFixed(ckpData.Vecs[ckputil.TableObjectsAttr_PhysicalAddr_Idx], rowID, false, mp)
		}
	}

	if dataBatch != nil {
		compatibilityFn(dataBatch, ckputil.ObjectType_Data)
	}
	if tombstoneBatch != nil {
		compatibilityFn(tombstoneBatch, ckputil.ObjectType_Tombstone)
	}
	ckpData.SetRowCount(ckpData.Vecs[0].Length())
	return
}

func (reader *CKPReader) ForEachRow(
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
	if reader.withTableID {
		panic("not support")
	}
	tmpBatch := ckputil.MakeDataScanTableIDBatch()
	defer tmpBatch.Clean(reader.mp)
	defer reader.ckpDataReader.Reset(ctx)
	for {
		tmpBatch.CleanOnlyData()
		var end bool
		if end, err = reader.ckpDataReader.Read(ctx, tmpBatch, reader.mp); err != nil {
			return
		}
		if end {
			return
		}
		accouts := vector.MustFixedColNoTypeCheck[uint32](tmpBatch.Vecs[ckputil.TableObjectsAttr_Accout_Idx])
		dbids := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[ckputil.TableObjectsAttr_DB_Idx])
		tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[ckputil.TableObjectsAttr_Table_Idx])
		objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBatch.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx])
		objectStatsVec := tmpBatch.Vecs[ckputil.TableObjectsAttr_ID_Idx]
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx])
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx])
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](tmpBatch.Vecs[ckputil.TableObjectsAttr_PhysicalAddr_Idx])
		for i, rows := 0, tmpBatch.RowCount(); i < rows; i++ {
			if err = forEachRow(
				accouts[i],
				dbids[i],
				tableIds[i],
				objectTypes[i],
				objectio.ObjectStats(objectStatsVec.GetBytesAt(i)),
				createTSs[i],
				deleteTSs[i],
				rowids[i],
			); err != nil {
				return
			}
		}
	}
}

func (reader *CKPReader) ConsumeCheckpointWithTableID(
	ctx context.Context,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
) (err error) {
	if !reader.withTableID {
		panic("not support")
	}
	if reader.version <= CheckpointVersion12 {
		tmpBatch := ckputil.MakeDataScanTableIDBatch()
		defer tmpBatch.Clean(reader.mp)
		defer reader.ckpDataReader.Reset(ctx)
		for {
			tmpBatch.CleanOnlyData()
			var end bool
			if end, err = reader.ckpDataReader.Read(ctx, tmpBatch, reader.mp); err != nil {
				return
			}
			if end {
				return
			}

			tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[ckputil.TableObjectsAttr_Table_Idx])
			objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBatch.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx])
			objectStatsVec := tmpBatch.Vecs[ckputil.TableObjectsAttr_ID_Idx]
			createTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx])
			deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx])

			for i := 0; i < tmpBatch.RowCount(); i++ {
				if tableIds[i] != reader.tid {
					continue
				}
				obj := objectio.ObjectEntry{
					ObjectStats: objectio.ObjectStats(objectStatsVec.GetBytesAt(i)),
					CreateTime:  createTSs[i],
					DeleteTime:  deleteTSs[i],
				}
				var isTombstone bool
				switch objectTypes[i] {
				case ckputil.ObjectType_Data:
					isTombstone = false
				case ckputil.ObjectType_Tombstone:
					isTombstone = true
				default:
					panic(fmt.Sprintf("invalid object type %d", objectTypes[i]))
				}
				if err = forEachObject(ctx, obj, isTombstone); err != nil {
					return
				}
			}
		}
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
		defer iter.Close()
		for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
			entry := iter.Entry()
			if err := forEachObject(ctx, entry, false); err != nil {
				return err
			}
		}
	}
	if tombstoneRanges != nil {
		iter := ckputil.NewObjectIter(ctx, tombstoneRanges, mp, fs)
		defer iter.Close()
		for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
			entry := iter.Entry()
			if err := forEachObject(ctx, entry, true); err != nil {
				return err
			}
		}
	}
	return
}

func ReplayCheckpoint(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	reader *CKPReader,
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
	reader *CKPReader,
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

func getMetaInfo(
	files map[uint64]*tableinfo,
	id uint64,
	objBatchLength int,
	tombstoneInfo map[uint64]*tableinfo,
	tombstone map[string]struct{},
) (res *ObjectInfoJson, err error) {
	tableinfos := make([]*tableinfo, 0)
	objectCount := uint64(0)
	addCount := uint64(0)
	deleteCount := uint64(0)
	for _, count := range files {
		tableinfos = append(tableinfos, count)
		objectCount += count.add
		addCount += count.add
		objectCount += count.delete
		deleteCount += count.delete
	}
	sort.Slice(tableinfos, func(i, j int) bool {
		return tableinfos[i].add > tableinfos[j].add
	})
	tableJsons := make([]TableInfoJson, 0, objBatchLength)
	tables := make(map[uint64]int)
	for i := range len(tableinfos) {
		tablejson := TableInfoJson{
			ID:     tableinfos[i].tid,
			Add:    tableinfos[i].add,
			Delete: tableinfos[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tables[tablejson.ID] = len(tableJsons)
			tableJsons = append(tableJsons, tablejson)
		}
	}
	tableinfos2 := make([]*tableinfo, 0)
	objectCount2 := uint64(0)
	addCount2 := uint64(0)
	for _, count := range tombstoneInfo {
		tableinfos2 = append(tableinfos2, count)
		objectCount2 += count.add
		addCount2 += count.add
	}
	sort.Slice(tableinfos2, func(i, j int) bool {
		return tableinfos2[i].add > tableinfos2[j].add
	})

	for i := range len(tableinfos2) {
		if idx, ok := tables[tableinfos2[i].tid]; ok {
			tablejson := &tableJsons[idx]
			tablejson.TombstoneRows = tableinfos2[i].add
			tablejson.TombstoneCount = tableinfos2[i].delete
			continue
		}
		tablejson := TableInfoJson{
			ID:             tableinfos2[i].tid,
			TombstoneRows:  tableinfos2[i].add,
			TombstoneCount: tableinfos2[i].delete,
		}
		if id == 0 || tablejson.ID == id {
			tableJsons = append(tableJsons, tablejson)
		}
	}

	res = &ObjectInfoJson{
		TableCnt:     len(tableJsons),
		ObjectCnt:    objectCount,
		ObjectAddCnt: addCount,
		ObjectDelCnt: deleteCount,
		TombstoneCnt: len(tombstone),
	}
	return
}
func LoadCheckpointLocations(
	ctx context.Context,
	sid string,
	reader *CKPReader,
) (map[string]objectio.Location, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	locationMap := make(map[string]objectio.Location)
	locations := reader.GetLocations()
	for _, loc := range locations {
		locationMap[loc.Name().String()] = loc
	}
	return locationMap, nil
}

func GetTableIDsFromCheckpoint(
	ctx context.Context,
	reader *CKPReader,
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
	reader *CKPReader,
	pinned map[string]bool,
) (err error) {

	locations := reader.GetLocations()
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

	readers := make([]*CKPReader, 0)
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
