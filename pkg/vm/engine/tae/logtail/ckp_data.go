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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type CheckpointData_V2 struct {
	batch     *batch.Batch
	sinker    *ioutil.Sinker
	allocator *mpool.MPool
}

func NewCheckpointData_V2(
	allocator *mpool.MPool,
	objSize int,
	fs fileservice.FileService,
) *CheckpointData_V2 {
	return &CheckpointData_V2{
		batch: ckputil.NewObjectListBatch(),
		sinker: ckputil.NewDataSinker(
			allocator, fs, ioutil.WithMemorySizeThreshold(objSize),
		),
		allocator: allocator,
	}
}

func (data *CheckpointData_V2) WriteTo(
	ctx context.Context,
	fs fileservice.FileService,
) (CNLocation, TNLocation objectio.Location, ckpfiles []string, err error) {
	if data.batch.RowCount() != 0 {
		err = data.sinker.Write(ctx, data.batch)
		if err != nil {
			return
		}
	}
	if err = data.sinker.Sync(ctx); err != nil {
		return
	}
	files, inMems := data.sinker.GetResult()
	if len(inMems) != 0 {
		panic("logic error")
	}
	ranges := ckputil.MakeTableRangeBatch()
	defer ranges.Clean(data.allocator)
	if err = ckputil.CollectTableRanges(
		ctx, files, ranges, data.allocator, fs,
	); err != nil {
		return
	}
	segmentid := objectio.NewSegmentid()
	fileNum := uint16(0)
	name := objectio.BuildObjectName(segmentid, fileNum)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
	if err != nil {
		return
	}
	if _, err = writer.WriteBatch(ranges); err != nil {
		return
	}
	var blks []objectio.BlockObject
	if blks, _, err = writer.Sync(ctx); err != nil {
		return
	}
	CNLocation = objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	TNLocation = CNLocation
	ckpfiles = make([]string, 0)
	for _, obj := range files {
		ckpfiles = append(ckpfiles, obj.ObjectName().String())
	}
	return
}
func (data *CheckpointData_V2) Close() {
	data.batch.FreeColumns(data.allocator)
	data.sinker.Close()
}
func (data *CheckpointData_V2) ExportStats(prefix string) []zap.Field {
	fields := make([]zap.Field, 0)
	size := data.batch.Allocated()
	rows := data.batch.RowCount()
	persisted, _ := data.sinker.GetResult()
	for _, obj := range persisted {
		size += int(obj.Size())
		rows += int(obj.Rows())
	}
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalSize", prefix), size))
	fields = append(fields, zap.Int(fmt.Sprintf("%stotalRow", prefix), rows))
	return fields
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

func PrefetchCheckpoint(
	ctx context.Context,
	sid string,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, location, mp, fs,
	); err != nil {
		return
	}
	defer release()
	objs := ckputil.ScanObjectStats(metaBatch)
	for _, stats := range objs {
		ioutil.Prefetch(sid, fs, stats.ObjectLocation())
	}
	return
}

func ReplayCheckpoint(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	dataFactory catalog.DataFactory,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, location, mp, fs,
	); err != nil {
		return
	}
	defer release()
	objs := ckputil.ScanObjectStats(metaBatch)
	for _, obj := range objs {
		if err = ckputil.ForEachFile(
			ctx,
			obj,
			func(
				accout uint32,
				dbid, tid uint64,
				objectType int8,
				objectStats objectio.ObjectStats,
				create, delete types.TS,
				rowID types.Rowid,
			) error {
				if forSys == pkgcatalog.IsSystemTable(tid) {
					c.OnReplayObjectBatch_V2(
						dbid, tid, objectType, objectStats, create, delete, dataFactory,
					)
				}
				return nil
			},
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

func ForEachRowInCheckpointData(
	ctx context.Context,
	forEachRow func(
		account uint32,
		dbid, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		create, delete types.TS,
		rowID types.Rowid,
	) error,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, location, mp, fs,
	); err != nil {
		return
	}
	defer release()
	objs := ckputil.ScanObjectStats(metaBatch)
	for _, obj := range objs {

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
func GetCheckpointMetaInfo(
	ctx context.Context,
	location objectio.Location,
	id uint64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (res *ObjectInfoJson, err error) {
	tombstone := make(map[string]struct{})
	tombstoneInfo := make(map[uint64]*tableinfo)

	files := make(map[uint64]*tableinfo)
	objBatchLength := 0
	if err = ForEachRowInCheckpointData(
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
		location,
		common.CheckpointAllocator,
		fs,
	); err != nil {
		return
	}
	res, err = getMetaInfo(
		files, id, objBatchLength, tombstoneInfo, tombstone,
	)
	return
}

func GetTableIDsFromCheckpoint(
	ctx context.Context,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (result []uint64, err error) {
	seen := make(map[uint64]struct{})

	if err = ForEachRowInCheckpointData(
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
		location,
		common.CheckpointAllocator,
		fs,
	); err != nil {
		return
	}
	return
}

func GetObjectsFromCKPMeta(
	ctx context.Context,
	loc objectio.Location,
	pinned map[string]bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, loc, mp, fs,
	); err != nil {
		return
	}
	defer release()

	objs := ckputil.ScanObjectStats(metaBatch)
	for _, obj := range objs {
		pinned[obj.ObjectLocation().Name().String()] = true
	}
	return
}

func PrefetchCheckpointWithTableID(
	ctx context.Context,
	sid string,
	tid uint64,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, location, mp, fs,
	); err != nil {
		return
	}
	defer release()
	ranges := ckputil.ExportToTableRanges(metaBatch, tid, ckputil.ObjectType_Data)
	for _, table := range ranges {
		ioutil.Prefetch(sid, fs, table.ObjectStats.ObjectLocation())
	}
	ranges = ckputil.ExportToTableRanges(metaBatch, tid, ckputil.ObjectType_Tombstone)
	for _, table := range ranges {
		ioutil.Prefetch(sid, fs, table.ObjectStats.ObjectLocation())
	}
	return
}

func ConsumeCheckpointWithTableID(
	ctx context.Context,
	tid uint64,
	location objectio.Location,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var release func()
	var metaBatch *batch.Batch
	if metaBatch, release, err = readMetaBatch(
		ctx, location, mp, fs,
	); err != nil {
		return
	}
	defer release()
	ranges := ckputil.ExportToTableRanges(metaBatch, tid, ckputil.ObjectType_Data)
	iter := ckputil.NewObjectIter(ctx, ranges, mp, fs)
	for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
		entry := iter.Entry()
		if err := forEachObject(ctx, entry, false); err != nil {
			return err
		}
	}
	iter.Close()
	ranges = ckputil.ExportToTableRanges(metaBatch, tid, ckputil.ObjectType_Tombstone)
	iter = ckputil.NewObjectIter(ctx, ranges, mp, fs)
	for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
		entry := iter.Entry()
		if err := forEachObject(ctx, entry, true); err != nil {
			return err
		}
	}
	iter.Close()
	return
}

type CheckpointReplayer struct {
	location objectio.Location
	mp       *mpool.MPool

	locations          map[string]objectio.Location
	tombstoneLocations map[string]objectio.Location

	metaBatch      *containers.Batch
	objBatches     *containers.Batch
	tombstoneBatch *containers.Batch

	closeCB []func()
}

func NewCheckpointReplayer(location objectio.Location, mp *mpool.MPool) *CheckpointReplayer {
	return &CheckpointReplayer{
		location:           location,
		mp:                 mp,
		locations:          make(map[string]objectio.Location),
		tombstoneLocations: make(map[string]objectio.Location),
		closeCB:            make([]func(), 0),
	}
}

func (replayer *CheckpointReplayer) ReadMetaForV12(
	ctx context.Context,
	fs fileservice.FileService,
) (err error) {
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(fs, replayer.location); err != nil {
		return
	}
	attrs := append(BaseAttr, MetaSchemaAttr...)
	typs := append(BaseTypes, MetaShcemaTypes...)
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		CheckpointVersion12, ctx, typs, attrs, MetaIDX, reader, replayer.mp,
	); err != nil {
		return
	}
	replayer.metaBatch = bats[0]
	dataLocationsVec := replayer.metaBatch.Vecs[MetaSchema_DataObject_Idx+2]
	tombstoneLocationsVec := replayer.metaBatch.Vecs[MetaSchema_TombstoneObject_Idx+2]
	for i := 0; i < dataLocationsVec.Length(); i++ {
		dataLocations := BlockLocations(dataLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it := dataLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				replayer.locations[str] = loc
			}
		}
		tombstoneLocations := BlockLocations(tombstoneLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it = tombstoneLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				replayer.tombstoneLocations[str] = loc
			}
		}
	}
	return
}
func (replayer *CheckpointReplayer) ReadMetaForV12WithTableID(
	ctx context.Context,
	tid uint64,
	fs fileservice.FileService,
) (err error) {
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(fs, replayer.location); err != nil {
		return
	}
	attrs := append(BaseAttr, MetaSchemaAttr...)
	typs := append(BaseTypes, MetaShcemaTypes...)
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		CheckpointVersion12, ctx, typs, attrs, MetaIDX, reader, replayer.mp,
	); err != nil {
		return
	}
	replayer.metaBatch = bats[0]

	tids := vector.MustFixedColNoTypeCheck[uint64](replayer.metaBatch.Vecs[MetaSchema_Tid_Idx+2].GetDownstreamVector())
	dataLocationsVec := replayer.metaBatch.Vecs[MetaSchema_DataObject_Idx+2]
	tombstoneLocationsVec := replayer.metaBatch.Vecs[MetaSchema_TombstoneObject_Idx+2]
	start := vector.OrderedFindFirstIndexInSortedSlice(tid, tids)
	if start == -1 {
		return
	}
	for i := start; i < dataLocationsVec.Length(); i++ {
		if tids[i] != tid {
			break
		}
		dataLocations := BlockLocations(dataLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it := dataLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				replayer.locations[str] = loc
			}
		}
		tombstoneLocations := BlockLocations(tombstoneLocationsVec.GetDownstreamVector().GetBytesAt(i))
		it = tombstoneLocations.MakeIterator()
		for it.HasNext() {
			loc := it.Next().GetLocation()
			if !loc.IsEmpty() {
				str := loc.String()
				replayer.tombstoneLocations[str] = loc
			}
		}
	}
	return
}
func (replayer *CheckpointReplayer) ReadDataForV12(
	ctx context.Context,
	fs fileservice.FileService,
) (err error) {
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
				CheckpointVersion12, ctx, typs, destBatch.Attrs, batchIdx, reader, replayer.mp,
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
	replayer.objBatches = makeRespBatchFromSchema(ObjectInfoSchema, replayer.mp)
	readFn(replayer.locations, ObjectInfoIDX, replayer.objBatches)
	replayer.tombstoneBatch = makeRespBatchFromSchema(ObjectInfoSchema, replayer.mp)
	readFn(replayer.tombstoneLocations, TombstoneObjectInfoIDX, replayer.tombstoneBatch)
	return
}
func (replayer *CheckpointReplayer) PrefetchData(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
) {
	for _, loc := range replayer.locations {
		ioutil.Prefetch(sid, fs, loc)
	}
	for _, loc := range replayer.tombstoneLocations {
		ioutil.Prefetch(sid, fs, loc)
	}
}

func (replayer *CheckpointReplayer) ReplayObjectlist(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	dataFactory catalog.DataFactory) {
	replayFn := func(src *containers.Batch, objectType int8) {
		if src == nil || src.Length() == 0 {
			return
		}
		dbids := vector.MustFixedColNoTypeCheck[uint64](src.Vecs[ObjectInfo_DBID_Idx+2].GetDownstreamVector())
		tids := vector.MustFixedColNoTypeCheck[uint64](src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector())
		objectstatsVec := src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector()
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector())
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector())
		for i := 0; i < src.Length(); i++ {
			if forSys == pkgcatalog.IsSystemTable(tids[i]) {
				c.OnReplayObjectBatch_V2(
					dbids[i],
					tids[i],
					objectType,
					objectio.ObjectStats(objectstatsVec.GetBytesAt(i)),
					createTSs[i],
					deleteTSs[i],
					dataFactory,
				)
			}
		}

	}
	replayFn(replayer.objBatches, ckputil.ObjectType_Data)
	replayFn(replayer.tombstoneBatch, ckputil.ObjectType_Tombstone)
}
func (replayer *CheckpointReplayer) ConsumeCheckpointWithTableID(
	ctx context.Context,
	tid uint64,
	forEachObject func(ctx context.Context, obj objectio.ObjectEntry, isTombstone bool) (err error),
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
			if tids[i] != tid {
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
	replayFn(replayer.objBatches, false)
	replayFn(replayer.tombstoneBatch, true)
	return
}
func (replayer *CheckpointReplayer) GetCheckpointMetaInfo(
	id uint64,
) (res *ObjectInfoJson, err error) {
	tombstone := make(map[string]struct{})
	tombstoneInfo := make(map[uint64]*tableinfo)

	files := make(map[uint64]*tableinfo)
	objectBatchLength := replayer.objBatches.Length()

	tids := vector.MustFixedColNoTypeCheck[uint64](
		replayer.objBatches.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector(),
	)
	deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](
		replayer.objBatches.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector(),
	)
	for i := 0; i < objectBatchLength; i++ {
		tid := tids[i]
		if files[tid] == nil {
			files[tid] = &tableinfo{
				tid: tid,
			}
		}
		deleteTs := deleteTSs[i]
		if deleteTs.IsEmpty() {
			files[tid].add++
		} else {
			files[tid].delete++
		}
	}
	res, err = getMetaInfo(
		files, id, objectBatchLength, tombstoneInfo, tombstone,
	)
	return
}
func (replayer *CheckpointReplayer) GetTableIDs() (result []uint64, err error) {
	seen := make(map[uint64]struct{})

	tids := vector.MustFixedColNoTypeCheck[uint64](
		replayer.objBatches.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector(),
	)
	for i := 0; i < replayer.objBatches.Length(); i++ {
		tid := tids[i]
		if _, ok := seen[tid]; !ok {
			result = append(result, tid)
			seen[tid] = struct{}{}
		}
	}
	return
}
func (replayer *CheckpointReplayer) GetObjects(
	ctx context.Context,
	loc objectio.Location,
	pinned map[string]bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	for _, loc := range replayer.locations {
		pinned[loc.Name().String()] = true
	}
	for _, loc := range replayer.tombstoneLocations {
		pinned[loc.Name().String()] = true
	}
	return
}
func (replayer *CheckpointReplayer) ForEachRow(
	fn func(
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
			objStats := objectio.ObjectStats(objectstatsVec.GetBytesAt(i))
			fn(0, dbids[i], tids[i], objectType, objStats, createTSs[i], deleteTSs[i], types.Rowid{})
		}
	}
	if replayer.objBatches != nil {
		scanFn(replayer.objBatches, ckputil.ObjectType_Data)
	}
	if replayer.tombstoneBatch != nil {
		scanFn(replayer.tombstoneBatch, ckputil.ObjectType_Tombstone)
	}
	return nil
}
func (replayer *CheckpointReplayer) Close() {
	if replayer.metaBatch != nil {
		replayer.metaBatch.Close()
		replayer.metaBatch = nil
	}
	if replayer.tombstoneBatch != nil {
		replayer.tombstoneBatch.Close()
		replayer.tombstoneBatch = nil
	}
	if replayer.objBatches != nil {
		replayer.objBatches.Close()
		replayer.objBatches = nil
	}
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
	fs fileservice.FileService) error {
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

	locations := make([]objectio.Location, 0)
	replayers := make([]*CheckpointReplayer, 0)
	for i := 0; i < len(locationsAndVersions); i += 2 {
		key := locationsAndVersions[i]
		version, err := strconv.ParseUint(locationsAndVersions[i+1], 10, 32)
		if err != nil {
			logutil.Error(
				"Parse-CKP-Name-Error",
				zap.String("loc", metaLoc),
				zap.Int("i", i),
				zap.Error(err),
			)
			return err
		}
		if version <= uint64(CheckpointVersion12) {
			location, err := objectio.StringToLocation(key)
			if err != nil {
				logutil.Error(
					"Parse-CKP-Name-Error",
					zap.String("loc", metaLoc),
					zap.Int("i", i),
					zap.Error(err),
				)
				return err
			}
			replayer := NewCheckpointReplayer(location, mp)
			replayers = append(replayers, replayer)
		} else {
			location, err := objectio.StringToLocation(key)
			if err != nil {
				logutil.Error(
					"Parse-CKP-Name-Error",
					zap.String("loc", metaLoc),
					zap.Int("i", i),
					zap.Error(err),
				)
				return err
			}
			locations = append(locations, location)
		}
	}

	for _, replayer := range replayers {
		ioutil.Prefetch(sid, fs, replayer.location)
	}
	for _, loc := range locations {
		ioutil.Prefetch(sid, fs, loc)
	}

	for _, replayer := range replayers {
		if err := replayer.ReadMetaForV12WithTableID(ctx, tableID, fs); err != nil {
			return err
		}
		replayer.PrefetchData(ctx, sid, fs)
	}
	for _, loc := range locations {
		if err := PrefetchCheckpointWithTableID(
			ctx, sid, tableID, loc, mp, fs,
		); err != nil {
			return err
		}
	}

	for _, loc := range locations {
		if err := ConsumeCheckpointWithTableID(
			ctx, tableID, loc, forEachObject, mp, fs,
		); err != nil {
			return err
		}
	}
	for _, replayer := range replayers {
		if err := replayer.ReadDataForV12(ctx, fs); err != nil {
			return err
		}
		if err := replayer.ConsumeCheckpointWithTableID(
			ctx, tableID, forEachObject,
		); err != nil {
			return err
		}
	}

	for _, replayer := range replayers {
		replayer.Close()
	}

	return nil
}

type BaseCollector_V2 struct {
	*catalog.LoopProcessor
	data       *CheckpointData_V2
	start, end types.TS
	packer     *types.Packer
}

func NewBaseCollector_V2(start, end types.TS, size int, fs fileservice.FileService) *BaseCollector_V2 {
	collector := &BaseCollector_V2{
		LoopProcessor: &catalog.LoopProcessor{},
		data:          NewCheckpointData_V2(common.CheckpointAllocator, size, fs),
		start:         start,
		end:           end,
		packer:        types.NewPacker(),
	}
	collector.ObjectFn = collector.visitObject
	collector.TombstoneFn = collector.visitObject
	return collector
}
func NewBackupCollector_V2(start, end types.TS, fs fileservice.FileService) *BaseCollector_V2 {
	collector := &BaseCollector_V2{
		LoopProcessor: &catalog.LoopProcessor{},
		data:          NewCheckpointData_V2(common.CheckpointAllocator, 0, fs),
		start:         start,
		end:           end,
		packer:        types.NewPacker(),
	}
	collector.ObjectFn = collector.visitObjectForBackup
	collector.TombstoneFn = collector.visitObjectForBackup
	return collector
}

func (collector *BaseCollector_V2) Close() {
	collector.packer.Close()
}
func (collector *BaseCollector_V2) OrphanData() *CheckpointData_V2 {
	data := collector.data
	collector.data = nil
	return data
}
func (collector *BaseCollector_V2) Collect(c *catalog.Catalog) (err error) {
	err = c.RecurLoop(collector)
	return
}

func (collector *BaseCollector_V2) visitObject(entry *catalog.ObjectEntry) error {
	mvccNodes := entry.GetMVCCNodeInRange(collector.start, collector.end)

	for _, node := range mvccNodes {
		if node.IsAborted() {
			continue
		}
		isObjectTombstone := node.End.Equal(&entry.CreatedAt)
		if err := collectObjectBatch(
			collector.data.batch, entry, isObjectTombstone, collector.packer, collector.data.allocator,
		); err != nil {
			return err
		}
		if collector.data.batch.RowCount() >= DefaultCheckpointBlockRows {
			collector.data.sinker.Write(context.Background(), collector.data.batch)
			collector.data.batch.CleanOnlyData()
		}
	}
	return nil
}

func (collector *BaseCollector_V2) visitObjectForBackup(entry *catalog.ObjectEntry) error {
	createTS := entry.GetCreatedAt()
	if createTS.GT(&collector.start) {
		return nil
	}
	return collector.visitObject(entry)
}

func NewGlobalCollector_V2(
	fs fileservice.FileService,
	end types.TS,
	versionInterval time.Duration,
) *GlobalCollector_V2 {
	versionThresholdTS := types.BuildTS(end.Physical()-versionInterval.Nanoseconds(), end.Logical())
	collector := &GlobalCollector_V2{
		BaseCollector_V2: *NewBaseCollector_V2(types.TS{}, end, 0, fs),
		versionThershold: versionThresholdTS,
	}
	collector.ObjectFn = collector.visitObjectForGlobal
	collector.TombstoneFn = collector.visitObjectForGlobal

	return collector
}

type GlobalCollector_V2 struct {
	BaseCollector_V2
	// not collect objects deleted before versionThershold
	versionThershold types.TS
}

func (collector *GlobalCollector_V2) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
	entry.RLock()
	defer entry.RUnlock()
	return entry.DeleteBeforeLocked(collector.versionThershold)
}

func (collector *GlobalCollector_V2) visitObjectForGlobal(entry *catalog.ObjectEntry) error {
	if entry.DeleteBefore(collector.versionThershold) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
		return nil
	}
	if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
		return nil
	}
	return collector.BaseCollector_V2.visitObject(entry)
}

func collectObjectBatch(
	data *batch.Batch,
	srcObjectEntry *catalog.ObjectEntry,
	isObjectTombstone bool,
	encoder *types.Packer,
	mp *mpool.MPool,
) (err error) {
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_Accout_Idx], srcObjectEntry.GetTable().GetDB().GetTenantID(), false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_DB_Idx], srcObjectEntry.GetTable().GetDB().ID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_Table_Idx], srcObjectEntry.GetTable().ID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendBytes(
		data.Vecs[ckputil.TableObjectsAttr_ID_Idx], srcObjectEntry.ObjectStats[:], false, mp,
	); err != nil {
		return
	}
	encoder.Reset()
	objType := ckputil.ObjectType_Data
	if srcObjectEntry.IsTombstone {
		objType = ckputil.ObjectType_Tombstone
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], objType, false, mp,
	); err != nil {
		return
	}
	ckputil.EncodeCluser(encoder, srcObjectEntry.GetTable().ID, objType, srcObjectEntry.ID())
	if err = vector.AppendBytes(
		data.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], encoder.Bytes(), false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed(
		data.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], srcObjectEntry.CreatedAt, false, mp,
	); err != nil {
		return
	}
	if isObjectTombstone {
		if err = vector.AppendFixed(
			data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp,
		); err != nil {
			return
		}
	} else {
		if err = vector.AppendFixed(
			data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], srcObjectEntry.DeletedAt, false, mp,
		); err != nil {
			return
		}
	}
	data.SetRowCount(data.Vecs[0].Length())
	return
}
