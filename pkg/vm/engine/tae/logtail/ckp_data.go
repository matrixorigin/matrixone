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

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
)

type CheckpointData_V2 struct {
	batch     *batch.Batch
	sinker    *ioutil.Sinker
	allocator *mpool.MPool
}

func NewCheckpointData_V2(allocator *mpool.MPool, fs fileservice.FileService) *CheckpointData_V2 {
	return &CheckpointData_V2{
		batch:     ckputil.NewObjectListBatch(),
		sinker:    ckputil.NewDataSinker(allocator, fs),
		allocator: allocator,
	}
}

func (data *CheckpointData_V2) WriteTo(
	ctx context.Context,
	fs fileservice.FileService,
) (CNLocation, TNLocation objectio.Location, ckpfiles, err error) {
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
	return
}
func (data *CheckpointData_V2) Close() {
	data.batch.FreeColumns(data.allocator)
	data.sinker.Close()
}

type CheckpointReplayer struct {
	location objectio.Location
	version  uint32
	mp       *mpool.MPool

	meta               containers.Vectors
	locations          map[string]objectio.Location
	tombstoneLocations map[string]objectio.Location
	maxBlkIDs          map[string]uint16
	objectInfo         []containers.Vectors

	metaBatch      *containers.Batch
	objBatches     *containers.Batch
	tombstoneBatch *containers.Batch

	closeCB []func()
}

type CheckpointReplayer_V2 struct {
	location          objectio.Location
	tableObjectsStats []objectio.ObjectStats
}

func PrefetchCheckpoint(
	ctx context.Context,
	sid string,
	location objectio.Location,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	metaVecs := containers.NewVectors(len(ckputil.MetaAttrs))
	var release func()
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
	defer release()
	metaBatch := batch.New(ckputil.MetaAttrs)
	for i, vec := range metaVecs {
		metaBatch.Vecs[i] = &vec
	}
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
	metaVecs := containers.NewVectors(len(ckputil.MetaAttrs))
	var release func()
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
	defer release()
	metaBatch := batch.New(ckputil.MetaAttrs)
	for i, vec := range metaVecs {
		metaBatch.Vecs[i] = &vec
	}
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

func NewCheckpointReplayer(location objectio.Location, mp *mpool.MPool) *CheckpointReplayer {
	return &CheckpointReplayer{
		location:           location,
		mp:                 mp,
		locations:          make(map[string]objectio.Location),
		maxBlkIDs:          make(map[string]uint16),
		tombstoneLocations: make(map[string]objectio.Location),
		objectInfo:         make([]containers.Vectors, 0),
		closeCB:            make([]func(), 0),
	}
}

func (replayer *CheckpointReplayer) ReadMetaForV12(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
) (err error) {
	replayer.meta = containers.NewVectors(len(MetaSchemaAttr))
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewObjectReader(fs, replayer.location); err != nil {
		return
	}
	attrs := append(BaseAttr, MetaSchemaAttr...)
	typs := append(BaseTypes, MetaShcemaTypes...)
	var bats []*containers.Batch
	if bats, err = LoadBlkColumnsByMeta(
		replayer.version, ctx, typs, attrs, MetaIDX, reader, replayer.mp,
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
				replayer.version, ctx, typs, destBatch.Attrs, batchIdx, reader, replayer.mp,
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

func (replayer *CheckpointReplayer) UpgradeDate() (err error) {
	replayer.objectInfo = make([]containers.Vectors, 0)
	upgradeFn := func(objectType int8, src *containers.Batch) (err error) {
		if src == nil || src.Length() == 0 {
			return
		}
		dest := containers.NewVectors(len(ckputil.TableObjectsAttrs))
		dest[ckputil.TableObjectsAttr_Table_Idx] = *src.Vecs[ObjectInfo_TID_Idx+2].GetDownstreamVector()
		dest[ckputil.TableObjectsAttr_ID_Idx] = *src.Vecs[ObjectInfo_ObjectStats_Idx+2].GetDownstreamVector()
		dest[ckputil.TableObjectsAttr_DB_Idx] = *src.Vecs[ObjectInfo_DBID_Idx+2].GetDownstreamVector()
		dest[ckputil.TableObjectsAttr_CreateTS_Idx] = *src.Vecs[ObjectInfo_CreateAt_Idx+2].GetDownstreamVector()
		dest[ckputil.TableObjectsAttr_DeleteTS_Idx] = *src.Vecs[ObjectInfo_DeleteAt_Idx+2].GetDownstreamVector()
		objTypeVec := vector.NewVec(types.T_int8.ToType())
		if err = vector.AppendMultiFixed(
			objTypeVec, objectType, false, src.Length(), replayer.mp,
		); err != nil {
			return
		}
		dest[ckputil.TableObjectsAttr_ObjectType_Idx] = *objTypeVec
		replayer.objectInfo = append(replayer.objectInfo, dest)
		return
	}

	if err = upgradeFn(ckputil.ObjectType_Data, replayer.objBatches); err != nil {
		return
	}
	if err = upgradeFn(ckputil.ObjectType_Tombstone, replayer.tombstoneBatch); err != nil {
		return
	}
	return
}

func (replayer *CheckpointReplayer) ReadData(ctx context.Context, sid string, fs fileservice.FileService) (err error) {
	for str, loc := range replayer.locations {
		maxBlkID := replayer.maxBlkIDs[str]
		for i := 0; i <= int(maxBlkID); i++ {
			loc.SetID(uint16(i))
			dataVecs := containers.NewVectors(len(ckputil.TableObjectsAttrs))
			var release func()
			if _, release, err = ioutil.LoadColumnsData(
				ctx,
				ckputil.TableObjectsSeqnums,
				ckputil.TableObjectsTypes,
				fs,
				loc,
				dataVecs,
				replayer.mp,
				0,
			); err != nil {
				return
			}
			replayer.closeCB = append(replayer.closeCB, release)
			replayer.objectInfo = append(replayer.objectInfo, dataVecs)
		}
	}
	return
}
func (replayer *CheckpointReplayer) ReplayObjectlist(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	dataFactory *tables.DataFactory) {
	for _, vecs := range replayer.objectInfo {
		// startOffset, endOffset := 0, 0
		startOffset, _ := 0, 0
		tids := vector.MustFixedColNoTypeCheck[uint64](&vecs[ckputil.TableObjectsAttr_Table_Idx])
		prevTid := tids[0]
		for i := 0; i < len(tids); i++ {
			if tids[i] != prevTid {
				// endOffset = i
				if forSys == pkgcatalog.IsSystemTable(prevTid) {
					panic("todo")
					// c.OnReplayObjectBatch_V2(vecs, dataFactory, startOffset, endOffset)
				}
				startOffset = i
				prevTid = tids[i]
			}
		}
		if startOffset != len(tids) {
			if forSys == pkgcatalog.IsSystemTable(prevTid) {
				// c.OnReplayObjectBatch_V2(vecs, dataFactory, startOffset, len(tids))
			}
		}
	}
}

type BaseCollector_V2 struct {
	*catalog.LoopProcessor
	data       *CheckpointData_V2
	start, end types.TS
	packer     *types.Packer
}

func NewBaseCollector_V2(start, end types.TS, fs fileservice.FileService, mp *mpool.MPool) *BaseCollector_V2 {
	collector := &BaseCollector_V2{
		LoopProcessor: &catalog.LoopProcessor{},
		data:          NewCheckpointData_V2(mp, fs),
		start:         start,
		end:           end,
		packer:        types.NewPacker(),
	}
	collector.ObjectFn = collector.visitObject
	collector.TombstoneFn = collector.visitObject
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
