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

func (data *CheckpointData_V2) WriteTo(ctx context.Context, fs fileservice.FileService) (CNLocation, TNLocation objectio.Location, ckpfiles, err error) {
	if data.batch.RowCount() != 0 {
		err = data.sinker.Write(ctx, data.batch)
		if err != nil {
			return
		}
	}
	err = data.sinker.Sync(ctx)
	if err != nil {
		return
	}
	files, inMems := data.sinker.GetResult()
	if len(inMems) != 0 {
		panic("logic error")
	}
	ranges := ckputil.MakeTableRangeBatch()
	defer ranges.Clean(data.allocator)
	err = ckputil.CollectTableRanges(ctx, files, ranges, data.allocator, fs)
	if err != nil {
		return
	}
	segmentid := objectio.NewSegmentid()
	fileNum := uint16(0)
	name := objectio.BuildObjectName(segmentid, fileNum)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
	if err != nil {
		return
	}
	_, err = writer.WriteBatch(ranges)
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(ctx)
	if err != nil {
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
	mp       *mpool.MPool

	meta       containers.Vectors
	locations  map[string]objectio.Location
	objectInfo []containers.Vectors

	closeCB []func()
}

func NewCheckpointReplayer(location objectio.Location, mp *mpool.MPool) *CheckpointReplayer {
	return &CheckpointReplayer{
		location:   location,
		mp:         mp,
		locations:  make(map[string]objectio.Location),
		objectInfo: make([]containers.Vectors, 0),
		closeCB:    make([]func(), 0),
	}
}

func (replayer *CheckpointReplayer) PrefetchMeta(sid string, fs fileservice.FileService) {
	ioutil.Prefetch(sid, fs, replayer.location)
}

func (replayer *CheckpointReplayer) ReadMeta(ctx context.Context, sid string, fs fileservice.FileService) (err error) {
	replayer.meta = containers.NewVectors(len(ckputil.MetaAttrs))
	_, release, err := ioutil.LoadColumnsData(
		ctx,
		ckputil.MetaSeqnums,
		ckputil.MetaTypes,
		fs,
		replayer.location,
		replayer.meta,
		replayer.mp,
		0,
	)
	if err != nil {
		return
	}
	replayer.closeCB = append(replayer.closeCB, release)
	locationVec := replayer.meta[ckputil.MetaAttr_Location_Idx]
	for i := 0; i < locationVec.Length(); i++ {
		location := objectio.Location(locationVec.GetBytesAt(i))
		replayer.locations[location.String()] = location
	}
	return
}

func (replayer *CheckpointReplayer) PrefetchData(sid string, fs fileservice.FileService) {
	for _, loc := range replayer.locations {
		ioutil.Prefetch(sid, fs, loc)
	}
}

func (replayer *CheckpointReplayer) ReadData(ctx context.Context, sid string, fs fileservice.FileService) (err error) {
	for _, loc := range replayer.locations {
		dataVecs := containers.NewVectors(len(ckputil.TableObjectsAttrs))
		var release func()
		_, release, err = ioutil.LoadColumnsData(
			ctx,
			ckputil.TableObjectsSeqnums,
			ckputil.TableObjectsTypes,
			fs,
			loc,
			dataVecs,
			replayer.mp,
			0,
		)
		if err != nil {
			return
		}
		replayer.closeCB = append(replayer.closeCB, release)
		replayer.objectInfo = append(replayer.objectInfo, dataVecs)
	}
	return
}
func (replayer *CheckpointReplayer) ReplayObjectlist(
	ctx context.Context,
	c *catalog.Catalog,
	forSys bool,
	dataFactory *tables.DataFactory) {
	for _, vecs := range replayer.objectInfo {
		startOffset, endOffset := 0, 0
		tids := vector.MustFixedColNoTypeCheck[uint64](&vecs[ckputil.TableObjectsAttr_Table_Idx])
		prevTid := tids[0]
		for i := 0; i < len(tids); i++ {
			if tids[i] != prevTid {
				endOffset = i
				if forSys == pkgcatalog.IsSystemTable(prevTid) {
					c.OnReplayObjectBatch_V2(vecs, dataFactory, startOffset, endOffset)
				}
				startOffset = i
				prevTid = tids[i]
			}
		}
		if startOffset != len(tids) {
			if forSys == pkgcatalog.IsSystemTable(prevTid) {
				c.OnReplayObjectBatch_V2(vecs, dataFactory, startOffset, len(tids))
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
		create := node.End.Equal(&entry.CreatedAt)
		visitObject_V2(collector.data.batch, entry, create, false, types.TS{}, collector.packer, collector.data.allocator)
		if collector.data.batch.RowCount() >= DefaultCheckpointBlockRows {
			collector.data.sinker.Write(context.Background(), collector.data.batch)
			collector.data.batch.CleanOnlyData()
		}
	}
	return nil
}

func visitObject_V2(
	bat *batch.Batch, entry *catalog.ObjectEntry, create, push bool, commitTS types.TS, packer *types.Packer, mp *mpool.MPool) {
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_Accout_Idx], entry.GetTable().GetDB().GetTenantID(), false, mp)
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DB_Idx], entry.GetTable().GetDB().ID, false, mp)
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_Table_Idx], entry.GetTable().ID, false, mp)
	vector.AppendBytes(bat.Vecs[ckputil.TableObjectsAttr_ID_Idx], entry.ObjectStats[:], false, mp)
	packer.Reset()
	objType := ckputil.ObjectType_Data
	if entry.IsTombstone {
		objType = ckputil.ObjectType_Tombstone
	}
	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_ObjectType_Idx], objType, false, mp)
	ckputil.EncodeCluser(packer, entry.GetTable().ID, objType, entry.ID())
	vector.AppendBytes(bat.Vecs[ckputil.TableObjectsAttr_Cluster_Idx], packer.Bytes(), false, mp)
	if create {
		// if push {
		// 	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], commitTS, false, mp)
		// 	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp)
		// } else {
		vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.CreatedAt, false, mp)
		vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], types.TS{}, false, mp)
		// }
	} else {
		// if push {
		// 	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.DeletedAt, false, mp)
		// 	vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], commitTS, false, mp)
		// } else {
		vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_CreateTS_Idx], entry.DeletedAt, false, mp)
		vector.AppendFixed(bat.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], entry.CreatedAt, false, mp)
		// }
	}
	bat.SetRowCount(bat.Vecs[0].Length())
}
