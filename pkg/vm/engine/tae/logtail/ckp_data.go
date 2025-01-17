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
	mp       *mpool.MPool

	meta       containers.Vectors
	locations  map[string]objectio.Location
	maxBlkIDs  map[string]uint16
	objectInfo []containers.Vectors

	closeCB []func()
}

func NewCheckpointReplayer(location objectio.Location, mp *mpool.MPool) *CheckpointReplayer {
	return &CheckpointReplayer{
		location:   location,
		mp:         mp,
		locations:  make(map[string]objectio.Location),
		maxBlkIDs:  make(map[string]uint16),
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
	endRowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](&replayer.meta[ckputil.MetaAttr_End_Idx])
	for i := 0; i < locationVec.Length(); i++ {
		location := objectio.Location(locationVec.GetBytesAt(i))
		rowID := endRowIDs[i]
		_, blkOffset := rowID.BorrowBlockID().Offsets()
		str := location.String()
		replayer.locations[str] = location
		maxBlkID, ok := replayer.maxBlkIDs[str]
		if !ok {
			replayer.maxBlkIDs[str] = blkOffset
		} else {
			if maxBlkID < blkOffset {
				replayer.maxBlkIDs[str] = blkOffset
			}
		}
	}
	return
}

func (replayer *CheckpointReplayer) PrefetchData(sid string, fs fileservice.FileService) {
	for _, loc := range replayer.locations {
		ioutil.Prefetch(sid, fs, loc)
	}
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
			data.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx], srcObjectEntry.CreatedAt, false, mp,
		); err != nil {
			return
		}
	}
	data.SetRowCount(data.Vecs[0].Length())
	return
}
