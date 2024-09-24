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

package gc

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type ObjectEntry struct {
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

func NewGCTable() *GCTable {
	table := GCTable{
		objects: make(map[string]*ObjectEntry),
	}
	return &table
}

type GCTable struct {
	sync.Mutex
	objects map[string]*ObjectEntry
	mp      *mpool.MPool
}

func (t *GCTable) addObjectLocked(
	name string,
	objEntry *ObjectEntry,
	objects map[string]*ObjectEntry,
) {
	object := objects[name]
	if object == nil {
		objects[name] = objEntry
		return
	}
	objects[name] = objEntry
}

func (t *GCTable) Merge(_ *GCTable) {

}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(
	table *GCTable,
	ts types.TS,
	snapShotList map[uint32]containers.Vector,
	meta *logtail.SnapshotMeta,
) ([]string, map[uint32][]types.TS) {
	var gc []string
	snapList := make(map[uint32][]types.TS)
	for acct, snap := range snapShotList {
		snapList[acct] = vector.MustFixedColWithTypeCheck[types.TS](snap.GetDownstreamVector())
	}
	t.Lock()
	meta.Lock()
	defer func() {
		meta.Unlock()
		t.Unlock()
	}()
	gc = t.objectsComparedAndDeleteLocked(t.objects, table.objects, meta, snapList, ts)
	return gc, snapList
}

func (t *GCTable) objectsComparedAndDeleteLocked(
	objects, comparedObjects map[string]*ObjectEntry,
	meta *logtail.SnapshotMeta,
	snapList map[uint32][]types.TS,
	ts types.TS,
) []string {
	gc := make([]string, 0)
	for name, entry := range objects {
		objectEntry := comparedObjects[name]
		tsList := meta.GetSnapshotListLocked(snapList, entry.table)
		if tsList == nil {
			if objectEntry == nil &&
				(entry.createTS.Less(&ts) && entry.dropTS.Less(&ts)) {
				gc = append(gc, name)
				delete(t.objects, name)
			}
			continue
		}
		if objectEntry == nil &&
			entry.createTS.Less(&ts) &&
			entry.dropTS.Less(&ts) &&
			!isSnapshotRefers(entry, tsList, name) {
			gc = append(gc, name)
			delete(t.objects, name)
		}
	}
	return gc
}

func isSnapshotRefers(obj *ObjectEntry, snapVec []types.TS, name string) bool {
	if len(snapVec) == 0 {
		return false
	}
	if obj.dropTS.IsEmpty() {
		logutil.Debug("[soft GC]Snapshot Refers",
			zap.String("name", name),
			zap.String("snapTS", snapVec[0].ToString()),
			zap.String("createTS", obj.createTS.ToString()),
			zap.String("dropTS", obj.dropTS.ToString()))
		return true
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&obj.createTS) && snapTS.Less(&obj.dropTS) {
			logutil.Debug("[soft GC]Snapshot Refers",
				zap.String("name", name),
				zap.String("snapTS", snapTS.ToString()),
				zap.String("createTS", obj.createTS.ToString()),
				zap.String("dropTS", obj.dropTS.ToString()))
			return true
		} else if snapTS.Less(&obj.createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins := data.GetObjectBatchs()
	t.Lock()
	defer t.Unlock()
	t.updateObjectListLocked(ins, t.objects)
}

func (t *GCTable) updateObjectListLocked(ins *containers.Batch, objects map[string]*ObjectEntry) {
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](dbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](tid, i),
		}
		t.addObjectLocked(objectStats.ObjectName().String(), object, objects)
	}
}

func (t *GCTable) makeBatchWithGCTable() []*containers.Batch {
	bats := make([]*containers.Batch, 2)
	bats[ObjectList] = containers.NewBatch()
	bats[TombstoneList] = containers.NewBatch()
	return bats
}

func (t *GCTable) closeBatch(bs []*containers.Batch) {
	for i := range bs {
		bs[i].Close()
	}
}

// collectData collects data from memory that can be written to s3
func (t *GCTable) collectData() *batch.Batch {
	bat := NewObjectTableBatch()
	for name, entry := range t.objects {
		addObjectToBatch(bat, name, entry.createTS, entry.dropTS, entry.db, entry.table, t.mp)
	}
	return bat
}

// SaveTable is to write data to s3
func (t *GCTable) SaveTable(start, end types.TS, fs *objectio.ObjectFS, files []string) ([]objectio.ObjectStats, error) {
	bat := t.collectData()
	defer bat.Clean(t.mp)
	factory := engine_util.NewFSinkerImplFactory(
		ObjectTableSeqnums,
		ObjectTablePrimaryKeyIdx,
		true,
		false,
		ObjectTableVersion,
	)
	sinker := engine_util.NewSinker(ObjectTablePrimaryKeyIdx, ObjectTableAttrs, ObjectTableTypes, factory, t.mp, fs.Service)
	err := sinker.Write(context.Background(), bat)
	if err != nil {
		return nil, err
	}
	err = sinker.Sync(context.Background())
	if err != nil {
		return nil, err
	}
	blocks, _ := sinker.GetResult()

	name := blockio.EncodeCheckpointMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	ret := batch.New(false, ObjectTableMetaAttrs)
	ret.SetVector(0, vector.NewVec(ObjectTableMetaTypes[0]))
	for _, block := range blocks {
		vector.AppendBytes(ret.GetVector(0), block[:], false, t.mp)
	}
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs.Service)
	if err != nil {
		return nil, err
	}
	if _, err := writer.WriteWithoutSeqnum(ret); err != nil {
		return nil, err
	}

	_, err = writer.WriteEnd(context.Background())
	return blocks, err
}

// SaveFullTable is to write data to s3
func (t *GCTable) SaveFullTable(start, end types.TS, fs *objectio.ObjectFS, files []string) ([]objectio.BlockObject, error) {
	now := time.Now()
	var bats []*containers.Batch
	var blocks []objectio.BlockObject
	var err error
	var writer *objectio.ObjectWriter
	var collectCost, writeCost time.Duration
	logutil.Info("[DiskCleaner]", zap.String("op", "SaveFullTable-Start"),
		zap.String("max consumed :", start.ToString()+"-"+end.ToString()))
	defer func() {
		size := uint32(0)
		objectCount := 0
		tombstoneCount := 0
		if len(blocks) > 0 && err == nil {
			ss := writer.GetObjectStats()
			size = ss.OriginSize()
		}
		if bats != nil {
			objectCount = bats[ObjectList].Length()
			tombstoneCount = bats[TombstoneList].Length()
			t.closeBatch(bats)
		}
		logutil.Info("[DiskCleaner]", zap.String("op", "SaveFullTable-End"),
			zap.String("collect cost :", collectCost.String()),
			zap.String("write cost :", writeCost.String()),
			zap.Uint32("gc table size :", size),
			zap.Int("object count :", objectCount),
			zap.Int("tombstone count :", tombstoneCount))
	}()
	bats = t.collectData()
	collectCost = time.Since(now)
	now = time.Now()
	name := blockio.EncodeGCMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	writer, err = objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs.Service)
	if err != nil {
		return nil, err
	}
	for i := range bats {
		if _, err := writer.WriteWithoutSeqnum(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err = writer.WriteEnd(context.Background())
	writeCost = time.Since(now)
	return blocks, err
}

func (t *GCTable) rebuildTable(bats []*containers.Batch, idx BatchType, objects map[string]*ObjectEntry) {
	for i := 0; i < bats[idx].Length(); i++ {
		name := string(bats[idx].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		creatTS := bats[idx].GetVectorByName(GCCreateTS).Get(i).(types.TS)
		deleteTS := bats[idx].GetVectorByName(GCDeleteTS).Get(i).(types.TS)
		tid := bats[idx].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		if t.objects[name] != nil {
			continue
		}
		object := &ObjectEntry{
			createTS: creatTS,
			dropTS:   deleteTS,
			table:    tid,
		}
		t.addObjectLocked(name, object, objects)
	}
}

func (t *GCTable) replayData(
	ctx context.Context,
	typ BatchType,
	attrs []string,
	types []types.Type,
	bats []*containers.Batch,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) (func(), error) {
	idxes := make([]uint16, len(attrs))
	for i := range attrs {
		idxes[i] = uint16(i)
	}
	mobat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[typ].GetID(), common.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	for i := range attrs {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i], common.DefaultAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.DefaultAllocator)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return release, nil
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS, ts types.TS) error {
	var release1, release2 func()
	defer func() {
		if release1 != nil {
			release1()
		}
		if release2 != nil {
			release2()
		}
	}()
	reader, err := blockio.NewFileReaderNoCache(fs.Service, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DefaultAllocator)
	if err != nil {
		return err
	}
	bats := t.makeBatchWithGCTable()
	defer t.closeBatch(bats)
	release1, err = t.replayData(ctx, ObjectList, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	release2, err = t.replayData(ctx, TombstoneList, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	t.Lock()
	t.rebuildTable(bats, ObjectList, t.objects)
	t.Unlock()
	return nil
}

// For test

func (t *GCTable) Compare(table *GCTable) bool {
	if !t.compareObjects(t.objects, table.objects) {
		logutil.Infof("objects are not equal")
		return false
	}
	return true
}

func (t *GCTable) compareObjects(objects, compareObjects map[string]*ObjectEntry) bool {
	for name, entry := range compareObjects {
		object := objects[name]
		if object == nil {
			logutil.Infof("object %s is nil, create %v, drop %v",
				name, entry.createTS.ToString(), entry.dropTS.ToString())
			return false
		}
		if !entry.commitTS.Equal(&object.commitTS) {
			logutil.Infof("object %s commitTS is not equal", name)
			return false
		}
	}

	return len(compareObjects) == len(objects)
}

func (t *GCTable) String() string {
	if len(t.objects) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("objects:[\n")
	for name, entry := range t.objects {
		_, _ = w.WriteString(fmt.Sprintf("name: %s, commitTS: %v ", name, entry.commitTS.ToString()))
	}
	_, _ = w.WriteString("]\n")
	_, _ = w.WriteString("tombstones:[\n")
	for name, entry := range t.tombstones {
		_, _ = w.WriteString(fmt.Sprintf("name: %s, commitTS: %v ", name, entry.commitTS.ToString()))
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
