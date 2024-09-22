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

package v2

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type ObjectEntry struct {
	commitTS types.TS
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

// GCTable is a data structure in memory after consuming checkpoint
type GCTable struct {
	sync.Mutex
	objects    map[string]*ObjectEntry
	tombstones map[string]*ObjectEntry
}

func NewGCTable() *GCTable {
	table := GCTable{
		objects:    make(map[string]*ObjectEntry),
		tombstones: make(map[string]*ObjectEntry),
	}
	return &table
}

func (t *GCTable) addObject(
	name string,
	objEntry *ObjectEntry,
	commitTS types.TS,
	objects map[string]*ObjectEntry,
) {
	t.Lock()
	defer t.Unlock()
	t.addObjectLocked(name, objEntry, commitTS, objects)
}

func (t *GCTable) addObjectLocked(
	name string,
	objEntry *ObjectEntry,
	commitTS types.TS,
	objects map[string]*ObjectEntry,
) {
	object := objects[name]
	if object == nil {
		objects[name] = objEntry
		return
	}
	objects[name] = objEntry
	if object.commitTS.LT(&commitTS) {
		objects[name].commitTS = commitTS
	}
}

// Merge can merge two GCTables
func (t *GCTable) Merge(GCTable *GCTable) {
	for name, entry := range GCTable.objects {
		t.addObject(name, entry, entry.commitTS, t.objects)
	}

	for name, entry := range GCTable.tombstones {
		t.addObject(name, entry, entry.commitTS, t.tombstones)
	}
}

func (t *GCTable) getObjects() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.objects
}

func (t *GCTable) getTombstones() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.tombstones
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(
	table *GCTable,
	ts types.TS,
	snapShotList map[uint32]containers.Vector,
	pitrs *logtail.PitrInfo,
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
	gc = t.objectsComparedAndDeleteLocked(t.objects, table.objects, meta, snapList, pitrs, ts)
	gc = append(gc, t.objectsComparedAndDeleteLocked(t.tombstones, table.tombstones, meta, snapList, pitrs, ts)...)
	return gc, snapList
}

func (t *GCTable) objectsComparedAndDeleteLocked(
	objects, comparedObjects map[string]*ObjectEntry,
	meta *logtail.SnapshotMeta,
	snapList map[uint32][]types.TS,
	pitrList *logtail.PitrInfo,
	ts types.TS,
) []string {
	gc := make([]string, 0)
	for name, entry := range objects {
		objectEntry := comparedObjects[name]
		tsList := meta.GetSnapshotListLocked(snapList, entry.table)
		pList := meta.GetPitrLocked(pitrList, entry.db, entry.table)
		if tsList == nil && pList.IsEmpty() {
			if objectEntry == nil && entry.commitTS.LT(&ts) {
				gc = append(gc, name)
				delete(t.objects, name)
			}
			continue
		}
		if objectEntry == nil && entry.commitTS.LT(&ts) && !isSnapshotRefers(entry, tsList, pList, name) {
			gc = append(gc, name)
			delete(t.objects, name)
		}
	}
	return gc
}

func isSnapshotRefers(obj *ObjectEntry, snapVec []types.TS, pitrVec types.TS, name string) bool {
	if len(snapVec) == 0 && len(pitrVec) == 0 {
		logutil.Info("[soft GC]Snapshot Refers", zap.String("name", name))
		return false
	}
	if obj.dropTS.IsEmpty() {
		return true
	}

	if !pitrVec.IsEmpty() {
		if obj.dropTS.GT(&pitrVec) {
			logutil.Info("[soft GC]Pitr Refers",
				zap.String("name", name),
				zap.String("snapTS", pitrVec.ToString()),
				zap.String("createTS", obj.createTS.ToString()),
				zap.String("dropTS", obj.dropTS.ToString()))
			return true
		}
	}

	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&obj.createTS) && snapTS.LT(&obj.dropTS) {
			logutil.Debug("[soft GC]Snapshot Refers",
				zap.String("name", name),
				zap.String("snapTS", snapTS.ToString()),
				zap.String("createTS", obj.createTS.ToString()),
				zap.String("dropTS", obj.dropTS.ToString()))
			return true
		} else if snapTS.LT(&obj.createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins := data.GetObjectBatchs()
	tombstoneIns := data.GetTombstoneObjectBatchs()
	t.Lock()
	defer t.Unlock()
	t.updateObjectListLocked(ins, t.objects)
	t.updateObjectListLocked(tombstoneIns, t.tombstones)
}

func (t *GCTable) updateObjectListLocked(ins *containers.Batch, objects map[string]*ObjectEntry) {
	insCommitTSVec := ins.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		commitTS := vector.GetFixedAtNoTypeCheck[types.TS](insCommitTSVec, i)
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			commitTS: commitTS,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](dbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](tid, i),
		}
		t.addObjectLocked(objectStats.ObjectName().String(), object, commitTS, objects)
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
func (t *GCTable) collectData() []*containers.Batch {
	bats := t.makeBatchWithGCTable()
	for i, attr := range BlockSchemaAttr {
		bats[ObjectList].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], common.DefaultAllocator))
	}
	for i, attr := range BlockSchemaAttr {
		bats[TombstoneList].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], common.DefaultAllocator))
	}
	for name, entry := range t.objects {
		bats[ObjectList].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
		bats[ObjectList].GetVectorByName(GCCreateTS).Append(entry.createTS, false)
		bats[ObjectList].GetVectorByName(GCDeleteTS).Append(entry.dropTS, false)
		bats[ObjectList].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
		bats[ObjectList].GetVectorByName(GCAttrTableId).Append(entry.table, false)
	}

	for name, entry := range t.tombstones {
		bats[TombstoneList].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
		bats[TombstoneList].GetVectorByName(GCCreateTS).Append(entry.createTS, false)
		bats[TombstoneList].GetVectorByName(GCDeleteTS).Append(entry.dropTS, false)
		bats[TombstoneList].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
		bats[TombstoneList].GetVectorByName(GCAttrTableId).Append(entry.table, false)
	}

	return bats
}

// SaveTable is to write data to s3
func (t *GCTable) SaveTable(start, end types.TS, fs *objectio.ObjectFS, files []string) ([]objectio.BlockObject, error) {
	bats := t.collectData()
	defer t.closeBatch(bats)
	name := blockio.EncodeCheckpointMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, name, fs.Service)
	if err != nil {
		return nil, err
	}
	for i := range bats {
		if _, err := writer.WriteWithoutSeqnum(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.WriteEnd(context.Background())
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
			stats := writer.GetObjectStats()
			size = stats.OriginSize()
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
		commitTS := bats[idx].GetVectorByName(GCAttrCommitTS).Get(i).(types.TS)
		tid := bats[idx].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		if t.objects[name] != nil {
			continue
		}
		object := &ObjectEntry{
			createTS: creatTS,
			dropTS:   deleteTS,
			commitTS: commitTS,
			table:    tid,
		}
		t.addObjectLocked(name, object, commitTS, objects)
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
	t.rebuildTable(bats, TombstoneList, t.tombstones)
	t.Unlock()
	return nil
}

// For test

func (t *GCTable) Compare(table *GCTable) bool {
	if !t.compareObjects(t.objects, table.objects) {
		logutil.Infof("objects are not equal")
		return false
	}
	if !t.compareObjects(t.tombstones, table.tombstones) {
		logutil.Infof("tombstones are not equal")
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
