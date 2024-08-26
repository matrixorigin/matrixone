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

package v1

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
	"sync"
	"time"

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

type TombstoneEntry struct {
	objects  map[string]struct{}
	commitTS types.TS
}

// GCTable is a data structure in memory after consuming checkpoint
type GCTable struct {
	sync.Mutex
	objects    map[string]*ObjectEntry
	tombstones map[string]*TombstoneEntry
}

func NewGCTable() *GCTable {
	table := GCTable{
		objects:    make(map[string]*ObjectEntry),
		tombstones: make(map[string]*TombstoneEntry),
	}
	return &table
}

func (t *GCTable) addObject(name string, objEntry *ObjectEntry, commitTS types.TS) {
	t.Lock()
	defer t.Unlock()
	object := t.objects[name]
	if object == nil {
		t.objects[name] = objEntry
		return
	}
	t.objects[name] = objEntry
	if object.commitTS.Less(&commitTS) {
		t.objects[name].commitTS = commitTS
	}
}

func (t *GCTable) addTombstone(name, objectName string, commitTS types.TS) {
	t.Lock()
	defer t.Unlock()
	tombstone := t.tombstones[name]
	if tombstone == nil {
		t.tombstones[name] = &TombstoneEntry{
			objects:  make(map[string]struct{}),
			commitTS: commitTS,
		}
	}
	if t.tombstones[name].commitTS.Less(&commitTS) {
		t.tombstones[name].commitTS = commitTS
	}
	if _, ok := t.tombstones[name].objects[objectName]; !ok {
		t.tombstones[name].objects[objectName] = struct{}{}
	}
}

func (t *GCTable) deleteObject(name string) {
	t.Lock()
	defer t.Unlock()
	delete(t.objects, name)
}

func (t *GCTable) deleteTombstone(name string) {
	t.Lock()
	defer t.Unlock()
	for obj := range t.tombstones[name].objects {
		delete(t.tombstones[name].objects, obj)
	}
	delete(t.tombstones, name)
}

// Merge can merge two GCTables
func (t *GCTable) Merge(GCTable *GCTable) {
	for name, entry := range GCTable.objects {
		t.addObject(name, entry, entry.commitTS)
	}

	for name, entry := range GCTable.tombstones {
		for obj := range entry.objects {
			t.addTombstone(name, obj, entry.commitTS)
		}
	}
}

func (t *GCTable) getObjects() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.objects
}

func (t *GCTable) getTombstones() map[string]*TombstoneEntry {
	t.Lock()
	defer t.Unlock()
	return t.tombstones
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(
	table *GCTable,
	ts types.TS,
	snapShotList map[uint32]containers.Vector,
	meta *logtail.SnapshotMeta,
) ([]string, map[uint32][]types.TS) {
	gc := make([]string, 0)
	snapList := make(map[uint32][]types.TS)
	objects := t.getObjects()
	for acct, snap := range snapShotList {
		snapList[acct] = vector.MustFixedCol[types.TS](snap.GetDownstreamVector())
	}
	for name, entry := range objects {
		objectEntry := table.objects[name]
		tsList := meta.GetSnapshotList(snapList, entry.table)
		if tsList == nil {
			if objectEntry == nil && entry.commitTS.Less(&ts) {
				gc = append(gc, name)
				t.deleteObject(name)
			}
			continue
		}
		if objectEntry == nil && entry.commitTS.Less(&ts) && !isSnapshotRefers(entry, tsList, name) {
			gc = append(gc, name)
			t.deleteObject(name)
		}
	}

	objects = t.getObjects()
	tombstones := t.getTombstones()
	for name, tombstone := range tombstones {
		ok := true
		sameName := false
		if tombstone.commitTS.GreaterEq(&ts) {
			continue
		}
		for obj := range tombstone.objects {
			if obj == name {
				// tombstone for aObject is same as aObject, skip it
				sameName = true
			}
			objectEntry := objects[obj]
			if objectEntry != nil {
				// TODO: remove log
				logutil.Debug("[soft GC] Refers object",
					zap.String("tombstone", name),
					zap.String("obj", obj),
					zap.Bool("objectEntry", objectEntry != nil))
				ok = false
				break
			}
		}
		if ok {
			if !sameName {
				gc = append(gc, name)
			}
			// TODO: remove log
			logutil.Debug("[soft GC] Delete tombstone",
				zap.String("tombstone", name),
				zap.Int("tombstone", len(t.tombstones)),
				zap.Int("object", len(tombstone.objects)),
				zap.Bool("same", sameName),
				zap.Int("data", len(objects)))
			t.deleteTombstone(name)
		}
	}
	return gc, snapList
}

func isSnapshotRefers(obj *ObjectEntry, snapVec []types.TS, name string) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&obj.createTS) && (obj.dropTS.IsEmpty() || snapTS.Less(&obj.dropTS)) {
			logutil.Debug("[soft GC]Snapshot Refers", zap.String("name", name), zap.String("snapTS", snapTS.ToString()),
				zap.String("createTS", obj.createTS.ToString()), zap.String("dropTS", obj.dropTS.ToString()))
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
	insCommitTSVec := ins.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		commitTS := vector.GetFixedAt[types.TS](insCommitTSVec, i)
		deleteTS := vector.GetFixedAt[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			commitTS: commitTS,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAt[uint64](dbid, i),
			table:    vector.GetFixedAt[uint64](tid, i),
		}
		t.addObject(objectStats.ObjectName().String(), object, commitTS)
	}

	// tombstone, _, _, _ := data.GetBlkBatchs()
	// tombstoneBlockIDVec := vector.MustFixedCol[types.Blockid](tombstone.GetVectorByName(catalog2.BlockMeta_ID).GetDownstreamVector())
	// tombstoneCommitTSVec := vector.MustFixedCol[types.TS](tombstone.GetVectorByName(catalog2.BlockMeta_CommitTs).GetDownstreamVector())
	// for i := 0; i < tombstone.Length(); i++ {
	// 	blockID := tombstoneBlockIDVec[i]
	// 	commitTS := tombstoneCommitTSVec[i]
	// 	deltaLoc := objectio.Location(tombstone.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
	// 	t.addTombstone(deltaLoc.Name().String(), blockID.ObjectNameString(), commitTS)
	// }
}

func (t *GCTable) makeBatchWithGCTable() []*containers.Batch {
	bats := make([]*containers.Batch, 3)
	bats[Versions] = containers.NewBatch()
	bats[ObjectList] = containers.NewBatch()
	bats[TombstoneList] = containers.NewBatch()
	return bats
}

func (t *GCTable) makeBatchWithGCTableV2() []*containers.Batch {
	bats := make([]*containers.Batch, 1)
	bats[CreateBlock] = containers.NewBatch()
	return bats
}

func (t *GCTable) makeBatchWithGCTableV1() []*containers.Batch {
	bats := make([]*containers.Batch, 2)
	bats[CreateBlock] = containers.NewBatch()
	bats[DeleteBlock] = containers.NewBatch()
	return bats
}

func (t *GCTable) closeBatch(bs []*containers.Batch) {
	for i := range bs {
		bs[i].Close()
	}
}

// collectData collects data from memory that can be written to s3
func (t *GCTable) collectData(files []string) []*containers.Batch {
	bats := t.makeBatchWithGCTable()
	for i, attr := range BlockSchemaAttr {
		bats[ObjectList].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], common.DefaultAllocator))
	}
	for i, attr := range TombstoneSchemaAttr {
		bats[TombstoneList].AddVector(attr, containers.MakeVector(TombstoneSchemaTypes[i], common.DefaultAllocator))
	}
	for i, attr := range VersionsSchemaAttr {
		bats[Versions].AddVector(attr, containers.MakeVector(VersionsSchemaTypes[i], common.DefaultAllocator))
	}
	bats[Versions].GetVectorByName(GCAttrVersion).Append(CurrentVersion, false)
	for name, entry := range t.objects {
		bats[ObjectList].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
		bats[ObjectList].GetVectorByName(GCCreateTS).Append(entry.createTS, false)
		bats[ObjectList].GetVectorByName(GCDeleteTS).Append(entry.dropTS, false)
		bats[ObjectList].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
		bats[ObjectList].GetVectorByName(GCAttrTableId).Append(entry.table, false)
	}

	for name, entry := range t.tombstones {
		for obj := range entry.objects {
			bats[TombstoneList].GetVectorByName(GCAttrObjectName).Append([]byte(obj), false)
			bats[TombstoneList].GetVectorByName(GCAttrTombstone).Append([]byte(name), false)
			bats[TombstoneList].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
		}
	}

	return bats
}

// SaveTable is to write data to s3
func (t *GCTable) SaveTable(start, end types.TS, fs *objectio.ObjectFS, files []string) ([]objectio.BlockObject, error) {
	bats := t.collectData(files)
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
			size = writer.GetObjectStats()[0].OriginSize()
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
	bats = t.collectData(files)
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

func (t *GCTable) rebuildTableV3(bats []*containers.Batch) {
	t.rebuildTableV2(bats, ObjectList)
	for i := 0; i < bats[TombstoneList].Length(); i++ {
		name := string(bats[TombstoneList].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		tombstone := string(bats[TombstoneList].GetVectorByName(GCAttrTombstone).Get(i).([]byte))
		commitTS := bats[TombstoneList].GetVectorByName(GCAttrCommitTS).Get(i).(types.TS)
		t.addTombstone(tombstone, name, commitTS)
	}
}

func (t *GCTable) rebuildTableV2(bats []*containers.Batch, idx BatchType) {
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
		t.addObject(name, object, commitTS)
	}
}

func (t *GCTable) rebuildTable(bats []*containers.Batch, ts types.TS) {
	for i := 0; i < bats[CreateBlock].Length(); i++ {
		name := string(bats[CreateBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		if t.objects[name] != nil {
			continue
		}
		object := &ObjectEntry{
			createTS: ts,
			commitTS: ts,
		}
		t.addObject(name, object, ts)
	}
	for i := 0; i < bats[DeleteBlock].Length(); i++ {
		name := string(bats[DeleteBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		if t.objects[name] == nil {
			logutil.Fatalf("delete object should not be nil")
		}
		object := &ObjectEntry{
			dropTS:   ts,
			commitTS: ts,
		}
		t.addObject(name, object, ts)
	}
}

func (t *GCTable) replayData(ctx context.Context,
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
	var release1, release2, release3 func()
	defer func() {
		if release1 != nil {
			release1()
		}
		if release2 != nil {
			release2()
		}
		if release3 != nil {
			release3()
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
	if len(bs) == 3 {
		bats := t.makeBatchWithGCTable()
		defer t.closeBatch(bats)
		release1, err = t.replayData(ctx, Versions, VersionsSchemaAttr, VersionsSchemaTypes, bats, bs, reader)
		if err != nil {
			return err
		}
		version := getVersion(bats[Versions])
		if version != CurrentVersion {
			panic(fmt.Sprintf("version %d is not supported", version))
		}
		release2, err = t.replayData(ctx, ObjectList, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
		if err != nil {
			return err
		}
		release3, err = t.replayData(ctx, TombstoneList, TombstoneSchemaAttr, TombstoneSchemaTypes, bats, bs, reader)
		if err != nil {
			return err
		}
		t.rebuildTableV3(bats)
		return nil
	}
	if len(bs) == 1 {
		bats := t.makeBatchWithGCTableV2()
		defer t.closeBatch(bats)
		release1, err = t.replayData(ctx, CreateBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
		if err != nil {
			return err
		}
		t.rebuildTableV2(bats, CreateBlock)
		return nil
	}
	bats := t.makeBatchWithGCTableV1()
	defer t.closeBatch(bats)
	release2, err = t.replayData(ctx, CreateBlock, BlockSchemaAttrV1, BlockSchemaTypesV1, bats, bs, reader)
	if err != nil {
		return err
	}
	release3, err = t.replayData(ctx, DeleteBlock, BlockSchemaAttrV1, BlockSchemaTypesV1, bats, bs, reader)
	if err != nil {
		return err
	}
	t.rebuildTable(bats, ts)
	return nil
}

func getVersion(bat *containers.Batch) uint16 {
	return vector.GetFixedAt[uint16](bat.GetVectorByName(GCAttrVersion).GetDownstreamVector(), 0)
}

// For test
func (t *GCTable) Compare(table *GCTable) bool {
	for name, entry := range table.objects {
		object := t.objects[name]
		if object == nil {
			logutil.Infof("object %s is nil, create %v, drop %v", name, entry.createTS.ToString(), entry.dropTS.ToString())
			return false
		}
		if !entry.commitTS.Equal(&object.commitTS) {
			logutil.Infof("object %s commitTS is not equal", name)
			return false
		}
	}

	return len(t.objects) == len(table.objects)
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
	return w.String()
}
