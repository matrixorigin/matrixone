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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"sync"

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
	fileIterm map[int][]uint32
}

// GCTable is a data structure in memory after consuming checkpoint
type GCTable struct {
	sync.Mutex
	objects map[string]*ObjectEntry
}

func NewGCTable() *GCTable {
	table := GCTable{
		objects: make(map[string]*ObjectEntry),
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

func (t *GCTable) addObjectForSnapshot(name string, objEntry *ObjectEntry, commitTS types.TS, num int, row uint32) {
	t.Lock()
	defer t.Unlock()
	object := t.objects[name]
	if object == nil {
		t.objects[name] = objEntry
		objEntry.fileIterm = make(map[int][]uint32)
		objEntry.fileIterm[num] = append(objEntry.fileIterm[num], row)
		return
	}
	t.objects[name] = objEntry
	if object.commitTS.Less(&commitTS) {
		t.objects[name].commitTS = commitTS
	}
	if t.objects[name].fileIterm == nil {
		objEntry.fileIterm = make(map[int][]uint32)
	}
	objEntry.fileIterm[num] = append(objEntry.fileIterm[num], row)
}

func (t *GCTable) deleteObject(name string) {
	t.Lock()
	defer t.Unlock()
	delete(t.objects, name)
}

// Merge can merge two GCTables
func (t *GCTable) Merge(GCTable *GCTable) {
	for name, entry := range GCTable.objects {
		t.addObject(name, entry, entry.commitTS)
	}
}

func (t *GCTable) getObjects() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.objects
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(table *GCTable, ts types.TS, snapShotList []types.TS) []string {
	gc := make([]string, 0)
	objects := t.getObjects()
	for _, snap := range snapShotList {
		logutil.Infof("SoftGC: snap %v", snap.ToString())
	}
	for name, entry := range objects {
		objectEntry := table.objects[name]
		if objectEntry == nil && entry.commitTS.Less(&ts) && !isSnapshotRefers(entry, snapShotList) {
			gc = append(gc, name)
			t.deleteObject(name)
		}
	}
	return gc
}

func isSnapshotRefers(obj *ObjectEntry, snapShotList []types.TS) bool {
	if len(snapShotList) == 0 {
		return false
	}
	left, right := 0, len(snapShotList)-1
	for left <= right {
		mid := left + (right-left)/2
		if snapShotList[mid].GreaterEq(&obj.createTS) && snapShotList[mid].Less(&obj.dropTS) {
			logutil.Infof("isSnapshotRefers: %s, create %v, drop %v", snapShotList[mid].ToString(), obj.createTS.ToString(), obj.dropTS.ToString())
			return false
		} else if snapShotList[mid].Less(&obj.createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return true
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
}

func (t *GCTable) UpdateTableForSnapshot(data *logtail.CheckpointData, num int) {
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
		t.addObjectForSnapshot(objectStats.ObjectName().String(), object, commitTS, num, uint32(i))

	}
}

func (t *GCTable) makeBatchWithGCTable() []*containers.Batch {
	bats := make([]*containers.Batch, 1)
	bats[CreateBlock] = containers.NewBatch()
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
		bats[CreateBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], common.CheckpointAllocator))
	}
	for name, entry := range t.objects {
		bats[CreateBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
		bats[CreateBlock].GetVectorByName(GCCreateTS).Append(entry.createTS, false)
		bats[CreateBlock].GetVectorByName(GCDeleteTS).Append(entry.dropTS, false)
		bats[CreateBlock].GetVectorByName(GCAttrCommitTS).Append(entry.commitTS, false)
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
	bats := t.collectData(files)
	defer t.closeBatch(bats)
	name := blockio.EncodeGCMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
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

func (t *GCTable) rebuildTableV2(bats []*containers.Batch) {
	for i := 0; i < bats[CreateBlock].Length(); i++ {
		name := string(bats[CreateBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		creatTS := bats[CreateBlock].GetVectorByName(GCCreateTS).Get(i).(types.TS)
		deleteTS := bats[CreateBlock].GetVectorByName(GCDeleteTS).Get(i).(types.TS)
		commitTS := bats[CreateBlock].GetVectorByName(GCAttrCommitTS).Get(i).(types.TS)
		if t.objects[name] != nil {
			continue
		}
		object := &ObjectEntry{
			createTS: creatTS,
			dropTS:   deleteTS,
			commitTS: commitTS,
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
		if t.objects[name] != nil {
			logutil.Fatalf("delete object should not be nil")
		}
		object := &ObjectEntry{
			dropTS: ts,
			commitTS: ts,
		}
		t.addObject(name,object,  ts)
	}
}

func (t *GCTable) replayData(ctx context.Context,
	typ BatchType,
	attrs []string,
	types []types.Type,
	bats []*containers.Batch,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) error {
	idxes := make([]uint16, len(attrs))
	for i := range attrs {
		idxes[i] = uint16(i)
	}
	mobat, _, err := reader.LoadColumns(ctx, idxes, nil, bs[typ].GetID(), common.DefaultAllocator)
	if err != nil {
		return err
	}
	for i := range attrs {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i], common.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(pkgVec, common.CheckpointAllocator)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return nil
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS, ts types.TS) error {
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
	err = t.replayData(ctx, CreateBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	if len(bats) == 1 {
		t.rebuildTableV2(bats)
		return nil
	}
	err = t.replayData(ctx, DeleteBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	t.rebuildTable(bats, ts)
	return nil
}

// For test
func (t *GCTable) Compare(table *GCTable) bool {
	if len(t.objects) != len(table.objects) {
		return false
	}
	for name, entry := range t.objects {
		object := table.objects[name]
		if object == nil {
			return false
		}
		if !entry.commitTS.Equal(&object.commitTS) {
			return false
		}
	}
	return true
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
