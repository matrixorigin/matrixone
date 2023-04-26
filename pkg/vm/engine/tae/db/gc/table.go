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
	"sync"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// GCTable is a data structure in memory after consuming checkpoint
type GCTable struct {
	sync.Mutex
	dbs map[uint64]*dropDB
}

func NewGCTable() *GCTable {
	table := GCTable{
		dbs: make(map[uint64]*dropDB),
	}
	return &table
}

func (t *GCTable) addBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	db := t.dbs[id.DbID]
	if db == nil {
		db = NewDropDB(id.DbID)
	}
	db.addBlock(id, name)
	t.dbs[id.DbID] = db
}

func (t *GCTable) deleteBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	db := t.dbs[id.DbID]
	if db == nil {
		db = NewDropDB(id.DbID)
	}
	db.deleteBlock(id, name)
	t.dbs[id.DbID] = db
}

// Merge can merge two GCTables
func (t *GCTable) Merge(GCTable *GCTable) {
	for did, entry := range GCTable.dbs {
		db := t.dbs[did]
		if db == nil {
			db = NewDropDB(did)
		}
		db.merge(entry)
		if !db.drop {
			db.drop = entry.drop
		}
		t.dbs[did] = db
	}
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC() []string {
	gc := make([]string, 0)
	for id, db := range t.dbs {
		if t.dbs[id] == nil {
			panic(any("error"))
		}
		objects := db.softGC()
		gc = append(gc, objects...)
	}
	return gc
}

func (t *GCTable) dropDB(id common.ID) {
	t.Lock()
	defer t.Unlock()
	db := t.dbs[id.DbID]
	if db == nil {
		db = NewDropDB(id.DbID)
	}
	db.drop = true
	t.dbs[id.DbID] = db
}

func (t *GCTable) dropTable(id common.ID) {
	t.Lock()
	defer t.Unlock()
	db := t.dbs[id.DbID]
	if db == nil {
		db = NewDropDB(id.DbID)
	}
	db.DropTable(id)
	t.dbs[id.DbID] = db
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins, insTxn, del, delTxn := data.GetBlkBatchs()
	for i := 0; i < ins.Length(); i++ {
		dbid := insTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := insTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		blkID := ins.GetVectorByName(pkgcatalog.BlockMeta_ID).Get(i).(types.Blockid)
		metaLoc := objectio.Location(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		id := common.ID{
			DbID:    dbid,
			TableID: tid,
			BlockID: blkID,
		}
		t.addBlock(id, metaLoc.Name().String())
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		blkID := del.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
		metaLoc := objectio.Location(delTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))

		id := common.ID{
			TableID: tid,
			BlockID: *blkID.GetBlockid(),
			DbID:    dbid,
		}
		t.deleteBlock(id, metaLoc.Name().String())
	}
	_, _, _, del, delTxn = data.GetTblBatchs()
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		id := common.ID{
			DbID:    dbid,
			TableID: tid,
		}
		t.dropTable(id)
	}

	_, _, del, delTxn = data.GetDBBatchs()
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		id := common.ID{
			DbID: dbid,
		}
		t.dropDB(id)
	}
}

func (t *GCTable) rebuildTable(bats []*containers.Batch) {
	files := make(map[string]bool)
	for i := 0; i < bats[DeleteFile].Length(); i++ {
		name := string(bats[DeleteFile].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		files[name] = true
	}
	for i := 0; i < bats[CreateBlock].Length(); i++ {
		dbid := bats[CreateBlock].GetVectorByName(GCAttrDBId).Get(i).(uint64)
		tid := bats[CreateBlock].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		blkID := bats[CreateBlock].GetVectorByName(GCAttrBlockId).Get(i).(types.Blockid)
		name := string(bats[CreateBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		if files[name] {
			continue
		}
		id := common.ID{
			TableID: tid,
			BlockID: blkID,
			DbID:    dbid,
		}
		t.addBlock(id, name)
	}
	for i := 0; i < bats[DeleteBlock].Length(); i++ {
		dbid := bats[DeleteBlock].GetVectorByName(GCAttrDBId).Get(i).(uint64)
		tid := bats[DeleteBlock].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		blkID := bats[DeleteBlock].GetVectorByName(GCAttrBlockId).Get(i).(types.Blockid)
		name := string(bats[DeleteBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		if files[name] {
			continue
		}
		id := common.ID{
			TableID: tid,
			BlockID: blkID,
			DbID:    dbid,
		}
		t.deleteBlock(id, name)
	}
	for i := 0; i < bats[DropTable].Length(); i++ {
		dbid := bats[DropTable].GetVectorByName(GCAttrDBId).Get(i).(uint64)
		tid := bats[DropTable].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		id := common.ID{
			TableID: tid,
			DbID:    dbid,
		}
		t.dropTable(id)
	}
	for i := 0; i < bats[DropDB].Length(); i++ {
		dbid := bats[DropDB].GetVectorByName(GCAttrDBId).Get(i).(uint64)
		id := common.ID{
			DbID: dbid,
		}
		t.dropDB(id)
	}
}

func (t *GCTable) makeBatchWithGCTable() []*containers.Batch {
	bats := make([]*containers.Batch, 5)
	bats[CreateBlock] = containers.NewBatch()
	bats[DeleteBlock] = containers.NewBatch()
	bats[DropTable] = containers.NewBatch()
	bats[DropDB] = containers.NewBatch()
	bats[DeleteFile] = containers.NewBatch()
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
		bats[CreateBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i]))
		bats[DeleteBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i]))
	}
	for i, attr := range DropTableSchemaAttr {
		bats[DropTable].AddVector(attr, containers.MakeVector(DropTableSchemaTypes[i]))
	}
	for i, attr := range DropDBSchemaAtt {
		bats[DropDB].AddVector(attr, containers.MakeVector(DropDBSchemaTypes[i]))
	}
	for i, attr := range DeleteFileSchemaAtt {
		bats[DeleteFile].AddVector(attr, containers.MakeVector(DeleteFileSchemaTypes[i]))
	}
	for did, entry := range t.dbs {
		if entry.drop {
			bats[DropDB].GetVectorByName(GCAttrDBId).Append(did, false)
		}
		for tid, table := range entry.tables {
			if table.drop {
				bats[DropTable].GetVectorByName(GCAttrTableId).Append(tid, false)
				bats[DropTable].GetVectorByName(GCAttrDBId).Append(did, false)
			}

			for name, obj := range table.object {
				for _, block := range obj.table.blocks {
					bats[CreateBlock].GetVectorByName(GCAttrBlockId).Append(block.BlockID, false)
					bats[CreateBlock].GetVectorByName(GCAttrTableId).Append(block.TableID, false)
					bats[CreateBlock].GetVectorByName(GCAttrDBId).Append(block.DbID, false)
					bats[CreateBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
				}
				for _, block := range obj.table.delete {
					bats[DeleteBlock].GetVectorByName(GCAttrBlockId).Append(block.BlockID, false)
					bats[DeleteBlock].GetVectorByName(GCAttrTableId).Append(block.TableID, false)
					bats[DeleteBlock].GetVectorByName(GCAttrDBId).Append(block.DbID, false)
					bats[DeleteBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
				}
			}
		}
	}

	for _, name := range files {
		bats[DeleteFile].GetVectorByName(GCAttrObjectName).Append([]byte(name), false)
	}
	return bats
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
	mobat, err := reader.LoadColumns(ctx, idxes, bs[typ].GetID(), common.DefaultAllocator)
	if err != nil {
		return err
	}
	for i := range attrs {
		pkgVec := mobat.Vecs[i]
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i])
		} else {
			vec = containers.ToDNVector(pkgVec)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return nil
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
		if _, err := writer.Write(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.WriteEnd(context.Background())
	//logutil.Infof("SaveTable %v-%v, table: %v, gc: %v", start.ToString(), end.ToString(), t.String(), files)
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
		if _, err := writer.Write(containers.ToCNBatch(bats[i])); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.WriteEnd(context.Background())
	//logutil.Infof("SaveTable %v-%v, table: %v, gc: %v", start.ToString(), end.ToString(), t.String(), files)
	return blocks, err
}

func (t *GCTable) Prefetch(ctx context.Context, name string, size int64, fs *objectio.ObjectFS) error {
	return blockio.PrefetchFile(fs.Service, name)
}

// ReadTable reads an s3 file and replays a GCTable in memory
func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS) error {
	reader, err := blockio.NewFileReader(fs.Service, name)
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
	err = t.replayData(ctx, DeleteBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	err = t.replayData(ctx, DropTable, DropTableSchemaAttr, DropTableSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	err = t.replayData(ctx, DropDB, DropDBSchemaAtt, DropDBSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}
	err = t.replayData(ctx, DeleteFile, DeleteFileSchemaAtt, DeleteFileSchemaTypes, bats, bs, reader)
	if err != nil {
		return err
	}

	t.rebuildTable(bats)
	return nil
}

// For test
func (t *GCTable) Compare(table *GCTable) bool {
	if len(t.dbs) != len(table.dbs) {
		return false
	}
	for id, entry := range t.dbs {
		db := table.dbs[id]
		if db == nil {
			return false
		}
		ok := entry.Compare(db)
		if !ok {
			return ok
		}

	}
	return true
}

func (t *GCTable) String() string {
	if len(t.dbs) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("dbs:[\n")
	for id, entry := range t.dbs {
		_, _ = w.WriteString(fmt.Sprintf("db: %d, isdrop: %t ", id, entry.drop))
		_, _ = w.WriteString(entry.String())
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
