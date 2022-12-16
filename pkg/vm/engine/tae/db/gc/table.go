package gc

import (
	"bytes"
	"context"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
)

type GCTable struct {
	sync.Mutex
	table map[string]*ObjectEntry
	drop  []common.ID
}

func NewGCTable() GCTable {
	table := GCTable{
		table: make(map[string]*ObjectEntry),
		drop:  make([]common.ID, 0),
	}
	return table
}

func (t *GCTable) addBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	object := t.table[name]
	if object == nil {
		object = NewObjectEntry()
		object.table.tid = id.TableID
	}
	object.AddBlock(id)
	t.table[name] = object
}

func (t *GCTable) deleteBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	object := t.table[name]
	if object == nil {
		object = NewObjectEntry()
		object.table.tid = id.TableID
	}
	object.DelBlock(id)
	t.table[name] = object
}

func (t *GCTable) Merge(table GCTable) {
	for name, entry := range table.table {
		object := t.table[name]
		if object == nil {
			object = NewObjectEntry()
		}

		object.MergeEntry(*entry)
		t.table[name] = object
	}
}

func (t *GCTable) GetGCObject() []string {
	gc := make([]string, 0)
	for name := range t.table {
		if t.table[name] == nil {
			panic(any("error"))
		}
		if t.table[name].AllowGC() {
			gc = append(gc, name)
			delete(t.table, name)
		}
	}
	return gc
}

func (t *GCTable) dropEntry(id common.ID) {
	t.drop = append(t.drop, id)
	for name := range t.table {
		if t.table[name].table.tid == id.TableID {
			t.table[name].DropTable()
		}
	}
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins, insTxn, del, delTxn := data.GetBlkBatchs()
	for i := 0; i < ins.Length(); i++ {
		dbid := insTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := insTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		sid := insTxn.GetVectorByName(catalog.SnapshotAttr_SegID).Get(i).(uint64)
		blkID := ins.GetVectorByName(pkgcatalog.BlockMeta_ID).Get(i).(uint64)
		metaLoc := string(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		id := common.ID{
			SegmentID: sid,
			TableID:   tid,
			BlockID:   blkID,
			PartID:    uint32(dbid),
		}
		name, _, _ := blockio.DecodeMetaLoc(metaLoc)
		t.addBlock(id, name)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		sid := delTxn.GetVectorByName(catalog.SnapshotAttr_SegID).Get(i).(uint64)
		blkID := del.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
		metaLoc := string(delTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))

		id := common.ID{
			SegmentID: sid,
			TableID:   tid,
			BlockID:   rowIDToU64(blkID),
			PartID:    uint32(dbid),
		}
		name, _, _ := blockio.DecodeMetaLoc(metaLoc)
		t.deleteBlock(id, name)
	}
	_, _, _, del, delTxn = data.GetTblBatchs()
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		id := common.ID{
			TableID: tid,
			PartID:  uint32(dbid),
		}
		t.dropEntry(id)
	}
}

func rowIDToU64(rowID types.Rowid) uint64 {
	return types.DecodeUint64(rowID[:8])
}

func (t *GCTable) rebuildTable(bats []*containers.Batch) {
	for i := 0; i < bats[CreateBlock].Length(); i++ {
		dbid := bats[CreateBlock].GetVectorByName(GCAttrDBId).Get(i).(uint32)
		tid := bats[CreateBlock].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		sid := bats[CreateBlock].GetVectorByName(GCAttrSegmentId).Get(i).(uint64)
		blkID := bats[CreateBlock].GetVectorByName(GCAttrBlockId).Get(i).(uint64)
		name := string(bats[CreateBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		id := common.ID{
			SegmentID: sid,
			TableID:   tid,
			BlockID:   blkID,
			PartID:    uint32(dbid),
		}
		t.addBlock(id, name)
	}
	for i := 0; i < bats[DeleteBlock].Length(); i++ {
		dbid := bats[DeleteBlock].GetVectorByName(GCAttrDBId).Get(i).(uint32)
		tid := bats[DeleteBlock].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		sid := bats[DeleteBlock].GetVectorByName(GCAttrSegmentId).Get(i).(uint64)
		blkID := bats[DeleteBlock].GetVectorByName(GCAttrBlockId).Get(i).(uint64)
		name := string(bats[DeleteBlock].GetVectorByName(GCAttrObjectName).Get(i).([]byte))
		id := common.ID{
			SegmentID: sid,
			TableID:   tid,
			BlockID:   blkID,
			PartID:    uint32(dbid),
		}
		t.deleteBlock(id, name)
	}
	for i := 0; i < bats[DropTable].Length(); i++ {
		dbid := bats[DropTable].GetVectorByName(GCAttrDBId).Get(i).(uint32)
		tid := bats[DropTable].GetVectorByName(GCAttrTableId).Get(i).(uint64)
		id := common.ID{
			TableID: tid,
			PartID:  uint32(dbid),
		}
		t.dropEntry(id)
	}
}

func (t *GCTable) makeBatchWithGCTable() []*containers.Batch {
	add := containers.NewBatch()
	del := containers.NewBatch()
	drop := containers.NewBatch()
	bats := make([]*containers.Batch, 3)
	bats[CreateBlock] = add
	bats[DeleteBlock] = del
	bats[DropTable] = drop
	return bats
}

func (t *GCTable) closeBatch(bs []*containers.Batch) {
	for i := range bs {
		bs[i].Close()
	}
}

func (t *GCTable) collectData() []*containers.Batch {
	bats := t.makeBatchWithGCTable()
	for i, attr := range BlockSchemaAttr {
		bats[CreateBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], false))
		bats[DeleteBlock].AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], false))
	}
	for i, attr := range DropTableSchemaAttr {
		bats[DropTable].AddVector(attr, containers.MakeVector(DropTableSchemaTypes[i], false))
	}
	for name, object := range t.table {
		if object.table.drop {
			bats[DropTable].GetVectorByName(GCAttrTableId).Append(object.table.tid)
			bats[DropTable].GetVectorByName(GCAttrDBId).Append(object.table.tid)
			continue
		}
		for _, block := range object.table.blocks {
			bats[CreateBlock].GetVectorByName(GCAttrBlockId).Append(block.BlockID)
			bats[CreateBlock].GetVectorByName(GCAttrSegmentId).Append(block.SegmentID)
			bats[CreateBlock].GetVectorByName(GCAttrTableId).Append(block.TableID)
			bats[CreateBlock].GetVectorByName(GCAttrDBId).Append(block.PartID)
			bats[CreateBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name))
		}
		for _, block := range object.table.delete {
			bats[DeleteBlock].GetVectorByName(GCAttrBlockId).Append(block.BlockID)
			bats[DeleteBlock].GetVectorByName(GCAttrSegmentId).Append(block.SegmentID)
			bats[DeleteBlock].GetVectorByName(GCAttrTableId).Append(block.TableID)
			bats[DeleteBlock].GetVectorByName(GCAttrDBId).Append(block.PartID)
			bats[DeleteBlock].GetVectorByName(GCAttrObjectName).Append([]byte(name))
		}
	}
	return bats
}

func (t *GCTable) replayData(ctx context.Context,
	typ BatchType,
	attrs []string,
	types []types.Type,
	bats []*containers.Batch,
	bs []objectio.BlockObject) error {
	for i := range attrs {
		col, err := bs[typ].GetColumn(uint16(i))
		if err != nil {
			return err
		}
		colData, err := col.GetData(ctx, common.DefaultAllocator)
		if err != nil {
			return err
		}
		pkgVec := vector.New(types[i])
		v := make([]byte, len(colData.Entries[0].Object.([]byte)))
		copy(v, colData.Entries[0].Object.([]byte))
		if err = pkgVec.Read(v); err != nil {
			return err
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(types[i], false)
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec, false)
		}
		bats[typ].AddVector(attrs[i], vec)
	}
	return nil
}

func (t *GCTable) SaveTable(start, end types.TS, fs *objectio.ObjectFS) ([]objectio.BlockObject, error) {
	bats := t.collectData()
	defer t.closeBatch(bats)
	name := blockio.EncodeCheckpointMetadataFileName(GCMetaDir, PrefixGCMeta, start, end)
	writer := blockio.NewWriter(context.Background(), fs, name)
	for i := range bats {
		if _, err := writer.WriteBlock(bats[i]); err != nil {
			return nil, err
		}
	}

	blocks, err := writer.Sync()
	return blocks, err
}

func (t *GCTable) ReadTable(ctx context.Context, name string, size int64, fs *objectio.ObjectFS) error {
	reader, err := objectio.NewObjectReader(name, fs.Service)
	if err != nil {
		return err
	}
	bs, err := reader.ReadAllMeta(ctx, size, common.DefaultAllocator)
	if err != nil {
		return err
	}
	bats := t.makeBatchWithGCTable()
	defer t.closeBatch(bats)
	err = t.replayData(ctx, CreateBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs)
	if err != nil {
		return err
	}
	err = t.replayData(ctx, DeleteBlock, BlockSchemaAttr, BlockSchemaTypes, bats, bs)
	if err != nil {
		return err
	}
	err = t.replayData(ctx, DropTable, DropTableSchemaAttr, DropTableSchemaTypes, bats, bs)
	if err != nil {
		return err
	}

	t.rebuildTable(bats)
	return nil
}

func (t *GCTable) String() string {
	t.Lock()
	defer t.Unlock()
	if len(t.table) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("table:[\n")
	for name, entry := range t.table {
		_, _ = w.WriteString(fmt.Sprintf("name: %v ", name))
		_, _ = w.WriteString(entry.String())
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
