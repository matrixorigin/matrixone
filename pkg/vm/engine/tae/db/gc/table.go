package gc

import (
	"bytes"
	"context"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
}

func NewGCTable() GCTable {
	table := GCTable{
		table: make(map[string]*ObjectEntry),
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
	for name := range t.table {
		if t.table[name].table.tid == id.TableID {
			t.table[name].DropTable()
		}
	}
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	_, _, _, del, delTxn := data.GetTblBatchs()
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		id := common.ID{
			TableID: tid,
			PartID:  uint32(dbid),
		}
		t.dropEntry(id)
	}
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
}

func rowIDToU64(rowID types.Rowid) uint64 {
	return types.DecodeUint64(rowID[:8])
}

func (t *GCTable) collectData() []*containers.Batch {
	add := containers.NewBatch()
	del := containers.NewBatch()
	for i, attr := range BlockSchemaAttr {
		add.AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], false))
		del.AddVector(attr, containers.MakeVector(BlockSchemaTypes[i], false))
	}
	drop := containers.NewBatch()
	for i, attr := range DropTableSchemaAttr {
		drop.AddVector(attr, containers.MakeVector(DropTableSchemaTypes[i], false))
	}
	for name, object := range t.table {
		if object.table.drop {
			drop.GetVectorByName(GCAttrTableId).Append(object.table.tid)
			drop.GetVectorByName(GCAttrDbId).Append(object.table.tid)
			continue
		}
		for _, block := range object.table.blocks {
			add.GetVectorByName(GCAttrBlockId).Append(block.BlockID)
			add.GetVectorByName(GCAttrSegmentId).Append(block.SegmentID)
			add.GetVectorByName(GCAttrTableId).Append(block.TableID)
			add.GetVectorByName(GCAttrDbId).Append(block.PartID)
			add.GetVectorByName(GCAttrObjectName).Append([]byte(name))
		}
		for _, block := range object.table.delete {
			del.GetVectorByName(GCAttrBlockId).Append(block.BlockID)
			del.GetVectorByName(GCAttrSegmentId).Append(block.SegmentID)
			del.GetVectorByName(GCAttrTableId).Append(block.TableID)
			del.GetVectorByName(GCAttrDbId).Append(block.PartID)
			del.GetVectorByName(GCAttrObjectName).Append([]byte(name))
		}
	}
	bats := make([]*containers.Batch, 3)
	bats[CreateBlock] = add
	bats[DeleteBlock] = del
	bats[DropTable] = drop
	return bats
}

func (t *GCTable) SaveTable(start, end types.TS, fs *objectio.ObjectFS) ([]objectio.BlockObject, error) {
	bats := t.collectData()
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

func (t *GCTable) String() string {
	t.Lock()
	defer t.Unlock()
	if len(t.table) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("table:[")
	for name, entry := range t.table {
		_, _ = w.WriteString(fmt.Sprintf(" %v", name))
		_, _ = w.WriteString("block:[")
		for _, id := range entry.table.blocks {
			_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
		}
		_, _ = w.WriteString("]\n")
		_, _ = w.WriteString("delete:[")
		for _, id := range entry.table.delete {
			_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
		}
		_, _ = w.WriteString("]\n")
	}
	_, _ = w.WriteString("]\n")
	return w.String()
}
