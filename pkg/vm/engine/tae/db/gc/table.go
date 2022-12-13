package gc

import (
	"bytes"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
)

type GcTable struct {
	sync.Mutex
	table map[string]*ObjectEntry
}

func NewGcTable() GcTable {
	table := GcTable{
		table: make(map[string]*ObjectEntry),
	}
	return table
}

func (t *GcTable) addBlock(id common.ID, name string) {
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

func (t *GcTable) deleteBlock(id common.ID, name string) {
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

func (t *GcTable) Merge(table GcTable) {
	for name, entry := range table.table {
		object := t.table[name]
		if object == nil {
			object = NewObjectEntry()
		}

		object.MergeEntry(*entry)
		t.table[name] = object
	}
}

func (t *GcTable) GetGcObject() []string {
	gc := make([]string, 0)
	for name := range t.table {
		if t.table[name] == nil {
			panic(any("error"))
		}
		if t.table[name].AllowGc() {
			gc = append(gc, name)
			delete(t.table, name)
		}
	}
	return gc
}

func (t *GcTable) dropEntry(id common.ID) {
	for name := range t.table {
		if t.table[name].table.tid == id.TableID {
			t.table[name].DropTable()
		}
	}
}

func (t *GcTable) UpdateTable(data *logtail.CheckpointData) {
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
		if metaLoc == "" {
			continue
		}
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
		if metaLoc == "" {
			continue
		}
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
func (t *GcTable) String() string {
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
