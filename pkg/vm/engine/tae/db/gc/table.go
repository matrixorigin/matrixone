package gc

import (
	"bytes"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
)

type GcTable struct {
	sync.Mutex
	table  map[string][]common.ID
	delete map[string][]common.ID
	fs     *objectio.ObjectFS
}

func NewGcTable(fs *objectio.ObjectFS) GcTable {
	table := GcTable{
		table:  make(map[string][]common.ID),
		delete: make(map[string][]common.ID),
		fs:     fs,
	}
	return table
}

func (t *GcTable) addBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	blockList := t.table[name]
	if blockList != nil {
		blockList = make([]common.ID, 0)
	}
	blockList = append(blockList, id)
	t.table[name] = blockList
}

func (t *GcTable) deleteBlock(id common.ID, name string) {
	t.Lock()
	defer t.Unlock()
	blockList := t.delete[name]
	if blockList != nil {
		blockList = make([]common.ID, 0)
	}

	blockList = append(blockList, id)
	t.delete[name] = blockList
}

func (t *GcTable) UpdateTable(data *logtail.CheckpointData) {
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
func (t *GcTable) String() string {
	t.Lock()
	defer t.Unlock()
	if len(t.table) == 0 {
		return ""
	}
	var w bytes.Buffer
	_, _ = w.WriteString("table:[")
	for name, ids := range t.table {
		_, _ = w.WriteString(fmt.Sprintf(" %v", name))
		_, _ = w.WriteString("block:[")
		for _, id := range ids {
			_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
		}
		_, _ = w.WriteString("]\n")
	}
	_, _ = w.WriteString("]\n")
	if len(t.delete) != 0 {
		_, _ = w.WriteString("delete:[")
		for name, ids := range t.delete {
			_, _ = w.WriteString(fmt.Sprintf(" %v", name))
			_, _ = w.WriteString("block:[")
			for _, id := range ids {
				_, _ = w.WriteString(fmt.Sprintf(" %v", id.String()))
			}
			_, _ = w.WriteString("]\n")
		}
		_, _ = w.WriteString("]\n")
	}
	return w.String()
}
