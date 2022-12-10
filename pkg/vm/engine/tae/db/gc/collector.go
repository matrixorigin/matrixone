package gc

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type Collector struct {
	table *GcEntry
}

func NewCollector(table *GcEntry) *Collector {
	return &Collector{
		table: table,
	}
}

func (c *Collector) UpdateTable(data *logtail.CheckpointData) {
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
		c.table.AddBlock(id, name)
	}
	for i := 0; i < del.Length(); i++ {
		dbid := delTxn.GetVectorByName(catalog.SnapshotAttr_DBID).Get(i).(uint64)
		tid := delTxn.GetVectorByName(catalog.SnapshotAttr_TID).Get(i).(uint64)
		sid := delTxn.GetVectorByName(catalog.SnapshotAttr_SegID).Get(i).(uint64)
		blkID := del.GetVectorByName(catalog.AttrRowID).Get(i).(uint64)
		metaLoc := string(delTxn.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
		id := common.ID{
			SegmentID: sid,
			TableID:   tid,
			BlockID:   blkID,
			PartID:    uint32(dbid),
		}
		name, _, _ := blockio.DecodeMetaLoc(metaLoc)
		c.table.DeleteBlock(id, name)
	}
}
