package memtable

import (
	gbatch "matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type memTable struct {
	nodeHandle
	mgr       *manager
	TableData iface.ITableData
	Meta      *md.Block
	Block     iface.IBlock
	File      *dataio.TransientBlockFile
	Data      batch.IBatch
}

func newMemTable(mgr *manager, tableData iface.ITableData, data iface.IBlock) *memTable {
	mt := &memTable{
		mgr:        mgr,
		TableData:  tableData,
		Block:      data,
		Meta:       data.GetMeta(),
		nodeHandle: *newNodeHandle(mgr.nodemgr, *data.GetMeta().AsCommonID(), uint64(0)),
	}
	mt.loadFunc = mt.load
	mt.unloadFunc = mt.unload
	return mt
}

func (mt *memTable) load() {
	mt.Data = mt.File.LoadBatch(mt.Meta)
}

func (mt *memTable) unload() {
	ok := mt.File.PreSync(uint32(mt.Data.Length()))
	if !ok {
		return
	}
	vecs := make([]*vector.Vector, len(mt.Meta.Segment.Table.Schema.ColDefs))
	for i, _ := range mt.Meta.Segment.Table.Schema.ColDefs {
		iv := mt.Data.GetVectorByAttr(i)
		vecs[i] = iv.CopyToVector()
	}
	meta := mt.Meta.Copy()
	mt.File.Sync(vecs, meta, meta.Segment.Table.Conf.Dir)
	mt.Data.Close()
	mt.Data = nil
}

func (mt *memTable) Append(bat *gbatch.Batch, offset uint64, index *metadata.LogIndex) (n uint64, err error) {
	var na int
	for idx, attr := range mt.Data.GetAttrs() {
		if na, err = mt.Data.GetVectorByAttr(attr).AppendVector(bat.Vecs[idx], int(offset)); err != nil {
			return n, err
		}
	}
	n = uint64(na)
	index.Count = n
	mt.Meta.SetIndex(*index)
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	mt.Meta.AddCount(n)
	mt.TableData.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	if uint64(mt.Data.Length()) == mt.Meta.MaxRowCount {
		mt.Meta.DataState = md.FULL
	}
	return n, err
}

func (mt *memTable) GetID() common.ID {
	return mt.Meta.AsCommonID().AsBlockID()
}

func (mt *memTable) String() string {
	// TODO
	return ""
}

func (mt *memTable) GetMeta() *md.Block {
	return mt.Meta
}

func (mt *memTable) Unpin() {
	mt.mgr.nodemgr.Unpin(mt)
}

func (mt *memTable) Pin() {
	mt.mgr.nodemgr.Pin(mt)
}

func (mt *memTable) IsFull() bool {
	return mt.Meta.IsFull()
}
