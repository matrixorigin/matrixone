package memtable

import (
	gbatch "matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
)

type memTable struct {
	buffer.Node
	mgr       *manager
	TableData iface.ITableData
	Meta      *md.Block
	Block     iface.IBlock
	File      *dataio.TransientBlockFile
	Data      batch.IBatch
}

func newMemTable(mgr *manager, tableData iface.ITableData, data iface.IBlock) *memTable {
	mt := &memTable{
		mgr:       mgr,
		TableData: tableData,
		Block:     data,
		Meta:      data.GetMeta(),
	}
	mt.Node = *buffer.NewNode(mt, mgr.nodemgr, *data.GetMeta().AsCommonID(), uint64(0))
	mt.LoadFunc = mt.load
	mt.UnloadFunc = mt.unload
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
	meta := mt.Meta.Copy()
	mt.File.Sync(mt.Data, meta)
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
