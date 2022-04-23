package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

type dataTable struct {
	meta        *catalog.TableEntry
	fileFactory file.SegmentFileFactory
	bufMgr      base.INodeManager
	aBlk        *dataBlock
}

func newTable(meta *catalog.TableEntry, fileFactory file.SegmentFileFactory, bufMgr base.INodeManager) *dataTable {
	return &dataTable{
		meta:        meta,
		fileFactory: fileFactory,
		bufMgr:      bufMgr,
	}
}

func (table *dataTable) GetHandle() data.TableHandle {
	return newHandle(table, table.aBlk)
}

func (table *dataTable) ApplyHandle(h data.TableHandle) {
	handle := h.(*tableHandle)
	table.aBlk = handle.block
}
