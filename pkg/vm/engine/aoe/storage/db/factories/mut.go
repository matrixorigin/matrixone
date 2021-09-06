package factories

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutFactory struct {
	flusher mb.BlockFlusher
	mgr     bb.INodeManager
}

func NewMutFactory(opts *engine.Options, mgr bb.INodeManager, flusher mb.BlockFlusher) *mutFactory {
	f := &mutFactory{
		mgr:     mgr,
		flusher: flusher,
	}
	return f
}

func (f *mutFactory) CreateNodeFactory(tdata iface.ITableData) base.NodeFactory {
	return newMutNodeFactory(f, tdata)
}
