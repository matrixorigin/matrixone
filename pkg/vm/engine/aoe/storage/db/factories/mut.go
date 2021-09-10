package factories

import (
	"matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutFactory struct {
	flusher mb.BlockFlusher
	mgr     bb.INodeManager
	// collectionFactory base.CollectionFactory
}

func NewMutFactory(mgr bb.INodeManager, flusher mb.BlockFlusher) *mutFactory {
	f := &mutFactory{
		mgr:     mgr,
		flusher: flusher,
	}
	return f
}

func (f *mutFactory) GetNodeFactroy(tdata interface{}) base.NodeFactory {
	return newMutNodeFactory(f, tdata.(iface.ITableData))
}

func (f *mutFactory) GetType() base.FactoryType {
	return base.MUTABLE
}

// func (f *mutFactory) GetCollectionFactory() base.CollectionFactory {
// 	return f.collectionFactory
// }

type normalFactory struct {
}

func NewNormalFactory() *normalFactory {
	f := &normalFactory{}
	return f
}

func (f *normalFactory) GetNodeFactroy(tdata interface{}) base.NodeFactory {
	return nil
}

func (f *normalFactory) GetType() base.FactoryType {
	return base.NORMAL
}
