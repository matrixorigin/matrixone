package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type FactoryType uint16

const (
	INVALID FactoryType = iota
	NORMAL
	MUTABLE
)

type CollectionFactory = func(iface.ITableData) imem.ICollection

type MutFactory interface {
	GetNodeFactroy(interface{}) NodeFactory
	GetType() FactoryType
	// GetCollectionFactory() CollectionFactory
}

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block, *mb.MockSize) bb.INode
	GetManager() bb.INodeManager
}
