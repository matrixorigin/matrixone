package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type MutFactory interface {
	CreateNodeFactory(iface.ITableData) NodeFactory
}

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block) bb.INode
	GetManager() bb.INodeManager
}
