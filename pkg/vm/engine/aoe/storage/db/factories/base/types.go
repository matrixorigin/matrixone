package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type MutFactory interface {
	CreateNodeFactory(interface{}) NodeFactory
}

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block, *mb.MockSize) bb.INode
	GetManager() bb.INodeManager
}
