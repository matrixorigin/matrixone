package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block) bb.INode
	GetManager() bb.INodeManager
}
