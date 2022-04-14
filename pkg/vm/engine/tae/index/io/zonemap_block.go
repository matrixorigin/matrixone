package io

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
)

type blockZoneMapIndexNode struct {
	*buffer.Node
	mgr  base.INodeManager
	host dataio.BlockFile
	meta *common.IndexMeta
	inner *basic.ZoneMap
}

func newBlockZoneMapIndexNode(mgr base.INodeManager, host dataio.BlockFile, meta *common.IndexMeta) *blockZoneMapIndexNode {
	return nil
}

// TODO: add more types of index node, and integrate with buffer manager
