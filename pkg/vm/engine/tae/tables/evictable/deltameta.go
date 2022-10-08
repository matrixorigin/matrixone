// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package evictable

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
)

type DeltaMetaNode struct {
	*buffer.Node
	// data
	objectio.BlockObject
	// used to load data
	blk      file.Block
	deltaloc string
}

func NewDeltaMetaNode(mgr base.INodeManager, metaKey string, blk file.Block, deltaloc string) *DeltaMetaNode {
	node := &DeltaMetaNode{
		blk:      blk,
		deltaloc: deltaloc,
	}
	_, ext := blockio.DecodeDeltaLoc(deltaloc)
	baseNode := buffer.NewNode(node, mgr, metaKey, uint64(ext.OriginSize()))
	node.Node = baseNode
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnLoad
	node.HardEvictableFunc = func() bool { return true }
	return node
}

func (n *DeltaMetaNode) onLoad() {
	if n.BlockObject != nil {
		return
	}
	// Do IO, fetch columnData
	meta := n.blk.GetDeltaFormKey(n.deltaloc)
	n.BlockObject = meta
}

func (n *DeltaMetaNode) onUnLoad() {
	n.BlockObject = nil
}
