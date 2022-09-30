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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BfNode struct {
	*buffer.Node

	Bf index.StaticFilter

	metaKey, bfKey string
	mgr            base.INodeManager
	colMetaFactory EvictableNodeFactory
}

func NewBfNode(mgr base.INodeManager, bfKey, metaKey string, col file.ColumnBlock, metaloc string, typ types.Type) (node *BfNode, err error) {
	node = &BfNode{
		bfKey:          bfKey,
		metaKey:        metaKey,
		mgr:            mgr,
		colMetaFactory: func() (base.INode, error) { return NewColumnMetaNode(mgr, metaKey, col, metaloc, typ), nil },
	}

	h, err := PinEvictableNode(mgr, metaKey, node.colMetaFactory)
	if err != nil {
		return
	}
	defer h.Close()
	meta := h.GetNode().(*ColumnMetaNode)
	size := meta.GetMeta().GetBloomFilter().OriginSize()
	node.Node = buffer.NewNode(node, mgr, bfKey, uint64(size))
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnload
	node.HardEvictableFunc = func() bool { return true }
	return
}

func (n *BfNode) onLoad() {
	var h base.INodeHandle
	var err error
	h, err = PinEvictableNode(n.mgr, n.metaKey, n.colMetaFactory)
	if err != nil {
		panic(err)
	}
	metaNode := h.GetNode().(*ColumnMetaNode)
	h.Close()

	stat := metaNode.GetMeta()
	compressTyp := stat.GetAlg()
	// Do IO, fetch bloomfilter buf
	fsData, err := metaNode.GetIndex(objectio.BloomFilterType)
	if err != nil {
		panic(err)
	}
	rawSize := stat.GetBloomFilter().OriginSize()
	buf := make([]byte, rawSize)
	data := fsData.(*objectio.BloomFilter).GetData()
	if err = common.Decompress(data, buf, common.CompressType(compressTyp)); err != nil {
		panic(err)
	}
	n.Bf, err = index.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
}

func (n *BfNode) onUnload() {
	n.Bf = nil
}
