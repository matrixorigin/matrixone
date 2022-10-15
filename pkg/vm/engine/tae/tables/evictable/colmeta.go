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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ColumnMetaNode struct {
	*buffer.Node
	// data
	objectio.ColumnObject
	Zonemap *index.ZoneMap
	typ     types.Type
	// used to load data
	col     file.ColumnBlock
	metaloc string
}

func NewColumnMetaNode(mgr base.INodeManager, metaKey string, col file.ColumnBlock, metaloc string, typ types.Type) *ColumnMetaNode {
	node := &ColumnMetaNode{
		col:     col,
		metaloc: metaloc,
		typ:     typ,
	}
	_, ext, _ := blockio.DecodeMetaLoc(metaloc)
	baseNode := buffer.NewNode(node, mgr, metaKey, uint64(ext.OriginSize()))
	node.Node = baseNode
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnLoad
	node.HardEvictableFunc = func() bool { return true }
	return node
}

func (n *ColumnMetaNode) onLoad() {
	if n.ColumnObject != nil {
		return
	}
	// Do IO, fetch columnData
	meta := n.col.GetDataObject(n.metaloc)
	n.ColumnObject = meta

	// deserialize zonemap
	zmData, err := meta.GetIndex(objectio.ZoneMapType, nil)

	// TODOa: Error Handling?
	if err != nil {
		panic(err)
	}
	data := zmData.(*objectio.ZoneMap)
	n.Zonemap = index.NewZoneMap(n.typ)
	err = n.Zonemap.Unmarshal(data.GetData())
	if err != nil {
		panic(err)
	}
}

func (n *ColumnMetaNode) onUnLoad() {
	n.Zonemap = nil
	n.ColumnObject = nil
}
