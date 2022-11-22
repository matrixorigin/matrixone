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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ColumnMetaNode struct {
	*buffer.Node
	// data
	objectio.ColumnObject
	Zonemap *index.ZoneMap
	typ     types.Type
	// the index number of the column
	idx     uint16
	metaloc string
	fs      *objectio.ObjectFS
}

func NewColumnMetaNode(
	idx uint16,
	typ types.Type,
	metaloc string,
	metaKey string,
	mgr base.INodeManager,
	fs *objectio.ObjectFS) *ColumnMetaNode {
	node := &ColumnMetaNode{
		idx:     idx,
		metaloc: metaloc,
		typ:     typ,
		fs:      fs,
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
	reader, err := blockio.NewReader(context.Background(), n.fs, n.metaloc)
	if err != nil {
		panic(err)
	}
	meta := reader.GetDataObject(n.idx, nil)
	n.ColumnObject = meta

	// deserialize zonemap
	zmData, err := meta.GetIndex(context.Background(), objectio.ZoneMapType, nil)

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
