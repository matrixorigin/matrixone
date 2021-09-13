// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"github.com/RoaringBitmap/roaring"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type Node struct {
	*bmgr.Node
	common.RefHelper
	Cols        *roaring.Bitmap
	PostCloseCB PostCloseCB
}

func newNode(bufMgr bmgrif.IBufferManager, vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor,
	cols *roaring.Bitmap, cb PostCloseCB) *Node {
	node := new(Node)
	node.Cols = cols
	node.Node = bufMgr.CreateNode(vf, useCompress, constructor).(*bmgr.Node)
	node.OnZeroCB = node.close
	node.PostCloseCB = cb
	node.Ref()
	return node
}

func (node *Node) close() {
	if node.Node != nil {
		node.Node.Close()
	}
	if node.PostCloseCB != nil {
		node.PostCloseCB(node)
	}
}

func (node *Node) ContainsCol(v uint64) bool {
	return node.Cols.Contains(uint32(v))
}

func (node *Node) ContainsOnlyCol(v uint64) bool {
	return node.Cols.Contains(uint32(v)) && node.Cols.GetCardinality() == 1
}

func (node *Node) AllCols() []uint32 {
	return node.Cols.ToArray()
}
