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

package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type NodeT interface {
	common.IRef

	IsPersisted() bool

	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(
		bat *containers.Batch,
		txn txnif.AsyncTxn,
	) (from int, err error)

	GetDataWindow(from, to uint32) (bat *containers.Batch, err error)
	GetColumnDataWindow(
		from uint32,
		to uint32,
		colIdx int,
	) (vec containers.Vector, err error)

	GetValueByRow(row, col int) (v any)
	GetRowsByKey(key any) (rows []uint32, err error)
	BatchDedup(
		keys containers.Vector,
		skipFn func(row uint32) error,
	) (sels *roaring.Bitmap, err error)
	ContainsKey(key any) (ok bool, err error)

	Rows() uint32
}

type Node struct {
	NodeT
}

func NewNode(node NodeT) *Node {
	return &Node{
		NodeT: node,
	}
}

func (n *Node) MustMNode() *memoryNode {
	return n.NodeT.(*memoryNode)
}

func (n *Node) MustPNode() *persistedNode {
	return n.NodeT.(*persistedNode)
}
