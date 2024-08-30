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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type NodeT interface {
	common.IRef

	IsPersisted() bool

	Contains(
		ctx context.Context,
		keys containers.Vector,
		keysZM index.ZM,
		txn txnif.TxnReader,
		isCommitting bool,
		mp *mpool.MPool,
	) (err error)
	GetDuplicatedRows(
		ctx context.Context,
		txn txnif.TxnReader,
		maxVisibleRow uint32,
		keys containers.Vector,
		keysZM index.ZM,
		rowIDs containers.Vector,
		isCommitting bool,
		checkWWConflict bool,
		mp *mpool.MPool,
	) (err error)

	Rows() (uint32, error)

	Scan(
		ctx context.Context,
		bat **containers.Batch,
		txn txnif.TxnReader,
		readSchema *catalog.Schema,
		blkID uint16,
		colIdxes []int,
		mp *mpool.MPool,
	) (err error)
	CollectObjectTombstoneInRange(
		ctx context.Context,
		start, end types.TS,
		objID *types.Objectid,
		bat **containers.Batch,
		mp *mpool.MPool,
		vpool *containers.VectorPool,
	) (err error)
	FillBlockTombstones(
		ctx context.Context,
		txn txnif.TxnReader,
		blkID *objectio.Blockid,
		deletes **nulls.Nulls,
		mp *mpool.MPool) error
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
