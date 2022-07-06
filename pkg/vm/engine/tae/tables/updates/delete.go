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

package updates

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type NodeType int8

const (
	NT_Normal NodeType = iota
	NT_Merge
)

type DeleteNode struct {
	*sync.RWMutex
	*common.DLNode
	chain      *DeleteChain
	txn        txnif.AsyncTxn
	logIndex   *wal.Index
	logIndexes []*wal.Index
	mask       *roaring.Bitmap
	startTs    uint64
	commitTs   uint64
	nt         NodeType
	id         *common.ID
	dt         handle.DeleteType
}

func NewMergedNode(commitTs uint64) *DeleteNode {
	n := &DeleteNode{
		RWMutex:    new(sync.RWMutex),
		commitTs:   commitTs,
		startTs:    commitTs,
		mask:       roaring.New(),
		nt:         NT_Merge,
		logIndexes: make([]*wal.Index, 0),
	}
	return n
}

func NewDeleteNode(txn txnif.AsyncTxn, dt handle.DeleteType) *DeleteNode {
	n := &DeleteNode{
		RWMutex: new(sync.RWMutex),
		mask:    roaring.New(),
		txn:     txn,
		nt:      NT_Normal,
		dt:      dt,
	}
	if txn != nil {
		n.startTs = txn.GetStartTS()
		n.commitTs = txn.GetCommitTS()
	}
	return n
}
func (node *DeleteNode) GetID() *common.ID {
	return node.id
}
func (node *DeleteNode) AddLogIndexesLocked(indexes []*wal.Index) {
	node.logIndexes = append(node.logIndexes, indexes...)
}

func (node *DeleteNode) SetDeletes(mask *roaring.Bitmap) {
	node.mask = mask
}

func (node *DeleteNode) AddLogIndexLocked(index *wal.Index) {
	node.logIndexes = append(node.logIndexes, index)
}

func (node *DeleteNode) IsMerged() bool { return node.nt == NT_Merge }
func (node *DeleteNode) AttachTo(chain *DeleteChain) {
	node.chain = chain
	node.DLNode = chain.Insert(node)
}

func (node *DeleteNode) Compare(o common.NodePayload) int {
	op := o.(*DeleteNode)
	node.RLock()
	defer node.RUnlock()
	op.RLock()
	defer op.RUnlock()
	if node.commitTs == op.commitTs {
		if node.startTs < op.startTs {
			return -1
		} else if node.startTs > op.startTs {
			return 1
		}
		return 0
	}
	if node.commitTs == txnif.UncommitTS {
		return 1
	} else if op.commitTs == txnif.UncommitTS {
		return -1
	}
	return 0
}

func (node *DeleteNode) GetChain() txnif.DeleteChain          { return node.chain }
func (node *DeleteNode) GetDeleteMaskLocked() *roaring.Bitmap { return node.mask }

func (node *DeleteNode) HasOverlapLocked(start, end uint32) bool {
	if node.mask == nil || node.mask.IsEmpty() {
		return false
	}
	var yes bool
	if start == end {
		yes = node.mask.Contains(start)
	} else {
		x2 := roaring.New()
		x2.AddRange(uint64(start), uint64(end+1))
		yes = node.mask.Intersects(x2)
	}
	return yes
}

func (node *DeleteNode) MergeLocked(o *DeleteNode, collectIndex bool) {
	if node.mask == nil {
		node.mask = roaring.New()
	}
	node.mask.Or(o.mask)
	if collectIndex {
		if o.logIndex != nil {
			node.AddLogIndexLocked(o.logIndex)
		}
		if o.logIndexes != nil {
			node.AddLogIndexesLocked(o.logIndexes)
		}
	}
}
func (node *DeleteNode) GetCommitTSLocked() uint64 { return node.commitTs }
func (node *DeleteNode) GetStartTS() uint64        { return node.startTs }

func (node *DeleteNode) IsDeletedLocked(row uint32) bool {
	return node.mask.Contains(row)
}

func (node *DeleteNode) RangeDeleteLocked(start, end uint32) {
	node.mask.AddRange(uint64(start), uint64(end+1))
}
func (node *DeleteNode) GetCardinalityLocked() uint32 { return uint32(node.mask.GetCardinality()) }

func (node *DeleteNode) PrepareCommit() (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	if node.commitTs != txnif.UncommitTS {
		return
	}
	node.commitTs = node.txn.GetCommitTS()
	node.chain.UpdateLocked(node)
	return
}

func (node *DeleteNode) ApplyCommit(index *wal.Index) (err error) {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("DeleteNode | ApplyCommit | LogicErr")
	}
	node.txn = nil
	node.logIndex = index
	if node.chain.mvcc != nil {
		node.chain.mvcc.SetMaxVisible(node.commitTs)
	}
	node.chain.AddDeleteCnt(uint32(node.mask.GetCardinality()))
	node.chain.mvcc.IncChangeNodeCnt()
	return
}

func (node *DeleteNode) GeneralString() string {
	return fmt.Sprintf("TS=%d;Cnt=%d;LogIndex%v", node.commitTs, node.mask.GetCardinality(), node.logIndex)
}

func (node *DeleteNode) GeneralDesc() string {
	return fmt.Sprintf("TS=%d;Cnt=%d", node.commitTs, node.mask.GetCardinality())
}

func (node *DeleteNode) GeneralVerboseString() string {
	return fmt.Sprintf("TS=%d;%v;Cnt=%d;Deletes=%v", node.commitTs, node.logIndex, node.mask.GetCardinality(), node.mask)
}

func (node *DeleteNode) StringLocked() string {
	ntype := "TXN"
	if node.nt == NT_Merge {
		ntype = "MERGE"
	}
	commitState := "C"
	if node.commitTs == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf("[%s:%s](%d-%d)[%d:%s]%s", ntype, commitState, node.startTs, node.commitTs, node.mask.GetCardinality(), node.mask.String(), node.logIndex.String())
	return s
}

func (node *DeleteNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(txnbase.MarshalID(node.chain.mvcc.GetID()))
	if err != nil {
		return
	}
	n += int64(cn)
	buf, err := node.mask.ToBytes()
	if err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return
	}
	sn := int(0)
	if sn, err = w.Write(buf); err != nil {
		return
	}
	n += int64(sn) + 4
	if err = binary.Write(w, binary.BigEndian, node.commitTs); err != nil {
		return
	}
	n += 8
	return
}

func (node *DeleteNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	buf := make([]byte, txnbase.IDSize)
	if sn, err = r.Read(buf); err != nil {
		return
	}
	n = int64(sn)
	node.id = txnbase.UnmarshalID(buf)
	cnt := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &cnt); err != nil {
		return
	}
	n += 4
	if cnt == 0 {
		return
	}
	buf = make([]byte, cnt)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n += int64(cnt)
	node.mask = roaring.New()
	err = node.mask.UnmarshalBinary(buf)
	if err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &node.commitTs); err != nil {
		return
	}
	n += 4
	return
}
func (node *DeleteNode) SetLogIndex(idx *wal.Index) {
	node.logIndex = idx
}
func (node *DeleteNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewDeleteCmd(id, node)
	return
}

func (node *DeleteNode) PrepareRollback() (err error) {
	node.chain.mvcc.Lock()
	defer node.chain.mvcc.Unlock()
	node.chain.RemoveNodeLocked(node)
	return
}

func (node *DeleteNode) ApplyRollback() (err error) { return }

func (node *DeleteNode) OnApply() (err error) {
	if node.dt == handle.DT_Normal {
		listener := node.chain.mvcc.GetDeletesListener()
		if listener == nil {
			return
		}
		err = listener(node.mask.GetCardinality(), node.mask.Iterator(), node.commitTs)
	}
	return
}

func (node *DeleteNode) GetRowMaskRefLocked() *roaring.Bitmap {
	return node.mask
}
