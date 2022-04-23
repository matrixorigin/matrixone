package updates

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type NodeType int8

const (
	NT_Normal NodeType = iota
	NT_Merge
)

type DeleteNode struct {
	*sync.RWMutex
	*common.DLNode
	chain    *DeleteChain
	txn      txnif.AsyncTxn
	mask     *roaring.Bitmap
	startTs  uint64
	commitTs uint64
	nt       NodeType
}

func NewMergedNode(commitTs uint64) *DeleteNode {
	n := &DeleteNode{
		RWMutex:  new(sync.RWMutex),
		commitTs: commitTs,
		startTs:  commitTs,
		mask:     roaring.New(),
		nt:       NT_Merge,
	}
	return n
}

func NewDeleteNode(txn txnif.AsyncTxn) *DeleteNode {
	n := &DeleteNode{
		RWMutex: new(sync.RWMutex),
		mask:    roaring.New(),
		txn:     txn,
		nt:      NT_Normal,
	}
	if txn != nil {
		n.startTs = txn.GetStartTS()
		n.commitTs = txn.GetCommitTS()
	}
	return n
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
	if node.mask == nil || node.mask.GetCardinality() == 0 {
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

func (node *DeleteNode) MergeLocked(o *DeleteNode) error {
	if node.mask == nil {
		node.mask = roaring.New()
	}
	node.mask.Or(o.mask)
	return nil
}
func (node *DeleteNode) GetCommitTSLocked() uint64 { return node.commitTs }
func (node *DeleteNode) GetStartTS() uint64        { return node.startTs }

func (node *DeleteNode) RangeDeleteLocked(start, end uint32) {
	node.mask.AddRange(uint64(start), uint64(end+1))
}
func (node *DeleteNode) GetCardinalityLocked() uint32 { return uint32(node.mask.GetCardinality()) }

func (node *DeleteNode) PrepareCommit() (err error) {
	node.chain.Lock()
	defer node.chain.Unlock()
	if node.commitTs != txnif.UncommitTS {
		return
	}
	// if node.commitTs != txnif.UncommitTS {
	// 	panic("logic error")
	// }
	node.commitTs = node.txn.GetCommitTS()
	node.chain.UpdateLocked(node)
	return
}

func (node *DeleteNode) ApplyCommit() (err error) {
	node.Lock()
	defer node.Unlock()
	if node.txn == nil {
		panic("not expected")
	}
	node.txn = nil
	if node.chain.controller != nil {
		node.chain.controller.SetMaxVisible(node.commitTs)
	}
	return
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
	s := fmt.Sprintf("[%s:%s](%d-%d)[%d:%s]", ntype, commitState, node.startTs, node.commitTs, node.mask.GetCardinality(), node.mask.String())
	return s
}

func (node *DeleteNode) WriteTo(w io.Writer) (err error) {
	buf, err := node.mask.ToBytes()
	if err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return
	}
	if _, err = w.Write(buf); err != nil {
		return
	}
	return
}

func (node *DeleteNode) ReadFrom(r io.Reader) (err error) {
	cnt := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &cnt); err != nil {
		return
	}
	if cnt == 0 {
		return
	}
	buf := make([]byte, cnt)
	if _, err = r.Read(buf); err != nil {
		return
	}
	node.mask = roaring.New()
	err = node.mask.UnmarshalBinary(buf)
	return
}

func (node *DeleteNode) MakeCommand(id uint32, forceFlush bool) (cmd txnif.TxnCmd, entry txnbase.NodeEntry, err error) {
	cmd = NewDeleteCmd(id, node)
	return
}

func (node *DeleteNode) ApplyDeletes(vec *gvec.Vector) *gvec.Vector {
	if node == nil {
		return vec
	}
	return compute.ApplyDeleteToVector(vec, node.mask)
}
