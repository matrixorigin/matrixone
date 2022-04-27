package updates

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ColumnNode struct {
	*common.DLNode
	*sync.RWMutex
	txnMask  *roaring.Bitmap
	txnVals  map[uint32]interface{}
	chain    *ColumnChain
	startTs  uint64
	commitTs uint64
	txn      txnif.AsyncTxn
	id       *common.ID
}

func NewSimpleColumnNode() *ColumnNode {
	n := &ColumnNode{
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
	return n
}
func NewCommittedColumnNode(startTs, commitTs uint64, id *common.ID, rwlocker *sync.RWMutex) *ColumnNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	n := &ColumnNode{
		RWMutex:  rwlocker,
		txnMask:  roaring.NewBitmap(),
		txnVals:  make(map[uint32]interface{}),
		startTs:  startTs,
		commitTs: commitTs,
		id:       id,
	}
	return n
}
func NewColumnNode(txn txnif.AsyncTxn, id *common.ID, rwlocker *sync.RWMutex) *ColumnNode {
	if rwlocker == nil {
		rwlocker = &sync.RWMutex{}
	}
	n := &ColumnNode{
		RWMutex: rwlocker,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
		txn:     txn,
		id:      id,
	}
	if txn != nil {
		n.startTs = txn.GetStartTS()
		n.commitTs = txn.GetCommitTS()
	}
	return n
}

func (n *ColumnNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmd = NewUpdateCmd(id, n)
	return
}

func (n *ColumnNode) AttachTo(chain *ColumnChain) {
	n.chain = chain
	n.DLNode = chain.Insert(n)
}

func (n *ColumnNode) GetID() *common.ID {
	return n.id
}

func (n *ColumnNode) GetChain() txnif.UpdateChain {
	return n.chain
}

func (n *ColumnNode) GetDLNode() *common.DLNode {
	return n.DLNode
}

func (n *ColumnNode) Compare(o common.NodePayload) int {
	op := o.(*ColumnNode)
	n.RLock()
	defer n.RUnlock()
	op.RLock()
	defer op.RUnlock()
	if n.commitTs == op.commitTs {
		if n.startTs < op.startTs {
			return -1
		} else if n.startTs > op.startTs {
			return 1
		}
		return 0
	}
	if n.commitTs == txnif.UncommitTS {
		return 1
	} else if op.commitTs == txnif.UncommitTS {
		return -1
	}
	return 0
}

func (n *ColumnNode) GetValueLocked(row uint32) (v interface{}, err error) {
	v = n.txnVals[row]
	if v == nil {
		err = txnbase.ErrNotFound
	}
	return
}
func (n *ColumnNode) HasUpdateLocked(row uint32) bool {
	return n.txnVals[row] != nil
}

func (n *ColumnNode) EqualLocked(o *ColumnNode) bool {
	if o == nil {
		return n == nil
	}
	for k, v := range n.txnVals {
		if v != o.txnVals[k] {
			return false
		}
	}
	return true
}

func (n *ColumnNode) GetUpdateCntLocked() int {
	return int(n.txnMask.GetCardinality())
}

// TODO: rewrite
func (n *ColumnNode) ReadFrom(r io.Reader) error {
	buf := make([]byte, txnbase.IDSize)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	n.id = txnbase.UnmarshalID(buf)
	n.txnMask = roaring.New()

	length := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	buf = make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	if err := n.txnMask.UnmarshalBinary(buf); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	buf = make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	vals := gvec.Vector{}
	vals.Nsp = &nulls.Nulls{}
	if err := vals.Read(buf); err != nil {
		return err
	}
	it := n.txnMask.Iterator()
	row := uint32(0)
	for it.HasNext() {
		key := it.Next()
		v := compute.GetValue(&vals, row)
		n.txnVals[key] = v
		row++
	}
	return nil
}

// TODO: rewrite later
func (n *ColumnNode) WriteTo(w io.Writer) error {
	_, err := w.Write(txnbase.MarshalID(n.chain.id))
	if err != nil {
		return err
	}

	buf, err := n.txnMask.ToBytes()
	if err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return err
	}

	if _, err = w.Write(buf); err != nil {
		return err
	}

	col := gvec.New(n.chain.GetMeta().GetSchema().ColDefs[n.chain.id.Idx].Type)
	it := n.txnMask.Iterator()
	for it.HasNext() {
		row := it.Next()
		compute.AppendValue(col, n.txnVals[row])
	}
	buf, err = col.Show()
	if err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (n *ColumnNode) UpdateLocked(row uint32, v interface{}) error {
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *ColumnNode) MergeLocked(o *ColumnNode) error {
	for k, v := range o.txnVals {
		if vv := n.txnVals[k]; vv == nil {
			n.txnMask.Add(k)
			n.txnVals[k] = v
		}
	}
	return nil
}

func (n *ColumnNode) GetStartTS() uint64        { return n.startTs }
func (n *ColumnNode) GetCommitTSLocked() uint64 { return n.commitTs }

func (n *ColumnNode) ApplyToColumn(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	vec = compute.ApplyUpdateToVector(vec, n.txnMask, n.txnVals)
	vec = compute.ApplyDeleteToVector(vec, deletes)
	return vec
}

func (n *ColumnNode) String() string {
	n.RLock()
	defer n.RUnlock()
	return n.StringLocked()
}

func (n *ColumnNode) StringLocked() string {
	commitState := "C"
	if n.commitTs == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf("[%s:%s](%d-%d)[", commitState, n.id.ToBlockFileName(), n.startTs, n.commitTs)
	for k, v := range n.txnVals {
		s = fmt.Sprintf("%s%d:%v,", s, k, v)
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

func (n *ColumnNode) PrepareCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	if n.commitTs != txnif.UncommitTS {
		return
	}
	// if n.commitTs != txnif.UncommitTS {
	// 	panic("logic error")
	// }
	n.commitTs = n.txn.GetCommitTS()
	n.chain.UpdateLocked(n)
	// TODO: merge updates
	return
}

func (n *ColumnNode) ApplyCommit() (err error) {
	n.Lock()
	defer n.Unlock()
	if n.txn == nil {
		panic("not expected")
	}
	n.txn = nil
	n.chain.controller.SetMaxVisible(n.commitTs)
	n.chain.controller.IncChangeNodeCnt()
	return
}

func (n *ColumnNode) PrepareRollback() (err error) { return }
func (n *ColumnNode) ApplyRollback() (err error)   { return }
