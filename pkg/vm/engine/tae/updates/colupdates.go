package updates

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ColumnUpdates struct {
	rwlock  *sync.RWMutex
	colDef  *catalog.ColDef
	target  *common.ID
	txnMask *roaring.Bitmap
	txnVals map[uint32]interface{}
}

func NewColumnUpdates(target *common.ID, colDef *catalog.ColDef, rwlock *sync.RWMutex) *ColumnUpdates {
	if rwlock == nil {
		rwlock = &sync.RWMutex{}
	}
	return &ColumnUpdates{
		rwlock:  rwlock,
		colDef:  colDef,
		target:  target,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
}

func (n *ColumnUpdates) StringLocked() string {
	s := "["
	for k, v := range n.txnVals {
		s = fmt.Sprintf("%s%d:%v,", s, k, v)
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

func (n *ColumnUpdates) HasUpdateLocked(row uint32) bool {
	return n.txnVals[row] != nil
}

func (n *ColumnUpdates) EqualLocked(o *ColumnUpdates) bool {
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

func (n *ColumnUpdates) GetUpdateCntLocked() int {
	return int(n.txnMask.GetCardinality())
}

// TODO: rewrite
func (n *ColumnUpdates) ReadFrom(r io.Reader) error {
	buf := make([]byte, txnbase.IDSize)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	n.target = txnbase.UnmarshalID(buf)
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
	if err := vals.Read(buf); err != nil {
		return err
	}
	it := n.txnMask.Iterator()
	for it.HasNext() {
		row := it.Next()
		v := compute.GetValue(&vals, row)
		n.txnVals[row] = v
	}
	return nil
}

// TODO: rewrite later
func (n *ColumnUpdates) WriteTo(w io.Writer) error {
	_, err := w.Write(txnbase.MarshalID(n.target))
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

	col := gvec.New(n.colDef.Type)
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

func (n *ColumnUpdates) Update(row uint32, v interface{}) error {
	n.rwlock.Lock()
	err := n.UpdateLocked(row, v)
	n.rwlock.Unlock()
	return err
}

func (n *ColumnUpdates) UpdateLocked(row uint32, v interface{}) error {
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *ColumnUpdates) MergeLocked(o *ColumnUpdates) error {
	for k, v := range o.txnVals {
		if vv := n.txnVals[k]; vv == nil {
			n.txnMask.Add(k)
			n.txnVals[k] = v
		}
	}
	return nil
}

func (n *ColumnUpdates) ApplyToColumn(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	vec = compute.ApplyUpdateToVector(vec, n.txnMask, n.txnVals)
	vec = compute.ApplyDeleteToVector(vec, deletes)
	return vec
}
