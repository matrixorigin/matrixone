package updates

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
		v := txnbase.GetValue(&vals, row)
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
		txnbase.AppendValue(col, n.txnVals[row])
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
	if _, ok := n.txnVals[row]; ok {
		return txnif.TxnWWConflictErr
	}
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *ColumnUpdates) MergeLocked(o txnif.ColumnUpdates) error {
	for k, v := range o.(*ColumnUpdates).txnVals {
		n.txnMask.Add(k)
		n.txnVals[k] = v
	}
	return nil
}

func (n *ColumnUpdates) ApplyToColumn(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	txnMaskIterator := n.txnMask.Iterator()
	col := vec.Col
	if txnMaskIterator.HasNext() {
		switch vec.Typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
			for txnMaskIterator.HasNext() {
				row := txnMaskIterator.Next()
				txnbase.SetFixSizeTypeValue(vec, row, n.txnVals[row])
				if vec.Nsp != nil && vec.Nsp.Np != nil {
					if vec.Nsp.Np.Contains(uint64(row)) {
						vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
					}
				}
			}
		case types.T_char, types.T_varchar, types.T_json:
			data := col.(*types.Bytes)
			pre := -1
			for txnMaskIterator.HasNext() {
				row := txnMaskIterator.Next()
				if pre != -1 {
					txnbase.UpdateOffsets(data, pre, int(row))
				}
				val := n.txnVals[row].([]byte)
				suffix := data.Data[data.Offsets[row]+data.Lengths[row]:]
				data.Lengths[row] = uint32(len(val))
				val = append(val, suffix...)
				data.Data = append(data.Data[:data.Offsets[row]], val...)
				pre = int(row)
				if vec.Nsp != nil && vec.Nsp.Np != nil {
					if vec.Nsp.Np.Contains(uint64(row)) {
						vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
					}
				}
			}
			if pre != -1 {
				txnbase.UpdateOffsets(data, pre, len(data.Lengths)-1)
			}
		}
	}
	if deletes == nil {
		return vec
	}
	deletesIterator := deletes.Iterator()
	if deletesIterator.HasNext() {
		nsp := &nulls.Nulls{}
		nsp.Np = &roaring64.Bitmap{}
		var nspIterator roaring64.IntPeekable64
		if vec.Nsp != nil && vec.Nsp.Np != nil {
			nspIterator = vec.Nsp.Np.Iterator()
		}
		deleted := 0
		switch vec.Typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
			vec.Col = common.InplaceDeleteRows(vec.Col, deletesIterator)
			deletesIterator = deletes.Iterator()
			for deletesIterator.HasNext() {
				row := deletesIterator.Next()
				if nspIterator != nil {
					var n uint64
					if nspIterator.HasNext() {
						for nspIterator.HasNext() {
							n = nspIterator.PeekNext()
							if uint32(n) < row {
								nspIterator.Next()
							} else {
								if uint32(n) == row {
									nspIterator.Next()
								}
								break
							}
							nsp.Np.Add(n - uint64(deleted))
						}
					}
				}
				deleted++
			}
			if nspIterator != nil {
				for nspIterator.HasNext() {
					n := nspIterator.Next()
					nsp.Np.Add(n - uint64(deleted))
				}
			}
		case types.T_char, types.T_varchar, types.T_json:
			data := col.(*types.Bytes)
			pre := -1
			for deletesIterator.HasNext() {
				row := deletesIterator.Next()
				currRow := row - uint32(deleted)
				if pre != -1 {
					if int(currRow) == len(data.Lengths)-1 {
						txnbase.UpdateOffsets(data, pre-1, int(currRow))
					} else {
						txnbase.UpdateOffsets(data, pre-1, int(currRow)+1)
					}
				}
				if int(currRow) == len(data.Lengths)-1 {
					data.Data = data.Data[:data.Offsets[currRow]]
					data.Lengths = data.Lengths[:currRow]
					data.Offsets = data.Offsets[:currRow]
				} else {
					data.Data = append(data.Data[:data.Offsets[currRow]], data.Data[data.Offsets[currRow+1]:]...)
					data.Lengths = append(data.Lengths[:currRow], data.Lengths[currRow+1:]...)
					data.Offsets = append(data.Offsets[:currRow], data.Offsets[currRow+1:]...)
				}
				if nspIterator != nil {
					var n uint64
					if nspIterator.HasNext() {
						for nspIterator.HasNext() {
							n = nspIterator.PeekNext()
							if uint32(n) < row {
								nspIterator.Next()
							} else {
								if uint32(n) == row {
									nspIterator.Next()
								}
								break
							}
							nsp.Np.Add(n - uint64(deleted))
						}
					}
				}
				deleted++
				pre = int(currRow)
			}
			if nspIterator != nil {
				for nspIterator.HasNext() {
					n := nspIterator.Next()
					nsp.Np.Add(n - uint64(deleted))
				}
			}
			if pre != -1 {
				txnbase.UpdateOffsets(data, pre-1, len(data.Lengths)-1)
			}
		}
		vec.Nsp = nsp
	}
	return vec
}
