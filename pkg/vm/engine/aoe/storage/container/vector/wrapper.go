package vector

import (
	"fmt"
	"io"
	"matrixone/pkg/container/types"
	base "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	// log "github.com/sirupsen/logrus"
)

type VectorWrapper struct {
	base.Vector
	FreeFunc  buf.MemoryFreeFunc
	AllocSize uint64
}

func VectorWrapperConstructor(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewVectorWrapperNode(capacity, freeFunc)
}

func NewVectorWrapperNode(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &VectorWrapper{
		AllocSize: capacity,
		FreeFunc:  freeFunc,
	}
}

func NewVectorWrapper(vec *base.Vector) *VectorWrapper {
	return &VectorWrapper{
		Vector: *vec,
	}
}

func NewEmptyWrapper(t types.Type) *VectorWrapper {
	return &VectorWrapper{
		Vector: *base.New(t),
	}
}

func (v *VectorWrapper) PlacementNew(t types.Type, capacity uint64) {
	panic("not supported")
}

func (v *VectorWrapper) SliceReference(start, end int) dbi.IVectorReader {
	panic("not supported")
}

func (v *VectorWrapper) GetType() dbi.VectorType {
	return dbi.Wrapper
}

func (v *VectorWrapper) Close() error {
	return nil
}

func (v *VectorWrapper) Capacity() int {
	return v.Vector.Length()
}

func (v *VectorWrapper) Length() int {
	return v.Vector.Length()
}

func (v *VectorWrapper) Free(p *process.Process) {
	panic("readonly")
}

func (v *VectorWrapper) Clean(p *process.Process) {
	panic("readonly")
}

func (v *VectorWrapper) SetCol(col interface{}) {
	panic("readonly")
}

func (v *VectorWrapper) FreeMemory() {
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *VectorWrapper) Append(n int, vals interface{}) error {
	panic("readonly")
}

func (v *VectorWrapper) GetMemorySize() uint64 {
	return uint64(len(v.Data))
}

func (v *VectorWrapper) GetMemoryCapacity() uint64 {
	return v.AllocSize
}

func (v *VectorWrapper) SetValue(idx int, val interface{}) {
	panic("not supported")
}

func (v *VectorWrapper) GetValue(idx int) interface{} {
	if idx >= v.Length() || idx < 0 {
		panic(fmt.Sprintf("idx %d is out of range", idx))
	}
	switch v.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		val := v.Col.(*types.Bytes)
		return val.Data[val.Offsets[idx] : val.Offsets[idx]+val.Lengths[idx]]
	case types.T_int8:
		return v.Col.([]int8)[idx]
	case types.T_int16:
		return v.Col.([]int16)[idx]
	case types.T_int32:
		return v.Col.([]int32)[idx]
	case types.T_int64:
		return v.Col.([]int64)[idx]
	case types.T_uint8:
		return v.Col.([]uint8)[idx]
	case types.T_uint16:
		return v.Col.([]uint16)[idx]
	case types.T_uint32:
		return v.Col.([]uint32)[idx]
	case types.T_uint64:
		return v.Col.([]uint64)[idx]
	case types.T_decimal:
		return v.Col.([]types.Decimal)[idx]
	case types.T_float32:
		return v.Col.([]float32)[idx]
	case types.T_float64:
		return v.Col.([]float64)[idx]
	case types.T_date:
		return v.Col.([]types.Date)[idx]
	case types.T_datetime:
		return v.Col.([]types.Datetime)[idx]
	case types.T_sel:
		return v.Col.([]int64)[idx]
	case types.T_tuple:
		return v.Col.([][]interface{})[idx]
	default:
		panic("not supported")
	}
}

func (v *VectorWrapper) IsNull(idx int) bool {
	panic("not supported")
}

func (v *VectorWrapper) HasNull() bool {
	panic("not supported")
}

func (v *VectorWrapper) NullCnt() int {
	panic("not supported")
}

func (v *VectorWrapper) IsReadonly() bool {
	panic("not supported")
}

func (v *VectorWrapper) GetLatestView() IVector {
	panic("not supported")
}

func (v *VectorWrapper) CopyToVector() *base.Vector {
	panic("not supported")
}

func (v *VectorWrapper) AppendVector(vec *base.Vector, offset int) (n int, err error) {
	panic("not supported")
}

func (vec *VectorWrapper) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Show()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (vec *VectorWrapper) ReadFrom(r io.Reader) (n int64, err error) {
	data := make([]byte, vec.AllocSize+mempool.CountSize)
	nr, err := r.Read(data[mempool.CountSize:])
	if err != nil {
		return n, err
	}
	t := encoding.DecodeType(data[mempool.CountSize : encoding.TypeSize+mempool.CountSize])
	v := base.New(t)
	vec.Col = v.Col
	err = vec.Vector.Read(data)
	return int64(nr), err
}

func (vec *VectorWrapper) Marshall() ([]byte, error) {
	return vec.Show()
}

func (vec *VectorWrapper) Unmarshall(data []byte) error {
	return vec.Vector.Read(data)
}
