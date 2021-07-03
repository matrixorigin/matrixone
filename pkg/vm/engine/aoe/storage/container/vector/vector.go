package vector

import (
	"bytes"
	"fmt"
	"io"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"
	// log "github.com/sirupsen/logrus"
)

func StdVectorConstructor(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStdVectorNode(capacity, freeFunc)
}

type IVector interface {
	SetValue(v interface{})
	GetValue() interface{}
	Append(interface{}) error
	AppendVector(*ro.Vector, int) (int, error)
	Length() int
	Capacity() int
	DataBytes() int
	Close() error
	IsReadonly() bool
	SliceReference(start, end int) IVector
	// ReadonlyView()
}

func NewStdVector(t types.Type, capacity uint64) *StdVector {
	return &StdVector{
		Type:  t,
		Data:  make([]byte, 0, capacity*uint64(t.Size)),
		VMask: &nulls.Nulls{},
	}
}

func NewStdVectorNode(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &StdVector{
		Data:         make([]byte, 0),
		VMask:        &nulls.Nulls{},
		NodeCapacity: capacity,
		AllocSize:    capacity,
		FreeFunc:     freeFunc,
	}
}

func NewEmptyStdVector() *StdVector {
	return &StdVector{
		Data:  make([]byte, 0),
		VMask: &nulls.Nulls{},
	}
}

func (v *StdVector) InplaceInit(t types.Type, capacity uint64) {
	v.Type = t
	v.Data = make([]byte, 0, capacity*uint64(t.Size))
}

func (v *StdVector) Close() error {
	v.VMask = nil
	v.Data = nil
	return nil
}

func (v *StdVector) HasNull() bool {
	return atomic.LoadUint64(&v.StatMask)&HasNullMask != 0
}

func (v *StdVector) IsReadonly() bool {
	return atomic.LoadUint64(&v.StatMask)&ReadonlyMask != 0
}

func (v *StdVector) Length() int {
	return int(atomic.LoadUint64(&v.StatMask) & PosMask)
}

func (v *StdVector) Capacity() int {
	return cap(v.Data) / int(v.Type.Size)
}

func (v *StdVector) DataBytes() int {
	return cap(v.Data)
}

func (v *StdVector) FreeMemory() {
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *StdVector) GetMemorySize() uint64 {
	return uint64(v.DataBytes())
}

func (v *StdVector) GetMemoryCapacity() uint64 {
	return v.AllocSize
}

func (v *StdVector) IsNull(idx int) bool {
	if idx >= v.Length() {
		panic(VecInvalidOffsetErr.Error())
	}
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	return v.VMask.Contains(uint64(idx))
}

func (v *StdVector) SetValue(idx int, val interface{}) {
	v.Lock()
	defer v.Unlock()
	if idx >= v.Length() {
		panic(fmt.Sprintf("idx %d is out of range", idx))
	}
	start := idx * int(v.Type.Size)
	switch v.Type.Oid {
	case types.T_int8:
		copy(v.Data[start:], encoding.EncodeInt8(val.(int8)))
	case types.T_int16:
		copy(v.Data[start:], encoding.EncodeInt16(val.(int16)))
	case types.T_int32:
		copy(v.Data[start:], encoding.EncodeInt32(val.(int32)))
	case types.T_int64:
		copy(v.Data[start:], encoding.EncodeInt64(val.(int64)))
	case types.T_uint8:
		copy(v.Data[start:], encoding.EncodeUint8(val.(uint8)))
	case types.T_uint16:
		copy(v.Data[start:], encoding.EncodeUint16(val.(uint16)))
	case types.T_uint32:
		copy(v.Data[start:], encoding.EncodeUint32(val.(uint32)))
	case types.T_uint64:
		copy(v.Data[start:], encoding.EncodeUint64(val.(uint64)))
	case types.T_float32:
		copy(v.Data[start:], encoding.EncodeFloat32(val.(float32)))
	case types.T_float64:
		copy(v.Data[start:], encoding.EncodeFloat64(val.(float64)))
	// case types.T_decimal:
	// case types.T_date:
	// case types.T_datetime:
	default:
		panic("not supported yet")
	}
}

func (v *StdVector) GetValue(idx int) interface{} {
	if idx >= v.Length() || idx < 0 {
		panic(fmt.Sprintf("idx %d is out of range", idx))
	}
	if !v.IsReadonly() {
		v.RLock()
	}
	start := idx * int(v.Type.Size)
	data := v.Data[start : start+int(v.Type.Size)]
	if !v.IsReadonly() {
		v.RUnlock()
	}
	switch v.Type.Oid {
	case types.T_int8:
		return encoding.DecodeInt8(data)
	case types.T_int16:
		return encoding.DecodeInt16(data)
	case types.T_int32:
		return encoding.DecodeInt32(data)
	case types.T_int64:
		return encoding.DecodeInt64(data)
	case types.T_uint8:
		return encoding.DecodeUint8(data)
	case types.T_uint16:
		return encoding.DecodeUint16(data)
	case types.T_uint32:
		return encoding.DecodeUint32(data)
	case types.T_uint64:
		return encoding.DecodeUint64(data)
	case types.T_float32:
		return encoding.DecodeFloat32(data)
	case types.T_float64:
		return encoding.DecodeFloat64(data)
	// case types.T_decimal:
	// case types.T_date:
	// case types.T_datetime:
	default:
		panic(fmt.Sprintf("type %v not supported yet", v.Type))
	}
}

func (v *StdVector) Append(n int, vals interface{}) error {
	if v.IsReadonly() {
		return VecWriteRoErr
	}
	v.Lock()
	defer v.Unlock()
	err := v.appendWithOffset(0, n, vals)
	if err != nil {
		return err
	}

	mask := v.StatMask & (^PosMask)
	pos := uint64(len(v.Data)/int(v.Type.Size)) & PosMask
	mask = mask | pos
	if len(v.Data) == cap(v.Data) {
		mask = mask | ReadonlyMask
	}
	atomic.StoreUint64(&v.StatMask, mask)
	return nil
}

func (v *StdVector) appendWithOffset(offset, n int, vals interface{}) error {
	var data []byte
	switch v.Type.Oid {
	case types.T_int8:
		data = encoding.EncodeInt8Slice(vals.([]int8)[offset : offset+n])
	case types.T_int16:
		data = encoding.EncodeInt16Slice(vals.([]int16)[offset : offset+n])
	case types.T_int32:
		data = encoding.EncodeInt32Slice(vals.([]int32)[offset : offset+n])
	case types.T_int64:
		data = encoding.EncodeInt64Slice(vals.([]int64)[offset : offset+n])
	case types.T_uint8:
		data = encoding.EncodeInt8Slice(vals.([]int8)[offset : offset+n])
	case types.T_uint16:
		data = encoding.EncodeInt16Slice(vals.([]int16)[offset : offset+n])
	case types.T_uint32:
		data = encoding.EncodeInt32Slice(vals.([]int32)[offset : offset+n])
	case types.T_uint64:
		data = encoding.EncodeInt64Slice(vals.([]int64)[offset : offset+n])

	case types.T_decimal:
		data = encoding.EncodeDecimalSlice(vals.([]types.Decimal)[offset : offset+n])
	case types.T_float32:
		data = encoding.EncodeFloat32Slice(vals.([]float32)[offset : offset+n])
	case types.T_float64:
		data = encoding.EncodeFloat64Slice(vals.([]float64)[offset : offset+n])
	case types.T_date:
		data = encoding.EncodeDateSlice(vals.([]types.Date)[offset : offset+n])
	case types.T_datetime:
		data = encoding.EncodeDatetimeSlice(vals.([]types.Datetime)[offset : offset+n])
	default:
		panic("not supported yet")
	}
	if len(v.Data)+len(data) > cap(v.Data) {
		panic(fmt.Sprintf("overflow: offset %d, %d + %d > %d", offset, len(v.Data), len(data), cap(v.Data)))
	}
	v.Data = append(v.Data, data...)
	return nil
}

func (v *StdVector) AppendVector(vec *ro.Vector, offset int) (n int, err error) {
	if offset < 0 || offset >= vec.Length() {
		return n, VecInvalidOffsetErr
	}
	if v.IsReadonly() {
		return 0, VecWriteRoErr
	}
	v.Lock()
	defer v.Unlock()
	n = v.Capacity() - v.Length()
	if n > vec.Length()-offset {
		n = vec.Length() - offset
	}
	startRow := v.Length()

	err = v.appendWithOffset(offset, n, vec.Col)
	if err != nil {
		return n, err
	}
	if vec.Nsp.Np != nil {
		for row := startRow; row < startRow+vec.Length(); row++ {
			if vec.Nsp.Contains(uint64(offset + row - startRow)) {
				v.VMask.Add(uint64(row))
			}
		}
	}
	mask := v.StatMask & (^PosMask)
	pos := uint64(len(v.Data)/int(v.Type.Size)) & PosMask
	mask = mask | pos
	if len(v.Data) == cap(v.Data) {
		mask = mask | ReadonlyMask
	}
	if v.VMask.Any() {
		mask = mask | HasNullMask
	}
	atomic.StoreUint64(&v.StatMask, mask)

	return n, err
}

func (v *StdVector) SliceReference(start, end int) *StdVector {
	if !v.IsReadonly() {
		panic("should call this in ro mode")
	}
	startIdx := start * int(v.Type.Size)
	endIdx := end * int(v.Type.Size)
	mask := ReadonlyMask | (uint64(end-start) & PosMask)
	vec := &StdVector{
		Type: v.Type,
		Data: v.Data[startIdx:endIdx],
	}
	if v.VMask.Np != nil {
		vmask := v.VMask.Range(uint64(start), uint64(end))
		vec.VMask = vmask
		if vmask.Any() {
			mask = mask | HasNullMask
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	vec.StatMask = mask
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vec.Data))
	hp.Cap = hp.Len
	vec.Data = *(*[]byte)(unsafe.Pointer(&hp))
	return vec
}

// func (v *StdVector) SetNull(idx int) error {
// 	v.Lock()
// 	mask := atomic.LoadUint64(&v.StatMask)
// 	if mask&ReadonlyMask != 0 {
// 		return VecWriteRoErr
// 	}
// 	pos := mask | PosMask
// 	if idx >= int(pos) {
// 		return VecInvalidOffsetErr
// 	}

// 	v.Unlock()
// 	newMask := mask | HasNullMask
// 	v.VMask.Add(uint64(idx))

// }

func (v *StdVector) GetLatestView() *StdVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & PosMask)
	endIdx := endPos * int(v.Type.Size)
	vec := &StdVector{
		StatMask: ReadonlyMask | mask,
		Type:     v.Type,
		Data:     v.Data[0:endIdx],
	}
	if mask&HasNullMask != 0 {
		if mask&ReadonlyMask == 0 {
			vec.VMask = v.VMask.Range(0, uint64(endPos))
		} else {
			vec.VMask = v.VMask.Range(0, uint64(endPos))
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vec.Data))
	hp.Cap = hp.Len
	vec.Data = *(*[]byte)(unsafe.Pointer(&hp))
	return vec
}

func (v *StdVector) CopyToVector() *ro.Vector {
	if atomic.LoadUint64(&v.StatMask)&ReadonlyMask == 0 {
		panic("should call in ro mode")
	}
	length := v.Length()
	vec := ro.New(v.Type)
	switch v.Type.Oid {
	case types.T_int8:
		col := make([]int8, length)
		curCol := encoding.DecodeInt8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_int16:
		col := make([]int16, length)
		curCol := encoding.DecodeInt16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_int32:
		col := make([]int32, length)
		curCol := encoding.DecodeInt32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_int64:
		col := make([]int64, length)
		curCol := encoding.DecodeInt64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_uint8:
		col := make([]uint8, length)
		curCol := encoding.DecodeUint8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_uint16:
		col := make([]uint16, length)
		curCol := encoding.DecodeUint16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_uint32:
		col := make([]uint32, length)
		curCol := encoding.DecodeUint32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_uint64:
		col := make([]uint64, length)
		curCol := encoding.DecodeUint64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))

	case types.T_decimal:
		col := make([]types.Decimal, length)
		curCol := encoding.DecodeDecimalSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_float32:
		col := make([]float32, length)
		curCol := encoding.DecodeFloat32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_float64:
		col := make([]float64, length)
		curCol := encoding.DecodeFloat64Slice(v.Data)
		copy(col[0:], curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_date:
		col := make([]types.Date, length)
		curCol := encoding.DecodeDateSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	case types.T_datetime:
		col := make([]types.Datetime, length)
		curCol := encoding.DecodeDatetimeSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = v.VMask.Range(uint64(0), uint64(length))
	default:
		panic("not supported yet")
	}
	return vec
}

func (vec *StdVector) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Marshall()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (vec *StdVector) ReadFrom(r io.Reader) (n int64, err error) {
	capBuf := make([]byte, 8)
	_, err = r.Read(capBuf)
	if err != nil {
		return n, err
	}

	// TODO: will remove below os.File type check.
	switch f := r.(type) {
	case *os.File:
		f.Seek(0, io.SeekStart)
	}
	realSize := encoding.DecodeUint64(capBuf)
	buf := make([]byte, realSize)
	_, err = r.Read(buf)
	if err != nil {
		return n, err
	}
	copy(buf[0:], capBuf)
	err = vec.Unmarshall(buf)
	return int64(realSize), err
}

func (vec *StdVector) Unmarshall(data []byte) error {
	buf := data
	vec.NodeCapacity = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	vec.StatMask = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	vec.Type = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	nb := encoding.DecodeUint32(buf[:4])
	buf = buf[4:]
	if nb > 0 {
		if err := vec.VMask.Read(buf[:nb]); err != nil {
			return err
		}
		buf = buf[nb:]
	}
	vec.Data = buf
	return nil
}

func (vec *StdVector) Marshall() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(uint64(0)))
	buf.Write(encoding.EncodeUint64(vec.StatMask))
	buf.Write(encoding.EncodeType(vec.Type))
	nb, err := vec.VMask.Show()
	if err != nil {
		return nil, err
	}
	buf.Write(encoding.EncodeUint32(uint32(len(nb))))
	if len(nb) > 0 {
		buf.Write(nb)
	}
	buf.Write(vec.Data)
	buffer := buf.Bytes()
	capBuf := encoding.EncodeUint64(uint64(len(buffer)))
	copy(buffer[0:], capBuf)
	vec.NodeCapacity = uint64(len(buffer))
	return buf.Bytes(), nil
}

func (vec *StdVector) Reset() {
}

func MockStdVector(t types.Type, rows uint64) *StdVector {
	vec := NewStdVector(t, rows)
	switch t.Oid {
	case types.T_int32:
		vals := []int32{}
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, int32(i))
		}
		vec.Append(len(vals), vals)
	case types.T_float64:
		vals := []float64{}
		for i := uint64(0); i < rows; i++ {
			vals = append(vals, float64(i))
		}
		vec.Append(len(vals), vals)
	default:
		panic("not supported")
	}
	return vec
}
