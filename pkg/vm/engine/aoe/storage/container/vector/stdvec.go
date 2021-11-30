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

package vector

import (
	"bytes"
	"errors"
	"io"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ro "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logutil"
)

func StdVectorConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStdVectorNode(vf, useCompress, freeFunc)
}

func NewStdVector(t types.Type, capacity uint64) *StdVector {
	v := &StdVector{
		BaseVector: BaseVector{
			Type:  t,
			VMask: &nulls.Nulls{},
		},
		// Data: make([]byte, 0, capacity*uint64(t.Size)),
	}
	size := capacity * uint64(t.Size)
	v.MNode = common.GPool.Alloc(size)
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v.MNode.Buf))
	hp.Len = 0
	hp.Cap = int(size)
	v.Data = *(*[]byte)(unsafe.Pointer(&hp))
	return v
}

func NewStdVectorNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &StdVector{
		Data:        make([]byte, 0),
		File:        vf,
		UseCompress: useCompress,
		FreeFunc:    freeFunc,
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
	}
}

func NewEmptyStdVector() *StdVector {
	return &StdVector{
		Data: make([]byte, 0),
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
	}
}

func (v *StdVector) PlacementNew(t types.Type) {
	v.Type = t
	capacity := uint64(v.File.Stat().OriginSize())
	if v.MNode != nil {
		common.GPool.Free(v.MNode)
	}
	v.MNode = common.GPool.Alloc(capacity)
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&v.MNode.Buf))
	hp.Len = 0
	hp.Cap = int(capacity)
	v.Data = *(*[]byte)(unsafe.Pointer(&hp))
	// v.DataSource = make([]byte, 0, capacity*uint64(t.Size))
}

func (v *StdVector) GetType() dbi.VectorType {
	return dbi.StdVec
}

func (v *StdVector) Close() error {
	if v.MNode != nil {
		common.GPool.Free(v.MNode)
	}
	v.VMask = nil
	v.Data = nil
	return nil
}

func (v *StdVector) Capacity() int {
	return cap(v.Data) / int(v.Type.Size)
}

func (v *StdVector) dataBytes() int {
	return cap(v.Data)
}

func (v *StdVector) FreeMemory() {
	if v.MNode != nil {
		common.GPool.Free(v.MNode)
	}
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *StdVector) GetMemorySize() uint64 {
	v.RLock()
	defer v.RUnlock()
	return uint64(len(v.Data))
}

func (v *StdVector) GetMemoryCapacity() uint64 {
	if v.UseCompress {
		return uint64(v.File.Stat().Size())
	} else {
		return uint64(v.File.Stat().OriginSize())
	}
}

func (v *StdVector) SetValue(idx int, val interface{}) error {
	return errors.New("not supported")
}

func (v *StdVector) GetValue(idx int) (interface{}, error) {
	if idx >= v.Length() || idx < 0 {
		return nil, VecInvalidOffsetErr
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
		return encoding.DecodeInt8(data), nil
	case types.T_int16:
		return encoding.DecodeInt16(data), nil
	case types.T_int32:
		return encoding.DecodeInt32(data), nil
	case types.T_int64:
		return encoding.DecodeInt64(data), nil
	case types.T_uint8:
		return encoding.DecodeUint8(data), nil
	case types.T_uint16:
		return encoding.DecodeUint16(data), nil
	case types.T_uint32:
		return encoding.DecodeUint32(data), nil
	case types.T_uint64:
		return encoding.DecodeUint64(data), nil
	case types.T_float32:
		return encoding.DecodeFloat32(data), nil
	case types.T_float64:
		return encoding.DecodeFloat64(data), nil
	// case types.T_decimal:
	case types.T_date:
		return encoding.DecodeDate(data), nil
	case types.T_datetime:
		return encoding.DecodeDatetime(data), nil
	default:
		return nil, VecTypeNotSupportErr
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

	mask := v.StatMask & (^container.PosMask)
	pos := uint64(len(v.Data)/int(v.Type.Size)) & container.PosMask
	mask = mask | pos
	if len(v.Data) == cap(v.Data) {
		mask = mask | container.ReadonlyMask
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
		data = encoding.EncodeUint8Slice(vals.([]uint8)[offset : offset+n])
	case types.T_uint16:
		data = encoding.EncodeUint16Slice(vals.([]uint16)[offset : offset+n])
	case types.T_uint32:
		data = encoding.EncodeUint32Slice(vals.([]uint32)[offset : offset+n])
	case types.T_uint64:
		data = encoding.EncodeUint64Slice(vals.([]uint64)[offset : offset+n])

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
		return VecTypeNotSupportErr
	}
	if len(v.Data)+len(data) > cap(v.Data) {
		return VecInvalidOffsetErr
	}
	v.Data = append(v.Data, data...)
	return nil
}

func (v *StdVector) AppendVector(vec *ro.Vector, offset int) (n int, err error) {
	if offset < 0 || offset >= ro.Length(vec) {
		return n, VecInvalidOffsetErr
	}
	if v.IsReadonly() {
		return 0, VecWriteRoErr
	}
	v.Lock()
	defer v.Unlock()
	n = v.Capacity() - v.Length()
	if n > ro.Length(vec)-offset {
		n = ro.Length(vec) - offset
	}
	startRow := v.Length()

	err = v.appendWithOffset(offset, n, vec.Col)
	if err != nil {
		return n, err
	}
	if vec.Nsp.Np != nil {
		for row := startRow; row < startRow+ro.Length(vec); row++ {
			if nulls.Contains(vec.Nsp, uint64(offset + row - startRow)) {
				nulls.Add(v.VMask, uint64(row))
			}
		}
	}
	mask := v.StatMask & (^container.PosMask)
	pos := uint64(len(v.Data)/int(v.Type.Size)) & container.PosMask
	mask = mask | pos
	if len(v.Data) == cap(v.Data) {
		mask = mask | container.ReadonlyMask
	}
	if nulls.Any(v.VMask) {
		mask = mask | container.HasNullMask
	}
	atomic.StoreUint64(&v.StatMask, mask)

	return n, err
}

func (v *StdVector) SliceReference(start, end int) (dbi.IVectorReader, error) {
	if !v.IsReadonly() {
		return nil, VecNotRoErr
	}
	startIdx := start * int(v.Type.Size)
	endIdx := end * int(v.Type.Size)
	mask := container.ReadonlyMask | (uint64(end-start) & container.PosMask)
	vec := &StdVector{
		BaseVector: BaseVector{
			Type: v.Type,
		},
		Data: v.Data[startIdx:endIdx],
	}
	if v.VMask.Np != nil {
		vmask := nulls.Range(v.VMask, uint64(start), uint64(end), &nulls.Nulls{})
		vec.VMask = vmask
		if nulls.Any(vmask) {
			mask = mask | container.HasNullMask
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	vec.StatMask = mask
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vec.Data))
	hp.Cap = hp.Len
	vec.Data = *(*[]byte)(unsafe.Pointer(&hp))
	return vec, nil
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

func (v *StdVector) GetLatestView() IVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & container.PosMask)
	endIdx := endPos * int(v.Type.Size)
	vec := &StdVector{
		BaseVector: BaseVector{
			StatMask: container.ReadonlyMask | mask,
			Type:     v.Type,
		},
		Data: v.Data[0:endIdx],
	}
	if mask&container.HasNullMask != 0 {
		if mask&container.ReadonlyMask == 0 {
			vec.VMask = nulls.Range(v.VMask, 0, uint64(endPos), &nulls.Nulls{})
		} else {
			vec.VMask = &nulls.Nulls{}
			vec.VMask.Np = v.VMask.Np.Clone()
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vec.Data))
	hp.Cap = hp.Len
	vec.Data = *(*[]byte)(unsafe.Pointer(&hp))
	return vec
}

func (v *StdVector) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, VecNotRoErr
	}
	nullSize := 0
	var nullbuf []byte
	var err error
	if nulls.Any(v.VMask) {
		nullbuf, err = v.VMask.Show()
		if err != nil {
			return nil, err
		}
		nullSize = len(nullbuf)
	}
	length := v.Length()
	vec := ro.New(v.Type)
	capacity := encoding.TypeSize + 4 + nullSize + length*int(v.Type.Size)
	deCompressed.Reset()
	if capacity > deCompressed.Cap() {
		deCompressed.Grow(capacity)
	}
	buf := deCompressed.Bytes()
	buf = buf[:capacity]
	dBuf := buf
	copy(dBuf, encoding.EncodeType(v.Type))
	dBuf = dBuf[encoding.TypeSize:]
	copy(dBuf, encoding.EncodeUint32(uint32(nullSize)))
	dBuf = dBuf[4:]
	if nullSize > 0 {
		copy(dBuf, nullbuf)
		dBuf = dBuf[nullSize:]
	}
	copy(dBuf, v.Data)
	err = vec.Read(buf)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (v *StdVector) CopyToVector() (*ro.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, VecNotRoErr
	}
	length := v.Length()
	vec := ro.New(v.Type)
	vec.Data = v.Data
	switch v.Type.Oid {
	case types.T_int8:
		col := make([]int8, length)
		curCol := encoding.DecodeInt8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_int16:
		col := make([]int16, length)
		curCol := encoding.DecodeInt16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_int32:
		col := make([]int32, length)
		curCol := encoding.DecodeInt32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_int64:
		col := make([]int64, length)
		curCol := encoding.DecodeInt64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_uint8:
		col := make([]uint8, length)
		curCol := encoding.DecodeUint8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_uint16:
		col := make([]uint16, length)
		curCol := encoding.DecodeUint16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_uint32:
		col := make([]uint32, length)
		curCol := encoding.DecodeUint32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_uint64:
		col := make([]uint64, length)
		curCol := encoding.DecodeUint64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})

	case types.T_decimal:
		col := make([]types.Decimal, length)
		curCol := encoding.DecodeDecimalSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_float32:
		col := make([]float32, length)
		curCol := encoding.DecodeFloat32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_float64:
		col := make([]float64, length)
		curCol := encoding.DecodeFloat64Slice(v.Data)
		copy(col[0:], curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_date:
		col := make([]types.Date, length)
		curCol := encoding.DecodeDateSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.T_datetime:
		col := make([]types.Datetime, length)
		curCol := encoding.DecodeDatetimeSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	default:
		return nil, VecTypeNotSupportErr
	}
	return vec, nil
}

func (vec *StdVector) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Marshal()
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
	err = vec.Unmarshal(buf)
	return int64(realSize), err
}

func (vec *StdVector) Unmarshal(data []byte) error {
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
	if vec.MNode != nil {
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&vec.Data))
		hp.Len = len(buf)
		vec.Data = *(*[]byte)(unsafe.Pointer(&hp))
		copy(vec.Data[0:], buf)
	} else {
		vec.Data = buf
	}
	return nil
}

func (vec *StdVector) Marshal() ([]byte, error) {
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
	vec.Data = nil
}
