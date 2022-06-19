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
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func StdVectorConstructor(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
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
	v.Data = v.MNode.Buf[:0:size]
	return v
}

func NewStdVectorNode(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
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
	v.Data = v.MNode.Buf[:0:capacity]
}

func (v *StdVector) GetType() container.VectorType {
	return container.StdVec
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

func (v *StdVector) SetValue(idx int, val any) (err error) {
	if idx >= v.Length() || idx < 0 {
		return ErrVecInvalidOffset
	}
	if v.IsReadonly() {
		return ErrVecWriteRo
	}
	_, isNull := val.(types.Null)

	v.Lock()
	defer v.Unlock()

	if isNull {
		if v.VMask.Np == nil {
			v.VMask.Np = roaring64.BitmapOf(uint64(idx))
		} else {
			v.VMask.Np.Add(uint64(idx))
		}
		return
	}

	if v.VMask.Np != nil && v.VMask.Np.Contains(uint64(idx)) {
		v.VMask.Np.Remove(uint64(idx))
	}

	start := idx * int(v.Type.Size)
	dest := v.Data[start:]
	switch v.Type.Oid {
	case types.Type_BOOL:
		types.CopyFixValueToBuf(dest, val.(bool))
	case types.Type_INT8:
		types.CopyFixValueToBuf(dest, val.(int8))
	case types.Type_INT16:
		types.CopyFixValueToBuf(dest, val.(int16))
	case types.Type_INT32:
		types.CopyFixValueToBuf(dest, val.(int32))
	case types.Type_INT64:
		types.CopyFixValueToBuf(dest, val.(int64))
	case types.Type_UINT8:
		types.CopyFixValueToBuf(dest, val.(uint8))
	case types.Type_UINT16:
		types.CopyFixValueToBuf(dest, val.(uint16))
	case types.Type_UINT32:
		types.CopyFixValueToBuf(dest, val.(uint32))
	case types.Type_UINT64:
		types.CopyFixValueToBuf(dest, val.(uint64))
	case types.Type_FLOAT32:
		types.CopyFixValueToBuf(dest, val.(float32))
	case types.Type_FLOAT64:
		types.CopyFixValueToBuf(dest, val.(float64))
	case types.Type_DECIMAL64:
		types.CopyFixValueToBuf(dest, val.(types.Decimal64))
	case types.Type_DECIMAL128:
		types.CopyFixValueToBuf(dest, val.(types.Decimal128))
	case types.Type_DATE:
		types.CopyFixValueToBuf(dest, val.(types.Date))
	case types.Type_DATETIME:
		types.CopyFixValueToBuf(dest, val.(types.Datetime))
	case types.Type_TIMESTAMP:
		types.CopyFixValueToBuf(dest, val.(types.Timestamp))
	default:
		panic(fmt.Errorf("%w: %v", ErrVecTypeNotSupport, val))
	}
	return
}

func (v *StdVector) GetValue(idx int) (val any, err error) {
	if idx >= v.Length() || idx < 0 {
		return nil, ErrVecInvalidOffset
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
	case types.Type_BOOL:
		val = types.DecodeFixed[bool](data)
	case types.Type_INT8:
		val = types.DecodeFixed[int8](data)
	case types.Type_INT16:
		val = types.DecodeFixed[int16](data)
	case types.Type_INT32:
		val = types.DecodeFixed[int32](data)
	case types.Type_INT64:
		val = types.DecodeFixed[int64](data)
	case types.Type_UINT8:
		val = types.DecodeFixed[uint8](data)
	case types.Type_UINT16:
		val = types.DecodeFixed[uint16](data)
	case types.Type_UINT32:
		val = types.DecodeFixed[uint32](data)
	case types.Type_UINT64:
		val = types.DecodeFixed[uint64](data)
	case types.Type_FLOAT32:
		val = types.DecodeFixed[float32](data)
	case types.Type_FLOAT64:
		val = types.DecodeFixed[float64](data)
	case types.Type_DECIMAL64:
		val = types.DecodeFixed[types.Decimal64](data)
	case types.Type_DECIMAL128:
		val = types.DecodeFixed[types.Decimal128](data)
	case types.Type_DATE:
		val = types.DecodeFixed[types.Date](data)
	case types.Type_DATETIME:
		val = types.DecodeFixed[types.Datetime](data)
	case types.Type_TIMESTAMP:
		val = types.DecodeFixed[types.Timestamp](data)
	default:
		panic(fmt.Errorf("%w: %v", ErrVecTypeNotSupport, v.Type))
	}
	return
}

func (v *StdVector) Append(n int, vals any) error {
	if v.IsReadonly() {
		return ErrVecWriteRo
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

func (v *StdVector) appendWithOffset(offset, n int, vals any) error {
	var data []byte
	switch v.Type.Oid {
	case types.Type_BOOL:
		data = types.EncodeBoolSlice(vals.([]bool)[offset : offset+n])
	case types.Type_INT8:
		data = types.EncodeInt8Slice(vals.([]int8)[offset : offset+n])
	case types.Type_INT16:
		data = types.EncodeInt16Slice(vals.([]int16)[offset : offset+n])
	case types.Type_INT32:
		data = types.EncodeInt32Slice(vals.([]int32)[offset : offset+n])
	case types.Type_INT64:
		data = types.EncodeInt64Slice(vals.([]int64)[offset : offset+n])
	case types.Type_UINT8:
		data = types.EncodeUint8Slice(vals.([]uint8)[offset : offset+n])
	case types.Type_UINT16:
		data = types.EncodeUint16Slice(vals.([]uint16)[offset : offset+n])
	case types.Type_UINT32:
		data = types.EncodeUint32Slice(vals.([]uint32)[offset : offset+n])
	case types.Type_UINT64:
		data = types.EncodeUint64Slice(vals.([]uint64)[offset : offset+n])
	case types.Type_DECIMAL64:
		data = types.EncodeDecimal64Slice(vals.([]types.Decimal64)[offset : offset+n])
	case types.Type_DECIMAL128:
		data = types.EncodeDecimal128Slice(vals.([]types.Decimal128)[offset : offset+n])
	case types.Type_FLOAT32:
		data = types.EncodeFloat32Slice(vals.([]float32)[offset : offset+n])
	case types.Type_FLOAT64:
		data = types.EncodeFloat64Slice(vals.([]float64)[offset : offset+n])
	case types.Type_DATE:
		data = types.EncodeDateSlice(vals.([]types.Date)[offset : offset+n])
	case types.Type_DATETIME:
		data = types.EncodeDatetimeSlice(vals.([]types.Datetime)[offset : offset+n])
	case types.Type_TIMESTAMP:
		data = types.EncodeTimestampSlice(vals.([]types.Timestamp)[offset : offset+n])
	default:
		return ErrVecTypeNotSupport
	}
	if len(v.Data)+len(data) > cap(v.Data) {
		return ErrVecInvalidOffset
	}
	v.Data = append(v.Data, data...)
	return nil
}

func (v *StdVector) AppendVector(vec *gvec.Vector, offset int) (n int, err error) {
	if offset < 0 || offset >= gvec.Length(vec) {
		return n, ErrVecInvalidOffset
	}
	if v.IsReadonly() {
		return 0, ErrVecWriteRo
	}
	v.Lock()
	defer v.Unlock()
	n = v.Capacity() - v.Length()
	if n > gvec.Length(vec)-offset {
		n = gvec.Length(vec) - offset
	}
	startRow := v.Length()

	err = v.appendWithOffset(offset, n, vec.Col)
	if err != nil {
		return n, err
	}
	if vec.Nsp.Np != nil {
		for row := startRow; row < startRow+gvec.Length(vec); row++ {
			if nulls.Contains(vec.Nsp, uint64(offset+row-startRow)) {
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

func (v *StdVector) SliceReference(start, end int) (container.IVectorReader, error) {
	if !v.IsReadonly() {
		return nil, ErrVecNotRo
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
	vec.Data = vec.Data[:len(vec.Data):len(vec.Data)]
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
	vec.Data = vec.Data[:len(vec.Data):len(vec.Data)]
	return vec
}

func (v *StdVector) Window(start, end uint32) IVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & container.PosMask)
	mask = mask & ^container.PosMask
	if end > uint32(endPos) {
		end = uint32(endPos)
	}
	newPos := uint64(end-start) & container.PosMask
	newMask := mask | newPos
	startIdx := int(start) * int(v.Type.Size)
	endIdx := int(end) * int(v.Type.Size)
	vec := &StdVector{
		BaseVector: BaseVector{
			StatMask: container.ReadonlyMask | newMask,
			Type:     v.Type,
		},
		Data: v.Data[startIdx:endIdx],
	}
	if mask&container.HasNullMask != 0 {
		np := common.BM64Window(v.VMask.Np, int(start), int(end))
		vec.VMask = &nulls.Nulls{Np: np}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	vec.Data = vec.Data[:len(vec.Data):len(vec.Data)]
	return vec
}

func (v *StdVector) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*gvec.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, ErrVecNotRo
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
	vec := gvec.New(v.Type)
	capacity := types.TypeSize + 4 + nullSize + length*int(v.Type.Size)
	deCompressed.Reset()
	if capacity > deCompressed.Cap() {
		deCompressed.Grow(capacity)
	}
	buf := deCompressed.Bytes()
	buf = buf[:capacity]
	dBuf := buf
	copy(dBuf, types.EncodeType(v.Type))
	dBuf = dBuf[types.TypeSize:]
	copy(dBuf, types.EncodeFixed(uint32(nullSize)))
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

func (v *StdVector) Clone() *StdVector {
	var nullmask *roaring64.Bitmap
	v.RLock()
	size := len(v.Data)
	if v.VMask.Np != nil {
		nullmask = v.VMask.Np.Clone()
	}
	statmask := v.StatMask
	v.RUnlock()
	capacity := uint64(size) / uint64(v.Type.Size)
	cloned := NewStdVector(v.Type, capacity)
	cloned.Data = append(cloned.Data, v.Data[:size]...)
	cloned.StatMask = statmask
	cloned.VMask.Np = nullmask
	return cloned
}

func (v *StdVector) CopyToVector() (*gvec.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, ErrVecNotRo
	}
	length := v.Length()
	vec := gvec.New(v.Type)
	vec.Data = v.Data
	switch v.Type.Oid {
	case types.Type_BOOL:
		col := make([]bool, length)
		curCol := types.DecodeBoolSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_INT8:
		col := make([]int8, length)
		curCol := types.DecodeInt8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_INT16:
		col := make([]int16, length)
		curCol := types.DecodeInt16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_INT32:
		col := make([]int32, length)
		curCol := types.DecodeInt32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_INT64:
		col := make([]int64, length)
		curCol := types.DecodeInt64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_UINT8:
		col := make([]uint8, length)
		curCol := types.DecodeUint8Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_UINT16:
		col := make([]uint16, length)
		curCol := types.DecodeUint16Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_UINT32:
		col := make([]uint32, length)
		curCol := types.DecodeUint32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_UINT64:
		col := make([]uint64, length)
		curCol := types.DecodeUint64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_FLOAT32:
		col := make([]float32, length)
		curCol := types.DecodeFloat32Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_FLOAT64:
		col := make([]float64, length)
		curCol := types.DecodeFloat64Slice(v.Data)
		copy(col[0:], curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_DECIMAL64:
		col := make([]types.Decimal64, length)
		curCol := types.DecodeDecimal64Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_DECIMAL128:
		col := make([]types.Decimal128, length)
		curCol := types.DecodeDecimal128Slice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_DATE:
		col := make([]types.Date, length)
		curCol := types.DecodeDateSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_DATETIME:
		col := make([]types.Datetime, length)
		curCol := types.DecodeDatetimeSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	case types.Type_TIMESTAMP:
		col := make([]types.Timestamp, length)
		curCol := types.DecodeTimestampSlice(v.Data)
		copy(col, curCol[:length])
		vec.Col = col
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(length), &nulls.Nulls{})
	default:
		return nil, ErrVecTypeNotSupport
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
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return
		}
	}
	realSize := types.DecodeFixed[uint64](capBuf)
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
	if len(data) == 0 {
		return nil
	}
	buf := data
	vec.NodeCapacity = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	vec.StatMask = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	vec.Type = types.DecodeType(buf[:types.TypeSize])
	buf = buf[types.TypeSize:]
	nb := types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	if nb > 0 {
		if err := vec.VMask.Read(buf[:nb]); err != nil {
			return err
		}
		buf = buf[nb:]
	}
	if vec.MNode != nil {
		vec.Data = vec.Data[:len(buf)]
		copy(vec.Data[0:], buf)
	} else {
		vec.Data = buf
	}
	return nil
}

func (vec *StdVector) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(types.EncodeFixed(uint64(0)))
	buf.Write(types.EncodeFixed(vec.StatMask))
	buf.Write(types.EncodeType(vec.Type))
	nb, err := vec.VMask.Show()
	if err != nil {
		return nil, err
	}
	buf.Write(types.EncodeFixed(uint32(len(nb))))
	if len(nb) > 0 {
		buf.Write(nb)
	}
	buf.Write(vec.Data)
	buffer := buf.Bytes()
	capBuf := types.EncodeFixed(uint64(len(buffer)))
	copy(buffer[0:], capBuf)
	vec.NodeCapacity = uint64(len(buffer))
	return buf.Bytes(), nil
}

func (vec *StdVector) Reset() {
	vec.Data = nil
}
