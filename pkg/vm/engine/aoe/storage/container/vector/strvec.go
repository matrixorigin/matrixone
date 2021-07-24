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
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"os"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

func StrVectorConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStrVectorNode(vf, useCompress, freeFunc)
}

func NewStrVector(t types.Type, capacity uint64) IVector {
	return &StrVector{
		BaseVector: BaseVector{
			Type:  t,
			VMask: &nulls.Nulls{},
		},
		Data: &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 0, capacity),
			Lengths: make([]uint32, 0, capacity),
		},
	}
}

func NewStrVectorNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	n := &StrVector{
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
		File:        vf,
		UseCompress: useCompress,
		FreeFunc:    freeFunc,
		// Data: &types.Bytes{
		// 	Data:    make([]byte, 0),
		// 	Offsets: make([]uint32, 0),
		// 	Lengths: make([]uint32, 0),
		// },
	}
	return n
}

func NewEmptyStrVector() IVector {
	return &StrVector{
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
		Data: &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 0),
			Lengths: make([]uint32, 0),
		},
	}
}

func (v *StrVector) PlacementNew(t types.Type, capacity uint64) {
	v.Type = t
	v.Data = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0, capacity),
		Lengths: make([]uint32, 0, capacity),
	}
}

func (v *StrVector) GetType() dbi.VectorType {
	return dbi.StrVec
}

func (v *StrVector) Close() error {
	v.VMask = nil
	v.Data = nil
	return nil
}

func (v *StrVector) Capacity() int {
	return cap(v.Data.Lengths)
}

func (v *StrVector) FreeMemory() {
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *StrVector) GetMemorySize() uint64 {
	v.RLock()
	defer v.RUnlock()
	return uint64(2*4*len(v.Data.Lengths) + len(v.Data.Data))
}

func (v *StrVector) GetMemoryCapacity() uint64 {
	if v.UseCompress {
		return uint64(v.File.Stat().Size())
	} else {
		return uint64(v.File.Stat().OriginSize())
	}
}

func (v *StrVector) SetValue(idx int, val interface{}) {
	panic("not supported")
}

func (v *StrVector) GetValue(idx int) interface{} {
	if idx >= v.Length() || idx < 0 {
		panic(fmt.Sprintf("idx %d is out of range", idx))
	}
	if !v.IsReadonly() {
		v.RLock()
	}
	data := v.Data.Get(int64(idx))
	if !v.IsReadonly() {
		v.RUnlock()
	}
	return data
}

func (v *StrVector) Append(n int, vals interface{}) error {
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
	pos := uint64(len(v.Data.Lengths)) & container.PosMask
	mask = mask | pos
	if len(v.Data.Lengths) == cap(v.Data.Lengths) {
		mask = mask | container.ReadonlyMask
	}
	atomic.StoreUint64(&v.StatMask, mask)
	return nil
}

func (v *StrVector) appendWithOffset(offset, n int, vals interface{}) error {
	var data [][]byte
	switch v.Type.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		data = vals.([][]byte)[offset : offset+n]
	default:
		panic("not supported yet")
	}
	if len(v.Data.Offsets)+len(data) > cap(v.Data.Offsets) {
		panic(fmt.Sprintf("overflow: offset %d, %d + %d > %d", offset, len(v.Data.Offsets), len(data), cap(v.Data.Offsets)))
	}
	v.Data.Append(vals.([][]byte)[offset : offset+n])
	return nil
}

func (v *StrVector) AppendVector(vec *ro.Vector, offset int) (n int, err error) {
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

	dataBytes := vec.Col.(*types.Bytes)
	insert := make([][]byte, 0, len(dataBytes.Lengths))
	for i := 0; i < len(dataBytes.Lengths); i++ {
		s := dataBytes.Offsets[i]
		e := s + dataBytes.Lengths[i]
		insert = append(insert, dataBytes.Data[s:e])
	}

	err = v.appendWithOffset(offset, n, insert)
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
	mask := v.StatMask & (^container.PosMask)
	pos := uint64(len(v.Data.Lengths)) & container.PosMask
	mask = mask | pos
	if len(v.Data.Lengths) == cap(v.Data.Lengths) {
		mask = mask | container.ReadonlyMask
	}
	if v.VMask.Any() {
		mask = mask | container.HasNullMask
	}
	atomic.StoreUint64(&v.StatMask, mask)

	return n, err
}

func (v *StrVector) SliceReference(start, end int) dbi.IVectorReader {
	if !v.IsReadonly() {
		panic("should call this in ro mode")
	}
	mask := container.ReadonlyMask | (uint64(end-start) & container.PosMask)
	vec := &StrVector{
		BaseVector: BaseVector{
			Type: v.Type,
		},
		Data: v.Data.Window(start, end),
	}
	if v.VMask.Np != nil {
		vmask := v.VMask.Range(uint64(start), uint64(end))
		vec.VMask = vmask
		if vmask.Any() {
			mask = mask | container.HasNullMask
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	vec.StatMask = mask
	return vec
}

func (v *StrVector) GetLatestView() IVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & container.PosMask)
	vec := &StrVector{
		BaseVector: BaseVector{
			StatMask: container.ReadonlyMask | mask,
			Type:     v.Type,
		},
		Data: v.Data.Window(0, int(endPos)),
	}
	if mask&container.HasNullMask != 0 {
		if mask&container.ReadonlyMask == 0 {
			vec.VMask = v.VMask.Range(0, uint64(endPos))
		} else {
			vec.VMask = v.VMask.Range(0, uint64(endPos))
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	return vec
}

func (v *StrVector) CopyToVector() *ro.Vector {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		panic("should call in ro mode")
	}
	vec := ro.New(v.Type)
	switch v.Type.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		col := vec.Col.(*types.Bytes)
		col.Data = make([]byte, len(v.Data.Data))
		col.Lengths = make([]uint32, len(v.Data.Lengths))
		col.Offsets = make([]uint32, len(v.Data.Offsets))
		copy(col.Data[0:], v.Data.Data)
		copy(col.Lengths[0:], v.Data.Lengths)
		copy(col.Offsets[0:], v.Data.Offsets)
	default:
		panic("not supported yet")
	}
	return vec
}

func (vec *StrVector) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Marshall()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (vec *StrVector) ReadFrom(r io.Reader) (n int64, err error) {
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

func (vec *StrVector) Unmarshall(data []byte) error {
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
	cnt := encoding.DecodeInt32(buf[:4])
	buf = buf[4:]
	if vec.Data != nil {
		vec.Data.Reset()
	} else {
		vec.Data = &types.Bytes{}
	}
	if cnt == 0 {
		return nil
	}
	vec.Data.Offsets = make([]uint32, cnt)
	vec.Data.Lengths = encoding.DecodeUint32Slice(buf[:4*cnt])
	vec.Data.Data = buf[4*cnt:]
	offset := uint32(0)
	for i, n := range vec.Data.Lengths {
		vec.Data.Offsets[i] = offset
		offset += n
	}

	return nil
}

func (vec *StrVector) Marshall() ([]byte, error) {
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
	cnt := int32(len(vec.Data.Lengths))
	buf.Write(encoding.EncodeInt32(cnt))
	if cnt > 0 {
		buf.Write(encoding.EncodeUint32Slice(vec.Data.Lengths))
		buf.Write(vec.Data.Data)
	}

	buffer := buf.Bytes()
	capBuf := encoding.EncodeUint64(uint64(len(buffer)))
	copy(buffer[0:], capBuf)
	vec.NodeCapacity = uint64(len(buffer))
	return buf.Bytes(), nil
}

func (vec *StrVector) Reset() {
	vec.Data = nil
}
