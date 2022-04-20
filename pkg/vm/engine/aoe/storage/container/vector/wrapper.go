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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	base "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/pierrec/lz4"
	"io"
	// log "github.com/sirupsen/logrus"
)

type VectorWrapper struct {
	MNode *common.MemNode
	base.Vector
	FreeFunc    buf.MemoryFreeFunc
	File        common.IVFile
	UseCompress bool
}

func (vec *VectorWrapper) Reset() {
	//panic("implement me")
}

func VectorWrapperConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewVectorWrapperNode(vf, useCompress, freeFunc)
}

func NewVectorWrapperNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &VectorWrapper{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
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

func (vec *VectorWrapper) PlacementNew(t types.Type) {
	panic("not supported")
}

func (vec *VectorWrapper) SliceReference(start, end int) (dbi.IVectorReader, error) {
	panic("not supported")
}

func (vec *VectorWrapper) GetType() dbi.VectorType {
	return dbi.Wrapper
}

func (vec *VectorWrapper) Close() error {
	return nil
}

func (vec *VectorWrapper) Capacity() int {
	return base.Length(&vec.Vector)
}

func (vec *VectorWrapper) Length() int {
	return base.Length(&vec.Vector)
}

func (vec *VectorWrapper) Free(p *process.Process) {
	panic("readonly")
}

func (vec *VectorWrapper) Clean(p *process.Process) {
	panic("readonly")
}

func (vec *VectorWrapper) SetCol(col interface{}) {
	panic("readonly")
}

func (vec *VectorWrapper) FreeMemory() {
	if vec.MNode != nil {
		common.GPool.Free(vec.MNode)
	}
	if vec.FreeFunc != nil {
		vec.FreeFunc(vec)
	}
}

func (vec *VectorWrapper) Append(n int, vals interface{}) error {
	return ErrVecWriteRo
}

func (vec *VectorWrapper) GetMemorySize() uint64 {
	return uint64(len(vec.Data))
}

func (vec *VectorWrapper) GetMemoryCapacity() uint64 {
	if vec.UseCompress {
		return uint64(vec.File.Stat().Size())
	} else {
		return uint64(vec.File.Stat().OriginSize())
	}
}

func (vec *VectorWrapper) SetValue(idx int, val interface{}) error {
	return ErrVecWriteRo
}

func (vec *VectorWrapper) GetValue(idx int) (interface{}, error) {
	if idx >= vec.Length() || idx < 0 {
		return nil, ErrVecInvalidOffset
	}
	switch vec.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		val := vec.Col.(*types.Bytes)
		return val.Data[val.Offsets[idx] : val.Offsets[idx]+val.Lengths[idx]], nil
	case types.T_int8:
		return vec.Col.([]int8)[idx], nil
	case types.T_int16:
		return vec.Col.([]int16)[idx], nil
	case types.T_int32:
		return vec.Col.([]int32)[idx], nil
	case types.T_int64:
		return vec.Col.([]int64)[idx], nil
	case types.T_uint8:
		return vec.Col.([]uint8)[idx], nil
	case types.T_uint16:
		return vec.Col.([]uint16)[idx], nil
	case types.T_uint32:
		return vec.Col.([]uint32)[idx], nil
	case types.T_uint64:
		return vec.Col.([]uint64)[idx], nil
	case types.T_float32:
		return vec.Col.([]float32)[idx], nil
	case types.T_float64:
		return vec.Col.([]float64)[idx], nil
	case types.T_date:
		return vec.Col.([]types.Date)[idx], nil
	case types.T_datetime:
		return vec.Col.([]types.Datetime)[idx], nil
	case types.T_sel:
		return vec.Col.([]int64)[idx], nil
	case types.T_tuple:
		return vec.Col.([][]interface{})[idx], nil
	case types.T_decimal64:
		return vec.Col.([]types.Decimal64)[idx], nil
	case types.T_decimal128:
		return vec.Col.([]types.Decimal128)[idx], nil
	default:
		return nil, ErrVecTypeNotSupport
	}
}

func (vec *VectorWrapper) IsNull(idx int) (bool, error) {
	panic("not supported")
}

func (vec *VectorWrapper) HasNull() bool {
	panic("not supported")
}

func (vec *VectorWrapper) NullCnt() int {
	panic("not supported")
}

func (vec *VectorWrapper) IsReadonly() bool {
	panic("not supported")
}

func (vec *VectorWrapper) GetLatestView() IVector {
	panic("not supported")
}

func (vec *VectorWrapper) CopyToVector() (*base.Vector, error) {
	return &vec.Vector, nil
}

func (vec *VectorWrapper) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*base.Vector, error) {
	panic("not supported")
}

func (vec *VectorWrapper) AppendVector(v *base.Vector, offset int) (n int, err error) {
	panic("not supported")
}

func (vec *VectorWrapper) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Show()
	if err != nil {
		return n, err
	}
	stat := vec.File.Stat()
	switch stat.CompressAlgo() {
	case compress.None:
		nw, err := w.Write(buf)
		return int64(nw), err
	case compress.Lz4:
		nb := lz4.CompressBlockBound(len(buf))
		tmp := make([]byte, nb)
		tmp, err = compress.Compress(buf, tmp, compress.Lz4)
		if err != nil {
			return 0, err
		}
		nw, err := w.Write(tmp)
		if err != nil {
			return 0, err
		}
		return int64(nw), nil
	default:
		panic("invalid compress algorithm")
	}
}

func (vec *VectorWrapper) ReadFrom(r io.Reader) (n int64, err error) {
	if vec.UseCompress {
		allocSize := uint64(vec.GetMemoryCapacity())
		vec.MNode = common.GPool.Alloc(allocSize)
		data := vec.MNode.Buf[:allocSize]
		nr, err := r.Read(data)
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}

		return int64(nr), err
	}

	stat := vec.File.Stat()
	// log.Infof("%d, %d, %d", stat.CompressAlgo(), stat.Size(), stat.OriginSize())
	switch stat.CompressAlgo() {
	case compress.None:
		allocSize := uint64(stat.Size())
		vec.MNode = common.GPool.Alloc(allocSize)
		data := vec.MNode.Buf
		nr, err := r.Read(data[:allocSize])
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}
		t := encoding.DecodeType(data[:encoding.TypeSize])
		v := base.New(t)
		vec.Col = v.Col
		err = vec.Vector.Read(data)
		return int64(nr), err
	case compress.Lz4:
		loadSize := uint64(stat.Size())
		originSize := uint64(stat.OriginSize())
		tmpNode := common.GPool.Alloc(loadSize)
		defer common.GPool.Free(tmpNode)
		nr, err := r.Read(tmpNode.Buf[:loadSize])
		if err != nil {
			return n, err
		}
		vec.MNode = common.GPool.Alloc(originSize)
		_, err = compress.Decompress(tmpNode.Buf[:loadSize], vec.MNode.Buf[:originSize], compress.Lz4)
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}
		data := vec.MNode.Buf[:originSize]
		t := encoding.DecodeType(data[:encoding.TypeSize])
		v := base.New(t)
		vec.Col = v.Col
		err = vec.Vector.Read(data)
		if err != nil {
			common.GPool.Free(vec.MNode)
		}
		return int64(nr), err
	default:
		panic("not supported")
	}
}

func (vec *VectorWrapper) ReadWithBuffer(r io.Reader, compressed *bytes.Buffer, deCompressed *bytes.Buffer) (n int64, err error) {
	stat := vec.File.Stat()
	switch stat.CompressAlgo() {
	case compress.None:
		deCompressed.Reset()
		vsize := int(vec.GetMemoryCapacity())
		if vsize > deCompressed.Cap() {
			deCompressed.Grow(vsize)
		}
		buf := deCompressed.Bytes()
		buf = buf[:vsize]
		nr, err := r.Read(buf)
		if err != nil {
			return n, err
		}
		err = vec.Vector.Read(buf)
		if err != nil {
			return n, err
		}
		return int64(nr), err
	case compress.Lz4:
		loadSize := stat.Size()
		originSize := stat.OriginSize()
		compressed.Reset()
		deCompressed.Reset()
		if int(loadSize) > compressed.Cap() {
			compressed.Grow(int(loadSize))
		}
		if int(originSize) > deCompressed.Cap() {
			deCompressed.Grow(int(originSize))
		}
		tmpBuf := compressed.Bytes()
		tmpBuf = tmpBuf[:loadSize]
		buf := deCompressed.Bytes()
		buf = buf[:originSize]
		nr, err := r.Read(tmpBuf)
		if err != nil {
			return n, err
		}
		buf, err = compress.Decompress(tmpBuf, buf, compress.Lz4)
		if err != nil {
			return n, err
		}
		if len(buf) != int(originSize) {
			panic(fmt.Sprintf("invalid decompressed size: %d, %d is expected", len(buf), originSize))
		}
		t := encoding.DecodeType(buf[:encoding.TypeSize])
		v := base.New(t)
		vec.Col = v.Col
		err = vec.Vector.Read(buf)
		return int64(nr), err
	default:
		panic("not supported")
	}
}

func (vec *VectorWrapper) Marshal() ([]byte, error) {
	return vec.Show()
}

func (vec *VectorWrapper) Unmarshal(data []byte) error {
	return vec.Vector.Read(data)
}
