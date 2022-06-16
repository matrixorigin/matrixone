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

	"github.com/matrixorigin/matrixone/pkg/compress"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/pierrec/lz4"
)

type VectorWrapper struct {
	MNode *common.MemNode
	gvec.Vector
	FreeFunc    base.MemoryFreeFunc
	File        common.IVFile
	UseCompress bool
}

func (v *VectorWrapper) Reset() {
	//panic("implement me")
}

func VectorWrapperConstructor(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
	return NewVectorWrapperNode(vf, useCompress, freeFunc)
}

func NewVectorWrapperNode(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
	return &VectorWrapper{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

func NewVectorWrapper(vec *gvec.Vector) *VectorWrapper {
	return &VectorWrapper{
		Vector: *vec,
	}
}

func NewEmptyWrapper(t types.Type) *VectorWrapper {
	return &VectorWrapper{
		Vector: *gvec.New(t),
	}
}

func (v *VectorWrapper) PlacementNew(t types.Type) {
	panic("not supported")
}

func (v *VectorWrapper) SliceReference(start, end int) (container.IVectorReader, error) {
	panic("not supported")
}

func (v *VectorWrapper) GetType() container.VectorType {
	return container.Wrapper
}

func (v *VectorWrapper) Close() error {
	return nil
}

func (v *VectorWrapper) Capacity() int {
	return gvec.Length(&v.Vector)
}

func (v *VectorWrapper) Length() int {
	return gvec.Length(&v.Vector)
}

func (v *VectorWrapper) Free(p *process.Process) {
	panic("readonly")
}

func (v *VectorWrapper) Clean(p *process.Process) {
	panic("readonly")
}

func (v *VectorWrapper) SetCol(col any) {
	panic("readonly")
}

func (v *VectorWrapper) FreeMemory() {
	if v.MNode != nil {
		common.GPool.Free(v.MNode)
	}
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *VectorWrapper) Append(n int, vals any) error {
	return ErrVecWriteRo
}

func (v *VectorWrapper) GetMemorySize() uint64 {
	return uint64(len(v.Data))
}

func (v *VectorWrapper) GetMemoryCapacity() uint64 {
	if v.UseCompress {
		return uint64(v.File.Stat().Size())
	} else {
		return uint64(v.File.Stat().OriginSize())
	}
}

func (v *VectorWrapper) SetValue(idx int, val any) error {
	return ErrVecWriteRo
}

func (v *VectorWrapper) GetValue(idx int) (any, error) {
	if idx >= v.Length() || idx < 0 {
		return nil, ErrVecInvalidOffset
	}
	switch v.Typ.Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		val := v.Col.(*types.Bytes)
		return val.Data[val.Offsets[idx] : val.Offsets[idx]+val.Lengths[idx]], nil
	case types.Type_INT8:
		return v.Col.([]int8)[idx], nil
	case types.Type_INT16:
		return v.Col.([]int16)[idx], nil
	case types.Type_INT32:
		return v.Col.([]int32)[idx], nil
	case types.Type_INT64:
		return v.Col.([]int64)[idx], nil
	case types.Type_UINT8:
		return v.Col.([]uint8)[idx], nil
	case types.Type_UINT16:
		return v.Col.([]uint16)[idx], nil
	case types.Type_UINT32:
		return v.Col.([]uint32)[idx], nil
	case types.Type_UINT64:
		return v.Col.([]uint64)[idx], nil
	case types.Type_DECIMAL64:
		return v.Col.([]types.Decimal64)[idx], nil
	case types.Type_FLOAT32:
		return v.Col.([]float32)[idx], nil
	case types.Type_FLOAT64:
		return v.Col.([]float64)[idx], nil
	case types.Type_DATE:
		return v.Col.([]types.Date)[idx], nil
	case types.Type_DATETIME:
		return v.Col.([]types.Datetime)[idx], nil
	case types.Type_SEL:
		return v.Col.([]int64)[idx], nil
	case types.Type_TUPLE:
		return v.Col.([][]any)[idx], nil
	default:
		return nil, ErrVecTypeNotSupport
	}
}

func (v *VectorWrapper) IsNull(idx int) (bool, error) {
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

func (v *VectorWrapper) CopyToVector() (*gvec.Vector, error) {
	return &v.Vector, nil
}

func (v *VectorWrapper) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*gvec.Vector, error) {
	panic("not supported")
}

func (v *VectorWrapper) AppendVector(vec *gvec.Vector, offset int) (n int, err error) {
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
	// logutil.Infof("%d, %d, %d", stat.CompressAlgo(), stat.Size(), stat.OriginSize())
	switch stat.CompressAlgo() {
	case compress.None:
		allocSize := uint64(stat.Size())
		vec.MNode = common.GPool.Alloc(allocSize)
		data := vec.MNode.Buf[:allocSize]
		nr, err := r.Read(data)
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}
		t := types.DecodeType(data[:types.TypeSize])
		v := gvec.New(t)
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
		t := types.DecodeType(data[:types.TypeSize])
		v := gvec.New(t)
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
		var tmpBuf []byte

		if compressed == nil {
			tmpNode := common.GPool.Alloc(uint64(loadSize))
			defer common.GPool.Free(tmpNode)
			tmpBuf = tmpNode.Buf
		} else {
			compressed.Reset()
			if int(loadSize) > compressed.Cap() {
				compressed.Grow(int(loadSize))
			}
			tmpBuf = compressed.Bytes()
		}
		tmpBuf = tmpBuf[:loadSize]

		deCompressed.Reset()
		if int(originSize) > deCompressed.Cap() {
			deCompressed.Grow(int(originSize))
		}
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
		t := types.DecodeType(buf[:types.TypeSize])
		v := gvec.New(t)
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
