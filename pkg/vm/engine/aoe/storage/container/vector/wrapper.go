package vector

import (
	"bytes"
	"fmt"
	"io"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	base "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/process"
	// log "github.com/sirupsen/logrus"
)

type VectorWrapper struct {
	MNode *common.MemNode
	base.Vector
	FreeFunc    buf.MemoryFreeFunc
	File        common.IVFile
	UseCompress bool
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

func (v *VectorWrapper) PlacementNew(t types.Type) {
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
	if v.MNode != nil {
		common.GPool.Free(v.MNode)
	}
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
	if v.UseCompress {
		return uint64(v.File.Stat().Size())
	} else {
		return uint64(v.File.Stat().OriginSize())
	}
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
	return &v.Vector
}

func (v *VectorWrapper) CopyToVectorWithProc(_ uint64, _ *process.Process) (*base.Vector, error) {
	panic("not supported")
}

func (v *VectorWrapper) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*base.Vector, error) {
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
		nr, err := r.Read(data[: allocSize])
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
		t := encoding.DecodeType(data[: encoding.TypeSize])
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
			deCompressed.Grow(vsize - deCompressed.Cap())
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
			compressed.Grow(int(loadSize) - compressed.Cap())
		}
		if int(originSize) > deCompressed.Cap() {
			deCompressed.Grow(int(originSize) - deCompressed.Cap())
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

func (vec *VectorWrapper) ReadWithProc(r io.Reader, ref uint64, proc *process.Process) (n int64, err error) {
	return 0, nil
}

func (vec *VectorWrapper) Marshall() ([]byte, error) {
	return vec.Show()
}

func (vec *VectorWrapper) Unmarshall(data []byte) error {
	return vec.Vector.Read(data)
}
