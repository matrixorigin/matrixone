package vector

import (
	"fmt"
	"io"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	base "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/mempool"
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
		vec.MNode = common.GPool.Alloc(allocSize + mempool.CountSize)
		data := vec.MNode.Buf
		nr, err := r.Read(data[mempool.CountSize : allocSize+mempool.CountSize])
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}
		t := encoding.DecodeType(data[mempool.CountSize : encoding.TypeSize+mempool.CountSize])
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
		vec.MNode = common.GPool.Alloc(originSize + mempool.CountSize)
		_, err = compress.Decompress(tmpNode.Buf[:loadSize], vec.MNode.Buf[mempool.CountSize:originSize+mempool.CountSize], compress.Lz4)
		if err != nil {
			common.GPool.Free(vec.MNode)
			return n, err
		}
		data := vec.MNode.Buf[:mempool.CountSize+originSize]
		t := encoding.DecodeType(data[mempool.CountSize : encoding.TypeSize+mempool.CountSize])
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

func (vec *VectorWrapper) ReadWithProc(r io.Reader, ref uint64, proc *process.Process) (n int64, err error) {
	stat := vec.File.Stat()
	// log.Infof("%d, %d, %d", stat.CompressAlgo(), stat.Size(), stat.OriginSize())
	switch stat.CompressAlgo() {
	case compress.None:
		allocSize := int64(vec.GetMemoryCapacity())
		data, err := proc.Alloc(allocSize)
		if err != nil {
			return n, err
		}
		// node := common.GPool.Alloc(uint64(allocSize) + mempool.CountSize)
		// data := node.Buf
		buf := data[:mempool.CountSize+allocSize]
		nr, err := r.Read(buf[mempool.CountSize:])
		if err != nil {
			proc.Free(data)
			return n, err
		}
		err = vec.Vector.Read(data[:mempool.CountSize+allocSize])
		if err != nil {
			proc.Free(data)
			return n, err
		}
		copy(data, encoding.EncodeUint64(ref))
		// common.GPool.Free(node)
		return int64(nr), err
	case compress.Lz4:
		loadSize := uint64(stat.Size())
		originSize := stat.OriginSize()
		tmpNode := common.GPool.Alloc(loadSize + mempool.CountSize)
		defer common.GPool.Free(tmpNode)
		nr, err := r.Read(tmpNode.Buf[mempool.CountSize : loadSize+mempool.CountSize])
		if err != nil {
			return n, err
		}
		// node := common.GPool.Alloc(uint64(originSize) + mempool.CountSize)
		// data := node.Buf
		data, err := proc.Alloc(originSize)
		buf, err := compress.Decompress(tmpNode.Buf[mempool.CountSize:mempool.CountSize+loadSize], data[mempool.CountSize:originSize+mempool.CountSize], compress.Lz4)
		if len(buf) != int(originSize) {
			panic(fmt.Sprintf("invalid decompressed size: %d, %d is expected", len(buf), originSize))
		}
		if err != nil {
			proc.Free(data)
			return n, err
		}
		t := encoding.DecodeType(data[mempool.CountSize : encoding.TypeSize+mempool.CountSize])
		v := base.New(t)
		vec.Col = v.Col
		err = vec.Vector.Read(data[:mempool.CountSize+originSize])
		copy(data, encoding.EncodeUint64(ref))
		// common.GPool.Free(node)
		return int64(nr), err
	default:
		panic("not supported")
	}
}

func (vec *VectorWrapper) Marshall() ([]byte, error) {
	return vec.Show()
}

func (vec *VectorWrapper) Unmarshall(data []byte) error {
	return vec.Vector.Read(data)
}
