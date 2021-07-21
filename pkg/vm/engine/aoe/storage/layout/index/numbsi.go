package index

import (
	"bytes"
	"io"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/index/bsi"
	// log "github.com/sirupsen/logrus"
)

func NumericBsiIndexConstructor(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return nil
}

type NumericBsiIndex struct {
	bsi.NumericBSI
	T         types.Type
	Col       int16
	AllocSize uint64
	FreeFunc  buf.MemoryFreeFunc
}

func initBsi(t types.Type, bitSize int) *bsi.NumericBSI {
	var bsiIdx bsi.BitSlicedIndex
	switch t.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		bsiIdx = bsi.NewNumericBSI(bitSize, bsi.SignedInt)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		bsiIdx = bsi.NewNumericBSI(bitSize, bsi.UnsignedInt)
	case types.T_float32, types.T_float64:
		bsiIdx = bsi.NewNumericBSI(bitSize, bsi.Float)
	default:
		panic("not supported")
	}
	return bsiIdx.(*bsi.NumericBSI)
}

func NewNumericBsiIndex(t types.Type, bitSize int, colIdx int16) *NumericBsiIndex {
	bsiIdx := initBsi(t, bitSize)
	return &NumericBsiIndex{
		T:          t,
		Col:        colIdx,
		NumericBSI: *bsiIdx,
	}
}

func NewNumericBsiEmptyNode(capacity uint64, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &NumericBsiIndex{
		AllocSize: capacity,
		FreeFunc:  freeFunc,
	}
}

func (i *NumericBsiIndex) GetCol() int16 {
	return i.Col
}

func (i *NumericBsiIndex) Eval(ctx *FilterCtx) error {
	return nil
}

func (i *NumericBsiIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *NumericBsiIndex) Type() base.IndexType {
	return base.NumBsi
}

func (i *NumericBsiIndex) GetMemorySize() uint64 {
	return i.AllocSize
}

func (i *NumericBsiIndex) GetMemoryCapacity() uint64 {
	return i.AllocSize
}

func (i *NumericBsiIndex) Reset() {
}

func (i *NumericBsiIndex) ReadFrom(r io.Reader) (n int64, err error) {
	data := make([]byte, i.AllocSize)
	nr, err := r.Read(data)
	if err != nil {
		return n, err
	}
	buf := data[2 : 2+encoding.TypeSize]
	i.T = encoding.DecodeType(buf)
	i.NumericBSI = *initBsi(i.T, 0)
	err = i.Unmarshall(data)
	return int64(nr), err
}

func (i *NumericBsiIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshall()
	if err != nil {
		return n, err
	}

	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *NumericBsiIndex) Unmarshall(data []byte) error {
	buf := data
	i.Col = encoding.DecodeInt16(buf[:2])
	buf = buf[2:]
	i.T = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	return i.NumericBSI.Unmarshall(buf)
}

func (i *NumericBsiIndex) Marshall() ([]byte, error) {
	var bw bytes.Buffer
	bw.Write(encoding.EncodeInt16(i.Col))
	bw.Write(encoding.EncodeType(i.T))
	indexBuf, err := i.NumericBSI.Marshall()
	if err != nil {
		return nil, err
	}
	bw.Write(indexBuf)
	return bw.Bytes(), nil
}
