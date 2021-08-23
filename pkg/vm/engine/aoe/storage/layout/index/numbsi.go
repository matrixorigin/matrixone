package index

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/index/bsi"
	// log "github.com/sirupsen/logrus"
)

func NumericBsiIndexConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewNumericBsiEmptyNode(vf, useCompress, freeFunc)
}

type NumericBsiIndex struct {
	bsi.NumericBSI
	T         types.Type
	Col       int16
	File        common.IVFile
	UseCompress bool
	FreeFunc  buf.MemoryFreeFunc
}

func initNumericBsi(t types.Type, bitSize int) *bsi.NumericBSI {
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
	bsiIdx := initNumericBsi(t, bitSize)
	return &NumericBsiIndex{
		T:          t,
		Col:        colIdx,
		NumericBSI: *bsiIdx,
	}
}

func NewNumericBsiEmptyNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &NumericBsiIndex{
		File: vf,
		UseCompress: useCompress,
		FreeFunc:  freeFunc,
	}
}

func (i *NumericBsiIndex) GetCol() int16 {
	return i.Col
}

func (i *NumericBsiIndex) Eval(ctx *FilterCtx) error {
	var err error
	switch ctx.Op {
	case OpEq:
		ctx.BMRes, err = i.Eq(ctx.Val, ctx.BMRes)
	case OpNe:
		ctx.BMRes, err = i.Ne(ctx.Val, ctx.BMRes)
	case OpGe:
		ctx.BMRes, err = i.Ge(ctx.Val, ctx.BMRes)
	case OpGt:
		ctx.BMRes, err = i.Gt(ctx.Val, ctx.BMRes)
	case OpLe:
		ctx.BMRes, err = i.Le(ctx.Val, ctx.BMRes)
	case OpLt:
		ctx.BMRes, err = i.Lt(ctx.Val, ctx.BMRes)
	case OpIn:
		bm := ctx.BMRes.Clone()
		ctx.BMRes, err = i.Ge(ctx.ValMin, ctx.BMRes)
		if err != nil {
			return err
		}
		bm, err = i.Le(ctx.ValMax, bm)
		if err != nil {
			return err
		}
		ctx.BMRes.And(bm)
	case OpOut:
		bm := ctx.BMRes.Clone()
		ctx.BMRes, err = i.Gt(ctx.ValMax, ctx.BMRes)
		if err != nil {
			return err
		}
		bm, err = i.Lt(ctx.ValMin, bm)
		if err != nil {
			return err
		}
		ctx.BMRes.Or(bm)
	}
	log.Info(ctx.BMRes)
	return err
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
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *NumericBsiIndex) GetMemoryCapacity() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *NumericBsiIndex) Reset() {
}

func (i *NumericBsiIndex) ReadFrom(r io.Reader) (n int64, err error) {
	buf := make([]byte, i.GetMemoryCapacity())
	nr, err := r.Read(buf)
	if err != nil {
		return int64(nr), err
	}
	err = i.Unmarshall(buf)
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
