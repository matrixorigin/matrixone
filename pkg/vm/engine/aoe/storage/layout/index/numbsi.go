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

package index

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/index/bsi"
	"io"
	// log "github.com/sirupsen/logrus"
)

func NumericBsiIndexConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewNumericBsiEmptyNode(vf, useCompress, freeFunc)
}

type NumericBsiIndex struct {
	bsi.NumericBSI
	T           types.Type
	Col         int16
	File        common.IVFile
	UseCompress bool
	FreeFunc    buf.MemoryFreeFunc
}

func initNumericBsi(t types.Type, bitSize int) *bsi.NumericBSI {
	var bsiIdx bsi.BitSlicedIndex
	switch t.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_date, types.T_datetime:
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
		File:        vf,
		UseCompress: useCompress,
		FreeFunc:    freeFunc,
	}
}

func (i *NumericBsiIndex) GetCol() int16 {
	return i.Col
}

func (i *NumericBsiIndex) Eval(ctx *FilterCtx) error {
	if ctx.BMRes.IsEmpty() {
		return nil
	}
	var err error
	if v, ok := ctx.Val.(types.Date); ok {
		ctx.Val = int32(v)
		defer func() {
			ctx.Val = v
		}()
	}
	if v, ok := ctx.ValMin.(types.Date); ok {
		ctx.ValMin = int32(v)
		defer func() {
			ctx.ValMin = v
		}()
	}
	if v, ok := ctx.ValMax.(types.Date); ok {
		ctx.ValMax = int32(v)
		defer func() {
			ctx.ValMax = v
		}()
	}
	if v, ok := ctx.Val.(types.Datetime); ok {
		ctx.Val = int64(v)
		defer func() {
			ctx.Val = v
		}()
	}
	if v, ok := ctx.ValMin.(types.Datetime); ok {
		ctx.ValMin = int64(v)
		defer func() {
			ctx.ValMin = v
		}()
	}
	if v, ok := ctx.ValMax.(types.Datetime); ok {
		ctx.ValMax = int64(v)
		defer func() {
			ctx.ValMax = v
		}()
	}
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
	return err
}

func (i *NumericBsiIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *NumericBsiIndex) IndexFile() common.IVFile {
	return i.File
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
	err = i.Unmarshal(buf)
	return int64(nr), err
}

func (i *NumericBsiIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshal()
	if err != nil {
		return n, err
	}

	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *NumericBsiIndex) Unmarshal(data []byte) error {
	buf := data
	i.Col = encoding.DecodeInt16(buf[:2])
	buf = buf[2:]
	i.T = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	return i.NumericBSI.Unmarshal(buf)
}

func (i *NumericBsiIndex) Marshal() ([]byte, error) {
	var bw bytes.Buffer
	bw.Write(encoding.EncodeInt16(i.Col))
	bw.Write(encoding.EncodeType(i.T))
	indexBuf, err := i.NumericBSI.Marshal()
	if err != nil {
		return nil, err
	}
	bw.Write(indexBuf)
	return bw.Bytes(), nil
}

func BuildNumericBsiIndex(data []*vector.Vector, t types.Type, colIdx int16) (bsi.BitSlicedIndex, error) {
	switch t.Oid {
	case types.T_int8:
		bsiIdx := NewNumericBsiIndex(t, 8, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]int8)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int16:
		bsiIdx := NewNumericBsiIndex(t, 16, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]int16)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]int32)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_int64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]int64)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint8:
		bsiIdx := NewNumericBsiIndex(t, 8, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]uint8)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint16:
		bsiIdx := NewNumericBsiIndex(t, 16, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]uint16)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]uint32)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_uint64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]uint64)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_float32:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]float32)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_float64:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]float64)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), val); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_datetime:
		bsiIdx := NewNumericBsiIndex(t, 64, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]types.Datetime)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), int64(val)); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	case types.T_date:
		bsiIdx := NewNumericBsiIndex(t, 32, colIdx)
		row := 0
		for _, part := range data {
			column := part.Col.([]types.Date)
			for _, val := range column {
				if err := bsiIdx.Set(uint64(row), int32(val)); err != nil {
					return nil, err
				}
				row++
			}
		}
		return bsiIdx, nil
	default:
		panic("unsupported type")
	}
}
