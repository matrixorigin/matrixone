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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/index/bsi"
	"io"
	// log "github.com/sirupsen/logrus"
)

func StringBsiIndexConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewStringBsiEmptyNode(vf, useCompress, freeFunc)
}

type StringBsiIndex struct {
	bsi.StringBSI
	T         types.Type
	Col       int16
	File        common.IVFile
	UseCompress bool
	FreeFunc    buf.MemoryFreeFunc
	Offset uint64
}

func NewStringBsiIndex(t types.Type, colIdx int16, startPos uint64) *StringBsiIndex {
	bsiIdx := getStringBsi(t)
	return &StringBsiIndex{
		T:         t,
		Col:       colIdx,
		StringBSI: *bsiIdx,
		Offset: startPos,
	}
}

func getStringBsi(t types.Type) *bsi.StringBSI {
	var bsiIdx bsi.BitSlicedIndex
	switch t.Oid {
	case types.T_char, types.T_varchar:
		// upper layer use `Width` to represent the maximum
		// number of characters, which is equal to our bsi
		// `charSize`
		bsiIdx = bsi.NewStringBSI(8, int(t.Width))
	default:
		panic("not supported")
	}
	return bsiIdx.(*bsi.StringBSI)
}

func NewStringBsiEmptyNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return &StringBsiIndex{
		File:        vf,
		UseCompress: useCompress,
		FreeFunc:    freeFunc,
	}
}

func (i *StringBsiIndex) GetCol() int16 {
	return i.Col
}

func (i *StringBsiIndex) Eval(ctx *FilterCtx) error {
	if ctx.BMRes.IsEmpty() {
		return nil
	}
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
	return err
}

func (i *StringBsiIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *StringBsiIndex) IndexFile() common.IVFile {
	return i.File
}

func (i *StringBsiIndex) Type() base.IndexType {
	return base.FixStrBsi
}

func (i *StringBsiIndex) GetMemorySize() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *StringBsiIndex) GetMemoryCapacity() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *StringBsiIndex) Reset() {
}

func (i *StringBsiIndex) ReadFrom(r io.Reader) (n int64, err error) {
	buf := make([]byte, i.GetMemoryCapacity())
	nr, err := r.Read(buf)
	if err != nil {
		return int64(nr), err
	}
	err = i.Unmarshal(buf)
	return int64(nr), err
}

func (i *StringBsiIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshal()
	if err != nil {
		return n, err
	}

	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *StringBsiIndex) Unmarshal(data []byte) error {
	buf := data
	i.Col = encoding.DecodeInt16(buf[:2])
	buf = buf[2:]
	i.T = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	i.Offset = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	return i.StringBSI.Unmarshal(buf)
}

func (i *StringBsiIndex) Marshal() ([]byte, error) {
	var bw bytes.Buffer
	bw.Write(encoding.EncodeInt16(i.Col))
	bw.Write(encoding.EncodeType(i.T))
	bw.Write(encoding.EncodeUint64(i.Offset))
	indexBuf, err := i.StringBSI.Marshal()
	if err != nil {
		return nil, err
	}
	bw.Write(indexBuf)
	return bw.Bytes(), nil
}

func (i *StringBsiIndex) Get(k uint64) (interface{}, bool) {
	k = k - i.Offset
	return i.StringBSI.Get(k)
}

func (i *StringBsiIndex) Set(k uint64, e interface{}) error {
	k = k - i.Offset
	return i.StringBSI.Set(k, e)
}

func (i *StringBsiIndex) Eq(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Eq(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) Ne(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Ne(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) Lt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Lt(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) Le(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Le(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) Gt(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Gt(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) Ge(e interface{}, filter *roaring.Bitmap) (*roaring.Bitmap, error) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res, err := i.StringBSI.Ge(e, filter)
	if err != nil {
		return nil, err
	}
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res, nil
}

func (i *StringBsiIndex) NotNull(filter *roaring.Bitmap) *roaring.Bitmap {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	res := i.StringBSI.NotNull(filter)
	if res != nil {
		arr := res.ToArray()
		out := roaring.NewBitmap()
		for _, n := range arr {
			out.Add(n + uint32(i.Offset))
		}
		res = out
	}
	return res
}

func (i *StringBsiIndex) Count(filter *roaring.Bitmap) uint64 {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	return i.StringBSI.Count(filter)
}

func (i *StringBsiIndex) NullCount(filter *roaring.Bitmap) uint64 {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	return i.StringBSI.NullCount(filter)
}

func (i *StringBsiIndex) Min(filter *roaring.Bitmap) (interface{}, uint64) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	return i.StringBSI.Min(filter)
}

func (i *StringBsiIndex) Max(filter *roaring.Bitmap) (interface{}, uint64) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	return i.StringBSI.Max(filter)
}

func (i *StringBsiIndex) Sum(filter *roaring.Bitmap) (interface{}, uint64) {
	if filter != nil {
		arr := filter.ToArray()
		in := roaring.NewBitmap()
		for _, n := range arr {
			if uint64(n) < i.Offset {
				continue
			}
			in.Add(n - uint32(i.Offset))
		}
		filter = in
	}
	return i.StringBSI.Sum(filter)
}