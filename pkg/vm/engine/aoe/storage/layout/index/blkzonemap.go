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
	"io"
)

func BlockZoneMapIndexConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewBlockZoneMapEmptyNode(vf, useCompress, freeFunc)
}

type BlockZoneMapIndex struct {
	T           types.Type
	MinV        interface{}
	MaxV        interface{}
	Col         int16
	FreeFunc    buf.MemoryFreeFunc
	File        common.IVFile
	UseCompress bool
}

func NewBlockZoneMap(t types.Type, minv, maxv interface{}, colIdx int16) Index {
	return &BlockZoneMapIndex{
		T:    t,
		MinV: minv,
		MaxV: maxv,
		Col:  colIdx,
	}
}

func NewBlockZoneMapEmptyNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) Index {
	return &BlockZoneMapIndex{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

func (i *BlockZoneMapIndex) GetCol() int16 {
	return i.Col
}

func (i *BlockZoneMapIndex) Eval(ctx *FilterCtx) error {
	switch ctx.Op {
	case OpEq:
		ctx.BoolRes = i.Eq(ctx.Val)
	case OpNe:
		ctx.BoolRes = i.Ne(ctx.Val)
	case OpGe:
		ctx.BoolRes = i.Ge(ctx.Val)
	case OpGt:
		ctx.BoolRes = i.Gt(ctx.Val)
	case OpLe:
		ctx.BoolRes = i.Le(ctx.Val)
	case OpLt:
		ctx.BoolRes = i.Lt(ctx.Val)
	case OpIn:
		ctx.BoolRes = i.Btw(ctx.ValMin, ctx.ValMax)
	case OpOut:
		ctx.BoolRes = i.Lt(ctx.ValMin) || i.Gt(ctx.ValMax)
	}
	return nil
}

func (i *BlockZoneMapIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *BlockZoneMapIndex) IndexFile() common.IVFile {
	return i.File
}

func (i *BlockZoneMapIndex) GetMemorySize() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *BlockZoneMapIndex) GetMemoryCapacity() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *BlockZoneMapIndex) Reset() {
}

func (i *BlockZoneMapIndex) ReadFrom(r io.Reader) (n int64, err error) {
	buf := make([]byte, i.GetMemoryCapacity())
	nr, err := r.Read(buf)
	if err != nil {
		return int64(nr), err
	}
	err = i.Unmarshal(buf)
	return int64(nr), err
}

func (i *BlockZoneMapIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshal()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *BlockZoneMapIndex) Unmarshal(data []byte) error {
	buf := data
	i.Col = encoding.DecodeInt16(buf[:2])
	buf = buf[2:]
	i.T = encoding.DecodeType(buf[:encoding.TypeSize])
	buf = buf[encoding.TypeSize:]
	switch i.T.Oid {
	case types.T_int8:
		i.MinV = encoding.DecodeInt8(buf[:1])
		buf = buf[1:]
		i.MaxV = encoding.DecodeInt8(buf[:1])
		return nil
	case types.T_int16:
		i.MinV = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		i.MaxV = encoding.DecodeInt16(buf[:2])
		return nil
	case types.T_int32:
		i.MinV = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeInt32(buf[:4])
		return nil
	case types.T_int64:
		i.MinV = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeInt64(buf[:8])
		return nil
	case types.T_uint8:
		i.MinV = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		i.MaxV = encoding.DecodeUint8(buf[:1])
		return nil
	case types.T_uint16:
		i.MinV = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		i.MaxV = encoding.DecodeUint16(buf[:2])
		return nil
	case types.T_uint32:
		i.MinV = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeUint32(buf[:4])
		return nil
	case types.T_uint64:
		i.MinV = encoding.DecodeUint32(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeUint32(buf[:8])
		return nil
	case types.T_float32:
		i.MinV = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeFloat32(buf[:4])
		return nil
	case types.T_float64:
		i.MinV = encoding.DecodeFloat64(buf[:8])
		buf = buf[4:]
		i.MaxV = encoding.DecodeFloat64(buf[:8])
		return nil
	case types.T_date:
		i.MinV = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeDate(buf[:4])
		buf = buf[4:]
		return nil
	case types.T_datetime:
		i.MinV = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeDatetime(buf[:8])
		buf = buf[8:]
		return nil
	case types.T_char, types.T_varchar, types.T_json:
		lenminv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		minBuf := make([]byte, int(lenminv))
		copy(minBuf, buf[:int(lenminv)])
		buf = buf[int(lenminv):]

		lenmaxv := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		maxBuf := make([]byte, int(lenmaxv))
		copy(maxBuf, buf[:int(lenmaxv)])
		i.MinV = minBuf
		i.MaxV = maxBuf
		return nil
	}
	panic("unsupported")
}

func (i *BlockZoneMapIndex) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeInt16(i.Col))
	switch i.T.Oid {
	case types.T_int8:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt8(i.MinV.(int8)))
		buf.Write(encoding.EncodeInt8(i.MaxV.(int8)))
		return buf.Bytes(), nil
	case types.T_int16:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt16(i.MinV.(int16)))
		buf.Write(encoding.EncodeInt16(i.MaxV.(int16)))
		return buf.Bytes(), nil
	case types.T_int32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt32(i.MinV.(int32)))
		buf.Write(encoding.EncodeInt32(i.MaxV.(int32)))
		return buf.Bytes(), nil
	case types.T_int64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt64(i.MinV.(int64)))
		buf.Write(encoding.EncodeInt64(i.MaxV.(int64)))
		return buf.Bytes(), nil
	case types.T_uint8:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint8(i.MinV.(uint8)))
		buf.Write(encoding.EncodeUint8(i.MaxV.(uint8)))
		return buf.Bytes(), nil
	case types.T_uint16:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint16(i.MinV.(uint16)))
		buf.Write(encoding.EncodeUint16(i.MaxV.(uint16)))
		return buf.Bytes(), nil
	case types.T_uint32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint32(i.MinV.(uint32)))
		buf.Write(encoding.EncodeUint32(i.MaxV.(uint32)))
		return buf.Bytes(), nil
	case types.T_uint64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint64(i.MinV.(uint64)))
		buf.Write(encoding.EncodeUint64(i.MaxV.(uint64)))
		return buf.Bytes(), nil
	case types.T_float32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeFloat32(i.MinV.(float32)))
		buf.Write(encoding.EncodeFloat32(i.MaxV.(float32)))
		return buf.Bytes(), nil
	case types.T_float64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeFloat64(i.MinV.(float64)))
		buf.Write(encoding.EncodeFloat64(i.MaxV.(float64)))
		return buf.Bytes(), nil
	case types.T_date:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeDate(i.MinV.(types.Date)))
		buf.Write(encoding.EncodeDate(i.MaxV.(types.Date)))
		return buf.Bytes(), nil
	case types.T_datetime:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeDatetime(i.MinV.(types.Datetime)))
		buf.Write(encoding.EncodeDatetime(i.MaxV.(types.Datetime)))
		return buf.Bytes(), nil
	case types.T_char, types.T_varchar, types.T_json:
		buf.Write(encoding.EncodeType(i.T))
		minv := i.MinV.([]byte)
		maxv := i.MaxV.([]byte)
		buf.Write(encoding.EncodeInt16(int16(len(minv))))
		buf.Write(minv)
		buf.Write(encoding.EncodeInt16(int16(len(maxv))))
		buf.Write(maxv)
		return buf.Bytes(), nil
	}
	panic("unsupported")
}

func (i *BlockZoneMapIndex) Type() base.IndexType {
	return base.ZoneMap
}

func (i *BlockZoneMapIndex) Eq(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) >= i.MinV.(int8) && v.(int8) <= i.MaxV.(int8)
	case types.T_int16:
		return v.(int16) >= i.MinV.(int16) && v.(int16) <= i.MaxV.(int16)
	case types.T_int32:
		return v.(int32) >= i.MinV.(int32) && v.(int32) <= i.MaxV.(int32)
	case types.T_int64:
		return v.(int64) >= i.MinV.(int64) && v.(int64) <= i.MaxV.(int64)
	case types.T_uint8:
		return v.(uint8) >= i.MinV.(uint8) && v.(uint8) <= i.MaxV.(uint8)
	case types.T_uint16:
		return v.(uint16) >= i.MinV.(uint16) && v.(uint16) <= i.MaxV.(uint16)
	case types.T_uint32:
		return v.(uint32) >= i.MinV.(uint32) && v.(uint32) <= i.MaxV.(uint32)
	case types.T_uint64:
		return v.(uint64) >= i.MinV.(uint64) && v.(uint64) <= i.MaxV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) >= i.MinV.(float32) && v.(float32) <= i.MaxV.(float32)
	case types.T_float64:
		return v.(float64) >= i.MinV.(float64) && v.(float64) <= i.MaxV.(float64)
	case types.T_date:
		return v.(types.Date) >= i.MinV.(types.Date) && v.(types.Date) <= i.MaxV.(types.Date)
	case types.T_datetime:
		return v.(types.Datetime) >= i.MinV.(types.Datetime) && v.(types.Datetime) <= i.MaxV.(types.Datetime)
	case types.T_sel:
		return v.(int64) >= i.MinV.(int64) && v.(int64) <= i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		if bytes.Compare(v.([]byte), i.MinV.([]byte)) < 0 {
			return false
		}
		if bytes.Compare(v.([]byte), i.MaxV.([]byte)) > 0 {
			return false
		}
		return true
	}
	panic("not supported")
}

func (i *BlockZoneMapIndex) Ne(v interface{}) bool {
	return !i.Eq(v)
}

func (i *BlockZoneMapIndex) Lt(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) > i.MinV.(int8)
	case types.T_int16:
		return v.(int16) > i.MinV.(int16)
	case types.T_int32:
		return v.(int32) > i.MinV.(int32)
	case types.T_int64:
		return v.(int64) > i.MinV.(int64)
	case types.T_uint8:
		return v.(uint8) > i.MinV.(uint8)
	case types.T_uint16:
		return v.(uint16) > i.MinV.(uint16)
	case types.T_uint32:
		return v.(uint32) > i.MinV.(uint32)
	case types.T_uint64:
		return v.(uint64) > i.MinV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) > i.MinV.(float32)
	case types.T_float64:
		return v.(float64) > i.MinV.(float64)
	case types.T_date:
		return v.(types.Date) > i.MinV.(types.Date)
	case types.T_datetime:
		return v.(types.Datetime) > i.MinV.(types.Datetime)
	case types.T_sel:
		return v.(int64) > i.MinV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MinV.([]byte)) > 0
	}
	panic("not supported")
}

func (i *BlockZoneMapIndex) Le(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) >= i.MinV.(int8)
	case types.T_int16:
		return v.(int16) >= i.MinV.(int16)
	case types.T_int32:
		return v.(int32) >= i.MinV.(int32)
	case types.T_int64:
		return v.(int64) >= i.MinV.(int64)
	case types.T_uint8:
		return v.(uint8) >= i.MinV.(uint8)
	case types.T_uint16:
		return v.(uint16) >= i.MinV.(uint16)
	case types.T_uint32:
		return v.(uint32) >= i.MinV.(uint32)
	case types.T_uint64:
		return v.(uint64) >= i.MinV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) >= i.MinV.(float32)
	case types.T_float64:
		return v.(float64) >= i.MinV.(float64)
	case types.T_date:
		return v.(types.Date) >= i.MinV.(types.Date)
	case types.T_datetime:
		return v.(types.Datetime) >= i.MinV.(types.Datetime)
	case types.T_sel:
		return v.(int64) >= i.MinV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MinV.([]byte)) >= 0
	}
	panic("not supported")
}

func (i *BlockZoneMapIndex) Gt(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) < i.MaxV.(int8)
	case types.T_int16:
		return v.(int16) < i.MaxV.(int16)
	case types.T_int32:
		return v.(int32) < i.MaxV.(int32)
	case types.T_int64:
		return v.(int64) < i.MaxV.(int64)
	case types.T_uint8:
		return v.(uint8) < i.MaxV.(uint8)
	case types.T_uint16:
		return v.(uint16) < i.MaxV.(uint16)
	case types.T_uint32:
		return v.(uint32) < i.MaxV.(uint32)
	case types.T_uint64:
		return v.(uint64) < i.MaxV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) < i.MaxV.(float32)
	case types.T_float64:
		return v.(float64) < i.MaxV.(float64)
	case types.T_date:
		return v.(types.Date) < i.MaxV.(types.Date)
	case types.T_datetime:
		return v.(types.Datetime) < i.MaxV.(types.Datetime)
	case types.T_sel:
		return v.(int64) < i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MaxV.([]byte)) < 0
	}
	panic("not supported")
}

func (i *BlockZoneMapIndex) Ge(v interface{}) bool {
	switch i.T.Oid {
	case types.T_int8:
		return v.(int8) <= i.MaxV.(int8)
	case types.T_int16:
		return v.(int16) <= i.MaxV.(int16)
	case types.T_int32:
		return v.(int32) <= i.MaxV.(int32)
	case types.T_int64:
		return v.(int64) <= i.MaxV.(int64)
	case types.T_uint8:
		return v.(uint8) <= i.MaxV.(uint8)
	case types.T_uint16:
		return v.(uint16) <= i.MaxV.(uint16)
	case types.T_uint32:
		return v.(uint32) <= i.MaxV.(uint32)
	case types.T_uint64:
		return v.(uint64) <= i.MaxV.(uint64)
	case types.T_decimal:
		panic("not supported")
	case types.T_float32:
		return v.(float32) <= i.MaxV.(float32)
	case types.T_float64:
		return v.(float64) <= i.MaxV.(float64)
	case types.T_date:
		return v.(types.Date) <= i.MaxV.(types.Date)
	case types.T_datetime:
		return v.(types.Datetime) <= i.MaxV.(types.Datetime)
	case types.T_sel:
		return v.(int64) <= i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MaxV.([]byte)) <= 0
	}
	panic("not supported")
}

func (i *BlockZoneMapIndex) Btw(minv interface{}, maxv interface{}) bool {
	return i.Le(minv) && i.Ge(maxv)
}

func BuildBlockZoneMapIndex(data *vector.Vector, t types.Type, colIdx int16, isSorted bool) (Index, error) {
	switch t.Oid {
	case types.T_int8:
		vec := data.Col.([]int8)
		var min, max int8
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int16:
		vec := data.Col.([]int16)
		var min, max int16
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int32:
		vec := data.Col.([]int32)
		var min, max int32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_int64:
		vec := data.Col.([]int64)
		var min, max int64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint8:
		vec := data.Col.([]uint8)
		var min, max uint8
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint16:
		vec := data.Col.([]uint16)
		var min, max uint16
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint32:
		vec := data.Col.([]uint32)
		var min, max uint32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_uint64:
		vec := data.Col.([]uint64)
		var min, max uint64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_float32:
		vec := data.Col.([]float32)
		var min, max float32
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_float64:
		vec := data.Col.([]float64)
		var min, max float64
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_date:
		vec := data.Col.([]types.Date)
		var min, max types.Date
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_datetime:
		vec := data.Col.([]types.Datetime)
		var min, max types.Datetime
		if isSorted {
			min = vec[0]
			max = vec[len(vec)-1]
		} else {
			min = vec[0]
			max = min
			for _, e := range vec {
				if e > max {
					max = e
				}
				if e < min {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	case types.T_char, types.T_varchar, types.T_json:
		vec := data.Col.(*types.Bytes)
		var min, max []byte
		if isSorted {
			min = vec.Get(0)
			max = vec.Get(int64(len(vec.Lengths) - 1))
		} else {
			min = vec.Get(0)
			max = min
			for i := 0; i < len(vec.Lengths); i++ {
				e := vec.Get(int64(i))
				if bytes.Compare(e, max) > 0 {
					max = e
				}
				if bytes.Compare(e, min) < 0 {
					min = e
				}
			}
		}
		zmi := NewBlockZoneMap(t, min, max, colIdx)
		return zmi, nil
	default:
		panic("unsupported")
	}
}

func MockInt32ZmIndices(cols int) (indices []Index) {
	t := types.Type{Oid: types.T_int32, Size: 4}
	for idx := 0; idx < cols; idx++ {
		minv := int32(1) + int32(idx)*100
		maxv := int32(99) + int32(idx)*100
		zm := NewBlockZoneMap(t, minv, maxv, int16(idx))
		indices = append(indices, zm)
	}
	return indices
}
