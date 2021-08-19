package index

import (
	"bytes"
	"io"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

func SegmentZoneMapIndexConstructor(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) buf.IMemoryNode {
	return NewSegmentZoneMapEmptyNode(vf, useCompress, freeFunc)
}

type SegmentZoneMapIndex struct {
	T           types.Type
	MinV        interface{}
	MaxV        interface{}
	Col         int16
	FreeFunc    buf.MemoryFreeFunc
	File        common.IVFile
	UseCompress bool
	BlkMin      []interface{}
	BlkMax      []interface{}
}

func NewSegmentZoneMap(t types.Type, minv, maxv interface{}, colIdx int16, blkMin []interface{}, blkMax []interface{}) Index {
	return &SegmentZoneMapIndex{
		T:    t,
		MinV: minv,
		MaxV: maxv,
		Col:  colIdx,
		BlkMax: blkMax,
		BlkMin: blkMin,
	}
}

func NewSegmentZoneMapEmptyNode(vf common.IVFile, useCompress bool, freeFunc buf.MemoryFreeFunc) Index {
	return &SegmentZoneMapIndex{
		FreeFunc:    freeFunc,
		File:        vf,
		UseCompress: useCompress,
	}
}

func (i *SegmentZoneMapIndex) GetCol() int16 {
	return i.Col
}

func (i *SegmentZoneMapIndex) Eval(ctx *FilterCtx) error {
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
		ctx.BoolRes = i.Ge(ctx.ValMax) && i.Le(ctx.ValMin)
	case OpOut:
		ctx.BoolRes = i.Lt(ctx.ValMin) || i.Gt(ctx.ValMax)
	}
	return nil
}

func (i *SegmentZoneMapIndex) FreeMemory() {
	if i.FreeFunc != nil {
		i.FreeFunc(i)
	}
}

func (i *SegmentZoneMapIndex) GetMemorySize() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *SegmentZoneMapIndex) GetMemoryCapacity() uint64 {
	if i.UseCompress {
		return uint64(i.File.Stat().Size())
	} else {
		return uint64(i.File.Stat().OriginSize())
	}
}

func (i *SegmentZoneMapIndex) Reset() {
}

func (i *SegmentZoneMapIndex) ReadFrom(r io.Reader) (n int64, err error) {
	buf := make([]byte, i.GetMemoryCapacity())
	nr, err := r.Read(buf)
	if err != nil {
		return int64(nr), err
	}
	err = i.Unmarshall(buf)
	return int64(nr), err
}

func (i *SegmentZoneMapIndex) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := i.Marshall()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (i *SegmentZoneMapIndex) Unmarshall(data []byte) error {
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
		buf = buf[1:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeInt8(buf[:1])
			buf = buf[1:]
			i.BlkMax[j] = encoding.DecodeInt8(buf[:1])
			buf = buf[1:]
		}
		return nil
	case types.T_int16:
		i.MinV = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		i.MaxV = encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeInt16(buf[:2])
			buf = buf[2:]
			i.BlkMax[j] = encoding.DecodeInt16(buf[:2])
			buf = buf[2:]
		}
		return nil
	case types.T_int32:
		i.MinV = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeInt32(buf[:4])
			buf = buf[4:]
			i.BlkMax[j] = encoding.DecodeInt32(buf[:4])
			buf = buf[4:]
		}
		return nil
	case types.T_int64:
		i.MinV = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeInt64(buf[:8])
		buf = buf[8:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeInt64(buf[:8])
			buf = buf[8:]
			i.BlkMax[j] = encoding.DecodeInt64(buf[:8])
			buf = buf[8:]
		}
		return nil
	case types.T_uint8:
		i.MinV = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		i.MaxV = encoding.DecodeUint8(buf[:1])
		buf = buf[1:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeUint8(buf[:1])
			buf = buf[1:]
			i.BlkMax[j] = encoding.DecodeUint8(buf[:1])
			buf = buf[1:]
		}
		return nil
	case types.T_uint16:
		i.MinV = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		i.MaxV = encoding.DecodeUint16(buf[:2])
		buf = buf[2:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeUint16(buf[:2])
			buf = buf[2:]
			i.BlkMax[j] = encoding.DecodeUint16(buf[:2])
			buf = buf[2:]
		}
		return nil
	case types.T_uint32:
		i.MinV = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeUint32(buf[:4])
		buf = buf[4:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeUint32(buf[:4])
			buf = buf[4:]
			i.BlkMax[j] = encoding.DecodeUint32(buf[:4])
			buf = buf[4:]
		}
		return nil
	case types.T_uint64:
		i.MinV = encoding.DecodeUint32(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeUint32(buf[:8])
		buf = buf[8:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeUint64(buf[:8])
			buf = buf[8:]
			i.BlkMax[j] = encoding.DecodeUint64(buf[:8])
			buf = buf[8:]
		}
		return nil
	case types.T_float32:
		i.MinV = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		i.MaxV = encoding.DecodeFloat32(buf[:4])
		buf = buf[4:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeFloat32(buf[:4])
			buf = buf[4:]
			i.BlkMax[j] = encoding.DecodeFloat32(buf[:4])
			buf = buf[4:]
		}
		return nil
	case types.T_float64:
		i.MinV = encoding.DecodeFloat64(buf[:8])
		buf = buf[8:]
		i.MaxV = encoding.DecodeFloat64(buf[:8])
		buf = buf[8:]
		len := encoding.DecodeInt32(buf[:4])
		buf = buf[4:]
		i.BlkMax = make([]interface{}, len)
		i.BlkMin = make([]interface{}, len)
		for j := 0; j < int(len); j++ {
			i.BlkMin[j] = encoding.DecodeFloat64(buf[:8])
			buf = buf[8:]
			i.BlkMax[j] = encoding.DecodeFloat64(buf[:8])
			buf = buf[8:]
		}
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
		buf = buf[int(lenmaxv):]
		i.MinV = minBuf
		i.MaxV = maxBuf
		blkMinLen := encoding.DecodeInt16(buf[:2])
		buf = buf[2:]
		i.BlkMax = make([]interface{}, blkMinLen)
		i.BlkMin = make([]interface{}, blkMinLen)
		for j := 0; j < int(blkMinLen); j++ {
			lenminv = encoding.DecodeInt16(buf[:2])
			buf = buf[2:]
			minBuf = make([]byte, int(lenminv))
			copy(minBuf, buf[:int(lenminv)])
			buf = buf[int(lenminv):]
			i.BlkMin[j] = minBuf

			lenmaxv = encoding.DecodeInt16(buf[:2])
			buf = buf[2:]
			maxBuf = make([]byte, int(lenmaxv))
			copy(maxBuf, buf[:int(lenmaxv)])
			buf = buf[int(lenmaxv):]
			i.BlkMax[j] = maxBuf
		}
		return nil
	}
	panic("unsupported")
}

func (i *SegmentZoneMapIndex) Marshall() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeInt16(i.Col))
	switch i.T.Oid {
	case types.T_int8:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt8(i.MinV.(int8)))
		buf.Write(encoding.EncodeInt8(i.MaxV.(int8)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeInt8(i.BlkMin[j].(int8)))
			buf.Write(encoding.EncodeInt8(i.BlkMax[j].(int8)))
		}
		return buf.Bytes(), nil
	case types.T_int16:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt16(i.MinV.(int16)))
		buf.Write(encoding.EncodeInt16(i.MaxV.(int16)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeInt16(i.BlkMin[j].(int16)))
			buf.Write(encoding.EncodeInt16(i.BlkMax[j].(int16)))
		}
		return buf.Bytes(), nil
	case types.T_int32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt32(i.MinV.(int32)))
		buf.Write(encoding.EncodeInt32(i.MaxV.(int32)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeInt32(i.BlkMin[j].(int32)))
			buf.Write(encoding.EncodeInt32(i.BlkMax[j].(int32)))
		}
		return buf.Bytes(), nil
	case types.T_int64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeInt64(i.MinV.(int64)))
		buf.Write(encoding.EncodeInt64(i.MaxV.(int64)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeInt64(i.BlkMin[j].(int64)))
			buf.Write(encoding.EncodeInt64(i.BlkMax[j].(int64)))
		}
		return buf.Bytes(), nil
	case types.T_uint8:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint8(i.MinV.(uint8)))
		buf.Write(encoding.EncodeUint8(i.MaxV.(uint8)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeUint8(i.BlkMin[j].(uint8)))
			buf.Write(encoding.EncodeUint8(i.BlkMax[j].(uint8)))
		}
		return buf.Bytes(), nil
	case types.T_uint16:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint16(i.MinV.(uint16)))
		buf.Write(encoding.EncodeUint16(i.MaxV.(uint16)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeUint16(i.BlkMin[j].(uint16)))
			buf.Write(encoding.EncodeUint16(i.BlkMax[j].(uint16)))
		}
		return buf.Bytes(), nil
	case types.T_uint32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint32(i.MinV.(uint32)))
		buf.Write(encoding.EncodeUint32(i.MaxV.(uint32)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeUint32(i.BlkMin[j].(uint32)))
			buf.Write(encoding.EncodeUint32(i.BlkMax[j].(uint32)))
		}
		return buf.Bytes(), nil
	case types.T_uint64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeUint64(i.MinV.(uint64)))
		buf.Write(encoding.EncodeUint64(i.MaxV.(uint64)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeUint64(i.BlkMin[j].(uint64)))
			buf.Write(encoding.EncodeUint64(i.BlkMax[j].(uint64)))
		}
		return buf.Bytes(), nil
	case types.T_float32:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeFloat32(i.MinV.(float32)))
		buf.Write(encoding.EncodeFloat32(i.MaxV.(float32)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeFloat32(i.BlkMin[j].(float32)))
			buf.Write(encoding.EncodeFloat32(i.BlkMax[j].(float32)))
		}
		return buf.Bytes(), nil
	case types.T_float64:
		buf.Write(encoding.EncodeType(i.T))
		buf.Write(encoding.EncodeFloat64(i.MinV.(float64)))
		buf.Write(encoding.EncodeFloat64(i.MaxV.(float64)))
		buf.Write(encoding.EncodeInt32(int32(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeFloat64(i.BlkMin[j].(float64)))
			buf.Write(encoding.EncodeFloat64(i.BlkMax[j].(float64)))
		}
		return buf.Bytes(), nil
	case types.T_char, types.T_varchar, types.T_json:
		buf.Write(encoding.EncodeType(i.T))
		minv := i.MinV.([]byte)
		maxv := i.MaxV.([]byte)
		buf.Write(encoding.EncodeInt16(int16(len(minv))))
		buf.Write(minv)
		buf.Write(encoding.EncodeInt16(int16(len(maxv))))
		buf.Write(maxv)
		buf.Write(encoding.EncodeInt16(int16(len(i.BlkMin))))
		for j := 0; j < len(i.BlkMin); j++ {
			buf.Write(encoding.EncodeInt16(int16(len(i.BlkMin[j].([]byte)))))
			buf.Write(i.BlkMin[j].([]byte))
			buf.Write(encoding.EncodeInt16(int16(len(i.BlkMax[j].([]byte)))))
			buf.Write(i.BlkMax[j].([]byte))
		}
		return buf.Bytes(), nil
	}
	panic("unsupported")
}

func (i *SegmentZoneMapIndex) Type() base.IndexType {
	return base.ZoneMap
}

func (i *SegmentZoneMapIndex) Eq(v interface{}) bool {
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
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
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

func (i *SegmentZoneMapIndex) Ne(v interface{}) bool {
	return !i.Eq(v)
}

func (i *SegmentZoneMapIndex) Lt(v interface{}) bool {
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
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
	case types.T_sel:
		return v.(int64) > i.MinV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MinV.([]byte)) > 0
	}
	panic("not supported")
}

func (i *SegmentZoneMapIndex) Le(v interface{}) bool {
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
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
	case types.T_sel:
		return v.(int64) >= i.MinV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MinV.([]byte)) >= 0
	}
	panic("not supported")
}

func (i *SegmentZoneMapIndex) Gt(v interface{}) bool {
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
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
	case types.T_sel:
		return v.(int64) < i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MaxV.([]byte)) < 0
	}
	panic("not supported")
}

func (i *SegmentZoneMapIndex) Ge(v interface{}) bool {
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
		panic("not supported")
	case types.T_datetime:
		panic("not supported")
	case types.T_sel:
		return v.(int64) <= i.MaxV.(int64)
	case types.T_tuple:
		panic("not supported")
	case types.T_char, types.T_varchar, types.T_json:
		return bytes.Compare(v.([]byte), i.MaxV.([]byte)) <= 0
	}
	panic("not supported")
}

func (i *SegmentZoneMapIndex) Btw(v interface{}) bool {
	panic("TODO")
}

