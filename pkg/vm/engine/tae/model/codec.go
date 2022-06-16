package model

import (
	"bytes"
	"encoding/binary"
	"fmt"

	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type CompoundKeyEncoder = func(*bytes.Buffer, ...any) []byte

// [48 Bit (BlockID) + 48 Bit (SegmentID)]
func EncodeBlockKeyPrefix(segmentId, blockId uint64) []byte {
	buf := make([]byte, 12)
	tempBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(tempBuf, segmentId)
	copy(buf[0:], tempBuf[2:])
	binary.BigEndian.PutUint64(tempBuf, blockId)
	copy(buf[6:], tempBuf[2:])
	return buf
}

func DecodeBlockKeyPrefix(buf []byte) (segmentId, blockId uint64) {
	tempBuf := make([]byte, 12)
	copy(tempBuf[2:], buf[0:6])
	segmentId = binary.BigEndian.Uint64(tempBuf)
	copy(tempBuf[2:], buf[6:])
	blockId = binary.BigEndian.Uint64(tempBuf)
	return
}

func EncodeHiddenKey(segmentId, blockId uint64, offset uint32) (key any) {
	buf := make([]byte, 16)
	prefix := EncodeBlockKeyPrefix(segmentId, blockId)
	offsetBuf := make([]byte, 4)
	EncodeHiddenKeyWithPrefix(buf, prefix, offsetBuf, offset)
	key = types.DecodeFixed[types.Decimal128](buf)
	return
}

func EncodeHiddenKeyWithPrefix(dest, prefix, offsetBuf []byte, offset uint32) {
	copy(dest, prefix)
	binary.BigEndian.PutUint32(offsetBuf, offset)
	copy(dest[12:], offsetBuf)
}

func DecodeHiddenKeyFromValue(v any) (segmentId, blockId uint64, offset uint32) {
	reflected := v.(types.Decimal128)
	src := types.EncodeFixed(reflected)
	return DecodeHiddenKey(src)
}

func DecodeHiddenKey(src []byte) (segmentId, blockId uint64, offset uint32) {
	segmentId, blockId = DecodeBlockKeyPrefix(src[:12])
	offset = binary.BigEndian.Uint32(src[12:])
	return
}

func EncodeTypedVals(w *bytes.Buffer, vals ...any) []byte {
	if w == nil {
		w = new(bytes.Buffer)
	} else {
		w.Reset()
	}
	for _, val := range vals {
		switch v := val.(type) {
		case []byte:
			_, _ = w.Write(v)
		case bool:
			_, _ = w.Write(types.EncodeFixed(v))
		case int8:
			_, _ = w.Write(types.EncodeFixed(v))
		case int16:
			_, _ = w.Write(types.EncodeFixed(v))
		case int32:
			_, _ = w.Write(types.EncodeFixed(v))
		case int64:
			_, _ = w.Write(types.EncodeFixed(v))
		case uint8:
			_, _ = w.Write(types.EncodeFixed(v))
		case uint16:
			_, _ = w.Write(types.EncodeFixed(v))
		case uint32:
			_, _ = w.Write(types.EncodeFixed(v))
		case uint64:
			_, _ = w.Write(types.EncodeFixed(v))
		case float32:
			_, _ = w.Write(types.EncodeFixed(v))
		case float64:
			_, _ = w.Write(types.EncodeFixed(v))
		case types.Date:
			_, _ = w.Write(types.EncodeFixed(v))
		case types.Datetime:
			_, _ = w.Write(types.EncodeFixed(v))
		case types.Timestamp:
			_, _ = w.Write(types.EncodeFixed(v))
		case types.Decimal64:
			_, _ = w.Write(types.EncodeFixed(v))
		case types.Decimal128:
			_, _ = w.Write(types.EncodeFixed(v))
		default:
			panic(fmt.Errorf("%T:%v not supported", v, v))
		}
	}
	return w.Bytes()
}

func EncodeTuple(w *bytes.Buffer, row uint32, cols ...*movec.Vector) []byte {
	vs := make([]any, len(cols))
	for i := range vs {
		vs[i] = compute.GetValue(cols[i], row)
	}
	return EncodeTypedVals(w, vs...)
}

// TODO: use buffer pool for cc
func EncodeCompoundColumn(cols ...*movec.Vector) (cc *movec.Vector) {
	if len(cols) == 1 {
		cc = cols[0]
		return
	}
	cc = movec.New(types.CompoundKeyType)
	var buf bytes.Buffer
	vs := make([]any, len(cols))
	for row := 0; row < movec.Length(cols[0]); row++ {
		buf.Reset()
		for i := range vs {
			vs[i] = compute.GetValue(cols[i], uint32(row))
		}
		v := EncodeTypedVals(&buf, vs...)
		compute.AppendValue(cc, v)
	}
	return cc
}
