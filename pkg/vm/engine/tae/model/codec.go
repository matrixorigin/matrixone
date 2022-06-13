package model

import (
	"bytes"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
)

var CompoundKeyType types.Type

func init() {
	CompoundKeyType = types.T_varchar.ToType()
	CompoundKeyType.Width = 100
}

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
	key = encoding.DecodeDecimal128(buf)
	return
}

func EncodeHiddenKeyWithPrefix(dest, prefix, offsetBuf []byte, offset uint32) {
	copy(dest, prefix)
	binary.BigEndian.PutUint32(offsetBuf, offset)
	copy(dest[12:], offsetBuf)
}

func DecodeHiddenKeyFromValue(v any) (segmentId, blockId uint64, offset uint32) {
	reflected := v.(types.Decimal128)
	src := encoding.EncodeDecimal128(reflected)
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
		case int8:
			_, _ = w.Write(encoding.EncodeInt8(v))
		case int16:
			_, _ = w.Write(encoding.EncodeInt16(v))
		case int32:
			_, _ = w.Write(encoding.EncodeInt32(v))
		case int64:
			_, _ = w.Write(encoding.EncodeInt64(v))
		case uint8:
			_, _ = w.Write(encoding.EncodeUint8(v))
		case uint16:
			_, _ = w.Write(encoding.EncodeUint16(v))
		case uint32:
			_, _ = w.Write(encoding.EncodeUint32(v))
		case uint64:
			_, _ = w.Write(encoding.EncodeUint64(v))
		case types.Decimal64:
			_, _ = w.Write(encoding.EncodeDecimal64(v))
		case types.Decimal128:
			_, _ = w.Write(encoding.EncodeDecimal128(v))
		case float32:
			_, _ = w.Write(encoding.EncodeFloat32(v))
		case float64:
			_, _ = w.Write(encoding.EncodeFloat64(v))
		case types.Date:
			_, _ = w.Write(encoding.EncodeDate(v))
		case types.Datetime:
			_, _ = w.Write(encoding.EncodeDatetime(v))
		case []byte:
			_, _ = w.Write(v)
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
	cc = movec.New(CompoundKeyType)
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
