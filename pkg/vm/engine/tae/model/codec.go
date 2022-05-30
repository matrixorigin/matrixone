package model

import (
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

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
