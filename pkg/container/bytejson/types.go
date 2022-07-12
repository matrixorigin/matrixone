package bytejson

import (
	"encoding/binary"
)

type TpCode byte

type ByteJson struct {
	Data []byte
	Type TpCode
}
type kv struct {
	key string
	val interface{}
}

const (
	TpCodeObject  = 0x01
	TpCodeArray   = 0x02
	TpCodeLiteral = 0x03
	TpCodeInt64   = 0x04
	TpCodeUint64  = 0x05
	TpCodeFloat64 = 0x06
	TpCodeString  = 0x07
)

const (
	headerSize   = 8 // element size + data size.
	dataSizeOff  = 4
	keyEntrySize = 6 // keyOff +  keyLen
	keyLenOff    = 4
	valTypeSize  = 1
	valEntrySize = 5
)

const (
	LiteralNull  byte = 0x00
	LiteralTrue  byte = 0x01
	LiteralFalse byte = 0x02
)

var endian = binary.LittleEndian
