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

package types

import (
	"encoding/binary"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/exp/constraints"
)

type T uint8

const (
	// any family
	T_any  T = 0
	T_star T = 1

	// bool family
	T_bool T = 10

	// T_bit bit family
	T_bit T = 11

	// numeric/integer family
	T_int8    T = 20
	T_int16   T = 21
	T_int32   T = 22
	T_int64   T = 23
	T_int128  T = 24
	T_uint8   T = 25
	T_uint16  T = 26
	T_uint32  T = 27
	T_uint64  T = 28
	T_uint128 T = 29

	// numeric/float family
	T_float32 T = 30
	T_float64 T = 31

	// numeric/decimals
	T_decimal64  T = 32
	T_decimal128 T = 33
	T_decimal256 T = 34

	// pseudo numerics, not used

	// date and time
	T_date      T = 50
	T_time      T = 51
	T_datetime  T = 52
	T_timestamp T = 53
	T_interval  T = 54

	// string family
	T_char      T = 60
	T_varchar   T = 61
	T_json      T = 62
	T_uuid      T = 63
	T_binary    T = 64
	T_varbinary T = 65
	T_enum      T = 66

	// blobs
	T_blob     T = 70
	T_text     T = 71
	T_datalink T = 72

	// Transaction TS
	T_TS      T = 100
	T_Rowid   T = 101
	T_Blockid T = 102

	// system family
	T_tuple T = 201

	// Array/Vector family
	T_array_float32 T = 224 // In SQL , it is vecf32
	T_array_float64 T = 225 // In SQL , it is vecf64

	//note: max value of uint8 is 255
)

const (
	TxnTsSize    = 12
	RowidSize    = 24
	ObjectidSize = 18
	BlockidSize  = 20
)

type Type struct {
	Oid T

	// XXX Dummies.  T is uint8, make it 4 bytes aligned, otherwise, it may contain
	// garbage data.  In theory these unused garbage should not be a problem, but
	// it is.  Give it a name will zero fill it ...
	Charset uint8
	notNull uint8
	dummy2  uint8

	Size int32
	// Width means max Display width for float and double, char and varchar
	// todo: need to add new attribute DisplayWidth ?
	Width int32
	// Scale means number of fractional digits for decimal, timestamp, float, etc.
	Scale int32
}

// ProtoSize is used by gogoproto.
func (t *Type) ProtoSize() int {
	return 2*4 + 4*3
}

// MarshalToSizedBuffer is used by gogoproto.
func (t *Type) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < t.ProtoSize() {
		panic("invalid byte slice")
	}
	binary.BigEndian.PutUint16(data[0:], uint16(t.Oid))
	binary.BigEndian.PutUint16(data[2:], uint16(t.Charset))
	binary.BigEndian.PutUint16(data[4:], uint16(t.notNull))
	binary.BigEndian.PutUint16(data[6:], uint16(t.dummy2))
	binary.BigEndian.PutUint32(data[8:], Int32ToUint32(t.Size))
	binary.BigEndian.PutUint32(data[12:], Int32ToUint32(t.Width))
	binary.BigEndian.PutUint32(data[16:], Int32ToUint32(t.Scale))
	return 20, nil
}

// MarshalTo is used by gogoproto.
func (t *Type) MarshalTo(data []byte) (int, error) {
	size := t.ProtoSize()
	return t.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (t *Type) Marshal() ([]byte, error) {
	data := make([]byte, t.ProtoSize())
	n, err := t.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// Unmarshal is used by gogoproto.
func (t *Type) Unmarshal(data []byte) error {
	if len(data) < t.ProtoSize() {
		panic("invalid byte slice")
	}
	t.Oid = T(binary.BigEndian.Uint16(data[0:]))
	t.Charset = uint8(binary.BigEndian.Uint16(data[2:]))
	t.notNull = uint8(binary.BigEndian.Uint16(data[4:]))
	t.dummy2 = uint8(binary.BigEndian.Uint16(data[6:]))
	t.Size = Uint32ToInt32(binary.BigEndian.Uint32(data[8:]))
	t.Width = Uint32ToInt32(binary.BigEndian.Uint32(data[12:]))
	t.Scale = Uint32ToInt32(binary.BigEndian.Uint32(data[16:]))
	return nil
}

func (t *Type) MarshalBinary() ([]byte, error) {
	return t.Marshal()
}

func (t *Type) UnmarshalBinary(data []byte) error {
	return t.Unmarshal(data)
}

type Date int32

type Datetime int64
type Timestamp int64
type Time int64

type Decimal64 uint64

type Enum uint16

type Decimal128 struct {
	B0_63   uint64
	B64_127 uint64
}

type Decimal256 struct {
	B0_63    uint64
	B64_127  uint64
	B128_191 uint64
	B192_255 uint64
}

type Varlena [VarlenaSize]byte

// UUID is Version 1 UUID based on the current NodeID and clock sequence, and the current time.
type Uuid [16]byte

// timestamp for transaction: physical time (higher 8 bytes) + logical (lower 4 bytes)
// See txts.go for impl.
type TS [TxnTsSize]byte

// ProtoSize is used by gogoproto.
func (ts *TS) ProtoSize() int {
	return TxnTsSize
}

// MarshalToSizedBuffer is used by gogoproto.
func (ts *TS) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < ts.ProtoSize() {
		panic("invalid byte slice")
	}
	n := copy(data, ts[:])
	return n, nil
}

// MarshalTo is used by gogoproto.
func (ts *TS) MarshalTo(data []byte) (int, error) {
	size := ts.ProtoSize()
	return ts.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (ts *TS) Marshal() ([]byte, error) {
	data := make([]byte, ts.ProtoSize())
	n, err := ts.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// Unmarshal is used by gogoproto.
func (ts *TS) Unmarshal(data []byte) error {
	if len(data) < ts.ProtoSize() {
		panic("invalid byte slice")
	}
	copy(ts[:], data)
	return nil
}

// Rowid
type Rowid [RowidSize]byte

// Segmentid
type Segmentid = Uuid

// Objectid
type Objectid [ObjectidSize]byte

// Blockid
type Blockid [BlockidSize]byte

// ProtoSize is used by gogoproto.
func (b *Blockid) ProtoSize() int {
	return BlockidSize
}

// MarshalToSizedBuffer is used by gogoproto.
func (b *Blockid) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < b.ProtoSize() {
		panic("invalid byte slice")
	}
	n := copy(data, b[:])
	return n, nil
}

// MarshalTo is used by gogoproto.
func (b *Blockid) MarshalTo(data []byte) (int, error) {
	size := b.ProtoSize()
	return b.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (b *Blockid) Marshal() ([]byte, error) {
	data := make([]byte, b.ProtoSize())
	n, err := b.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// Unmarshal is used by gogoproto.
func (b *Blockid) Unmarshal(data []byte) error {
	if len(data) < b.ProtoSize() {
		panic("invalid byte slice")
	}
	copy(b[:], data)
	return nil
}

// Fixed bytes.   Decimal64/128 and Varlena are not included because they
// has special meanings.  In general you cannot compare them as bytes.
type FixedBytes interface {
	TS | Rowid
}

type Ints interface {
	int8 | int16 | int32 | int64
}

type UInts interface {
	uint8 | uint16 | uint32 | uint64
}

type Floats interface {
	float32 | float64
}

type BuiltinNumber interface {
	Ints | UInts | Floats
}

type OrderedT interface {
	constraints.Ordered
}

type Decimal interface {
	Decimal64 | Decimal128 | Decimal256
}

type DecimalWithFormat interface {
	Decimal64 | Decimal128
	Format(scale int32) string
}

type FixedWithStringer interface {
	Date | Time | Datetime | Timestamp | Enum | Uuid
	String() string
}

// FixedSized types in our type system.   Esp, Varlena.
type FixedSizeT interface {
	FixedSizeTExceptStrType | Varlena
}

type RealNumbers interface {
	constraints.Float
}

type FixedSizeTExceptStrType interface {
	bool | OrderedT | Decimal | TS | Rowid | Uuid | Blockid
}

type Number interface {
	Ints | UInts | Floats | Decimal
}

var Types = map[string]T{
	"bool": T_bool,

	"bit":      T_bit,
	"tinyint":  T_int8,
	"smallint": T_int16,
	"int":      T_int32,
	"integer":  T_int32,
	"bigint":   T_int64,

	"tinyint unsigned":  T_uint8,
	"smallint unsigned": T_uint16,
	"int unsigned":      T_uint32,
	"integer unsigned":  T_uint32,
	"bigint unsigned":   T_uint64,

	"decimal64":  T_decimal64,
	"decimal":    T_decimal128,
	"decimal128": T_decimal128,
	"decimal256": T_decimal256,

	"float":  T_float32,
	"double": T_float64,

	"date":      T_date,
	"datetime":  T_datetime,
	"time":      T_time,
	"timestamp": T_timestamp,
	"interval":  T_interval,

	"char":    T_char,
	"varchar": T_varchar,

	"binary":    T_binary,
	"varbinary": T_varbinary,

	"enum": T_enum,

	"json":     T_json,
	"text":     T_text,
	"datalink": T_datalink,
	"blob":     T_blob,
	"uuid":     T_uuid,

	"transaction timestamp": T_TS,
	"rowid":                 T_Rowid,
	"blockid":               T_Blockid,

	"array float32": T_array_float32,
	"array float64": T_array_float64,
}

func New(oid T, width, scale int32) Type {
	typ := Type{}
	typ.Oid = oid
	typ.Size = int32(oid.TypeLen())
	typ.Width = width
	typ.Scale = scale
	typ.Charset = CharsetType(oid)
	return typ
}

func CharsetType(oid T) uint8 {
	switch oid {
	case T_blob, T_varbinary, T_binary:
		// binary charset
		return 1
	default:
		// utf8 charset
		return 0
	}
}

func TypeSize(oid T) int {
	return oid.TypeLen()
}

func (t *Type) SetNotNull(b bool) {
	if b {
		t.notNull = 1
	} else {
		t.notNull = 0
	}
}
func (t Type) GetNotNull() bool {
	return t.notNull == 1
}

func (t Type) GetSize() int32 {
	return t.Size
}

func (t Type) TypeSize() int {
	return int(t.Size)
}

func (t Type) IsBoolean() bool {
	return t.Oid == T_bool
}

func (t Type) IsFixedLen() bool {
	return t.Oid.FixedLength() >= 0
}

func (t Type) IsVarlen() bool {
	return t.Oid.FixedLength() < 0
}

// Special
func (t Type) IsTuple() bool {
	return t.Oid == T_tuple
}

func (t Type) IsInt() bool {
	switch t.Oid {
	case T_int8, T_int16, T_int32, T_int64:
		return true
	default:
		return false
	}
}

func (t Type) IsUInt() bool {
	switch t.Oid {
	case T_uint8, T_uint16, T_uint32, T_uint64:
		return true
	default:
		return false
	}
}

func (t Type) IsIntOrUint() bool {
	return t.IsInt() || t.IsUInt()
}

func (t Type) IsFloat() bool {
	switch t.Oid {
	case T_float32, T_float64:
		return true
	default:
		return false
	}
}

func (t Type) IsDecimal() bool {
	switch t.Oid {
	case T_decimal64, T_decimal128, T_decimal256:
		return true
	default:
		return false
	}
}

func (t Type) IsNumeric() bool {
	return t.IsIntOrUint() || t.IsFloat() || t.IsDecimal() || t.Oid == T_bit
}

func (t Type) IsTemporal() bool {
	switch t.Oid {
	case T_date, T_time, T_datetime, T_timestamp, T_interval:
		return true
	}
	return false
}

func (t Type) IsNumericOrTemporal() bool {
	return t.IsNumeric() || t.IsTemporal()
}

func (t Type) String() string {
	return t.Oid.String()
}

func (t Type) DescString() string {
	switch t.Oid {
	case T_bit:
		return fmt.Sprintf("BIT(%d)", t.Width)
	case T_char:
		return fmt.Sprintf("CHAR(%d)", t.Width)
	case T_varchar:
		return fmt.Sprintf("VARCHAR(%d)", t.Width)
	case T_binary:
		return fmt.Sprintf("BINARY(%d)", t.Width)
	case T_varbinary:
		return fmt.Sprintf("VARBINARY(%d)", t.Width)
	case T_decimal64:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Width, t.Scale)
	case T_decimal128:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Width, t.Scale)
	}
	return t.Oid.String()
}

func (t Type) Eq(b Type) bool {
	switch t.Oid {
	// XXX need to find out why these types have different size/width
	case T_bool, T_uint8, T_uint16, T_uint32, T_uint64, T_uint128, T_int8, T_int16, T_int32, T_int64, T_int128:
		return t.Oid == b.Oid
	default:
		return t.Oid == b.Oid && t.Size == b.Size && t.Width == b.Width && t.Scale == b.Scale
	}
}

func (t T) ToType() Type {
	var typ Type

	typ.Oid = t
	switch t {
	case T_bool:
		typ.Size = 1
	case T_bit:
		typ.Size = 8
		typ.Width = MaxBitLen
	case T_int8:
		typ.Size = 1
	case T_int16:
		typ.Size = 2
	case T_int32, T_date:
		typ.Size = 4
	case T_int64, T_datetime, T_time, T_timestamp:
		typ.Size = 8
	case T_uint8:
		typ.Size = 1
	case T_uint16:
		typ.Size = 2
	case T_uint32:
		typ.Size = 4
	case T_uint64:
		typ.Size = 8
	case T_float32:
		typ.Size = 4
	case T_float64:
		typ.Size = 8
	case T_decimal64:
		typ.Size = 8
		typ.Width = 18
	case T_decimal128:
		typ.Size = 16
		typ.Width = 38
	case T_decimal256:
		typ.Size = 32
		typ.Width = 76
	case T_uuid:
		typ.Size = 16
	case T_TS:
		typ.Size = TxnTsSize
	case T_Rowid:
		typ.Size = RowidSize
	case T_Blockid:
		typ.Size = BlockidSize
	case T_json, T_blob, T_text, T_datalink:
		typ.Size = VarlenaSize
	case T_char:
		typ.Size = VarlenaSize
		typ.Width = MaxCharLen
	case T_varchar:
		typ.Size = VarlenaSize
		typ.Width = MaxVarcharLen
	case T_array_float32, T_array_float64:
		typ.Size = VarlenaSize
		typ.Width = MaxArrayDimension
	case T_binary:
		typ.Size = VarlenaSize
		typ.Width = MaxBinaryLen
	case T_varbinary:
		typ.Size = VarlenaSize
		typ.Width = MaxVarBinaryLen
	case T_enum:
		typ.Size = 2
	case T_any:
		// XXX I don't know about this one ...
		typ.Size = 0
	default:
		panic("Unknown type")
	}
	return typ
}

func (t T) ToTypeWithScale(scale int32) Type {
	typ := t.ToType()
	typ.Scale = scale
	return typ
}

func (t T) String() string {
	switch t {
	case T_any:
		return "ANY"
	case T_bool:
		return "BOOL"
	case T_bit:
		return "BIT"
	case T_int8:
		return "TINYINT"
	case T_int16:
		return "SMALLINT"
	case T_int32:
		return "INT"
	case T_int64:
		return "BIGINT"
	case T_uint8:
		return "TINYINT UNSIGNED"
	case T_uint16:
		return "SMALLINT UNSIGNED"
	case T_uint32:
		return "INT UNSIGNED"
	case T_uint64:
		return "BIGINT UNSIGNED"
	case T_float32:
		return "FLOAT"
	case T_float64:
		return "DOUBLE"
	case T_date:
		return "DATE"
	case T_datetime:
		return "DATETIME"
	case T_time:
		return "TIME"
	case T_timestamp:
		return "TIMESTAMP"
	case T_char:
		return "CHAR"
	case T_varchar:
		return "VARCHAR"
	case T_binary:
		return "BINARY"
	case T_varbinary:
		return "VARBINARY"
	case T_json:
		return "JSON"
	case T_tuple:
		return "TUPLE"
	case T_decimal64:
		return "DECIMAL64"
	case T_decimal128:
		return "DECIMAL128"
	case T_decimal256:
		return "DECIMAL256"
	case T_blob:
		return "BLOB"
	case T_text:
		return "TEXT"
	case T_datalink:
		return "DATALINK"
	case T_TS:
		return "TRANSACTION TIMESTAMP"
	case T_Rowid:
		return "ROWID"
	case T_uuid:
		return "UUID"
	case T_Blockid:
		return "BLOCKID"
	case T_interval:
		return "INTERVAL"
	case T_array_float32:
		return "VECF32"
	case T_array_float64:
		return "VECF64"
	case T_enum:
		return "ENUM"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}

// OidString returns T string
func (t T) OidString() string {
	switch t {
	case T_uuid:
		return "T_uuid"
	case T_json:
		return "T_json"
	case T_bool:
		return "T_bool"
	case T_bit:
		return "T_bit"
	case T_int64:
		return "T_int64"
	case T_int32:
		return "T_int32"
	case T_int16:
		return "T_int16"
	case T_int8:
		return "T_int8"
	case T_float64:
		return "T_float64"
	case T_float32:
		return "T_float32"
	case T_uint8:
		return "T_uint8"
	case T_uint16:
		return "T_uint16"
	case T_uint32:
		return "T_uint32"
	case T_uint64:
		return "T_uint64"
	case T_char:
		return "T_char"
	case T_varchar:
		return "T_varchar"
	case T_binary:
		return "T_binary"
	case T_varbinary:
		return "T_varbinary"
	case T_date:
		return "T_date"
	case T_datetime:
		return "T_datetime"
	case T_time:
		return "T_time"
	case T_timestamp:
		return "T_timestamp"
	case T_decimal64:
		return "T_decimal64"
	case T_decimal128:
		return "T_decimal128"
	case T_decimal256:
		return "T_decimal256"
	case T_blob:
		return "T_blob"
	case T_text:
		return "T_text"
	case T_datalink:
		return "T_datalink"
	case T_TS:
		return "T_TS"
	case T_Rowid:
		return "T_Rowid"
	case T_Blockid:
		return "T_Blockid"
	case T_interval:
		return "T_interval"
	case T_enum:
		return "T_enum"
	case T_array_float32:
		return "T_array_float32"
	case T_array_float64:
		return "T_array_float64"
	}
	return "unknown_type"
}

// TypeLen returns type's length whose type oid is T
func (t T) TypeLen() int {
	switch t {
	case T_any:
		return 0
	case T_bit:
		return 8
	case T_int8, T_bool:
		return 1
	case T_int16:
		return 2
	case T_int32, T_date:
		return 4
	case T_int64, T_datetime, T_time, T_timestamp:
		return 8
	case T_uint8:
		return 1
	case T_uint16:
		return 2
	case T_uint32:
		return 4
	case T_uint64:
		return 8
	case T_float32:
		return 4
	case T_float64:
		return 8
	case T_char, T_varchar, T_json, T_blob, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_datalink:
		return VarlenaSize
	case T_decimal64:
		return 8
	case T_decimal128:
		return 16
	case T_decimal256:
		return 32
	case T_uuid:
		return 16
	case T_TS:
		return TxnTsSize
	case T_Rowid:
		return RowidSize
	case T_Blockid:
		return BlockidSize
	case T_tuple, T_interval:
		return 0
	case T_enum:
		return 2
	}
	panic(fmt.Sprintf("unknown type %d", t))
}

// FixedLength dangerous code, use TypeLen() if you don't want -8, -16, -24
func (t T) FixedLength() int {
	switch t {
	case T_any:
		return 0
	case T_bit:
		return 8
	case T_int8, T_uint8, T_bool:
		return 1
	case T_int16, T_uint16:
		return 2
	case T_int32, T_uint32, T_date, T_float32:
		return 4
	case T_int64, T_uint64, T_datetime, T_time, T_float64, T_timestamp:
		return 8
	case T_decimal64:
		return 8
	case T_decimal128:
		return 16
	case T_decimal256:
		return 32
	case T_uuid:
		return 16
	case T_TS:
		return TxnTsSize
	case T_Rowid:
		return RowidSize
	case T_Blockid:
		return BlockidSize
	case T_char, T_varchar, T_blob, T_json, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_datalink:
		return -24
	case T_enum:
		return 2
	}
	panic(moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown type %d", t)))
}

func (t T) IsFixedLen() bool {
	return t.FixedLength() > 0
}

func (t T) IsOrdered() bool {
	switch t {
	case T_int8, T_int16, T_int32, T_int64,
		T_uint8, T_uint16, T_uint32, T_uint64,
		T_float32, T_float64,
		T_date, T_time, T_datetime, T_timestamp,
		T_bit:
		return true
	default:
		return false
	}
}

// IsUnsignedInt return true if the types.T is UnSigned integer type
func (t T) IsUnsignedInt() bool {
	if t == T_uint8 || t == T_uint16 || t == T_uint32 || t == T_uint64 {
		return true
	}
	return false
}

// IsSignedInt return true if the types.T is Signed integer type
func (t T) IsSignedInt() bool {
	if t == T_int8 || t == T_int16 || t == T_int32 || t == T_int64 {
		return true
	}
	return false
}

// IsInteger if expr type is integer return true,else return false
func (t T) IsInteger() bool {
	if t.IsUnsignedInt() || t.IsSignedInt() {
		return true
	}
	return false
}

// IsFloat return true if the types.T is floating Point Types
func (t T) IsFloat() bool {
	if t == T_float32 || t == T_float64 {
		return true
	}
	return false
}

// IsEnum return true if the types.T is Enum type
func (t T) IsEnum() bool {
	return t == T_enum
}

// IsMySQLString return true if the types.T is a MySQL string type (https://dev.mysql.com/doc/refman/8.0/en/string-types.html)
// NOTE: types.IsVarlen() and t.IsMySQLString() are different. t.IsMySQLString() doesn't have T_Json type.
func (t T) IsMySQLString() bool {
	// NOTE: Don't replace this with t.FixedLength()<0
	// The t.FixedLength()<0 logic includes T_Json, which is not a MySQL string type.
	if t == T_char || t == T_varchar || t == T_blob || t == T_text || t == T_binary || t == T_varbinary {
		return true
	}
	return false
}

func (t T) IsDateRelate() bool {
	if t == T_date || t == T_datetime || t == T_timestamp || t == T_time {
		return true
	}
	return false
}

func (t T) IsArrayRelate() bool {
	if t == T_array_float32 || t == T_array_float64 {
		return true
	}
	return false
}

func (t T) IsDatalink() bool {
	return t == T_datalink
}

// IsDecimal return true if the types.T is decimal64 or decimal128
func (t T) IsDecimal() bool {
	if t == T_decimal64 || t == T_decimal128 || t == T_decimal256 {
		return true
	}
	return false
}
