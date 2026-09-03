// Copyright 2022 Matrix Origin
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

package bytejson

import (
	"bytes"
	"encoding/binary"
	"math"
)

type subPathType byte
type pathFlag byte

type ByteJson struct {
	Type TpCode
	Data []byte
}

type subPathIndices struct {
	tp  byte
	num int
}
type subPathRangeExpr struct {
	start *subPathIndices
	end   *subPathIndices
}

type subPath struct {
	key    string
	idx    *subPathIndices
	iRange *subPathRangeExpr
	tp     subPathType
}
type Path struct {
	paths []subPath
	flag  pathFlag
}
type pathGenerator struct {
	pathStr string
	pos     int
}

type UnnestResult map[string][]byte

const (
	numberIndices byte = iota + 1
	lastIndices
	lastKey    = "last"
	lastKeyLen = 4
	toKey      = "to"
	toKeyLen   = 2
)

const (
	subPathIdxALL = -1
	subPathIdxErr = -2
)

const (
	subPathDoubleStar subPathType = iota + 1
	subPathIdx
	subPathKey
	subPathKeyWildcard
	subPathRange
)

const (
	pathFlagSingleStar pathFlag = iota + 1
	pathFlagDoubleStar
)

const (
	headerSize   = 8 // element size + data size.
	docSizeOff   = 4 //
	keyEntrySize = 6 // keyOff +  keyLen
	keyOriginOff = 4 // offset -> uint32
	valTypeSize  = 1 // TpCode -> byte
	valEntrySize = 5 // TpCode + offset-or-inline-value
	numberSize   = 8 // float64|int64|uint64
)

const (
	LiteralNull byte = iota + 1
	LiteralTrue
	LiteralFalse
)

// search josn element cut off point.
const (
	binarySearchCutoff = 20
)

var (
	endian = binary.LittleEndian
)

var (
	Null = ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
)

type field struct {
	key string
	val any
}

var (
	escapedChars = map[byte]byte{
		'"': '"',
		'b': '\b',
		'f': '\f',
		'n': '\n',
		'r': '\r',
		't': '\t',
	}
)

type TpCode = byte

const (
	TpCodeObject   TpCode = 0x01
	TpCodeArray    TpCode = 0x03
	TpCodeLiteral  TpCode = 0x04
	TpCodeInt64    TpCode = 0x09
	TpCodeUint64   TpCode = 0x0a
	TpCodeFloat64  TpCode = 0x0b
	TpCodeString   TpCode = 0x0c
	TpCodeDecimal  TpCode = 0x0d
	TpCodeDate     TpCode = 0x0e
	TpCodeTime     TpCode = 0x0f
	TpCodeDatetime TpCode = 0x10
	// TpCodeBlob is the legacy base64-encoded BLOB representation. New binary
	// JSON values retain their bytes in TpCodeOpaque and encode only on output.
	TpCodeBlob   TpCode = 0x11
	TpCodeOpaque TpCode = 0x12
	TpCodeBit    TpCode = 0x13
)

func (bj ByteJson) TYPE() string {
	switch bj.Type {
	case TpCodeObject:
		return "OBJECT"
	case TpCodeArray:
		return "ARRAY"
	case TpCodeLiteral:
		return "LITERAL"
	case TpCodeInt64:
		return "INTEGER"
	case TpCodeUint64:
		return "INTEGER"
	case TpCodeFloat64:
		return "DOUBLE"
	case TpCodeString:
		return "STRING"
	case TpCodeDecimal:
		return "DECIMAL"
	case TpCodeDate:
		return "DATE"
	case TpCodeTime:
		return "TIME"
	case TpCodeDatetime:
		return "DATETIME"
	case TpCodeBlob:
		if _, ok := bj.persistedBitPayload(); ok {
			return "BIT"
		}
		return "BLOB"
	case TpCodeOpaque:
		return "BLOB"
	case TpCodeBit:
		return "BIT"
	default:
		return "OPAQUE"
	}
}

type jsonTypeRank byte

const (
	jsonRankNull jsonTypeRank = iota
	jsonRankNumber
	jsonRankString
	jsonRankObject
	jsonRankArray
	jsonRankBoolean
	jsonRankDate
	jsonRankTime
	jsonRankDatetime
	jsonRankBit
	jsonRankBlob
	jsonRankUnknown
)

type JsonModifyType byte

const (
	// JsonModifyInsert is for insert a new element into a JSON.
	// If an old elemList exists, it would NOT replace it.
	JsonModifyInsert JsonModifyType = 0x01
	// JsonModifyReplace is for replace an old elemList from a JSON.
	// If no elemList exists, it would NOT insert it.
	JsonModifyReplace JsonModifyType = 0x02
	// JsonModifySet = JsonModifyInsert | JsonModifyReplace
	JsonModifySet JsonModifyType = 0x03
	// JsonModifyArrayAppend appends a value to the array selected by a path.
	// A selected scalar or object is autowrapped as an array first.
	JsonModifyArrayAppend JsonModifyType = 0x04
)

func CompareByteJson(left, right ByteJson) int {
	leftBinary := isByteJsonBinary(left.Type)
	rightBinary := isByteJsonBinary(right.Type)
	if leftBinary && rightBinary {
		if !isValidByteJsonStringEncoding(left.Data) || !isValidByteJsonStringEncoding(right.Data) {
			return compareByteJsonFallback(left, right)
		}
		if cmp, ok := CompareBinaryJSON(left, right); ok {
			return cmp
		}
	}
	if isByteJsonNumeric(left.Type) && isByteJsonNumeric(right.Type) {
		if cmp, ok := CompareNumeric(left, right); ok {
			return cmp
		}
		return compareByteJsonFallback(left, right)
	}

	leftRank, leftKnown := byteJsonTypeRank(left)
	rightRank, rightKnown := byteJsonTypeRank(right)
	if !leftKnown || !rightKnown {
		return compareByteJsonFallback(left, right)
	}
	if leftRank != rightRank {
		return compareInt64(int64(leftRank), int64(rightRank))
	}

	var cmp int
	switch leftRank {
	case jsonRankNull:
		return 0
	case jsonRankBoolean:
		return compareInt64(int64(booleanLiteralOrder(left.Data[0])), int64(booleanLiteralOrder(right.Data[0])))
	case jsonRankString, jsonRankDate, jsonRankTime, jsonRankDatetime:
		cmp = bytes.Compare(left.GetString(), right.GetString())
	case jsonRankArray, jsonRankObject:
		return compareByteJsonContainer(left, right, leftRank)
	default:
		return compareByteJsonFallback(left, right)
	}
	return cmp
}

func byteJsonTypeRank(value ByteJson) (jsonTypeRank, bool) {
	switch value.Type {
	case TpCodeLiteral:
		if len(value.Data) == 0 {
			return jsonRankUnknown, false
		}
		switch value.Data[0] {
		case LiteralNull:
			return jsonRankNull, true
		case LiteralTrue, LiteralFalse:
			return jsonRankBoolean, true
		default:
			return jsonRankUnknown, false
		}
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64, TpCodeDecimal:
		return jsonRankNumber, isValidNumericEncoding(value)
	case TpCodeString:
		return jsonRankString, isValidByteJsonStringEncoding(value.Data)
	case TpCodeObject:
		return jsonRankObject, isValidByteJsonContainerHeader(value)
	case TpCodeArray:
		return jsonRankArray, isValidByteJsonContainerHeader(value)
	case TpCodeDate:
		return jsonRankDate, isValidByteJsonStringEncoding(value.Data)
	case TpCodeTime:
		return jsonRankTime, isValidByteJsonStringEncoding(value.Data)
	case TpCodeDatetime:
		return jsonRankDatetime, isValidByteJsonStringEncoding(value.Data)
	case TpCodeBit:
		return jsonRankBit, isValidByteJsonStringEncoding(value.Data)
	case TpCodeBlob:
		if !isValidByteJsonStringEncoding(value.Data) {
			return jsonRankUnknown, false
		}
		if _, ok := value.persistedBitPayload(); ok {
			return jsonRankBit, true
		}
		return jsonRankBlob, true
	case TpCodeOpaque:
		return jsonRankBlob, isValidByteJsonStringEncoding(value.Data)
	default:
		return jsonRankUnknown, false
	}
}

func isByteJsonBinary(tp TpCode) bool {
	return tp == TpCodeBlob || tp == TpCodeOpaque || tp == TpCodeBit
}

func isValidByteJsonStringEncoding(data []byte) bool {
	payloadLength, prefixLength := binary.Uvarint(data)
	return prefixLength > 0 && payloadLength == uint64(len(data)-prefixLength)
}

func isValidByteJsonContainerHeader(value ByteJson) bool {
	if len(value.Data) < headerSize {
		return false
	}
	count := uint64(endian.Uint32(value.Data))
	tableEntrySize := uint64(valEntrySize)
	if value.Type == TpCodeObject {
		tableEntrySize += uint64(keyEntrySize)
	}
	minimumSize := uint64(headerSize) + count*tableEntrySize
	documentSize := uint64(endian.Uint32(value.Data[docSizeOff:]))
	return minimumSize <= documentSize && documentSize <= uint64(len(value.Data))
}

func compareByteJsonContainer(left, right ByteJson, rank jsonTypeRank) (cmp int) {
	defer func() {
		if recover() != nil {
			cmp = compareByteJsonFallback(left, right)
		}
	}()

	leftCnt := left.GetElemCnt()
	rightCnt := right.GetElemCnt()
	if rank == jsonRankArray {
		for i := 0; i < leftCnt && i < rightCnt; i++ {
			cmp = CompareByteJson(left.getArrayElem(i), right.getArrayElem(i))
			if cmp != 0 {
				return cmp
			}
		}
		return leftCnt - rightCnt
	}

	if cmp = compareInt64(int64(leftCnt), int64(rightCnt)); cmp != 0 {
		return cmp
	}
	for i := 0; i < leftCnt; i++ {
		if cmp = bytes.Compare(left.getObjectKey(i), right.getObjectKey(i)); cmp != 0 {
			return cmp
		}
		if cmp = CompareByteJson(left.getObjectVal(i), right.getObjectVal(i)); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func booleanLiteralOrder(literal byte) byte {
	if literal == LiteralFalse {
		return 0
	}
	return 1
}

func compareByteJsonFallback(left, right ByteJson) int {
	if left.Type != right.Type {
		return compareInt64(int64(left.Type), int64(right.Type))
	}
	return bytes.Compare(left.Data, right.Data)
}

func isByteJsonNumeric(tp TpCode) bool {
	switch tp {
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64, TpCodeDecimal:
		return true
	default:
		return false
	}
}

// ParsedNumeric is an immutable, validated ByteJSON numeric scalar. Its fields
// are deliberately private so callers can only obtain a usable value through
// ParseNumeric. It lets a constant operand pay exact normalization once per
// batch instead of once per compared row.
type ParsedNumeric struct {
	key         numericKey
	nonFinite   float64
	isNonFinite bool
	valid       bool
}

// ParseNumeric validates and normalizes one ByteJSON numeric scalar without
// passing exact INT64, UINT64, or DECIMAL values through float64.
func ParseNumeric(value ByteJson) (ParsedNumeric, bool) {
	if !isValidNumericEncoding(value) {
		return ParsedNumeric{}, false
	}
	if value.Type == TpCodeFloat64 {
		floating := value.GetFloat64()
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return ParsedNumeric{
				nonFinite: floating, isNonFinite: true, valid: true,
			}, true
		}
	}
	key, ok := numericKeyFromByteJSON(value)
	if !ok {
		return ParsedNumeric{}, false
	}
	return ParsedNumeric{key: key, valid: true}, true
}

// CompareParsedNumeric compares two values returned by ParseNumeric. ok=false
// rejects zero-value or otherwise invalid ParsedNumeric inputs instead of
// treating them as numeric zero.
func CompareParsedNumeric(left, right ParsedNumeric) (comparison int, ok bool) {
	if !left.valid || !right.valid {
		return 0, false
	}
	if left.isNonFinite && right.isNonFinite {
		return compareFloat64(left.nonFinite, right.nonFinite), true
	}
	if left.isNonFinite {
		if math.IsInf(left.nonFinite, -1) {
			return -1, true
		}
		return 1, true
	}
	if right.isNonFinite {
		if math.IsInf(right.nonFinite, -1) {
			return 1, true
		}
		return -1, true
	}
	return compareNumericKeys(&left.key, &right.key), true
}

// CompareNumeric compares two well-formed ByteJSON numeric scalars. ok=false
// distinguishes a non-numeric or malformed internal value from an ordinary
// non-equal result, which lets cast/comparison boundaries fail closed while
// sharing the same exact numeric model as CompareByteJson.
func CompareNumeric(left, right ByteJson) (comparison int, ok bool) {
	if left.Type != TpCodeDecimal && right.Type != TpCodeDecimal {
		if !isValidNumericEncoding(left) || !isValidNumericEncoding(right) {
			return 0, false
		}
		return compareByteJsonNumeric(left, right), true
	}
	parsedLeft, leftOK := ParseNumeric(left)
	parsedRight, rightOK := ParseNumeric(right)
	if !leftOK || !rightOK {
		return 0, false
	}
	return CompareParsedNumeric(parsedLeft, parsedRight)
}

func isValidNumericEncoding(value ByteJson) bool {
	switch value.Type {
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		return len(value.Data) == 8
	case TpCodeDecimal:
		if len(value.Data) == 0 {
			return false
		}
		payloadLength, prefixLength := binary.Uvarint(value.Data)
		return prefixLength > 0 && payloadLength == uint64(len(value.Data)-prefixLength)
	default:
		return false
	}
}

func compareByteJsonNumeric(left, right ByteJson) int {
	if left.Type == TpCodeDecimal || right.Type == TpCodeDecimal {
		return compareByteJsonNumericExact(left, right)
	}
	switch left.Type {
	case TpCodeInt64:
		switch right.Type {
		case TpCodeInt64:
			return compareInt64(left.GetInt64(), right.GetInt64())
		case TpCodeUint64:
			return compareInt64Uint64(left.GetInt64(), right.GetUint64())
		case TpCodeFloat64:
			return -compareFloat64Int64(right.GetFloat64(), left.GetInt64())
		}
	case TpCodeUint64:
		switch right.Type {
		case TpCodeInt64:
			return -compareInt64Uint64(right.GetInt64(), left.GetUint64())
		case TpCodeUint64:
			return compareUint64(left.GetUint64(), right.GetUint64())
		case TpCodeFloat64:
			return -compareFloat64Uint64(right.GetFloat64(), left.GetUint64())
		}
	case TpCodeFloat64:
		switch right.Type {
		case TpCodeInt64:
			return compareFloat64Int64(left.GetFloat64(), right.GetInt64())
		case TpCodeUint64:
			return compareFloat64Uint64(left.GetFloat64(), right.GetUint64())
		case TpCodeFloat64:
			return compareFloat64(left.GetFloat64(), right.GetFloat64())
		}
	}
	return 0
}

func compareByteJsonNumericExact(left, right ByteJson) int {
	if cmp, handled := compareNonFiniteNumeric(left, right); handled {
		return cmp
	}
	leftKey, leftOK := numericKeyFromByteJSON(left)
	rightKey, rightOK := numericKeyFromByteJSON(right)
	if leftOK && rightOK {
		return compareNumericKeys(&leftKey, &rightKey)
	}
	if leftOK != rightOK {
		if leftOK {
			return -1
		}
		return 1
	}
	if left.Type != right.Type {
		return int(left.Type) - int(right.Type)
	}
	return bytes.Compare(left.Data, right.Data)
}

// compareFloat64Uint64 compares a float64 number and a uint64 number.
func compareFloat64Uint64(x float64, y uint64) int {
	if math.IsNaN(x) {
		return 1
	}
	if x < 0 {
		return -1
	}
	if x >= canonicalUint64LimitFloat {
		return 1
	}
	truncated := math.Trunc(x)
	if cmp := compareUint64(uint64(truncated), y); cmp != 0 {
		return cmp
	}
	return compareFloat64(x, truncated)
}

// compareInt64 compares two int64 numbers.
func compareInt64(x int64, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// compareFloat64 compares two float64 numbers.
func compareFloat64(x float64, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// compareUint64 compares two uint64 numbers.
func compareUint64(x uint64, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// compareInt64Uint64 compares an int64 number and a uint64 number.
func compareInt64Uint64(x int64, y uint64) int {
	if x < 0 {
		return -1
	}
	return compareUint64(uint64(x), y)
}

// compareFloat64Int64 compares a float64 number and an int64 number.
func compareFloat64Int64(x float64, y int64) int {
	if math.IsNaN(x) {
		return 1
	}
	if x < canonicalMinInt64Float {
		return -1
	}
	if x >= -canonicalMinInt64Float {
		return 1
	}
	truncated := math.Trunc(x)
	if cmp := compareInt64(int64(truncated), y); cmp != 0 {
		return cmp
	}
	return compareFloat64(x, truncated)
}
