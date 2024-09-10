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

var (
	endian = binary.LittleEndian
)

var (
	Null = ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
)

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
	TpCodeObject  TpCode = 0x01
	TpCodeArray   TpCode = 0x03
	TpCodeLiteral TpCode = 0x04
	TpCodeInt64   TpCode = 0x09
	TpCodeUint64  TpCode = 0x0a
	TpCodeFloat64 TpCode = 0x0b
	TpCodeString  TpCode = 0x0c
)

func (bj ByteJson) TYPE() string {
	switch bj.Type {
	case TpCodeObject:
		return "OBJECT"
	case TpCodeArray:
		return "ARRAY"
	case TpCodeLiteral:
		return "NULL"
	case TpCodeInt64:
		return "INTEGER"
	case TpCodeUint64:
		return "UNSIGNED INTEGER"
	case TpCodeFloat64:
		return "DOUBLE"
	case TpCodeString:
		return "STRING"
	default:
		return "UNKNOWN"
	}
}

var jsonTpOrder = map[string]int{
	"ARRAY":            -1,
	"OBJECT":           -2,
	"STRING":           -3,
	"INTEGER":          -4,
	"UNSIGNED INTEGER": -5,
	"DOUBLE":           -6,
	"NULL":             -7,
}

func CompareByteJson(left, right ByteJson) int {
	order1 := jsonTpOrder[left.TYPE()]
	order2 := jsonTpOrder[right.TYPE()]

	var cmp int
	if order1 == order2 {
		if order1 == jsonTpOrder["NULL"] {
			cmp = 0
		}
		switch left.Type {
		case TpCodeLiteral:
			cmp = int(left.Data[0]) - int(right.Data[0])
		case TpCodeInt64:
			cmp = compareInt64(left.GetInt64(), right.GetInt64())

		case TpCodeUint64:
			cmp = compareUint64(left.GetUint64(), right.GetUint64())
		case TpCodeFloat64:
			cmp = compareFloat64(left.GetFloat64(), right.GetFloat64())
		case TpCodeString:
			cmp = bytes.Compare(left.GetString(), right.GetString())
		case TpCodeArray:
			leftCnt := left.GetElemCnt()
			rightCnt := right.GetElemCnt()
			for i := 0; i < leftCnt && i < rightCnt; i++ {
				elem1 := left.getArrayElem(i)
				elem2 := right.getArrayElem(i)
				cmp = CompareByteJson(elem1, elem2)
				if cmp != 0 {
					return cmp
				}
				cmp = leftCnt - rightCnt
			}
		case TpCodeObject:
			leftCnt := left.GetElemCnt()
			rightCnt := right.GetElemCnt()
			cmp = compareInt64(int64(leftCnt), int64(rightCnt))
			if cmp != 0 {
				return cmp
			}
			for i := 0; i < leftCnt; i++ {
				leftKey := left.getObjectKey(i)
				rightKey := right.getObjectKey(i)
				cmp = bytes.Compare(leftKey, rightKey)
				if cmp != 0 {
					return cmp
				}
				cmp = CompareByteJson(left.getObjectVal(i), right.getObjectVal(i))
				if cmp != 0 {
					return cmp
				}
			}
		}
	} else {
		if (-6 <= order1 && order1 <= -4) && (-6 <= order2 && order2 <= -4) {
			switch left.Type {
			case TpCodeInt64:
				switch right.Type {
				case TpCodeInt64:
					cmp = compareInt64(left.GetInt64(), right.GetInt64())
				case TpCodeUint64:
					cmp = compareInt64Uint64((left.GetInt64()), right.GetUint64())
				case TpCodeFloat64:
					cmp = -compareFloat64Int64(right.GetFloat64(), left.GetInt64())
				}
			case TpCodeUint64:
				switch right.Type {
				case TpCodeInt64:
					cmp = -compareInt64Uint64(right.GetInt64(), left.GetUint64())
				case TpCodeUint64:
					cmp = compareUint64(left.GetUint64(), right.GetUint64())
				case TpCodeFloat64:
					cmp = -compareFloat64Uint64(right.GetFloat64(), left.GetUint64())
				}
			case TpCodeFloat64:
				switch right.Type {
				case TpCodeInt64:
					cmp = compareFloat64Int64(left.GetFloat64(), right.GetInt64())
				case TpCodeUint64:
					cmp = compareFloat64Uint64(left.GetFloat64(), right.GetUint64())
				case TpCodeFloat64:
					cmp = compareFloat64(left.GetFloat64(), right.GetFloat64())
				}
			}
			return cmp
		} else {
			cmp = order1 - order2
			if cmp > 0 {
				cmp = 1
			} else if cmp < 0 {
				cmp = -1
			}

		}
	}
	return cmp
}

const floatEpsilon = 1.e-8

// compareFloat64PrecisionLoss compares two float64 numbers with precision loss.
func compareFloat64PrecisionLoss(x, y float64) int {
	if x-y < floatEpsilon && y-x < floatEpsilon {
		return 0
	} else if x-y < 0 {
		return -1
	}
	return 1
}

// compareFloat64Uint64 compares a float64 number and a uint64 number.
func compareFloat64Uint64(x float64, y uint64) int {
	return compareFloat64PrecisionLoss(x, float64(y))
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
	return compareFloat64PrecisionLoss(x, float64(y))
}
