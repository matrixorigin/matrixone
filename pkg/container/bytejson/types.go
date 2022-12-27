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
	"encoding/binary"
)

type TpCode byte
type subPathType byte
type pathFlag byte

type ByteJson struct {
	Data []byte
	Type TpCode
}
type kv struct {
	key string
	val interface{}
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

type UnnestResult map[string]string

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
	TpCodeObject  TpCode = 0x01
	TpCodeArray   TpCode = 0x03
	TpCodeLiteral TpCode = 0x04
	TpCodeInt64   TpCode = 0x09
	TpCodeUint64  TpCode = 0x0a
	TpCodeFloat64 TpCode = 0x0b
	TpCodeString  TpCode = 0x0c
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
