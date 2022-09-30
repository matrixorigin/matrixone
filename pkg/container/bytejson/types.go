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
	"regexp"
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
type subPath struct {
	idx int
	key string
	tp  subPathType
}
type Path struct {
	paths []subPath
	flag  pathFlag
}

//type UnnestResult struct {
//	Key   string
//	Path  string
//	Index string
//	Value string
//	This  string
//}

type UnnestResult map[string]string

const subPathIdxALL = -1

const (
	subPathDoubleStar = 0x01
	subPathIdx        = 0x02
	subPathKey        = 0x03
)
const (
	pathFlagSingleStar = 0x01
	pathFlagDoubleStar = 0x02
)

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
	docSizeOff   = 4 //
	keyEntrySize = 6 // keyOff +  keyLen
	keyOriginOff = 4 // offset -> uint32
	valTypeSize  = 1 // TpCode -> byte
	valEntrySize = 5 // TpCode + offset-or-inline-value
	numberSize   = 8 // float64|int64|uint64
)

const (
	LiteralNull  byte = 0x00
	LiteralTrue  byte = 0x01
	LiteralFalse byte = 0x02
)

var (
	endian        = binary.LittleEndian
	jsonSubPathRe = regexp.MustCompile(`(\.\s*(([\$]*[a-zA-Z_][a-zA-Z0-9_]*)+|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)`)
)
