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
	"encoding/json"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

func ParseFromString(s string) (ret ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInputNoCtx("json text %s", s)
		return
	}
	data := util.UnsafeStringToBytes(s)
	ret, err = ParseFromByteSlice(data)
	return
}

func ParseFromByteSlice(s []byte) (bj ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInputNoCtx("json text %s", string(s))
		return
	}
	if !json.Valid(s) {
		err = moerr.NewInvalidInputNoCtx("json text %s", string(s))
		return
	}
	err = bj.UnmarshalJSON(s)
	return
}

func toString(buf, data []byte) []byte {
	bs, _ := appendString(buf, string(data))
	return bs
}

// extend slice to have n zero bytes
func extendByte(buf []byte, n int) []byte {
	buf = append(buf, make([]byte, n)...)
	return buf
}

func addString(buf []byte, in string) []byte {
	off := len(buf)
	//encoding length
	buf = extendByte(buf, binary.MaxVarintLen64)
	inLen := binary.PutUvarint(buf[off:], uint64(len(in)))
	//cut length
	buf = buf[:off+inLen]
	//add string
	buf = append(buf, in...)
	return buf
}

func checkFloat64(n float64) error {
	if math.IsInf(n, 0) || math.IsNaN(n) {
		return moerr.NewInvalidInputNoCtx("json float64 %f", n)
	}
	return nil
}

func calStrLen(buf []byte) (int, int) {
	strLen, lenLen := uint64(buf[0]), 1
	if strLen >= utf8.RuneSelf {
		strLen, lenLen = binary.Uvarint(buf)
	}
	return int(strLen), lenLen
}

func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if (i != 0 && s[i] >= '0' && s[i] <= '9') ||
			(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') ||
			s[i] == '_' || s[i] == '$' || s[i] >= 0x80 {
			continue
		}
		return false
	}
	return true
}

func ParseJsonPath(path string) (p Path, err error) {
	pg := newPathGenerator(path)
	pg.trimSpace()
	if !pg.hasNext() || pg.next() != '$' {
		err = moerr.NewInvalidInputNoCtx("invalid json path '%s'", path)
	}
	pg.trimSpace()
	subPaths := make([]subPath, 0, 8)
	var ok bool
	for pg.hasNext() {
		switch pg.front() {
		case '.':
			subPaths, ok = pg.generateKey(subPaths)
		case '[':
			subPaths, ok = pg.generateIndex(subPaths)
		case '*':
			subPaths, ok = pg.generateDoubleStar(subPaths)
		default:
			ok = false
		}
		if !ok {
			err = moerr.NewInvalidInputNoCtx("invalid json path '%s'", path)
			return
		}
		pg.trimSpace()
	}

	if len(subPaths) > 0 && subPaths[len(subPaths)-1].tp == subPathDoubleStar {
		err = moerr.NewInvalidInputNoCtx("invalid json path '%s'", path)
		return
	}
	p.init(subPaths)
	return
}

func addByteElem(buf []byte, entryStart int, elems []ByteJson) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = byte(elem.Type)
		if elem.Type == TpCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Data[0]
		} else {
			endian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Data...)
		}
	}
	return buf
}

func mergeToArray(origin []ByteJson) *ByteJson {
	totalSize := headerSize + len(origin)*valEntrySize
	for _, el := range origin {
		if el.Type != TpCodeLiteral {
			totalSize += len(el.Data)
		}
	}
	buf := make([]byte, headerSize+len(origin)*valEntrySize, totalSize)
	endian.PutUint32(buf, uint32(len(origin)))
	endian.PutUint32(buf[docSizeOff:], uint32(totalSize))
	buf = addByteElem(buf, headerSize, origin)
	return &ByteJson{Type: TpCodeArray, Data: buf}
}

// check unnest mode
func checkMode(mode string) bool {
	if mode == "both" || mode == "array" || mode == "object" {
		return true
	}
	return false
}

func genIndexOrKey(pathStr string) ([]byte, []byte) {
	if pathStr[len(pathStr)-1] == ']' {
		// find last '['
		idx := strings.LastIndex(pathStr, "[")
		return util.UnsafeStringToBytes(pathStr[idx : len(pathStr)-1]), nil
	}
	// find last '.'
	idx := strings.LastIndex(pathStr, ".")
	return nil, util.UnsafeStringToBytes(pathStr[idx+1:])
}

// for test
func (r UnnestResult) String() string {
	var buf bytes.Buffer
	if val, ok := r["key"]; ok && val != nil {
		buf.WriteString("key: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["path"]; ok && val != nil {
		buf.WriteString("path: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["index"]; ok && val != nil {
		buf.WriteString("index: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["value"]; ok && val != nil {
		buf.WriteString("value: ")
		bj := ByteJson{}
		bj.Unmarshal(val)
		val, _ = bj.MarshalJSON()
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["this"]; ok && val != nil {
		buf.WriteString("this: ")
		bj := ByteJson{}
		bj.Unmarshal(val)
		val, _ = bj.MarshalJSON()
		buf.WriteString(string(val))
	}
	return buf.String()
}

func checkAllNull(vals []ByteJson) bool {
	allNull := true
	for _, val := range vals {
		if !val.IsNull() {
			allNull = false
			break
		}
	}
	return allNull
}
