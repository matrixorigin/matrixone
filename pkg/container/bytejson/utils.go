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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func ParseFromString(s string) (ret ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInput("json text %s", s)
		return
	}
	data := string2Slice(s)
	ret, err = ParseFromByteSlice(data)
	return
}
func ParseFromByteSlice(s []byte) (bj ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInput("json text %s", string(s))
		return
	}
	if !json.Valid(s) {
		err = moerr.NewInvalidInput("json text %s", string(s))
		return
	}
	err = bj.UnmarshalJSON(s)
	return
}

func toString(buf, data []byte) []byte {
	return strconv.AppendQuote(buf, string(data))
}

func addElem(buf []byte, in interface{}) (TpCode, []byte, error) {
	var (
		tpCode TpCode
		err    error
	)
	switch x := in.(type) {
	case nil:
		tpCode = TpCodeLiteral
		buf = append(buf, LiteralNull)
	case bool:
		tpCode = TpCodeLiteral
		lit := LiteralFalse
		if x {
			lit = LiteralTrue
		}
		buf = append(buf, lit)
	case int64:
		tpCode = TpCodeInt64
		buf = addUint64(buf, uint64(x))
	case uint64:
		tpCode = TpCodeUint64
		buf = addUint64(buf, x)
	case json.Number:
		tpCode, buf, err = addJsonNumber(buf, x)
	case string:
		tpCode = TpCodeString
		buf = addString(buf, x)
	case ByteJson:
		tpCode = x.Type
		buf = append(buf, x.Data...)
	case []interface{}:
		tpCode = TpCodeArray
		buf, err = addArray(buf, x)
	case map[string]interface{}:
		tpCode = TpCodeObject
		buf, err = addObject(buf, x)
	default:
		return tpCode, nil, moerr.NewInvalidInput("json element %v", in)
	}
	return tpCode, buf, err
}

// extend slice to have n zero bytes
func extendByte(buf []byte, n int) []byte {
	buf = append(buf, make([]byte, n)...)
	return buf
}

// add a uint64 to slice
func addUint64(buf []byte, x uint64) []byte {
	off := len(buf)
	buf = extendByte(buf, numberSize)
	endian.PutUint64(buf[off:], x)
	return buf
}

func addInt64(buf []byte, x int64) []byte {
	return addUint64(buf, uint64(x))
}

func addFloat64(buf []byte, num float64) []byte {
	off := len(buf)
	buf = extendByte(buf, numberSize)
	endian.PutUint64(buf[off:], math.Float64bits(num))
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

func addKeyEntry(buf []byte, start, keyOff int, key string) ([]byte, error) {
	keyLen := uint32(len(key))
	if keyLen > math.MaxUint16 {
		return nil, moerr.NewInvalidInput("json key %s", key)
	}
	//put key offset
	endian.PutUint32(buf[start:], uint32(keyOff))
	//put key length
	endian.PutUint16(buf[start+keyOriginOff:], uint16(keyLen))
	buf = append(buf, key...)
	return buf, nil
}

func addObject(buf []byte, in map[string]interface{}) ([]byte, error) {
	off := len(buf)
	buf = addUint32(buf, uint32(len(in)))
	objStart := len(buf)
	buf = extendByte(buf, docSizeOff)
	keyEntryStart := len(buf)
	buf = extendByte(buf, len(in)*keyEntrySize)
	valEntryStart := len(buf)
	buf = extendByte(buf, len(in)*valEntrySize)
	kvs := make([]kv, 0, len(in))
	for k, v := range in {
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})
	for i, kv := range kvs {
		start := keyEntryStart + i*keyEntrySize
		keyOff := len(buf) - off
		var err error
		buf, err = addKeyEntry(buf, start, keyOff, kv.key)
		if err != nil {
			return nil, err
		}
	}
	for i, kv := range kvs {
		var err error
		valEntryOff := valEntryStart + i*valEntrySize
		buf, err = addValEntry(buf, off, valEntryOff, kv.val)
		if err != nil {
			return nil, err
		}
	}
	endian.PutUint32(buf[objStart:], uint32(len(buf)-off))
	return buf, nil
}
func addArray(buf []byte, in []interface{}) ([]byte, error) {
	off := len(buf)
	buf = addUint32(buf, uint32(len(in)))
	arrSizeStart := len(buf)
	buf = extendByte(buf, docSizeOff)
	valEntryStart := len(buf)
	buf = extendByte(buf, len(in)*valEntrySize)
	for i, v := range in {
		var err error
		buf, err = addValEntry(buf, off, valEntryStart+i*valEntrySize, v)
		if err != nil {
			return nil, err
		}
	}
	arrSize := len(buf) - off
	endian.PutUint32(buf[arrSizeStart:], uint32(arrSize))
	return buf, nil
}

func addValEntry(buf []byte, bufStart, entryStart int, in interface{}) ([]byte, error) {
	valStart := len(buf)
	tpCode, buf, err := addElem(buf, in)
	if err != nil {
		return nil, err
	}
	switch tpCode {
	case TpCodeLiteral:
		lit := buf[valStart]
		buf = buf[:valStart]
		buf[entryStart] = TpCodeLiteral
		buf[entryStart+1] = lit
		return buf, nil
	}
	buf[entryStart] = byte(tpCode)
	endian.PutUint32(buf[entryStart+1:], uint32(valStart-bufStart))
	return buf, nil
}

func addUint32(buf []byte, x uint32) []byte {
	off := len(buf)
	buf = extendByte(buf, 4)
	endian.PutUint32(buf[off:], x)
	return buf
}

func checkFloat64(n float64) error {
	if math.IsInf(n, 0) || math.IsNaN(n) {
		return moerr.NewInvalidInput("json float64 %f", n)
	}
	return nil
}

func addJsonNumber(buf []byte, in json.Number) (TpCode, []byte, error) {
	//check if it is a float
	if strings.ContainsAny(string(in), "Ee.") {
		val, err := in.Float64()
		if err != nil {
			return TpCodeFloat64, nil, moerr.NewInvalidInput("json number %v", in)
		}
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		return TpCodeFloat64, addFloat64(buf, val), nil
	}
	if val, err := in.Int64(); err == nil { //check if it is an int
		return TpCodeInt64, addInt64(buf, val), nil
	}
	if val, err := strconv.ParseUint(string(in), 10, 64); err == nil { //check if it is a uint
		return TpCodeUint64, addUint64(buf, val), nil
	}
	if val, err := in.Float64(); err == nil { //check if it is a float
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		return TpCodeFloat64, addFloat64(buf, val), nil
	}
	var tpCode TpCode
	return tpCode, nil, moerr.NewInvalidInput("json number %v", in)
}
func string2Slice(s string) []byte {
	str := (*reflect.StringHeader)(unsafe.Pointer(&s))
	var ret []byte
	retPtr := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	retPtr.Data = str.Data
	retPtr.Len = str.Len
	retPtr.Cap = str.Len
	return ret
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
	pg := NewPathGenerator(path)
	pg.trimSpace()
	if !pg.hasNext() || pg.next() != '$' {
		err = moerr.NewInvalidInput("invalid json path '%s'", path)
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
			err = moerr.NewInvalidInput("invalid json path '%s'", path)
			return
		}
		pg.trimSpace()
	}

	if len(subPaths) > 0 && subPaths[len(subPaths)-1].tp == subPathDoubleStar {
		err = moerr.NewInvalidInput("invalid json path '%s'", path)
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

func genIndexOrKey(pathStr string) (string, string) {
	if pathStr[len(pathStr)-1] == ']' {
		// find last '['
		idx := strings.LastIndex(pathStr, "[")
		return pathStr[idx : len(pathStr)-1], ""
	}
	// find last '.'
	idx := strings.LastIndex(pathStr, ".")
	return "", pathStr[idx+1:]
}

// for test
func (r UnnestResult) String() string {
	var buf bytes.Buffer
	if val, ok := r["key"]; ok {
		buf.WriteString("key: ")
		buf.WriteString(val + ", ")
	}
	if val, ok := r["path"]; ok {
		buf.WriteString("path: ")
		buf.WriteString(val + ", ")
	}
	if val, ok := r["index"]; ok {
		buf.WriteString("index: ")
		buf.WriteString(val + ", ")
	}
	if val, ok := r["value"]; ok {
		buf.WriteString("value: ")
		buf.WriteString(val + ", ")
	}
	if val, ok := r["this"]; ok {
		buf.WriteString("this: ")
		buf.WriteString(val)
	}
	return buf.String()
}
