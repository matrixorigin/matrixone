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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"
)

func ParseValueToByteJson(num *tree.NumVal) (ByteJson, error) {
	val := num.String()
	bj, err := ParseFromString(val)
	return *bj, err

}

func (bj ByteJson) String() string {
	ret, _ := bj.MarshalJSON()
	return string(ret)
}

func (bj ByteJson) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 0, len(bj.Data)*3/2)
	return bj.to(ret)
}

func (bj *ByteJson) UnmarshalJSON(data []byte) error {
	var decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var in interface{}
	err := decoder.Decode(&in)
	if err != nil {
		return nil
	}
	buf := make([]byte, 0, len(data))
	if tpCode, buf, err := addElem(buf, in); err != nil {
		return err
	} else {
		bj.Data = buf
		bj.Type = tpCode
	}
	return nil
}

func (bj ByteJson) GetElemCnt() int {
	return int(endian.Uint32(bj.Data))
}

func (bj ByteJson) GetInt64() int64 {
	return int64(bj.GetUint64())
}
func (bj ByteJson) GetUint64() uint64 {
	return endian.Uint64(bj.Data)
}

func (bj ByteJson) GetFloat64() float64 {
	return math.Float64frombits(bj.GetUint64())
}

func (bj ByteJson) GetString() []byte {
	num, length := calStrLen(bj.Data)
	return bj.Data[length : length+num]
}

func (bj ByteJson) to(buf []byte) ([]byte, error) {
	var err error
	switch bj.Type {
	case TpCodeArray:
		buf, err = bj.toArray(buf)
	case TpCodeObject:
		buf, err = bj.toObject(buf)
	case TpCodeInt64:
		buf = bj.toInt64(buf)
	case TpCodeUint64:
		buf = bj.toUint64(buf)
	case TpCodeLiteral:
		buf = bj.toLiteral(buf)
	case TpCodeFloat64:
		buf, err = bj.toFloat64(buf)
	case TpCodeString:
		buf = bj.toString(buf)
	default:
		err = errors.New(errno.UnSupportedJsonType, fmt.Sprintf("invalid type:%d", bj.Type))
	}
	return buf, err
}

func (bj ByteJson) toArray(buf []byte) ([]byte, error) {
	cnt := bj.GetElemCnt()
	buf = append(buf, '[')
	var err error
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		buf, err = bj.getArrayElem(i).to(buf)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, ']'), nil
}

func (bj ByteJson) toObject(buf []byte) ([]byte, error) {
	cnt := bj.GetElemCnt()
	buf = append(buf, '{')
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		var err error
		buf = toString(buf, bj.getObjectKey(i))
		buf = append(buf, ": "...)
		buf, err = bj.getObjectVal(i).to(buf)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, '}'), nil
}

func (bj ByteJson) toInt64(buf []byte) []byte {
	return strconv.AppendInt(buf, bj.GetInt64(), 10)
}
func (bj ByteJson) toUint64(buf []byte) []byte {
	return strconv.AppendUint(buf, bj.GetUint64(), 10)
}

func (bj ByteJson) toLiteral(buf []byte) []byte {
	litTp := bj.Data[0]
	switch litTp {
	case LiteralNull:
		buf = append(buf, "null"...)
	case LiteralTrue:
		buf = append(buf, "true"...)
	case LiteralFalse:
		buf = append(buf, "false"...)
	default:
		panic(fmt.Sprintf("invalid literal type:%d", litTp))
	}
	return buf
}

func (bj ByteJson) toFloat64(buf []byte) ([]byte, error) {
	f := bj.GetFloat64()
	err := checkFloat64(f)
	if err != nil {
		return nil, err
	}
	// https://github.com/golang/go/issues/14135
	var format byte
	abs := math.Abs(f)
	if abs == 0 || 1e-6 <= abs && abs < 1e21 {
		format = 'f'
	} else {
		format = 'e'
	}
	buf = strconv.AppendFloat(buf, f, format, -1, 64)
	return buf, nil
}

func toString(buf, data []byte) []byte {
	return strconv.AppendQuote(buf, string(data))
}

//transform byte string to visible string
func (bj ByteJson) toString(buf []byte) []byte {
	data := bj.GetString()
	return toString(buf, data)
}

func (bj ByteJson) getObjectKey(i int) []byte {
	keyOff := int(endian.Uint32(bj.Data[headerSize+i*keyEntrySize:]))
	keyLen := int(endian.Uint16(bj.Data[headerSize+i*keyEntrySize+keyOriginOff:]))
	return bj.Data[keyOff : keyOff+keyLen]
}

func (bj ByteJson) getArrayElem(i int) ByteJson {
	return bj.getValEntry(headerSize + i*valEntrySize)
}

func (bj ByteJson) getObjectVal(i int) ByteJson {
	cnt := bj.GetElemCnt()
	return bj.getValEntry(headerSize + cnt*keyEntrySize + i*valEntrySize)
}

func (bj ByteJson) getValEntry(off int) ByteJson {
	tpCode := bj.Data[off]
	valOff := endian.Uint32(bj.Data[off+valTypeSize:])
	switch tpCode {
	case TpCodeLiteral:
		return ByteJson{Type: TpCodeLiteral, Data: bj.Data[off+valTypeSize : off+valTypeSize+1]}
	case TpCodeUint64, TpCodeInt64, TpCodeFloat64:
		return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+numberSize]}
	case TpCodeString:
		num, length := calStrLen(bj.Data[valOff:])
		totalLen := uint32(num) + uint32(length)
		return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+totalLen]}
	}
	dataBytes := endian.Uint32(bj.Data[valOff+docSizeOff:])
	return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+dataBytes]}
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
		return tpCode, nil, errors.New(errno.InvalidJsonText, fmt.Sprintf("invalid type:%v", reflect.TypeOf(in)))
	}
	return tpCode, buf, err
}

// extend slice to have n zero bytes
func extendByte(buf []byte, n int) []byte {
	buf = append(buf, make([]byte, n)...)
	return buf
}

//add a uint64 to slice
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
		return nil, errors.New(errno.InvalidJsonKeyTooLong, fmt.Sprintf("key: %s", key))
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
		return errors.New(errno.InvalidJsonNumber, fmt.Sprintf("the number %v is Inf or NaN", n))
	}
	return nil
}

func addJsonNumber(buf []byte, in json.Number) (TpCode, []byte, error) {
	//check if it is a float
	if strings.ContainsAny(string(in), "Ee.") {
		val, err := in.Float64()
		if err != nil {
			return TpCodeFloat64, nil, errors.New(errno.InvalidJsonNumber, fmt.Sprintf("error occurs while transforming float,err :%v", err.Error()))
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
	return tpCode, nil, errors.New(errno.InvalidJsonNumber, fmt.Sprintf("error occurs while transforming number"))
}

func ParseFromString(s string) (*ByteJson, error) {
	if len(s) == 0 {
		return nil, errors.New(errno.EmptyJsonText, "")
	}
	data := string2Slice(s)
	return ParseFromByteSlice(data)
}
func ParseFromByteSlice(s []byte) (*ByteJson, error) {
	if len(s) == 0 {
		return nil, errors.New(errno.EmptyJsonText, "")
	}
	if !json.Valid(s) {
		return nil, errors.New(errno.InvalidJsonText, fmt.Sprintf("invalid json text:%s", s))
	}
	bj := &ByteJson{}
	return bj, bj.UnmarshalJSON(s)
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
