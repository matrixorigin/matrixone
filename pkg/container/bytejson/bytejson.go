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
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"
)

import (
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

func (bj ByteJson) String() string {
	ret, _ := bj.MarshalJSON()
	return string(ret)
}

func (bj ByteJson) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 0, len(bj.Data)*3/2)
	return bj.to(ret)
}
func (bj ByteJson) to(buf []byte) ([]byte, error) {
	switch bj.Type {
	case TpCodeLiteral:
		buf = toLiteral(buf, bj.Data[0])
	case TpCodeString:
		buf = addString(buf, string(bj.Data))

	}
	return buf, nil
}

func toLiteral(buf []byte, litTp byte) []byte {
	switch litTp {
	case LiteralNull:
		buf = append(buf, "null"...)
	case LiteralTrue:
		buf = append(buf, "true"...)
	case LiteralFalse:
		buf = append(buf, "false"...)
	}
	return buf
}

//transform byte string to visible string
func toString(buf, data []byte) ([]byte, error) {
	return buf, nil
}

func (bj ByteJson) GetString() []byte {
	num, length := uint64(bj.Data[0]), 1
	if num >= utf8.RuneSelf {
		num, length = binary.Uvarint(bj.Data)
	}
	return bj.Data[length : length+int(num)]
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
	tpCode, buf, err := addElem(buf, in)
	if err != nil {
		return errors.New(errno.InvalidJsonText, fmt.Sprintf("err:%v", err.Error()))
	}
	bj.Type = tpCode
	bj.Data = buf
	return nil
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
func addZero(buf []byte, n int) []byte {
	buf = append(buf, make([]byte, n)...)
	return buf
}

//add a uint64 to slice
func addUint64(buf []byte, x uint64) []byte {
	off := len(buf)
	buf = addZero(buf, 8)
	endian.PutUint64(buf[off:], x)
	return buf
}

func addFloat64(buf []byte, num float64) []byte {
	off := len(buf)
	buf = addZero(buf, 8)
	endian.PutUint64(buf[off:], math.Float64bits(num))
	return buf
}
func addString(buf []byte, in string) []byte {
	off := len(buf)
	//encoding length
	buf = addZero(buf, binary.MaxVarintLen64)
	inLen := binary.PutUvarint(buf[off:], uint64(len(in)))
	//cut length
	buf = buf[:off+inLen]
	//add string
	buf = append(buf, in...)
	return buf
}

func addObject(buf []byte, in map[string]interface{}) ([]byte, error) {
	off := len(buf)
	buf = addUint32(buf, uint32(len(in)))
	objSizeStart := len(buf)
	buf = addZero(buf, dataSizeOff)
	keyEntryStart := len(buf)
	buf = addZero(buf, len(in)*keyEntrySize)
	valEntryStart := len(buf)
	buf = addZero(buf, len(in)*valEntrySize)
	kvs := make([]kv, 0, len(in))
	for k, v := range in {
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})
	for i, kv := range kvs {
		keyEntryOff := keyEntryStart + i*keyEntrySize
		keyOff := len(buf) - off
		keyLen := uint32(len(kv.key))
		if keyLen > math.MaxUint16 {
			return nil, errors.New(errno.InvalidJsonKeyTooLong, fmt.Sprintf("key:%s", kv.key))
		}
		endian.PutUint32(buf[keyEntryOff:], uint32(keyOff))
		endian.PutUint16(buf[keyEntryOff+keyLenOff:], uint16(keyLen))
		buf = append(buf, kv.key...)
	}
	for i, kv := range kvs {
		var err error
		valEntryOff := valEntryStart + i*valEntrySize
		buf, err = addValEntry(buf, off, valEntryOff, kv.val)
		if err != nil {
			return nil, err
		}
	}
	endian.PutUint32(buf[objSizeStart:], uint32(len(buf)-off))
	return buf, nil
}
func addArray(buf []byte, in []interface{}) ([]byte, error) {
	off := len(buf)
	buf = addUint32(buf, uint32(len(in)))
	arrSizeStart := len(buf)
	buf = addZero(buf, dataSizeOff)
	valEntryStart := len(buf)
	buf = addZero(buf, len(in)*valEntrySize)
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
	buf = addZero(buf, 4)
	endian.PutUint32(buf[off:], x)
	return buf
}

func addJsonNumber(buf []byte, in json.Number) (TpCode, []byte, error) {
	if strings.ContainsAny(string(in), "Ee.") {
		num, err := in.Float64()
		if err != nil {
			return TpCodeFloat64, nil, errors.New(errno.InvalidJsonNumber, fmt.Sprintf("err:%v", err.Error()))
		}
		buf = addFloat64(buf, num)
		return TpCodeFloat64, buf, nil
	} else if val, err := in.Int64(); err == nil {
		return TpCodeInt64, addUint64(buf, uint64(val)), nil
	} else if val, err := strconv.ParseUint(string(in), 10, 64); err == nil {
		return TpCodeUint64, addUint64(buf, val), nil
	}
	val, err := in.Float64()
	if err == nil {
		return TpCodeFloat64, addFloat64(buf, val), nil
	}
	var tpCode TpCode
	return tpCode, nil, errors.New(errno.InvalidJsonNumber, fmt.Sprintf("err:%v", err.Error()))
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
		return nil, errors.New(errno.InvalidJsonText, "")
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
