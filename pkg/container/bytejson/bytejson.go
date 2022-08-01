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
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"math"
	"strconv"
)

func (bj ByteJson) String() string {
	ret, _ := bj.MarshalJSON()
	return string(ret)
}

// MarshalJSON transform bytejson to []byte,for visible
func (bj ByteJson) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 0, len(bj.Data)*3/2)
	return bj.to(ret)
}

// Marshal transform bytejson to []byte,for storage
func (bj ByteJson) Marshal() ([]byte, error) {
	buf := make([]byte, len(bj.Data)+1)
	buf[0] = byte(bj.Type)
	copy(buf[1:], bj.Data)
	return buf, nil
}

// Unmarshal transform storage []byte  to bytejson
func (bj *ByteJson) Unmarshal(buf []byte) error {
	//TODO add validate checker
	bj.Type = TpCode(buf[0])
	bj.Data = buf[1:]
	return nil
}

// UnmarshalJSON transform visible []byte to bytejson
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
