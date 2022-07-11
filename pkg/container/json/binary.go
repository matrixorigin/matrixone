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

package json

import (
	"bytes"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"reflect"
	"unsafe"
)

type ByteJson struct {
	Data []byte
	Type TpCode
}

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
		return errors.New("parse from string err", err.Error())
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
		//case map[string]interface{}:
		//	tpCode = TpCodeObject

	}
	return tpCode, buf, err
}

func ParseFromString(s string) (*ByteJson, error) {
	if len(s) == 0 {
		return nil, ErrEmptyJsonText
	}
	data := string2Slice(s)
	return ParseFromByteSlice(data)
}
func ParseFromByteSlice(s []byte) (*ByteJson, error) {
	if len(s) == 0 {
		return nil, ErrEmptyJsonText
	}
	if !json.Valid(s) {
		return nil, ErrInvalidJsonText
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
