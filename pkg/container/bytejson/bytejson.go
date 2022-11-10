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
	"math"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func (bj ByteJson) IsNull() bool {
	return bj.Type == TpCodeLiteral && bj.Data[0] == LiteralNull
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
		err = moerr.NewInvalidInput("invalid json type '%v'", bj.Type)
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

// transform byte string to visible string
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

func (bj ByteJson) queryValByKey(key []byte) ByteJson {
	cnt := bj.GetElemCnt()
	idx := sort.Search(cnt, func(i int) bool {
		return bytes.Compare(bj.getObjectKey(i), key) >= 0
	})
	if idx >= cnt || !bytes.Equal(bj.getObjectKey(idx), key) {
		dt := make([]byte, 1)
		dt[0] = LiteralNull
		return ByteJson{
			Type: TpCodeLiteral,
			Data: dt,
		}
	}
	return bj.getObjectVal(idx)
}

func (bj ByteJson) query(cur []ByteJson, path *Path) []ByteJson {
	if path.empty() {
		cur = append(cur, bj)
		return cur
	}
	sub, nPath := path.step()
	if sub.tp == subPathIdx && bj.Type == TpCodeObject && sub.idx == 0 {
		cur = bj.query(cur, &nPath)
		return cur
	}
	if sub.tp == subPathIdx && bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		if sub.idx < subPathIdxALL || sub.idx >= cnt { // idx is out of range
			tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
			cur = append(cur, tmp)
			return cur
		}
		if sub.idx == subPathIdxALL {
			for i := 0; i < cnt; i++ {
				cur = bj.getArrayElem(i).query(cur, &nPath)
			}
		} else {
			cur = bj.getArrayElem(sub.idx).query(cur, &nPath)
		}
	}
	if sub.tp == subPathKey && bj.Type == TpCodeObject {
		cnt := bj.GetElemCnt()
		if sub.key == "*" {
			for i := 0; i < cnt; i++ {
				cur = bj.getObjectVal(i).query(cur, &nPath)
			}
		} else {
			tmp := bj.queryValByKey(string2Slice(sub.key))
			cur = tmp.query(cur, &nPath)
		}
	}
	if sub.tp == subPathDoubleStar {
		cur = bj.query(cur, &nPath)
		if bj.Type == TpCodeObject {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				cur = bj.getObjectVal(i).query(cur, path) // take care here, the argument is path,not nPath
			}
		} else if bj.Type == TpCodeArray {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				cur = bj.getArrayElem(i).query(cur, path) // take care here, the argument is path,not nPath
			}
		}
	}
	return cur
}
func (bj ByteJson) Query(path *Path) *ByteJson {
	out := make([]ByteJson, 0, 1)
	out = bj.query(out, path)
	if len(out) == 0 {
		return &ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
	}
	if len(out) == 1 {
		return &out[0]
	}
	fullNull := true
	for i := 0; i < len(out); i++ {
		if out[i].Type != TpCodeLiteral || out[i].Data[0] != LiteralNull {
			fullNull = false
			break
		}
	}
	if fullNull {
		return &ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
	}
	return mergeToArray(out)
}

func (bj ByteJson) canUnnest() bool {
	return bj.Type == TpCodeArray || bj.Type == TpCodeObject
}

func (bj ByteJson) queryWithSubPath(keys []string, vals []ByteJson, path *Path, pathStr string) ([]string, []ByteJson) {
	if path.empty() {
		keys = append(keys, pathStr)
		vals = append(vals, bj)
		return keys, vals
	}
	sub, nPath := path.step()
	if sub.tp == subPathIdx && bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		if sub.idx < subPathIdxALL || sub.idx >= cnt { // idx is out of range
			tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
			newPathStr := fmt.Sprintf("%s[%d]", pathStr, sub.idx)
			keys = append(keys, newPathStr)
			vals = append(vals, tmp)
			return keys, vals
		}
		if sub.idx == subPathIdxALL {
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
				keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		} else {
			newPathStr := fmt.Sprintf("%s[%d]", pathStr, sub.idx)
			keys, vals = bj.getArrayElem(sub.idx).queryWithSubPath(keys, vals, &nPath, newPathStr)
		}
	}
	if sub.tp == subPathKey && bj.Type == TpCodeObject {
		cnt := bj.GetElemCnt()
		if sub.key == "*" {
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s.%s", pathStr, bj.getObjectKey(i))
				keys, vals = bj.getObjectVal(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		} else {
			tmp := bj.queryValByKey(string2Slice(sub.key))
			newPathStr := fmt.Sprintf("%s.%s", pathStr, sub.key)
			keys, vals = tmp.queryWithSubPath(keys, vals, &nPath, newPathStr)
		}
	}
	if sub.tp == subPathDoubleStar {
		keys, vals = bj.queryWithSubPath(keys, vals, &nPath, pathStr)
		if bj.Type == TpCodeObject {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s.%s", pathStr, bj.getObjectKey(i))
				keys, vals = bj.getObjectVal(i).queryWithSubPath(keys, vals, path, newPathStr) // take care here, the argument is path,not nPath
			}
		} else if bj.Type == TpCodeArray {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
				keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, path, newPathStr) // take care here, the argument is path,not nPath
			}
		}
	}
	return keys, vals
}

func (bj ByteJson) unnestWithParams(out []UnnestResult, outer, recursive bool, mode string, pathStr string, this *ByteJson, filters []string) []UnnestResult {
	if !bj.canUnnest() {
		index, key := genIndexOrKey(pathStr)
		tmp := UnnestResult{}
		maybePut(tmp, "path", pathStr, filters)
		maybePut(tmp, "value", bj.String(), filters)
		maybePut(tmp, "index", index, filters)
		maybePut(tmp, "key", key, filters)
		maybePut(tmp, "this", this.String(), filters)
		out = append(out, tmp)
		return out
	}
	if bj.Type == TpCodeObject && mode != "array" {
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			key := bj.getObjectKey(i)
			val := bj.getObjectVal(i)
			newPathStr := fmt.Sprintf("%s.%s", pathStr, key)
			tmp := UnnestResult{}
			maybePut(tmp, "path", newPathStr, filters)
			maybePut(tmp, "value", val.String(), filters)
			maybePut(tmp, "key", string(key), filters)
			maybePut(tmp, "this", this.String(), filters)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, &val, filters)
			}
		}
	}
	if bj.Type == TpCodeArray && mode != "object" {
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			val := bj.getArrayElem(i)
			newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
			tmp := UnnestResult{}
			maybePut(tmp, "path", newPathStr, filters)
			maybePut(tmp, "value", val.String(), filters)
			maybePut(tmp, "index", strconv.Itoa(i), filters)
			maybePut(tmp, "this", this.String(), filters)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, &val, filters)
			}
		}
	}
	return out
}

func (bj ByteJson) unnest(out []UnnestResult, path *Path, outer, recursive bool, mode string, filters []string) ([]UnnestResult, error) {

	keys := make([]string, 0, 1)
	vals := make([]ByteJson, 0, 1)
	keys, vals = bj.queryWithSubPath(keys, vals, path, "$")
	if len(keys) != len(vals) {
		return nil, moerr.NewInvalidInput("len(key) and len(val) are not equal, len(key)=%d, len(val)=%d", len(keys), len(vals))
	}
	for i := 0; i < len(keys); i++ {
		if vals[i].canUnnest() {
			out = vals[i].unnestWithParams(out, outer, recursive, mode, keys[i], &vals[i], filters)
		}
	}
	if len(out) == 0 && outer {
		for i := 0; i < len(keys); i++ {
			tmp := UnnestResult{}
			maybePut(tmp, "path", keys[i], filters)
			maybePut(tmp, "this", vals[i].String(), filters)
			out = append(out, tmp)
		}
	}
	return out, nil
}

// Unnest returns a slice of UnnestResult, each UnnestResult contains filtered data, if param filters is nil, return all fields.
func (bj ByteJson) Unnest(path *Path, outer, recursive bool, mode string, filters []string) ([]UnnestResult, error) {
	if !checkMode(mode) {
		return nil, moerr.NewInvalidInput("mode must be one of [object, array, both]")
	}
	out := make([]UnnestResult, 0, 1)
	out, err := bj.unnest(out, path, outer, recursive, mode, filters)
	return out, err
}

func maybePut(out UnnestResult, key, value string, filters []string) {
	if filters == nil {
		out[key] = value
		return
	}
	for _, filter := range filters {
		if filter == key {
			out[key] = value
			return
		}
	}
}
