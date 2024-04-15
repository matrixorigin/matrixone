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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

func (bj ByteJson) String() string {
	ret, _ := bj.MarshalJSON()
	return string(ret)
}

func (bj ByteJson) Unquote() (string, error) {
	if bj.Type != TpCodeString {
		return bj.String(), nil
	}
	str := bj.GetString()
	if len(str) < 2 || (str[0] != '"' || str[len(str)-1] != '"') {
		return string(str), nil
	}
	str = str[1 : len(str)-1]
	var sb strings.Builder
	for i := 0; i < len(str); i++ {
		if str[i] != '\\' {
			sb.WriteByte(str[i])
			continue
		}
		i++
		if trans, ok := escapedChars[str[i]]; ok {
			sb.WriteByte(trans)
			continue
		}
		if str[i] == 'u' { // transform unicode to utf8
			if i+4 > len(str) {
				return "", moerr.NewInvalidInputNoCtx("invalid unicode")
			}
			unicodeStr := string(str[i-1 : i+5])
			content := strings.Replace(strconv.Quote(unicodeStr), `\\u`, `\u`, -1)
			text, err := strconv.Unquote(content)
			if err != nil {
				return "", moerr.NewInvalidInputNoCtx("invalid unicode")
			}
			sb.WriteString(text)
			i += 4
			continue
		}
		sb.WriteByte(str[i])
	}
	return sb.String(), nil
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

func (bj *ByteJson) UnmarshalObject(obj interface{}) (err error) {
	buf := make([]byte, 0, 64)
	var tpCode TpCode
	if tpCode, buf, err = addElem(buf, obj); err != nil {
		return
	}
	bj.Type = tpCode
	bj.Data = buf
	return
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
		err = moerr.NewInvalidInputNoCtx("invalid json type '%v'", bj.Type)
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
	switch TpCode(tpCode) {
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
		return cur
	}

	if bj.Type == TpCodeObject {
		switch sub.tp {
		case subPathIdx:
			start, _, _ := sub.idx.genIndex(1)
			if start == 0 {
				cur = bj.query(cur, &nPath)
			}
		case subPathRange:
			se := sub.iRange.genRange(bj.GetElemCnt())
			if se[0] == 0 {
				cur = bj.query(cur, &nPath)
			}
		case subPathKey:
			cnt := bj.GetElemCnt()
			if sub.key == "*" {
				for i := 0; i < cnt; i++ {
					cur = bj.getObjectVal(i).query(cur, &nPath)
				}
			} else {
				tmp := bj.queryValByKey(util.UnsafeStringToBytes(sub.key))
				cur = tmp.query(cur, &nPath)
			}
		}
		return cur
	}

	if bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			idx, _, last := sub.idx.genIndex(cnt)
			if last && idx < 0 || cnt <= idx {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				cur = append(cur, tmp)
				return cur
			}
			if idx == subPathIdxALL {
				for i := 0; i < cnt; i++ {
					cur = bj.getArrayElem(i).query(cur, &nPath)
				}
			} else {
				cur = bj.getArrayElem(idx).query(cur, &nPath)
			}
		case subPathRange:
			se := sub.iRange.genRange(cnt)
			if se[0] == subPathIdxErr {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				cur = append(cur, tmp)
				return cur
			}
			for i := se[0]; i <= se[1]; i++ {
				cur = bj.getArrayElem(i).query(cur, &nPath)
			}
		}
	}
	return cur
}
func (bj ByteJson) Query(paths []*Path) *ByteJson {
	out := make([]ByteJson, 0, len(paths))
	for _, path := range paths {
		tmp := bj.query(nil, path)
		if len(tmp) > 0 {
			allNull := checkAllNull(tmp)
			if !allNull {
				out = append(out, tmp...)
			}
		}
	}
	if len(out) == 0 {
		return &ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
	}
	if len(out) == 1 && len(paths) == 1 {
		return &out[0]
	}
	allNull := checkAllNull(out)
	if allNull {
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
		return keys, vals
	}
	if bj.Type == TpCodeObject {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			start, _, _ := sub.idx.genIndex(1)
			if start == 0 {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, start)
				keys, vals = bj.queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathRange:
			se := sub.iRange.genRange(cnt)
			if se[0] == 0 {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, se[0])
				keys, vals = bj.queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathKey:
			if sub.key == "*" {
				for i := 0; i < cnt; i++ {
					newPathStr := fmt.Sprintf("%s.%s", pathStr, bj.getObjectKey(i))
					keys, vals = bj.getObjectVal(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
				}
			} else {
				tmp := bj.queryValByKey(util.UnsafeStringToBytes(sub.key))
				newPathStr := fmt.Sprintf("%s.%s", pathStr, sub.key)
				keys, vals = tmp.queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		}
	}
	if bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			idx, _, last := sub.idx.genIndex(cnt)
			if last && idx < 0 {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, sub.idx.num)
				keys = append(keys, newPathStr)
				vals = append(vals, tmp)
				return keys, vals
			}
			if idx == subPathIdxALL {
				for i := 0; i < cnt; i++ {
					newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
					keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
				}
			} else {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, idx)
				keys, vals = bj.getArrayElem(idx).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathRange:
			se := sub.iRange.genRange(cnt)
			if se[0] == subPathIdxErr {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				newPathStr := fmt.Sprintf("%s[%d to %d]", pathStr, sub.iRange.start.num, sub.iRange.end.num)
				keys = append(keys, newPathStr)
				vals = append(vals, tmp)
				return keys, vals
			}
			for i := se[0]; i <= se[1]; i++ {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
				keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		}
	}
	return keys, vals
}

func (bj ByteJson) unnestWithParams(out []UnnestResult, outer, recursive bool, mode string, pathStr string, this *ByteJson, filterMap map[string]struct{}) []UnnestResult {
	if !bj.canUnnest() {
		index, key := genIndexOrKey(pathStr)
		tmp := UnnestResult{}
		genUnnestResult(tmp, index, key, util.UnsafeStringToBytes(pathStr), &bj, this, filterMap)
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
			genUnnestResult(tmp, nil, key, util.UnsafeStringToBytes(newPathStr), &val, this, filterMap)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, &val, filterMap)
			}
		}
	}
	if bj.Type == TpCodeArray && mode != "object" {
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			val := bj.getArrayElem(i)
			newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
			tmp := UnnestResult{}
			genUnnestResult(tmp, util.UnsafeStringToBytes(strconv.Itoa(i)), nil, util.UnsafeStringToBytes(newPathStr), &val, this, filterMap)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, &val, filterMap)
			}
		}
	}
	return out
}

func (bj ByteJson) unnest(out []UnnestResult, path *Path, outer, recursive bool, mode string, filterMap map[string]struct{}) ([]UnnestResult, error) {

	keys := make([]string, 0, 1)
	vals := make([]ByteJson, 0, 1)
	keys, vals = bj.queryWithSubPath(keys, vals, path, "$")
	if len(keys) != len(vals) {
		return nil, moerr.NewInvalidInputNoCtx("len(key) and len(val) are not equal, len(key)=%d, len(val)=%d", len(keys), len(vals))
	}
	for i := 0; i < len(keys); i++ {
		if vals[i].canUnnest() {
			out = vals[i].unnestWithParams(out, outer, recursive, mode, keys[i], &vals[i], filterMap)
		}
	}
	if len(out) == 0 && outer {
		for i := 0; i < len(keys); i++ {
			tmp := UnnestResult{}
			out = append(out, tmp)
		}
		if _, ok := filterMap["path"]; ok {
			for i := 0; i < len(keys); i++ {
				out[i]["path"] = util.UnsafeStringToBytes(keys[i])
			}
		}
		if _, ok := filterMap["this"]; ok {
			for i := 0; i < len(vals); i++ {
				dt, err := vals[i].Marshal()
				if err != nil {
					return nil, err
				}
				out[i]["this"] = dt
			}
		}

	}
	return out, nil
}

// Unnest returns a slice of UnnestResult, each UnnestResult contains filtered data, if param filters is nil, return all fields.
func (bj ByteJson) Unnest(path *Path, outer, recursive bool, mode string, filterMap map[string]struct{}) ([]UnnestResult, error) {
	if !checkMode(mode) {
		return nil, moerr.NewInvalidInputNoCtx("mode must be one of [object, array, both]")
	}
	out := make([]UnnestResult, 0, 1)
	out, err := bj.unnest(out, path, outer, recursive, mode, filterMap)
	return out, err
}

func genUnnestResult(res UnnestResult, index, key, path []byte, value, this *ByteJson, filterMap map[string]struct{}) UnnestResult {
	if _, ok := filterMap["index"]; ok {
		res["index"] = index
	}
	if _, ok := filterMap["key"]; ok {
		res["key"] = key
	}
	if _, ok := filterMap["path"]; ok {
		res["path"] = path
	}
	if _, ok := filterMap["value"]; ok {
		dt, _ := value.Marshal()
		res["value"] = dt
	}
	if _, ok := filterMap["this"]; ok {
		dt, _ := this.Marshal()
		res["this"] = dt
	}
	return res
}

func ParseJsonByteFromString(s string) ([]byte, error) {
	var decoder = json.NewDecoder(strings.NewReader(s))
	decoder.UseNumber()
	var in interface{}
	err := decoder.Decode(&in)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1, len(s)+1)
	switch x := in.(type) {
	case nil:
		buf[0] = byte(TpCodeLiteral)
		buf = append(buf, LiteralNull)
	case bool:
		buf[0] = byte(TpCodeLiteral)
		lit := LiteralFalse
		if x {
			lit = LiteralTrue
		}
		buf = append(buf, lit)
	case int64:
		buf[0] = byte(TpCodeInt64)
		buf = addUint64(buf, uint64(x))
	case uint64:
		buf[0] = byte(TpCodeUint64)
		buf = addUint64(buf, x)
	case json.Number:
		if strings.ContainsAny(string(x), "Ee.") {
			val, err := x.Float64()
			buf[0] = byte(TpCodeFloat64)
			if err != nil {
				return nil, moerr.NewInvalidInputNoCtx("json number %v", in)
			}
			if err = checkFloat64(val); err != nil {
				return nil, err
			}
			return addFloat64(buf, val), nil
		}
		if val, err := x.Int64(); err == nil {
			buf[0] = byte(TpCodeInt64)
			return addInt64(buf, val), nil
		}
		if val, err := strconv.ParseUint(string(x), 10, 64); err == nil {
			buf[0] = byte(TpCodeUint64)
			return addUint64(buf, val), nil
		}
		if val, err := x.Float64(); err == nil {
			buf[0] = byte(TpCodeFloat64)
			if err = checkFloat64(val); err != nil {
				return nil, err
			}
			return addFloat64(buf, val), nil
		}
	case string:
		buf[0] = byte(TpCodeString)
		buf = addString(buf, x)
	case ByteJson:
		buf[0] = byte(x.Type)
		buf = append(buf, x.Data...)
	case []interface{}:
		buf[0] = byte(TpCodeArray)
		buf, err = addArray(buf, x)
	case map[string]interface{}:
		buf[0] = byte(TpCodeObject)
		buf, err = addObject(buf, x)
	default:
		return nil, moerr.NewInvalidInputNoCtx("json element %v", in)
	}
	return buf, err
}
