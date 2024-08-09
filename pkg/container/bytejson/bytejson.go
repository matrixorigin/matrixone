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
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	json2 "github.com/segmentio/encoding/json"
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
	bs, err := ParseJsonByte(data)
	if err != nil {
		return err
	}
	bj.Data = bs[1:]
	bj.Type = TpCode(bs[0])
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
		buf, err = bj.toString(buf)
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
		buf, err = toString(buf, bj.getObjectKey(i))
		if err != nil {
			return nil, err
		}
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
func (bj ByteJson) toString(buf []byte) ([]byte, error) {
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
		return Null
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

func (bj ByteJson) Query(paths []*Path) ByteJson {
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
		return ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
	}
	if len(out) == 1 && len(paths) == 1 {
		return out[0]
	}
	allNull := checkAllNull(out)
	if allNull {
		return ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
	}
	return mergeToArray(out)
}

func (bj ByteJson) querySimple(path *Path) ByteJson {
	cur := bj
	// don't go through th step(), recursive call route.  We know
	// we have a simple path, each step will bring us to ONE SINGLE next value.

	for _, sub := range path.paths {
		if cur.Type == TpCodeObject {
			switch sub.tp {
			case subPathIdx:
				start, _, _ := sub.idx.genIndex(1)
				if start != 0 {
					return Null
				}
				// obj[0] is itself, continue
			case subPathKey:
				if sub.key == "*" {
					panic("bytejson simple path should not contain *")
				} else {
					cur = cur.queryValByKey(util.UnsafeStringToBytes(sub.key))
				}
			default:
				return Null
			}
		} else if cur.Type == TpCodeArray {
			if sub.tp != subPathIdx {
				return Null
			}
			cnt := cur.GetElemCnt()
			idx, _, _ := sub.idx.genIndex(cnt)
			// don't bother checking last -- idx < 0 and not last means the path
			// is not valid, we should have caught this earlier.
			// if (last && idx < 0) || cnt <= idx {
			if idx < 0 || cnt <= idx {
				// out of range
				return Null
			} else {
				cur = cur.getArrayElem(idx)
			}
		} else {
			return Null
		}
	}
	return cur
}

func (bj ByteJson) QuerySimple(paths []*Path) ByteJson {
	if len(paths) == 0 {
		// not retrieve anything
		return Null
	} else if len(paths) == 1 {
		// only retrieve one path
		return bj.querySimple(paths[0])
	} else {
		// retrieve multiple paths, merge them into an array
		out := make([]ByteJson, 0, len(paths))
		for _, path := range paths {
			tmp := bj.querySimple(path)
			// strange behavior, skipping Null value.
			if !tmp.IsNull() {
				out = append(out, tmp)
			}
		}
		// strange behavior, we actually return Null instead of an array of nulls.
		if checkAllNull(out) {
			return Null
		}
		return mergeToArray(out)
	}
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
	return ParseJsonByte(util.UnsafeStringToBytes(s))
}

func ParseJsonByte(data []byte) ([]byte, error) {
	n, err := ParseNode(data)
	if err != nil {
		return nil, err
	}
	w := byteJsonWriter{
		buf: make([]byte, 0, len(data)*2),
	}
	_, _, err = w.writeNode(true, n)
	n.Free()
	if err != nil {
		return nil, err
	}
	return w.buf, nil
}

func ParseNodeString(s string) (Node, error) {
	return ParseNode(util.UnsafeStringToBytes(s))
}

func ParseNode(data []byte) (Node, error) {
	p := parser{src: data}
	return p.do()
}

type parser struct {
	src   []byte
	stack []*Group
	tz    *json2.Tokenizer
	state func(*parser) int
	top   Node
}

func (p *parser) do() (Node, error) {
	p.stack = make([]*Group, 0, 2)
	p.tz = json2.NewTokenizer(p.src)
	p.state = (*parser).stateBeginValue
	var z Node
	for {
		if !p.tz.Next() {
			for _, g := range p.stack {
				g.free()
			}
			return z, io.ErrUnexpectedEOF
		}
		switch p.state(p) {
		case scanError:
			for _, g := range p.stack {
				g.free()
			}
			if errors.Is(p.tz.Err, io.EOF) {
				return z, io.ErrUnexpectedEOF
			}
			var se *json.SyntaxError
			if p.tz.Remaining() == 0 && errors.As(p.tz.Err, &se) {
				return z, io.ErrUnexpectedEOF
			}
			return z, moerr.NewInternalErrorNoCtx("parse json: %v", p.tz.Err)
		case scanEnd:
			if p.tz.Next() {
				p.top.Free()
				return z, moerr.NewInvalidInputNoCtx("invalid json: %s", p.src)
			}
			if p.tz.Err != nil {
				return z, moerr.NewInternalErrorNoCtx("parse json: %v", p.tz.Err)
			}
			return p.top, nil
		}
	}
}

const (
	scanContinue = iota
	scanEnd      // top-level value ended *before* this byte; known to be first "stop" result
	scanError    // hit an error, scanner.err.
)

func (p *parser) stateBeginValue() int {
	k := p.tz.Kind()

	switch k.Class() {
	case json2.Array:
		p.openGroup(k)
		p.state = (*parser).stateBeginValueOrEmpty
		return scanContinue
	case json2.Object:
		p.openGroup(k)
		p.state = (*parser).stateObjectKeyOrEmpty
		return scanContinue
	}

	var n Node
	switch k.Class() {
	case json2.String:
		n = Node{string(p.tz.String())}
	case json2.Num:
		n = Node{json.Number(p.tz.Value)}
	case json2.Bool:
		n = Node{p.tz.Bool()}
	case json2.Null:
		n = Node{nil}
	default:
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: looking for beginning of value")
		return scanError
	}
	if p.tz.Depth != 0 {
		p.appendToLastGroup(n)
		p.state = (*parser).stateEndValue
		return scanContinue
	}

	p.top = n
	p.state = nil
	return scanEnd
}

func (p *parser) stateBeginValueOrEmpty() int {
	if p.tz.Delim == ']' {
		return p.stateEndValue()
	}

	return p.stateBeginValue()
}

func (p *parser) stateObjectKey() int {
	if p.tz.Kind().Class() != json2.String || !p.tz.IsKey {
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: object key")
		return scanError
	}

	g := p.stack[len(p.stack)-1]
	g.Keys = append(g.Keys, string(p.tz.String()))
	p.state = (*parser).stateColon
	return scanContinue
}

func (p *parser) stateObjectKeyOrEmpty() int {
	if p.tz.Delim == '}' {
		return p.stateEndValue()
	}

	return p.stateObjectKey()
}

func (p *parser) stateColon() int {
	if p.tz.Delim != ':' {
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: after object key")
		return scanError
	}
	p.state = (*parser).stateBeginValue
	return scanContinue
}

func (p *parser) stateEndValue() int {
	if p.tz.Delim == ']' || p.tz.Delim == '}' {
		p.closeGroup()
		if p.tz.Depth == 0 {
			p.state = nil
			return scanEnd
		}
		p.state = (*parser).stateEndValue
		return scanContinue
	}

	if p.tz.Delim == ',' {
		g := p.stack[len(p.stack)-1]
		if g.Obj {
			p.state = (*parser).stateObjectKey
		} else {
			p.state = (*parser).stateBeginValue
		}
		return scanContinue
	}

	p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: end value")
	return scanError
}

func (p *parser) openGroup(k json2.Kind) {
	g := reuse.Alloc[Group](nil)
	g.Obj = k == json2.Object
	p.stack = append(p.stack, g)
}

func (p *parser) closeGroup() {
	n := len(p.stack) - 1
	g := p.stack[n]
	p.stack = p.stack[:n]

	if g.Obj {
		g.sortKeys()
	}

	if len(p.stack) == 0 {
		p.top = Node{g}
		return
	}
	p.appendToLastGroup(Node{g})
}

func (p *parser) appendToLastGroup(n Node) {
	g := p.stack[len(p.stack)-1]
	if !g.Obj || len(g.Keys) <= 1 {
		g.Values = append(g.Values, n)
		return
	}

	last := len(g.Keys) - 1
	dupIdx := slices.Index(g.Keys[:last], g.Keys[last])
	if dupIdx < 0 {
		g.Values = append(g.Values, n)
		return
	}
	old := g.Values[dupIdx]
	old.Free()
	g.Keys = g.Keys[:last]
	g.Values[dupIdx] = n
}

func init() {
	reuse.CreatePool[Group](
		func() *Group {
			return &Group{}
		},
		func(g *Group) { g.reset() },
		reuse.DefaultOptions[Group](),
	)
}

type Group struct {
	Obj    bool
	Keys   []string
	Values []Node
}

func (g Group) TypeName() string {
	return "bytejson.group"
}

func (g *Group) reset() {
	g.Obj = false
	g.Keys = g.Keys[:0]
	g.Values = g.Values[:0]
}

func (g *Group) free() {
	g.Obj = false
	g.Keys = g.Keys[:0]
	for _, sub := range g.Values {
		sg, ok := sub.V.(*Group)
		if !ok {
			continue
		}
		sg.free()
	}
	g.Values = g.Values[:0]
	reuse.Free(g, nil)
}

func (g *Group) sortKeys() {
	sort.Sort((*groupSortKeys)(g))
}

type groupSortKeys Group

func (g *groupSortKeys) Len() int { return len(g.Keys) }

func (g *groupSortKeys) Less(i, j int) bool { return g.Keys[i] < g.Keys[j] }

func (g *groupSortKeys) Swap(i, j int) {
	g.Keys[i], g.Keys[j] = g.Keys[j], g.Keys[i]
	g.Values[i], g.Values[j] = g.Values[j], g.Values[i]
}

type byteJsonWriter struct {
	buf []byte
}

func (w *byteJsonWriter) writeNode(root bool, node Node) (TpCode, uint32, error) {
	start := len(w.buf)
	switch val := node.V.(type) {
	case *Group:
		if val.Obj {
			obj := val
			keys := obj.Keys
			n := len(keys)
			baseOffset := start
			if root {
				w.buf = append(w.buf, byte(TpCodeObject))
				baseOffset += valTypeSize
			}
			w.buf = endian.AppendUint32(w.buf, uint32(n))
			w.buf = endian.AppendUint32(w.buf, 0) // object buf length

			w.buf = extendByte(w.buf, n*(keyEntrySize+valEntrySize))

			loc := uint32(headerSize + n*(keyEntrySize+valEntrySize))
			for i, k := range keys {
				o := baseOffset + headerSize + i*keyEntrySize
				length := uint32(len(k))
				if length > math.MaxUint16 {
					return 0, 0, moerr.NewInvalidInputNoCtx("json key %s", k)
				}
				endian.PutUint32(w.buf[o:], loc)
				endian.PutUint16(w.buf[o+keyOriginOff:], uint16(length))
				loc += length
				w.buf = append(w.buf, k...)
			}

			for i := range keys {
				tp, length, err := w.writeNode(false, obj.Values[i])
				if err != nil {
					return 0, 0, err
				}
				o := baseOffset + headerSize + n*keyEntrySize + i*valEntrySize
				w.buf[o] = byte(tp)
				if tp == TpCodeLiteral {
					endian.PutUint32(w.buf[o+valTypeSize:], length)
					continue
				}
				endian.PutUint32(w.buf[o+valTypeSize:], loc)
				loc += length
			}

			endian.PutUint32(w.buf[baseOffset+4:], loc) // object buf length
			return TpCodeObject, uint32(len(w.buf) - start), nil
		}

		arr := val
		n := len(arr.Values)
		baseOffset := start
		if root {
			w.buf = append(w.buf, byte(TpCodeArray))
			baseOffset++
		}
		w.buf = endian.AppendUint32(w.buf, uint32(n))
		w.buf = endian.AppendUint32(w.buf, 0) // array buf length
		w.buf = extendByte(w.buf, n*5)

		loc := uint32(headerSize + n*valEntrySize)
		for i := range arr.Values {
			tp, length, err := w.writeNode(false, arr.Values[i])
			if err != nil {
				return 0, 0, err
			}
			o := baseOffset + headerSize + i*valEntrySize
			w.buf[o] = byte(tp)
			if tp == TpCodeLiteral {
				endian.PutUint32(w.buf[o+valTypeSize:], length)
				continue
			}
			endian.PutUint32(w.buf[o+valTypeSize:], loc)
			loc += length
		}

		endian.PutUint32(w.buf[baseOffset+4:], loc) // array buf length
		return TpCodeArray, uint32(len(w.buf) - start), nil
	case bool:
		lit := LiteralFalse
		if val {
			lit = LiteralTrue
		}
		if root {
			w.buf = append(w.buf, byte(TpCodeLiteral), lit)
		}
		return TpCodeLiteral, uint32(lit), nil
	case nil:
		if root {
			w.buf = append(w.buf, byte(TpCodeLiteral), LiteralNull)
		}
		return TpCodeLiteral, uint32(LiteralNull), nil
	case json.Number:
		tp, data, err := w.parseNumber(val)
		if err != nil {
			return 0, 0, err
		}
		if root {
			w.buf = append(w.buf, byte(tp))
		}
		w.buf = append(w.buf, data...)
		return tp, uint32(len(w.buf) - start), nil
	case string:
		if root {
			w.buf = append(w.buf, byte(TpCodeString))
		}
		w.buf = addString(w.buf, val)
		return TpCodeString, uint32(len(w.buf) - start), nil
	default:
		return 0, 0, moerr.NewInvalidInputNoCtx("unknown type %T", node)
	}
}

func (w *byteJsonWriter) parseNumber(in json.Number) (TpCode, []byte, error) {
	var data [8]byte
	//check if it is a float
	if strings.ContainsAny(string(in), "Ee.") {
		val, err := in.Float64()
		if err != nil {
			return TpCodeFloat64, nil, moerr.NewInvalidInputNoCtx("json number %v", in)
		}
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		endian.PutUint64(data[:], math.Float64bits(val))
		return TpCodeFloat64, data[:], nil
	}
	if val, err := in.Int64(); err == nil { //check if it is an int
		endian.PutUint64(data[:], uint64(val))
		return TpCodeInt64, data[:], nil
	}
	if val, err := strconv.ParseUint(string(in), 10, 64); err == nil { //check if it is a uint
		endian.PutUint64(data[:], val)
		return TpCodeUint64, data[:], nil
	}
	if val, err := in.Float64(); err == nil { //check if it is a float
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		endian.PutUint64(data[:], math.Float64bits(val))
		return TpCodeFloat64, data[:], nil
	}
	var tpCode TpCode
	return tpCode, nil, moerr.NewInvalidInputNoCtx("json number %v", in)
}

type Node struct {
	V any
}

func (n Node) Free() {
	g, ok := n.V.(*Group)
	if ok {
		g.free()
	}
}

func (n Node) ByteJson() (ByteJson, error) {
	buf, err := n.ByteJsonRaw()
	if err != nil {
		return ByteJson{}, err
	}
	return ByteJson{
		Data: buf[1:],
		Type: TpCode(buf[0]),
	}, nil
}

func (n Node) ByteJsonRaw() ([]byte, error) {
	w := byteJsonWriter{}
	_, _, err := w.writeNode(true, n)
	if err != nil {
		return nil, err
	}
	return w.buf, nil
}

func (n Node) String() string {
	switch v := n.V.(type) {
	case *Group:
		if !v.Obj {
			return fmt.Sprint(v.Values)
		}
		m := make(map[string]Node, len(v.Keys))
		for i, key := range v.Keys {
			m[key] = v.Values[i]
		}
		return fmt.Sprint(m)
	default:
		return fmt.Sprint(v)
	}
}
