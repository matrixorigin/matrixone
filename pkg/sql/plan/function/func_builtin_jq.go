// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/itchyny/gojq"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// jq: see https://github.com/itchyny/gojq
//
// jq(json, query): jq is a function that takes a json string and a jq query.
// It returns the result of the jq query on the json string. If either json
// or query is NULL, the result is NULL.
//
// try_jq: try_jq is the same as jq, but it will not return an error
// if either the json data or jq query has errors.  Instead, it will
// return a NULL value.

const (
	jqMapSizeLimit = 10
)

type opBuiltInJq struct {
	jqCache map[string]*gojq.Code
	enc     encoder
}

func newOpBuiltInJq() *opBuiltInJq {
	var op opBuiltInJq
	op.jqCache = make(map[string]*gojq.Code)
	op.enc.intialize(false, 0)
	return &op
}

func (op *opBuiltInJq) jq(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryJqImpl(params, result, proc, length, selectList, false)
}

func (op *opBuiltInJq) tryJq(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.tryJqImpl(params, result, proc, length, selectList, true)
}

func (op *opBuiltInJq) tryJqImpl(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList,
	isTry bool) error {
	p1 := vector.GenerateFunctionStrParameter(params[0])
	p2 := vector.GenerateFunctionStrParameter(params[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	// special case
	if selectList.IgnoreAllRow() {
		rs.AddNullRange(0, uint64(length))
		return nil
	}

	c1, c2 := params[0].IsConst(), params[1].IsConst()
	// if both parameters are constant, just eval
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetStrValue(0)
		if null1 || null2 {
			rs.AddNullRange(0, uint64(length))
		} else {
			code, err := op.getJqCode(string(v2))
			if err == nil {
				err = op.jqImpl(v1, code)
			}
			if err != nil {
				if isTry {
					rs.AddNullRange(0, uint64(length))
					return nil
				} else {
					return err
				}
			}
			rs.AppendBytes(op.enc.bytes(), false)
			op.enc.done()
		}
		return nil
	} else if c1 {
		// this is the strange version, we eval many jq again one piece
		// of json string.
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			rs.AddNullRange(0, uint64(length))
			return nil
		} else {
			for i := uint64(0); i < uint64(length); i++ {
				v2, null2 := p2.GetStrValue(i)
				if null2 || selectList.Contains(i) {
					rs.AppendBytes(nil, true)
				} else {
					code, err := op.getJqCode(string(v2))
					if err == nil {
						err = op.jqImpl(v1, code)
					}
					if err != nil {
						if isTry {
							rs.AppendBytes(nil, true)
						} else {
							return err
						}
					} else {
						rs.AppendBytes(op.enc.bytes(), false)
						op.enc.done()
					}
				}
			}
		}
		return nil
	} else if c2 {
		// this is the common case that need to be optimized.
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			rs.AddNullRange(0, uint64(length))
			return nil
		}
		code, err := op.getJqCode(string(v2))
		if err != nil {
			if isTry {
				rs.AddNullRange(0, uint64(length))
				return nil
			} else {
				return err
			}
		}

		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 || selectList.Contains(i) {
				rs.AppendBytes(nil, true)
			} else {
				err = op.jqImpl(v1, code)
				if err != nil {
					if isTry {
						rs.AppendBytes(nil, true)
					} else {
						return err
					}
				} else {
					rs.AppendBytes(op.enc.bytes(), false)
					op.enc.done()
				}
			}
		}
	} else {
		// both are not constant, this is the less likely case in real life.
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 || selectList.Contains(i) {
				rs.AppendBytes(nil, true)
			} else {
				code, err := op.getJqCode(string(v2))
				if err == nil {
					err = op.jqImpl(v1, code)
				}

				if err != nil {
					if isTry {
						rs.AppendBytes(nil, true)
						// continue
					} else {
						return err
					}
				} else {
					rs.AppendBytes(op.enc.bytes(), false)
					op.enc.done()
				}
			}
		}
	}
	return nil
}

// run jq.  The result is stored in the encoder bytes().  If succeeded, caller
// must call .done() to reset the encoder.
func (op *opBuiltInJq) jqImpl(jsonStr []byte, code *gojq.Code) error {
	// first, turn jsonstr to any
	var jv any
	err := json.Unmarshal(jsonStr, &jv)
	if err != nil {
		return err
	}

	iter := code.Run(jv)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if verr, ok := v.(error); ok {
			op.enc.done()
			return verr
		}

		if err := op.enc.encode(v); err != nil {
			op.enc.done()
			return err
		}
	}
	return nil
}

func (op *opBuiltInJq) getJqCode(jq string) (*gojq.Code, error) {
	code, ok := op.jqCache[jq]
	if ok {
		return code, nil
	}

	pq, err := gojq.Parse(jq)
	if err != nil {
		return nil, err
	}

	code, err = gojq.Compile(pq)
	if err != nil {
		return nil, err
	}

	// if we have cached too many, we need to remove some
	if len(op.jqCache) == jqMapSizeLimit {
		for key := range op.jqCache {
			delete(op.jqCache, key)
			// regexp folks has a interesting way of doing this,
			// they break here, just remove one element.   It
			// depends on go map implementation to remove the right
			// element.   Not convinced it is the right thing to do.
			// Here, we remove all elements.
		}
	}
	op.jqCache[jq] = code
	return code, nil
}

// This is a simplified version of the encoder in gojq/cli/encode.go.
// It is used to encode the result of jq functions.
// We removed all the terminal color related code and we write to buffer w
// and do not flush until the encoding is done.
type encoder struct {
	w      *bytes.Buffer
	tab    bool
	indent int
	depth  int
	buf    [64]byte
}

func (e *encoder) intialize(tab bool, indent int) {
	e.w = new(bytes.Buffer)
	e.tab = tab
	e.indent = indent
}

func (e *encoder) bytes() []byte {
	return e.w.Bytes()
}
func (e *encoder) done() {
	e.w.Reset()
	e.depth = 0
}

func (e *encoder) encode(v any) error {
	switch v := v.(type) {
	case nil:
		e.w.Write([]byte("null"))
	case bool:
		if v {
			e.w.Write([]byte("true"))
		} else {
			e.w.Write([]byte("false"))
		}
	case int:
		e.w.Write(strconv.AppendInt(e.buf[:0], int64(v), 10))
	case float64:
		e.encodeFloat64(v)
	case *big.Int:
		e.w.Write(v.Append(e.buf[:0], 10))
	case string:
		e.encodeString(v)
	case []any:
		if err := e.encodeArray(v); err != nil {
			return err
		}
	case map[string]any:
		if err := e.encodeObject(v); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("invalid type: %[1]T (%[1]v)", v))
	}
	return nil
}

// ref: floatEncoder in encoding/json
func (e *encoder) encodeFloat64(f float64) {
	if math.IsNaN(f) {
		e.w.Write([]byte("null"))
		return
	}
	if f >= math.MaxFloat64 {
		f = math.MaxFloat64
	} else if f <= -math.MaxFloat64 {
		f = -math.MaxFloat64
	}
	format := byte('f')
	if x := math.Abs(f); x != 0 && x < 1e-6 || x >= 1e21 {
		format = 'e'
	}
	buf := strconv.AppendFloat(e.buf[:0], f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		if n := len(buf); n >= 4 && buf[n-4] == 'e' && buf[n-3] == '-' && buf[n-2] == '0' {
			buf[n-2] = buf[n-1]
			buf = buf[:n-1]
		}
	}
	e.w.Write(buf)
}

// ref: encodeState#string in encoding/json
func (e *encoder) encodeString(s string) {
	e.w.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if ' ' <= b && b <= '~' && b != '"' && b != '\\' {
				i++
				continue
			}
			if start < i {
				e.w.WriteString(s[start:i])
			}
			switch b {
			case '"':
				e.w.WriteString(`\"`)
			case '\\':
				e.w.WriteString(`\\`)
			case '\b':
				e.w.WriteString(`\b`)
			case '\f':
				e.w.WriteString(`\f`)
			case '\n':
				e.w.WriteString(`\n`)
			case '\r':
				e.w.WriteString(`\r`)
			case '\t':
				e.w.WriteString(`\t`)
			default:
				const hex = "0123456789abcdef"
				e.w.WriteString(`\u00`)
				e.w.WriteByte(hex[b>>4])
				e.w.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				e.w.WriteString(s[start:i])
			}
			e.w.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		e.w.WriteString(s[start:])
	}
	e.w.WriteByte('"')
}

func (e *encoder) encodeArray(vs []any) error {
	e.writeByte('[')
	e.depth += e.indent
	for i, v := range vs {
		if i > 0 {
			e.writeByte(',')
		}
		if e.indent != 0 {
			e.writeIndent()
		}
		if err := e.encode(v); err != nil {
			return err
		}
	}
	e.depth -= e.indent
	if len(vs) > 0 && e.indent != 0 {
		e.writeIndent()
	}
	e.writeByte(']')
	return nil
}

func (e *encoder) encodeObject(vs map[string]any) error {
	e.writeByte('{')
	e.depth += e.indent
	type keyVal struct {
		key string
		val any
	}
	kvs := make([]keyVal, len(vs))
	var i int
	for k, v := range vs {
		kvs[i] = keyVal{k, v}
		i++
	}
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})
	for i, kv := range kvs {
		if i > 0 {
			e.writeByte(',')
		}
		if e.indent != 0 {
			e.writeIndent()
		}
		e.encodeString(kv.key)
		e.writeByte(':')
		if e.indent != 0 {
			e.w.WriteByte(' ')
		}
		if err := e.encode(kv.val); err != nil {
			return err
		}
	}
	e.depth -= e.indent
	if len(vs) > 0 && e.indent != 0 {
		e.writeIndent()
	}
	e.writeByte('}')
	return nil
}

func (e *encoder) writeIndent() {
	e.w.WriteByte('\n')
	if n := e.depth; n > 0 {
		if e.tab {
			e.writeIndentInternal(n, "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t")
		} else {
			e.writeIndentInternal(n, "                                ")
		}
	}
}

func (e *encoder) writeIndentInternal(n int, spaces string) {
	if l := len(spaces); n <= l {
		e.w.WriteString(spaces[:n])
	} else {
		e.w.WriteString(spaces)
		for n -= l; n > 0; n, l = n-l, l*2 {
			if n < l {
				l = n
			}
			e.w.Write(e.w.Bytes()[e.w.Len()-l:])
		}
	}
}

func (e *encoder) writeByte(b byte) {
	e.w.WriteByte(b)
}
