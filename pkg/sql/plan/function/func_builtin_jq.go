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
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/itchyny/gojq"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
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
	enc     JqEncoder
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
	var scratchOutput functionScratchOutput
	if result.HasFunctionScratch() {
		scratchOutput.result = result
		op.enc.useWriter(&scratchOutput)
		defer op.enc.restoreWriter()
	}

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
				if isTry && !isJqOutputError(err) {
					rs.AddNullRange(0, uint64(length))
					return nil
				} else {
					return err
				}
			}
			if err := rs.AppendBytes(op.enc.bytes(), false); err != nil {
				return err
			}
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
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
				} else {
					code, err := op.getJqCode(string(v2))
					if err == nil {
						err = op.jqImpl(v1, code)
					}
					if err != nil {
						if isTry && !isJqOutputError(err) {
							if err := rs.AppendBytes(nil, true); err != nil {
								return err
							}
						} else {
							return err
						}
					} else {
						if err := rs.AppendBytes(op.enc.bytes(), false); err != nil {
							return err
						}
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
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				err = op.jqImpl(v1, code)
				if err != nil {
					if isTry && !isJqOutputError(err) {
						if err := rs.AppendBytes(nil, true); err != nil {
							return err
						}
					} else {
						return err
					}
				} else {
					if err := rs.AppendBytes(op.enc.bytes(), false); err != nil {
						return err
					}
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
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				code, err := op.getJqCode(string(v2))
				if err == nil {
					err = op.jqImpl(v1, code)
				}

				if err != nil {
					if isTry && !isJqOutputError(err) {
						if err := rs.AppendBytes(nil, true); err != nil {
							return err
						}
						// continue
					} else {
						return err
					}
				} else {
					if err := rs.AppendBytes(op.enc.bytes(), false); err != nil {
						return err
					}
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
type JqEncoder struct {
	legacy jqLegacyOutput
	w      jqOutput
	tab    bool
	indent int
	depth  int
	buf    [64]byte
}

type jqOutputError struct {
	err error
}

func (e *jqOutputError) Error() string { return e.err.Error() }
func (e *jqOutputError) Unwrap() error { return e.err }

func isJqOutputError(err error) bool {
	var outputErr *jqOutputError
	return errors.As(err, &outputErr)
}

type jqOutput interface {
	formatBuffer
	Bytes() []byte
	Len() int
	Reset()
	Err() error
}

type jqLegacyOutput struct {
	bytes.Buffer
}

func (*jqLegacyOutput) Err() error { return nil }

type functionScratchOutput struct {
	result  vector.FunctionResultWrapper
	data    []byte
	written int
	err     error
}

func (w *functionScratchOutput) ensure(required int) error {
	if w.err != nil {
		return w.err
	}
	if required < 0 {
		w.err = mpool.ErrAllocationAccountInvalid
		return w.err
	}
	if required <= cap(w.data) {
		w.data = w.data[:required]
		return nil
	}
	capacity, ok := mpool.GrowCapacity(int64(cap(w.data)), int64(required))
	if !ok || capacity > int64(math.MaxInt) {
		w.err = mpool.ErrAllocationAccountInvalid
		return w.err
	}
	data, selected, err := w.result.ResizeFunctionScratch(int(capacity))
	if err != nil {
		w.err = err
		return err
	}
	if !selected {
		w.err = mpool.ErrAllocationAccountInvalid
		return w.err
	}
	w.data = data[:required]
	return nil
}

func (w *functionScratchOutput) Write(value []byte) (int, error) {
	if len(value) > math.MaxInt-w.written {
		w.err = io.ErrShortBuffer
		return 0, w.err
	}
	if err := w.ensure(w.written + len(value)); err != nil {
		return 0, err
	}
	copy(w.data[w.written:], value)
	w.written += len(value)
	return len(value), nil
}

func (w *functionScratchOutput) WriteString(value string) (int, error) {
	if len(value) > math.MaxInt-w.written {
		w.err = io.ErrShortBuffer
		return 0, w.err
	}
	if err := w.ensure(w.written + len(value)); err != nil {
		return 0, err
	}
	copy(w.data[w.written:], value)
	w.written += len(value)
	return len(value), nil
}

func (w *functionScratchOutput) WriteByte(value byte) error {
	if w.written == math.MaxInt {
		w.err = io.ErrShortBuffer
		return w.err
	}
	if err := w.ensure(w.written + 1); err != nil {
		return err
	}
	w.data[w.written] = value
	w.written++
	return nil
}

func (w *functionScratchOutput) WriteRune(value rune) (int, error) {
	var encoded [utf8.UTFMax]byte
	size := utf8.EncodeRune(encoded[:], value)
	return w.Write(encoded[:size])
}

func (w *functionScratchOutput) Grow(size int) {
	if size < 0 || size > math.MaxInt-w.written {
		w.err = io.ErrShortBuffer
		return
	}
	_ = w.ensure(w.written + size)
}

func (w *functionScratchOutput) Bytes() []byte {
	return w.data[:w.written]
}

func (w *functionScratchOutput) Len() int { return w.written }

func (w *functionScratchOutput) Reset() {
	w.data = w.data[:0]
	w.written = 0
	w.err = nil
}

func (w *functionScratchOutput) Err() error { return w.err }

func (e *JqEncoder) intialize(tab bool, indent int) {
	e.legacy.Reset()
	e.w = &e.legacy
	e.tab = tab
	e.indent = indent
}

func (e *JqEncoder) useWriter(w jqOutput) {
	e.w = w
	e.done()
}

func (e *JqEncoder) restoreWriter() {
	e.done()
	e.w = &e.legacy
	e.legacy.Reset()
}

func (e *JqEncoder) bytes() []byte {
	return e.w.Bytes()
}
func (e *JqEncoder) done() {
	e.w.Reset()
	e.depth = 0
}

func (e *JqEncoder) err() error {
	if err := e.w.Err(); err != nil {
		return &jqOutputError{err: err}
	}
	return nil
}

func (e *JqEncoder) encode(v any) error {
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
	return e.err()
}

// ref: floatEncoder in encoding/json
func (e *JqEncoder) encodeFloat64(f float64) {
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
func (e *JqEncoder) encodeString(s string) {
	e.encodeBytes(functionUtil.QuickStrToBytes(s))
}

// encodeBytes preserves JSON_ROW's legacy replacement behavior for invalid
// UTF-8 while avoiding a per-row []byte-to-string allocation.
func (e *JqEncoder) encodeBytes(value []byte) {
	e.w.WriteByte('"')
	start := 0
	for i := 0; i < len(value); {
		if b := value[i]; b < utf8.RuneSelf {
			if ' ' <= b && b <= '~' && b != '"' && b != '\\' {
				i++
				continue
			}
			if start < i {
				e.w.Write(value[start:i])
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
		c, size := utf8.DecodeRune(value[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				e.w.Write(value[start:i])
			}
			e.w.WriteString(`\ufffd`)
			i++
			start = i
			continue
		}
		i += size
	}
	if start < len(value) {
		e.w.Write(value[start:])
	}
	e.w.WriteByte('"')
}

func (e *JqEncoder) encodeArray(vs []any) error {
	e.w.WriteByte('[')
	e.depth += e.indent
	for i, v := range vs {
		if i > 0 {
			e.w.WriteByte(',')
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
	e.w.WriteByte(']')
	return nil
}

func (e *JqEncoder) encodeObject(vs map[string]any) error {
	e.w.WriteByte('{')
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
	slices.SortFunc(kvs, func(a, b keyVal) int {
		return cmp.Compare(a.key, b.key)
	})
	for i, kv := range kvs {
		if i > 0 {
			e.w.WriteByte(',')
		}
		if e.indent != 0 {
			e.writeIndent()
		}
		e.encodeString(kv.key)
		e.w.WriteByte(':')
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
	e.w.WriteByte('}')
	return nil
}

func (e *JqEncoder) writeIndent() {
	e.w.WriteByte('\n')
	if n := e.depth; n > 0 {
		if e.tab {
			e.writeIndentInternal(n, "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t")
		} else {
			e.writeIndentInternal(n, "                                ")
		}
	}
}

func (e *JqEncoder) writeIndentInternal(n int, spaces string) {
	for n > 0 {
		length := min(n, len(spaces))
		e.w.WriteString(spaces[:length])
		n -= length
	}
}

type opBuiltInJsonRow struct {
	enc     JqEncoder
	columns []jsonRowColumnEncoder
}

func newOpBuiltInJsonRow() *opBuiltInJsonRow {
	var op opBuiltInJsonRow
	op.enc.intialize(false, 0)
	return &op
}

func (op *opBuiltInJsonRow) jsonRow(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	var scratchOutput functionScratchOutput
	if result.HasFunctionScratch() {
		scratchOutput.result = result
		op.enc.useWriter(&scratchOutput)
		defer op.enc.restoreWriter()
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	if cap(op.columns) < len(params) {
		op.columns = make([]jsonRowColumnEncoder, len(params))
	} else {
		op.columns = op.columns[:len(params)]
	}
	for idx, param := range params {
		column, err := prepareJSONRowColumn(param, proc)
		if err != nil {
			clear(op.columns)
			return err
		}
		op.columns[idx] = column
	}
	defer clear(op.columns)

	op.enc.done()
	defer op.enc.done()
	for row := uint64(0); row < uint64(length); row++ {
		if selectList.Contains(row) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		op.enc.w.WriteByte('[')
		for paramIdx := range op.columns {
			if paramIdx > 0 {
				op.enc.w.WriteByte(',')
			}
			if err := op.columns[paramIdx](&op.enc, row); err != nil {
				return err
			}
		}
		op.enc.w.WriteByte(']')
		if err := op.enc.err(); err != nil {
			return err
		}
		if err := rs.AppendBytes(op.enc.bytes(), false); err != nil {
			return err
		}
		op.enc.done()
	}
	return nil
}

type jsonRowColumnEncoder func(*JqEncoder, uint64) error

func encodeJSONRowNull(e *JqEncoder, _ uint64) error {
	e.w.WriteString("null")
	return nil
}

func prepareJSONRowColumn(
	v *vector.Vector,
	proc *process.Process,
) (jsonRowColumnEncoder, error) {
	switch fromType := v.GetType(); fromType.Oid {
	case types.T_any:
		return encodeJSONRowNull, nil
	case types.T_bool:
		param := vector.GenerateFunctionFixedTypeParameter[bool](v)
		return func(e *JqEncoder, row uint64) error {
			value, isNull := param.GetValue(row)
			if isNull {
				return encodeJSONRowNull(e, row)
			}
			if value {
				e.w.WriteString("true")
			} else {
				e.w.WriteString("false")
			}
			return nil
		}, nil
	case types.T_int8:
		return prepareJSONRowSignedColumn[int8](v), nil
	case types.T_int16:
		return prepareJSONRowSignedColumn[int16](v), nil
	case types.T_int32:
		return prepareJSONRowSignedColumn[int32](v), nil
	case types.T_int64:
		return prepareJSONRowSignedColumn[int64](v), nil
	case types.T_uint8:
		return prepareJSONRowUnsignedColumn[uint8](v), nil
	case types.T_uint16:
		return prepareJSONRowUnsignedColumn[uint16](v), nil
	case types.T_uint32:
		return prepareJSONRowUnsignedColumn[uint32](v), nil
	case types.T_uint64:
		return prepareJSONRowUnsignedColumn[uint64](v), nil
	case types.T_float32:
		return prepareJSONRowFloatColumn[float32](v), nil
	case types.T_float64:
		return prepareJSONRowFloatColumn[float64](v), nil
	case types.T_decimal64:
		return prepareJSONRowDecimalColumn[types.Decimal64](v), nil
	case types.T_decimal128:
		return prepareJSONRowDecimalColumn[types.Decimal128](v), nil
	case types.T_date:
		return prepareJSONRowStringerColumn[types.Date](v), nil
	case types.T_time:
		return prepareJSONRowStringerColumn[types.Time](v), nil
	case types.T_datetime:
		return prepareJSONRowStringerColumn[types.Datetime](v), nil
	case types.T_timestamp:
		return prepareJSONRowStringerColumn[types.Timestamp](v), nil
	case types.T_char, types.T_varchar, types.T_text:
		return prepareJSONRowStringColumn(v), nil
	case types.T_array_float32:
		return prepareJSONRowArrayColumn(v,
			func(value float32) float64 { return float64(value) }), nil
	case types.T_array_float64:
		return prepareJSONRowArrayColumn(v,
			func(value float64) float64 { return value }), nil
	case types.T_array_bf16:
		return prepareJSONRowArrayColumn(v,
			func(value types.BF16) float64 { return float64(value.ToFloat32()) }), nil
	case types.T_array_float16:
		return prepareJSONRowArrayColumn(v,
			func(value types.Float16) float64 { return float64(value.ToFloat32()) }), nil
	case types.T_array_int8:
		return prepareJSONRowArrayColumn(v,
			func(value int8) float64 { return float64(value) }), nil
	case types.T_array_uint8:
		return prepareJSONRowArrayColumn(v,
			func(value uint8) float64 { return float64(value) }), nil
	case types.T_uuid:
		return prepareJSONRowStringerColumn[types.Uuid](v), nil
	case types.T_json:
		param := vector.GenerateFunctionStrParameter(v)
		return func(e *JqEncoder, row uint64) error {
			value, isNull := param.GetStrValue(row)
			if isNull {
				return encodeJSONRowNull(e, row)
			}
			return bytejson.WriteJSONText(e.w, types.DecodeJson(value))
		}, nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return nil, moerr.NewInvalidInputf(proc.Ctx,
			"binary data not supported json_row: %v",
			fromType.String())
	default:
		return nil, moerr.NewInvalidInputf(proc.Ctx,
			"unsupported type for json_row: %v",
			fromType.String())
	}
}

func prepareJSONRowSignedColumn[T constraints.Signed](
	v *vector.Vector,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionFixedTypeParameter[T](v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.w.Write(strconv.AppendInt(e.buf[:0], int64(value), 10))
		return nil
	}
}

func prepareJSONRowUnsignedColumn[T constraints.Unsigned](
	v *vector.Vector,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionFixedTypeParameter[T](v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.w.Write(strconv.AppendUint(e.buf[:0], uint64(value), 10))
		return nil
	}
}

func prepareJSONRowFloatColumn[T constraints.Float](
	v *vector.Vector,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionFixedTypeParameter[T](v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.encodeFloat64(float64(value))
		return nil
	}
}

func prepareJSONRowDecimalColumn[T types.DecimalWithFormat](
	v *vector.Vector,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionFixedTypeParameter[T](v)
	scale := v.GetType().Scale
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.w.WriteString(value.Format(scale))
		return nil
	}
}

func prepareJSONRowStringerColumn[T types.FixedWithStringer](
	v *vector.Vector,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionFixedTypeParameter[T](v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.encodeString(value.String())
		return nil
	}
}

func prepareJSONRowStringColumn(v *vector.Vector) jsonRowColumnEncoder {
	param := vector.GenerateFunctionStrParameter(v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetStrValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		e.encodeBytes(value)
		return e.err()
	}
}

func prepareJSONRowArrayColumn[T types.ArrayElement](
	v *vector.Vector,
	toFloat64 func(T) float64,
) jsonRowColumnEncoder {
	param := vector.GenerateFunctionStrParameter(v)
	return func(e *JqEncoder, row uint64) error {
		value, isNull := param.GetStrValue(row)
		if isNull {
			return encodeJSONRowNull(e, row)
		}
		encodeJSONRowArray(e, types.BytesToArray[T](value), toFloat64)
		return nil
	}
}

func encodeJSONRowArray[T types.ArrayElement](
	e *JqEncoder,
	values []T,
	toFloat64 func(T) float64,
) {
	e.w.WriteByte('[')
	for idx, value := range values {
		if idx > 0 {
			e.w.WriteByte(',')
		}
		e.encodeFloat64(toFloat64(value))
	}
	e.w.WriteByte(']')
}
