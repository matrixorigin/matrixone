// Copyright 2021 Matrix Origin
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

package common

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type PPLevel int8

const (
	PPL0 PPLevel = iota
	PPL1
	PPL2
	PPL3
	PPL4
)

type ZonemapPrintKind int

const (
	ZonemapPrintKindNormal ZonemapPrintKind = iota
	ZonemapPrintKindCompose
	ZonemapPrintKindHex
)

const DefaultMaxRowsToPrint = 3

func RepeatStr(str string, times int) string {
	for i := 0; i < times; i++ {
		str = fmt.Sprintf("%s\t", str)
	}
	return str
}

func DoIfFatalEnabled(fn func()) {
	if logutil.GetSkip1Logger().Core().Enabled(zapcore.FatalLevel) {
		fn()
	}
}
func DoIfErrorEnabled(fn func()) {
	if logutil.GetSkip1Logger().Core().Enabled(zapcore.ErrorLevel) {
		fn()
	}
}
func DoIfWarnEnabled(fn func()) {
	if logutil.GetSkip1Logger().Core().Enabled(zapcore.WarnLevel) {
		fn()
	}
}
func DoIfInfoEnabled(fn func()) {
	if logutil.GetSkip1Logger().Core().Enabled(zapcore.InfoLevel) {
		fn()
	}
}
func DoIfDebugEnabled(fn func()) {
	if logutil.GetSkip1Logger().Core().Enabled(zapcore.DebugLevel) {
		fn()
	}
}

type opt struct {
	doNotPrintBinary bool
	specialRowid     bool // just a uint64, blockid
}

type TypePrintOpt interface {
	apply(*opt)
}

type WithDoNotPrintBin struct{}

func (w WithDoNotPrintBin) apply(o *opt) { o.doNotPrintBinary = true }

type WithSpecialRowid struct{}

func (w WithSpecialRowid) apply(o *opt) { o.specialRowid = true }

func TypeStringValue(t types.Type, v any, isNull bool, opts ...TypePrintOpt) string {
	if isNull {
		return "null"
	}
	opt := &opt{}
	for _, o := range opts {
		o.apply(opt)
	}

	switch t.Oid {
	case types.T_bool, types.T_int8, types.T_int16, types.T_int32,
		types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32,
		types.T_uint64, types.T_float32, types.T_float64:
		return fmt.Sprintf("%v", v)
	case types.T_bit:
		return fmt.Sprintf("%v", v)
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		buf := v.([]byte)
		printable := true
		for _, c := range buf {
			if !strconv.IsPrint(rune(c)) {
				printable = false
				break
			}
		}
		if printable {
			if len(buf) > 500 {
				buf = buf[:500]
			}
			return string(buf)
		} else if opt.doNotPrintBinary {
			return fmt.Sprintf("binary[%d]", len(buf))
		} else {
			return fmt.Sprintf("%x", buf)
		}
	case types.T_array_float32:
		// The parent function is mostly used to print the vector content for debugging.
		return types.BytesToArrayToString[float32](v.([]byte))
	case types.T_array_float64:
		return types.BytesToArrayToString[float64](v.([]byte))
	case types.T_date:
		val := v.(types.Date)
		return val.String()
	case types.T_datetime:
		val := v.(types.Datetime)
		return val.String2(6)
	case types.T_time:
		val := v.(types.Time)
		return val.String2(6)
	case types.T_timestamp:
		val := v.(types.Timestamp)
		return val.String2(time.Local, 6)
	case types.T_decimal64:
		val := v.(types.Decimal64)
		return val.Format(t.Scale)
	case types.T_decimal128:
		val := v.(types.Decimal128)
		return val.Format(t.Scale)
	case types.T_json:
		val := v.([]byte)
		j := types.DecodeJson(val)
		return j.String()
	case types.T_uuid:
		val := v.(types.Uuid)
		return val.String()
	case types.T_TS:
		val := v.(types.TS)
		return val.ToString()
	case types.T_Rowid:
		val := v.(types.Rowid)
		if opt.specialRowid {
			return strconv.FormatUint(types.DecodeUint64(val[:]), 10)
		} else {
			val := v.(types.Rowid)
			return val.String()
		}
	case types.T_Blockid:
		val := v.(types.Blockid)
		return val.String()
	case types.T_enum:
		val := v.(types.Enum)
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func vec2Str[T any](vec []T, v *vector.Vector, opts ...TypePrintOpt) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", v.Length()))
	first := true
	for i := 0; i < len(vec); i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		if v.GetNulls().Contains(uint64(i)) {
			_, _ = w.WriteString(TypeStringValue(*v.GetType(), nil, true))
		} else {
			_, _ = w.WriteString(TypeStringValue(*v.GetType(), vec[i], false, opts...))
		}
		first = false
	}
	return w.String()
}

func MoVectorToString(v *vector.Vector, printN int, opts ...TypePrintOpt) string {
	switch v.GetType().Oid {
	case types.T_bool:
		return vec2Str(vector.MustFixedCol[bool](v)[:printN], v)
	case types.T_int8:
		return vec2Str(vector.MustFixedCol[int8](v)[:printN], v)
	case types.T_int16:
		return vec2Str(vector.MustFixedCol[int16](v)[:printN], v)
	case types.T_int32:
		return vec2Str(vector.MustFixedCol[int32](v)[:printN], v)
	case types.T_int64:
		return vec2Str(vector.MustFixedCol[int64](v)[:printN], v)
	case types.T_uint8:
		return vec2Str(vector.MustFixedCol[uint8](v)[:printN], v)
	case types.T_uint16:
		return vec2Str(vector.MustFixedCol[uint16](v)[:printN], v)
	case types.T_uint32:
		return vec2Str(vector.MustFixedCol[uint32](v)[:printN], v)
	case types.T_uint64:
		return vec2Str(vector.MustFixedCol[uint64](v)[:printN], v)
	case types.T_float32:
		return vec2Str(vector.MustFixedCol[float32](v)[:printN], v)
	case types.T_float64:
		return vec2Str(vector.MustFixedCol[float64](v)[:printN], v)
	case types.T_date:
		return vec2Str(vector.MustFixedCol[types.Date](v)[:printN], v)
	case types.T_datetime:
		return vec2Str(vector.MustFixedCol[types.Datetime](v)[:printN], v)
	case types.T_time:
		return vec2Str(vector.MustFixedCol[types.Time](v)[:printN], v)
	case types.T_timestamp:
		return vec2Str(vector.MustFixedCol[types.Timestamp](v)[:printN], v)
	case types.T_enum:
		return vec2Str(vector.MustFixedCol[types.Enum](v)[:printN], v)
	case types.T_decimal64:
		return vec2Str(vector.MustFixedCol[types.Decimal64](v)[:printN], v)
	case types.T_decimal128:
		return vec2Str(vector.MustFixedCol[types.Decimal128](v)[:printN], v)
	case types.T_uuid:
		return vec2Str(vector.MustFixedCol[types.Uuid](v)[:printN], v)
	case types.T_TS:
		return vec2Str(vector.MustFixedCol[types.TS](v)[:printN], v)
	case types.T_Rowid:
		return vec2Str(vector.MustFixedCol[types.Rowid](v)[:printN], v)
	case types.T_Blockid:
		return vec2Str(vector.MustFixedCol[types.Blockid](v)[:printN], v)
	}
	if v.GetType().IsVarlen() {
		return vec2Str(vector.InefficientMustBytesCol(v)[:printN], v, opts...)
	}
	return fmt.Sprintf("unkown type vec... %v", *v.GetType())
}

func MoBatchToString(moBat *batch.Batch, printN int) string {
	n := moBat.RowCount()
	if n > printN {
		n = printN
	}
	buf := new(bytes.Buffer)
	for i, vec := range moBat.Vecs {
		if len(moBat.Attrs) == 0 {
			fmt.Fprintf(buf, "[col%v] = %v\n", i, MoVectorToString(vec, n))
		} else {
			fmt.Fprintf(buf, "[%v] = %v\n", moBat.Attrs[i], MoVectorToString(vec, n, WithDoNotPrintBin{}))
		}
	}
	return buf.String()
}

func ApiBatchToString(apiBat *api.Batch, printN int) string {
	if apiBat == nil {
		return ""
	}
	bat, _ := batch.ProtoBatchToBatch(apiBat)
	return MoBatchToString(bat, printN)
}

func DebugMoBatch(moBat *batch.Batch) string {
	if !logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		return "not debug level"
	}
	return MoBatchToString(moBat, DefaultMaxRowsToPrint)
}
