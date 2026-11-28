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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	isComposite      bool
}

type TypePrintOpt interface {
	apply(*opt)
}

type WithDoNotPrintBin struct{}

func (w WithDoNotPrintBin) apply(o *opt) { o.doNotPrintBinary = true }

type WithSpecialRowid struct{}

func (w WithSpecialRowid) apply(o *opt) { o.specialRowid = true }

type WithIsComposite struct{}

func (w WithIsComposite) apply(o *opt) { o.isComposite = true }

// TypeStringValue returns the text of v, a value of type t, for TAE's logs
// and error messages. It is types.ValueText, with these display options for
// key and string bytes: a composite key is unpacked into its parts, a
// non-printable string is shown in hex (or as its length with
// WithDoNotPrintBin), a printable one is cut at 500 bytes, and
// WithSpecialRowid shows a rowid as the uint64 of its first 8 bytes.
func TypeStringValue(t types.Type, v any, isNull bool, opts ...TypePrintOpt) string {
	if isNull {
		return "null"
	}
	opt := &opt{}
	for _, o := range opts {
		o.apply(opt)
	}
	if t.Oid == types.T_Rowid && opt.specialRowid {
		val := v.(types.Rowid)
		return strconv.FormatUint(types.DecodeUint64(val[:]), 10)
	}
	if buf, ok := v.([]byte); ok && (t.Oid.IsMySQLString() || t.Oid.IsDatalink()) {
		if opt.isComposite {
			tuple, err := types.Unpack(buf)
			if err == nil {
				var w bytes.Buffer
				content := strings.Join(tuple.SQLStrings(nil), ",")
				if len(content) > 0 {
					w.WriteString("(")
					w.WriteString(content)
					w.WriteString(")")
				}
				return w.String()
			}
		}
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
	}
	return types.ValueText(t, v)
}

// MoVectorToString returns the first printN rows of v as text.
func MoVectorToString(v *vector.Vector, printN int, opts ...TypePrintOpt) string {
	if v == nil || v.Length() == 0 {
		return "empty vector"
	}
	if printN > v.Length() {
		printN = v.Length()
	}
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", v.Length()))
	for i := 0; i < printN; i++ {
		if i > 0 {
			_ = w.WriteByte(',')
		}
		if v.IsNull(uint64(i)) {
			_, _ = w.WriteString(TypeStringValue(*v.GetType(), nil, true))
		} else {
			_, _ = w.WriteString(TypeStringValue(*v.GetType(), vector.GetAny(v, i, false), false, opts...))
		}
	}
	return w.String()
}

func MoBatchToString(moBat *batch.Batch, printN int) string {
	return moBatchToString(moBat, nil, printN)
}

// moBatchToString prints moBat's columns, labelled with attrs when given.
func moBatchToString(moBat *batch.Batch, attrs []string, printN int) string {
	if moBat == nil {
		return "empty batch"
	}
	n := 0
	buf := new(bytes.Buffer)
	for i, vec := range moBat.Vecs {
		if vec == nil {
			fmt.Fprintf(buf, "[col%v] = nil\n", i)
			continue
		}
		if n = vec.Length(); n < printN {
			printN = n
		}
		if i >= len(attrs) {
			fmt.Fprintf(buf, "[col%v] = %v\n", i, MoVectorToString(vec, printN))
		} else {
			fmt.Fprintf(buf, "[%v] = %v\n", attrs[i], MoVectorToString(vec, printN, WithDoNotPrintBin{}))
		}
	}
	return buf.String()
}

func DebugMoBatch(moBat *batch.Batch) string {
	if !logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		return "not debug level"
	}
	return MoBatchToString(moBat, DefaultMaxRowsToPrint)
}
