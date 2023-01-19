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
	"fmt"
	"strconv"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type PPLevel int8

const (
	PPL0 PPLevel = iota
	PPL1
	PPL2
	PPL3
)

func RepeatStr(str string, times int) string {
	for i := 0; i < times; i++ {
		str = fmt.Sprintf("%s\t", str)
	}
	return str
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

func TypeStringValue(t types.Type, v any, opts ...TypePrintOpt) string {
	if types.IsNull(v) {
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
	case types.T_char, types.T_varchar, types.T_text, types.T_blob:
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
		return val.String()
	case types.T_decimal128:
		val := v.(types.Decimal128)
		return val.String()
	case types.T_json:
		val := v.([]byte)
		j := types.DecodeJson(val)
		return j.String()
	case types.T_uuid:
		val := v.(types.Uuid)
		return val.ToString()
	case types.T_TS:
		val := v.(types.TS)
		return val.ToString()
	case types.T_Rowid:
		val := v.(types.Rowid)
		if opt.specialRowid {
			return strconv.FormatUint(types.DecodeUint64(val[:]), 10)
		} else {
			val := v.(types.Rowid)
			blkID, offset := pkgcatalog.DecodeRowid(val)
			return fmt.Sprintf("BLK-%d:Off-%d", blkID, offset)
		}
	default:
		return fmt.Sprintf("%v", v)
	}
}
