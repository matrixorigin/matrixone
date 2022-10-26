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

package memtable

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

type ID = memoryengine.ID

type Text string

func (t Text) Less(than Text) bool {
	return t < than
}

type Bool bool

func (b Bool) Less(than Bool) bool {
	return bool(!b && than)
}

type Int int64

func (i Int) Less(than Int) bool {
	return i < than
}

type Uint int64

func (i Uint) Less(than Uint) bool {
	return i < than
}

type Float float64

func (f Float) Less(than Float) bool {
	return f < than
}

type Bytes []byte

func (b Bytes) Less(than Bytes) bool {
	return bytes.Compare(b, than) < 0
}

func ToOrdered(v any) any {
	if v == nil {
		panic("should not be nil")
	}
	switch v := v.(type) {
	case bool:
		return Bool(v)
	case int:
		return Int(v)
	case int8:
		return Int(v)
	case int16:
		return Int(v)
	case int32:
		return Int(v)
	case int64:
		return Int(v)
	case uint:
		return Uint(v)
	case uint8:
		return Uint(v)
	case uint16:
		return Uint(v)
	case uint32:
		return Uint(v)
	case uint64:
		return Uint(v)
	case float32:
		return Float(v)
	case float64:
		return Float(v)
	case []byte:
		return Bytes(v)
	case types.Date:
		return Int(v)
	case types.Datetime:
		return Int(v)
	case types.Timestamp:
		return Int(v)
	case types.Decimal64:
		return Bytes(v[:])
	case types.Decimal128:
		return Bytes(v[:])
	case types.TS:
		return Bytes(v[:])
	case types.Rowid:
		return Bytes(v[:])
	case types.Uuid:
		return Bytes(v[:])
	case ID:
		return v
	}

	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Bool:
		return Bool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Int(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Uint(v.Uint())
	case reflect.Float32, reflect.Float64:
		return Float(v.Float())
	}

	panic(fmt.Sprintf("unknown type: %T", v))
}

func TypeMatch(v any, typ types.T) bool {
	if v == nil {
		panic("should not be nil")
	}
	var ok bool
	switch typ {
	case types.T_bool:
		_, ok = v.(bool)
	case types.T_int8:
		_, ok = v.(int8)
	case types.T_int16:
		_, ok = v.(int16)
	case types.T_int32:
		_, ok = v.(int32)
	case types.T_int64:
		_, ok = v.(int64)
	case types.T_uint8:
		_, ok = v.(uint8)
	case types.T_uint16:
		_, ok = v.(uint16)
	case types.T_uint32:
		_, ok = v.(uint32)
	case types.T_uint64:
		_, ok = v.(uint64)
	case types.T_float32:
		_, ok = v.(float32)
	case types.T_float64:
		_, ok = v.(float64)
	case types.T_decimal64:
		_, ok = v.(types.Decimal64)
	case types.T_decimal128:
		_, ok = v.(types.Decimal128)
	case types.T_date:
		_, ok = v.(types.Date)
	case types.T_time:
		_, ok = v.(types.Time)
	case types.T_datetime:
		_, ok = v.(types.Datetime)
	case types.T_timestamp:
		_, ok = v.(types.Timestamp)
	case types.T_interval:
		_, ok = v.(types.IntervalType)
	case types.T_char:
		_, ok = v.([]byte)
	case types.T_varchar:
		_, ok = v.([]byte)
	case types.T_json:
		_, ok = v.([]byte)
	case types.T_blob:
		_, ok = v.([]byte)
	case types.T_text:
		_, ok = v.([]byte)
	case types.T_uuid:
		_, ok = v.(types.Uuid)
	case types.T_TS:
		_, ok = v.(types.TS)
	case types.T_Rowid:
		_, ok = v.(types.Rowid)
	default:
		panic(fmt.Sprintf("fixme: %v", typ))
	}
	return ok
}
