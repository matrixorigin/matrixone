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

package memorytable

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

// ID represents a unique id
type ID = memoryengine.ID

// Text represents a string value
type Text string

// Less compares tow Texts
func (t Text) Less(than Text) bool {
	return t < than
}

// Bool represents a boolean value
type Bool bool

// Less compares tow Bools
func (b Bool) Less(than Bool) bool {
	return bool(!b && than)
}

// Int represents an integer value
type Int int64

// Less compares tow Ints
func (i Int) Less(than Int) bool {
	return i < than
}

// Unit represents an unsigned integer value
type Uint int64

// Less compares tow Uints
func (i Uint) Less(than Uint) bool {
	return i < than
}

// Float represents a floating-point value
type Float float64

// Less compares two Floats
func (f Float) Less(than Float) bool {
	return f < than
}

// Bytes represents a slice of bytes
type Bytes []byte

// Less compares two Bytes's
func (b Bytes) Less(than Bytes) bool {
	return bytes.Compare(b, than) < 0
}

// Decimal represents a Decimal128 value
type Decimal types.Decimal128

// Less compares two Decimals
func (b Decimal) Less(than Decimal) bool {
	return types.Decimal128(b).Compare(types.Decimal128(than)) < 0
}

// ToOrdered convert v to Ordered value
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
		if v>>63 == 0 {
			return Decimal{uint64(v), 0}
		}
		return Decimal{uint64(v), ^uint64(0)}
	case types.Decimal128:
		return Decimal(v)
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

// TypeMatch reports whether v is typ typed
func TypeMatch(v any, typ types.T) bool {
	if v == nil {
		panic("should not be nil")
	}
	var ok bool
	switch typ {
	case types.T_bool:
		_, ok = v.(bool)
	case types.T_bit:
		_, ok = v.(uint64)
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
		_, ok = v.(types.TimeType)
	case types.T_datetime:
		_, ok = v.(types.Datetime)
	case types.T_timestamp:
		_, ok = v.(types.Timestamp)
	case types.T_enum:
		_, ok = v.(types.Enum)
	case types.T_interval:
		_, ok = v.(types.IntervalType)
	case types.T_char:
		_, ok = v.([]byte)
	case types.T_varchar:
		_, ok = v.([]byte)
	case types.T_binary:
		_, ok = v.([]byte)
	case types.T_array_float32:
		// NOTE 1: This function is used by TAE catalog to check if the value set for the schema in the code is
		// matching the Attribute storage format. It is used by verifyAttr() and used to verify
		// `MoDatabaseTypes = []types.Type` declaration and value set are accordingly.
		// NOTE 2: If you are ever going to use vector in catalog (which you would not be needing in the most part),
		// make sure to convert []float32 to []byte. Else this check will fail.
		_, ok = v.([]byte)
	case types.T_array_float64:
		_, ok = v.([]byte)
	case types.T_varbinary:
		_, ok = v.([]byte)
	case types.T_json:
		_, ok = v.([]byte)
	case types.T_blob:
		_, ok = v.([]byte)
	case types.T_text, types.T_datalink:
		_, ok = v.([]byte)
	case types.T_uuid:
		_, ok = v.(types.Uuid)
	case types.T_TS:
		_, ok = v.(types.TS)
	case types.T_Rowid:
		_, ok = v.(types.Rowid)
	case types.T_Blockid:
		_, ok = v.(types.Blockid)
	default:
		panic(fmt.Sprintf("fixme: %v", typ))
	}
	return ok
}
