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

package txnstorage

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

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

func typeConv(v any) any {
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
	default:
		panic(fmt.Errorf("unknown type: %T", v))
	}
}
