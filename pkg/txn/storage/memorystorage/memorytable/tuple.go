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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Tuple represents multiple ordered values
type Tuple []any

var _ Ordered[Tuple] = Tuple{}

// Less compares two tuples
func (t Tuple) Less(than Tuple) bool {
	i := 0
	for i < len(t) {
		a := t[i]
		if i >= len(than) {
			return false
		}
		b := than[i]

		// min, max
		if a == Min {
			return b != Min
		}
		if a == Max {
			return false
		}
		if b == Min {
			return false
		}
		if b == Max {
			return a != Max
		}

		switch a := a.(type) {

		case Text:
			b := b.(Text)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case Bool:
			b := b.(Bool)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case Int:
			b := b.(Int)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case Uint:
			b := b.(Uint)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case Float:
			b := b.(Float)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case Bytes:
			b := b.(Bytes)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case ID:
			b := b.(ID)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		case types.TS:
			b := b.(types.TS)
			if a.LT(&b) {
				return true
			} else if b.LT(&a) {
				return false
			}

		case Decimal:
			b := b.(Decimal)
			if a.Less(b) {
				return true
			} else if b.Less(a) {
				return false
			}

		default:
			panic(fmt.Sprintf("unknown item type: %T %#v", a, a))
		}

		i++
	}

	return i != len(than)
}
