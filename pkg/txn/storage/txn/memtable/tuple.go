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

import "fmt"

type Tuple []any

var _ Ordered[Tuple] = Tuple{}

func (t Tuple) Less(than Tuple) bool {
	if len(t) < len(than) {
		return true
	}
	if len(t) > len(than) {
		return false
	}
	for i, key := range t {
		switch key := key.(type) {

		case Text:
			key2 := than[i].(Text)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Bool:
			key2 := than[i].(Bool)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Int:
			key2 := than[i].(Int)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Uint:
			key2 := than[i].(Uint)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Float:
			key2 := than[i].(Float)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Bytes:
			key2 := than[i].(Bytes)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case ID:
			key2 := than[i].(ID)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		default:
			panic(fmt.Sprintf("unknown key type: %T", key))
		}
	}
	// equal
	return false
}
