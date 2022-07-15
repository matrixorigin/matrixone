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

package hashmap

import "github.com/matrixorigin/matrixone/pkg/container/hashtable"

const (
	UnitLimit = 256
)

// StrHashMap, key is []byte, value a uint64 value (starting from 1)
// 	each time a new key is inserted, the hashtable returns a lastvalue+1 or, if the old key is inserted, the value corresponding to that key
type StrHashMap struct {
	hasNull       bool
	rows          uint64
	keys          [][]byte
	values        []uint64
	strHashStates [][3]uint64
	hashMap       *hashtable.StringHashMap
}
