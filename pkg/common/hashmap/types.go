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

import (
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	UnitLimit = 256
)

var OneInt64s []int64

// Iterator allows you to batch insert/find values
type Iterator interface {
	// Insert vecs[start, start+count) into hashmap
	// the return value corresponds to the corresponding group number(start with 1)
	// WATCH THAT: we do not update the rows of Hash Map at Insert Method because of Speed Performance,
	// If need it, you should call the hash map's AddGroup function by yourself.
	Insert(start, count int, vecs []*vector.Vector, scales []int32) ([]uint64, []int64)
	// Find vecs[start, start+count) int hashmap
	// return value is the corresponding the group number,
	// if it is 0 it means that the corresponding value cannot be found
	Find(start, count int, vecs []*vector.Vector, inBuckets []uint8, scales []int32) ([]uint64, []int64)
}

// StrHashMap key is []byte, value a uint64 value (starting from 1)
// 	each time a new key is inserted, the hashtable returns a lastvalue+1 or, if the old key is inserted, the value corresponding to that key
type StrHashMap struct {
	hasNull bool
	rows    uint64
	keys    [][]byte
	values  []uint64
	// zValues, 0 indicates the presence null, 1 indicates the absence of a null
	zValues       []int64
	strHashStates [][3]uint64
	hashMap       *hashtable.StringHashMap

	decimal64Slice  []types.Decimal64
	decimal128Slice []types.Decimal128
}

type strHashmapIterator struct {
	ibucket, nbucket uint64
	mp               *StrHashMap
}
