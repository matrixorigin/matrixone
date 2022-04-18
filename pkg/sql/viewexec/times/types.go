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

package times

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// size limit on the number of tuples to be processed at a time
const (
	UnitLimit = 256
)

// attributes's size of join result: (not include aggregation)
// 	H0:  size == 0 bytes
//  H8:  0 < size <= 8 bytes
//  H24: 8 < size <= 24 bytes
//  H32: 24 < size <= 32 bytes
//  H40: 32 < size <= 40 bytes
//  HStr: size > 40 bytes
const (
	H0 = iota
	H8
	H24
	H32
	H40
	HStr
)

// a slice whose size is equal to UnitLimit and whose value is all 1
var OneInt64s []int64

// a view represent a dimension table with a hashtable
type view struct {
	isPure bool // true: primary key join

	is  []int // subscript in the result attribute
	ris []int // subscript in the result ring
	ois []int // subscript in the origin batch

	attrs []string // attributes of dimension table
	// each element corresponds to a specific group number.
	//   group number is  an indication of the location of join key in the hashtable,
	//	 and this location information is provided by the hashtable.
	values []uint64
	sels   [][]int64        // row list
	bat    *batch.Batch     // tuples of dimension table
	vecs   []*vector.Vector // vector of dimension table
	// only one type of hashtable will exist in a view,
	// if the join key size is zero, then hashtable does not need.
	//  intHashMap:  0 < key size <= 8
	//  strHashMap:  8 < key size
	intHashMap *hashtable.Int64HashMap
	strHashMap *hashtable.StringHashMap
}

type probeContainer struct {
	// the following values are available:
	//   H0, H8, H24, H32, H40, HStr
	typ int

	// number of rows of result
	rows uint64

	// tuples of result batch
	bat *batch.Batch

	// only one type of hashtable will exist in a view,
	// if the result key size is zero, then hashtable does not need.
	//  intHashMap:  0 < key size <= 8
	//  strHashMap:  8 < key size
	intHashMap *hashtable.Int64HashMap
	strHashMap *hashtable.StringHashMap
}

type Container struct {
	isPure bool // true: primary key join

	is  []int // subscript in the result attribute
	ois []int // subscript in the origin batch

	zs []int64 // each element is used to store the number of occurrences of the tuple

	sels []int64 // row number list

	attrs []string // attributes of result

	oattrs []string // order of attributes of input batch

	result map[string]uint8 // result attributes of join

	mx, mx0, mx1 [][]int64 // matrix buffer

	// each element corresponds to a specific group number.
	//   group number is  an indication of the location of result key in the hashtable,
	//	 and this location information is provided by the hashtable.
	values []uint64

	// all elements are zero, used to reset values
	zValues []int64

	// multiple attributes are combined into a single key for hashtable,
	// and since they are processed one by one,
	// the length of the key needs to be recorded after processing an attribute to
	//   facilitate the processing of the next attribute
	keyOffs []uint32
	// all elements are zero, used to reset keyOffs
	zKeyOffs []uint32

	views []*view

	// parameters of hashtable, for the meaning of the parameters please read the information of hashtable.
	hashes        []uint64
	strHashStates [][3]uint64

	hstr struct {
		keys [][]byte
	}

	h8 struct {
		keys  []uint64
		zKeys []uint64
	}
	h24 struct {
		keys  [][3]uint64
		zKeys [][3]uint64
	}
	h32 struct {
		keys  [][4]uint64
		zKeys [][4]uint64
	}
	h40 struct {
		keys  [][5]uint64
		zKeys [][5]uint64
	}
	pctr *probeContainer // container for probe and store result
}

type Argument struct {
	// attributes of join result
	Result []string
	// join key of dimension tables
	Vars [][]string
	ctr  *Container
	// tuples of dimension tables
	Bats []*batch.Batch
}
