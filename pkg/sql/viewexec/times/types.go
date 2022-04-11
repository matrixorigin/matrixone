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

const (
	UnitLimit = 256
)

const (
	H0 = iota
	H8
	H24
	H32
	H40
	HStr
)

var OneInt64s []int64

type view struct {
	isPure bool // true: primary key join

	is  []int // subscript in the result attribute
	ris []int // subscript in the result ring
	ois []int // subscript in the origin batch

	attrs      []string
	values     []uint64
	sels       [][]int64
	bat        *batch.Batch
	vecs       []*vector.Vector
	intHashMap *hashtable.Int64HashMap
	strHashMap *hashtable.StringHashMap
}

type probeContainer struct {
	typ  int
	rows uint64

	bat *batch.Batch

	intHashMap *hashtable.Int64HashMap
	strHashMap *hashtable.StringHashMap
}

type Container struct {
	isPure bool // true: primary key join

	is  []int // subscript in the result attribute
	ois []int // subscript in the origin batch

	zs []int64

	sels []int64

	attrs []string

	oattrs []string // order of attributes of input batch

	result map[string]uint8 // result attributes of join

	mx, mx0, mx1 [][]int64 // matrix buffer

	values []uint64

	zValues []int64

	keyOffs  []uint32
	zKeyOffs []uint32

	hashes        []uint64
	strHashStates [][3]uint64

	views []*view

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
	pctr *probeContainer
}

type Argument struct {
	Result []string
	Vars   [][]string
	ctr    *Container
	Bats   []*batch.Batch
}
