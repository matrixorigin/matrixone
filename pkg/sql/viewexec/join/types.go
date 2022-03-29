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

package join

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	UnitLimit = 256
)

type view struct {
	is         []int // subscript in the result attribute
	ris        []int // subscript in the result ring
	attrs      []string
	values     []uint64
	sels       [][]int64
	bat        *batch.Batch
	vecs       []*vector.Vector
	strHashMap *hashtable.StringHashMap
}

type HashTable struct {
	Sels       [][]int64
	StrHashMap *hashtable.StringHashMap
}

type Container struct {
	zs []int64

	attrs []string

	mx0, mx1 [][]int64 // matrix buffer

	strHashStates [][3]uint64

	views []*view

	hstr struct {
		keys [][]byte
	}
}

type Argument struct {
	Vars [][]string
	ctr  *Container
	Bats []*batch.Batch
}
