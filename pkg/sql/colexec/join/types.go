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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/joincondition"
)

const (
	Build = iota
	Probe
	End
)

const (
	UnitLimit = 256
)

var OneInt64s []int64

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type Container struct {
	flg     bool // incicates if addition columns need to be copied
	state   int
	rows    uint64
	keys    [][]byte
	values  []uint64
	zValues []int64
	// hashes        []uint64
	inserted      []uint8
	zInserted     []uint8
	strHashStates [][3]uint64
	strHashMap    *hashtable.StringHashMap

	poses []int32 // pos of vectors need to be copied

	sels [][]int64

	bat *batch.Batch

	vecs []evalVector

	decimal64Slice  []types.Decimal64
	decimal128Slice []types.Decimal128
}

type ResultPos struct {
	Rel int32
	Pos int32
}

type Argument struct {
	ctr        *Container
	IsPreBuild bool // hashtable is pre-build
	Result     []ResultPos
	Conditions [][]joincondition.Condition
}
