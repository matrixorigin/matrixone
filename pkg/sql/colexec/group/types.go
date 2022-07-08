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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
)

const (
	UnitLimit = 256
)

const (
	H8 = iota
	H24
	H32
	H40
	HStr
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type Container struct {
	typ           int
	rows          uint64
	keyOffs       []uint32
	zKeyOffs      []uint32
	inserted      []uint8
	zInserted     []uint8
	hashes        []uint64
	strHashStates [][3]uint64
	values        []uint64
	intHashMap    *hashtable.Int64HashMap
	strHashMap    *hashtable.StringHashMap

	aggVecs   []evalVector
	groupVecs []evalVector

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
	hstr struct {
		keys [][]byte
	}
	bat *batch.Batch
}

type Argument struct {
	ctr   *Container
	Exprs []*plan.Expr // group Expressions
	Types []types.Type
	Aggs  []aggregate.Aggregate // aggregations
}
