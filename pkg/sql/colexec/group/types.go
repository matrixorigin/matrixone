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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregate"
)

const (
	H8 = iota
	HStr
)

type evalVector struct {
	needFree bool
	vec      *vector.Vector
}

type container struct {
	typ       int
	inserted  []uint8
	zInserted []uint8

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap

	aggVecs   []evalVector
	groupVecs []evalVector

	vecs []*vector.Vector

	bat *batch.Batch
}

type Argument struct {
	ctr      *container
	NeedEval bool // need to projection the aggregate column
	Ibucket  uint64
	Nbucket  uint64
	Exprs    []*plan.Expr // group Expressions
	Types    []types.Type
	Aggs     []aggregate.Aggregate // aggregations
}
