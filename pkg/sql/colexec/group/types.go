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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_col/group_concat"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	H8 = iota
	HStr
	HIndex
)

const (
	UnaryAgg = iota
	MultiAgg
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	typ       int
	inserted  []uint8
	zInserted []uint8

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap
	// idx        *index.LowCardinalityIndex

	aggVecs           []evalVector
	groupVecs         []evalVector
	keyWidth          int // keyWidth is the width of group by columns, it determines which hash map to use.
	groupVecsNullable bool

	// multiVecs are used for group_concat,
	// cause that group_concat can have many cols like group(a,b,c)
	// in this cases, len(multiVecs[0]) will be 3
	multiVecs [][]evalVector

	vecs []*vector.Vector

	bat *batch.Batch

	hasAggResult bool

	tmpVecs []*vector.Vector // for reuse

	state vm.CtrState
}

type Argument struct {
	ctr          *container
	IsShuffle    bool // is shuffle group
	PreAllocSize uint64
	NeedEval     bool // need to projection the aggregate column
	Ibucket      uint64
	Nbucket      uint64
	Exprs        []*plan.Expr // group Expressions
	Types        []types.Type
	Aggs         []agg.Aggregate         // aggregations
	MultiAggs    []group_concat.Argument // multiAggs, for now it's group_concat

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) WithAggs(aggs []agg.Aggregate) *Argument {
	arg.Aggs = aggs
	return arg
}

func (arg *Argument) WithExprs(exprs []*plan.Expr) *Argument {
	arg.Exprs = exprs
	return arg
}

func (arg *Argument) WithTypes(types []types.Type) *Argument {
	arg.Types = types
	return arg
}

func (arg *Argument) WithMultiAggs(multiAggs []group_concat.Argument) *Argument {
	arg.MultiAggs = multiAggs
	return arg
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanHashMap()
		ctr.cleanAggVectors()
		ctr.cleanGroupVectors()
		ctr.cleanMultiAggVecs()
		ctr.tmpVecs = nil
	}
}

func (ctr *container) ToInputType(idx int) (t []types.Type) {
	for i := range ctr.multiVecs[idx] {
		t = append(t, *ctr.multiVecs[idx][i].vec.GetType())
	}
	return
}

func (ctr *container) ToVectors(idx int) (vecs []*vector.Vector) {
	for i := range ctr.multiVecs[idx] {
		vecs = append(vecs, ctr.multiVecs[idx][i].vec)
	}
	return
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanAggVectors() {
	for i := range ctr.aggVecs {
		if ctr.aggVecs[i].executor != nil {
			ctr.aggVecs[i].executor.Free()
		}
		ctr.aggVecs[i].vec = nil
	}
}

func (ctr *container) cleanMultiAggVecs() {
	for i := range ctr.multiVecs {
		for j := range ctr.multiVecs[i] {
			if ctr.multiVecs[i][j].executor != nil {
				ctr.multiVecs[i][j].executor.Free()
			}
			ctr.multiVecs[i][j].vec = nil
		}
	}
}

func (ctr *container) cleanGroupVectors() {
	for i := range ctr.groupVecs {
		if ctr.groupVecs[i].executor != nil {
			ctr.groupVecs[i].executor.Free()
		}
		ctr.groupVecs[i].vec = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.intHashMap != nil {
		ctr.intHashMap.Free()
		ctr.intHashMap = nil
	}
	if ctr.strHashMap != nil {
		ctr.strHashMap.Free()
		ctr.strHashMap = nil
	}
}
