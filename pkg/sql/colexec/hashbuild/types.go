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

package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(HashBuild)

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	SendJoinMap
)

type container struct {
	state              int
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	runtimeFilterIn    bool
	multiSels          [][]int32
	batches            []*batch.Batch
	inputBatchRowCount int
	buf                *batch.Batch

	executor []colexec.ExpressionExecutor
	vecs     [][]*vector.Vector

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap

	uniqueJoinKeys []*vector.Vector
}

type HashBuild struct {
	ctr container
	// need to generate a push-down filter expression
	NeedExpr          bool
	NeedHashMap       bool
	HashOnPK          bool
	NeedMergedBatch   bool
	NeedAllocateSels  bool
	Typs              []types.Type
	Conditions        []*plan.Expr
	JoinMapTag        int32
	JoinMapRefCnt     int32
	RuntimeFilterSpec *pbplan.RuntimeFilterSpec
	vm.OperatorBase
}

func (hashBuild *HashBuild) GetOperatorBase() *vm.OperatorBase {
	return &hashBuild.OperatorBase
}

func init() {
	reuse.CreatePool[HashBuild](
		func() *HashBuild {
			return &HashBuild{}
		},
		func(a *HashBuild) {
			*a = HashBuild{}
		},
		reuse.DefaultOptions[HashBuild]().
			WithEnableChecker(),
	)
}

func (hashBuild HashBuild) TypeName() string {
	return opName
}

func NewArgument() *HashBuild {
	return reuse.Alloc[HashBuild](nil)
}

func (hashBuild *HashBuild) Release() {
	if hashBuild != nil {
		reuse.Free[HashBuild](hashBuild, nil)
	}
}

func (hashBuild *HashBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashBuild.ctr
	ctr.state = BuildHashMap
	message.FinalizeRuntimeFilter(hashBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), hashBuild.JoinMapTag, false, 0, pipelineFailed, err)
	if ctr.batches != nil {
		ctr.batches = ctr.batches[:0]
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	ctr.intHashMap = nil
	ctr.strHashMap = nil
	if ctr.multiSels != nil {
		ctr.multiSels = ctr.multiSels[:0]
	}
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].ResetForNextQuery()
		}
	}
}

func (hashBuild *HashBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashBuild.ctr
	message.FinalizeRuntimeFilter(hashBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), hashBuild.JoinMapTag, false, 0, pipelineFailed, err)
	ctr.batches = nil
	ctr.buf = nil
	ctr.intHashMap = nil
	ctr.strHashMap = nil
	ctr.multiSels = nil
	ctr.cleanEvalVectors()
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].Free()
		}
	}
	ctr.executor = nil
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
