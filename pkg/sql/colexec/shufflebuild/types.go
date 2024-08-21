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

package shufflebuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(ShuffleBuild)

const (
	ReceiveBatch = iota
	BuildHashMap
	SendJoinMap
)

type container struct {
	state              int
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	multiSels          [][]int32
	batches            []*batch.Batch
	inputBatchRowCount int
	buf                *batch.Batch
	executor           []colexec.ExpressionExecutor
	vecs               [][]*vector.Vector
	intHashMap         *hashmap.IntHashMap
	strHashMap         *hashmap.StrHashMap
	uniqueJoinKeys     []*vector.Vector
}

type ShuffleBuild struct {
<<<<<<< HEAD
	ctr container
	// need to generate a push-down filter expression
	NeedExpr         bool
=======
	ctr              *container
>>>>>>> 143c670761c78c40d30563b42019ea533fdf4db5
	HashOnPK         bool
	NeedBatches      bool
	NeedAllocateSels bool
	Conditions       []*plan.Expr

	RuntimeFilterSpec *pbplan.RuntimeFilterSpec
	JoinMapTag        int32
	ShuffleIdx        int32
	vm.OperatorBase
}

func (shuffleBuild *ShuffleBuild) GetOperatorBase() *vm.OperatorBase {
	return &shuffleBuild.OperatorBase
}

func init() {
	reuse.CreatePool[ShuffleBuild](
		func() *ShuffleBuild {
			return &ShuffleBuild{}
		},
		func(a *ShuffleBuild) {
			*a = ShuffleBuild{}
		},
		reuse.DefaultOptions[ShuffleBuild]().
			WithEnableChecker(),
	)
}

func (shuffleBuild ShuffleBuild) TypeName() string {
	return opName
}

func NewArgument() *ShuffleBuild {
	return reuse.Alloc[ShuffleBuild](nil)
}

func (shuffleBuild *ShuffleBuild) Release() {
	if shuffleBuild != nil {
		reuse.Free[ShuffleBuild](shuffleBuild, nil)
	}
}

func (shuffleBuild *ShuffleBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &shuffleBuild.ctr
	ctr.state = ReceiveBatch
	message.FinalizeRuntimeFilter(shuffleBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), shuffleBuild.JoinMapTag, true, shuffleBuild.ShuffleIdx, pipelineFailed, err)
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

func (shuffleBuild *ShuffleBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &shuffleBuild.ctr
	ctr.intHashMap = nil
	ctr.strHashMap = nil
	ctr.multiSels = nil
	ctr.batches = nil
	if ctr.buf != nil {
		ctr.buf.Clean(proc.GetMPool())
		ctr.buf = nil
	}
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
