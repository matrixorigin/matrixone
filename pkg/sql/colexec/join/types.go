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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashmap_util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(InnerJoin)

const (
	Build = iota
	Probe
	LoopSpilledPartitions
	ProcessSpilledPartition
	End
)

type probeState int

const (
	psNextBatch probeState = iota
	psSelsForOneRow
	psBatchRow
)

type container struct {
	state int

	itr hashmap.Iterator

	batchRowCount int64
	inbat         *batch.Batch
	rbat          *batch.Batch

	// fileds for pagination

	probeState probeState

	// at any time, lastRow is the index of not processed row in inbat
	lastRow int
	// process idx for zvs and vs, which returned by hashmap.Iterator.Find()
	// guarantee: vs[ctr.vsidx] is the result of inbat[ctr.lastRow]
	vsidx int
	zvs   []int64
	vs    []uint64
	// sels
	sels []int32

	expr colexec.ExpressionExecutor

	joinBats []*batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	executor []colexec.ExpressionExecutor
	vecs     []*vector.Vector

	mp *message.JoinMap

	// for spilling
	spilled          bool
	partitionCnt     int
	currentPartition int
	hashmapBuilder   hashmap_util.HashmapBuilder

	maxAllocSize int64
}

/*
InnerJoin.container has 5 *batch or []*batch
1.
2. inbat means the data of left table. It's created by InnerJoin.Children[0] and cleaned by InnerJoin.Children[0].
3. rbat means the result of InnerJoin. InnerJoin.Probe() create rbat once when it's called first time and use CleanOnlyData() after that.
   InnerJoin.Reset() doesn't need to reset or clean rbat, because the result always has same types. InnerJoin.Free() will clean rbat.
4. joinBat1 means some data from left table used to evaluate join conditions. InnerJoin.Probe() create joinBat1 once.
   joinBat1 will always be overwritten by colexec.SetJoinBatchValues().
   InnerJoin.Reset() doesn't need to reset or clean joinBat1. InnerJoin.Free() will clean joinBat1.
5. joinBat2 means some data from right table, same as joinBat1.
*/

type InnerJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	Cond       *plan.Expr
	Conditions [][]*plan.Expr

	HashOnPK           bool
	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	vm.OperatorBase
}

func (innerJoin *InnerJoin) GetOperatorBase() *vm.OperatorBase {
	return &innerJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *InnerJoin {
			return &InnerJoin{}
		},
		func(a *InnerJoin) {
			*a = InnerJoin{}
		},
		reuse.DefaultOptions[InnerJoin]().
			WithEnableChecker(),
	)
}

func (innerJoin InnerJoin) TypeName() string {
	return opName
}

func NewArgument() *InnerJoin {
	return reuse.Alloc[InnerJoin](nil)
}

func (innerJoin *InnerJoin) Release() {
	if innerJoin != nil {
		reuse.Free(innerJoin, nil)
	}
}

func (innerJoin *InnerJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &innerJoin.ctr
	ctr.itr = nil
	ctr.resetExecutor()
	ctr.resetExprExecutor()
	ctr.cleanHashMap()
	ctr.inbat = nil
	ctr.lastRow = 0
	ctr.state = Build
	ctr.batchRowCount = 0

	ctr.vsidx = 0
	ctr.sels = nil
	ctr.vs = nil
	ctr.zvs = nil
	ctr.probeState = psNextBatch

	ctr.spilled = false
	ctr.partitionCnt = 0

	if innerJoin.OpAnalyzer != nil {
		innerJoin.OpAnalyzer.Alloc(innerJoin.ctr.maxAllocSize)
	}
	innerJoin.ctr.maxAllocSize = 0

}

func (innerJoin *InnerJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &innerJoin.ctr

	ctr.cleanExecutor()
	ctr.cleanExprExecutor()
	ctr.cleanBatch(proc)

}

func (innerJoin *InnerJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
	}
	for i := range ctr.joinBats {
		if ctr.joinBats[i] != nil {
			ctr.joinBats[i].Clean(proc.Mp())
		}
		ctr.joinBats[i] = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) resetExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) cleanExecutor() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].Free()
		}
	}
}
