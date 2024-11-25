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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashmap_util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(ShuffleBuild)

const (
	ReceiveBatch = iota
	BuildHashMap
	SendJoinMap
	SendSucceed
)

type container struct {
	state          int
	hashmapBuilder hashmap_util.HashmapBuilder
}

type ShuffleBuild struct {
	ctr               container
	HashOnPK          bool
	NeedBatches       bool
	NeedAllocateSels  bool
	Conditions        []*plan.Expr
	RuntimeFilterSpec *plan.RuntimeFilterSpec
	JoinMapTag        int32
	ShuffleIdx        int32

	IsDedup           bool
	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type

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
	runtimeSucceed := shuffleBuild.ctr.state > ReceiveBatch
	mapSucceed := shuffleBuild.ctr.state == SendSucceed

	shuffleBuild.ctr.hashmapBuilder.Reset(proc, !mapSucceed)
	shuffleBuild.ctr.state = ReceiveBatch
	message.FinalizeRuntimeFilter(shuffleBuild.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), shuffleBuild.JoinMapTag, true, shuffleBuild.ShuffleIdx, mapSucceed)
}

func (shuffleBuild *ShuffleBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	shuffleBuild.ctr.hashmapBuilder.Free(proc)
}

func (shuffleBuild *ShuffleBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
