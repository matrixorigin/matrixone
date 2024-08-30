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
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashmap_util"
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
	state          int
	hashmapBuilder hashmap_util.HashmapBuilder
}

type ShuffleBuild struct {
	ctr               container
	HashOnPK          bool
	NeedBatches       bool
	NeedAllocateSels  bool
	Conditions        []*plan.Expr
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
	shuffleBuild.ctr.state = ReceiveBatch
	message.FinalizeRuntimeFilter(shuffleBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), shuffleBuild.JoinMapTag, true, shuffleBuild.ShuffleIdx, pipelineFailed, err)

	if pipelineFailed || err != nil {
		shuffleBuild.ctr.hashmapBuilder.FreeWithError(proc)
	} else {
		shuffleBuild.ctr.hashmapBuilder.Reset()
	}
}

func (shuffleBuild *ShuffleBuild) Free(proc *process.Process, pipelineFailed bool, err error) {

	if pipelineFailed || err != nil {
		shuffleBuild.ctr.hashmapBuilder.FreeWithError(proc)
	} else {
		shuffleBuild.ctr.hashmapBuilder.Free(proc)
	}
}
