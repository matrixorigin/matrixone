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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashmap_util"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(HashBuild)

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	SendJoinMap
	SendSucceed
)

type container struct {
	state           int
	runtimeFilterIn bool
	hashmapBuilder  hashmap_util.HashmapBuilder
}

type HashBuild struct {
	ctr               container
	NeedHashMap       bool
	HashOnPK          bool
	NeedBatches       bool
	NeedAllocateSels  bool
	Conditions        []*plan.Expr
	JoinMapTag        int32
	JoinMapRefCnt     int32
	RuntimeFilterSpec *plan.RuntimeFilterSpec

	IsDedup           bool
	DelColIdx         int32
	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type

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
	runtimeSucceed := hashBuild.ctr.state > HandleRuntimeFilter
	mapSucceed := hashBuild.ctr.state == SendSucceed

	hashBuild.ctr.hashmapBuilder.Reset(proc, !mapSucceed)
	hashBuild.ctr.state = BuildHashMap
	hashBuild.ctr.runtimeFilterIn = false
	message.FinalizeRuntimeFilter(hashBuild.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
	message.FinalizeJoinMapMessage(proc.GetMessageBoard(), hashBuild.JoinMapTag, false, 0, mapSucceed)
}
func (hashBuild *HashBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.hashmapBuilder.Free(proc)
}

func (hashBuild *HashBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
