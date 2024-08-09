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

package indexbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(IndexBuild)

const (
	ReceiveBatch = iota
	HandleRuntimeFilter
	End
)

type container struct {
	state int
	buf   *batch.Batch
}

type IndexBuild struct {
	ctr               container
	RuntimeFilterSpec *plan.RuntimeFilterSpec
	vm.OperatorBase
}

func (indexBuild *IndexBuild) GetOperatorBase() *vm.OperatorBase {
	return &indexBuild.OperatorBase
}

func init() {
	reuse.CreatePool[IndexBuild](
		func() *IndexBuild {
			return &IndexBuild{}
		},
		func(a *IndexBuild) {
			*a = IndexBuild{}
		},
		reuse.DefaultOptions[IndexBuild]().
			WithEnableChecker(),
	)
}

func (indexBuild IndexBuild) TypeName() string {
	return opName
}

func NewArgument() *IndexBuild {
	return reuse.Alloc[IndexBuild](nil)
}

func (indexBuild *IndexBuild) Release() {
	if indexBuild != nil {
		reuse.Free[IndexBuild](indexBuild, nil)
	}
}

func (indexBuild *IndexBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	message.FinalizeRuntimeFilter(indexBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	indexBuild.ctr.state = ReceiveBatch
	if indexBuild.ctr.buf != nil {
		indexBuild.ctr.buf.CleanOnlyData()
	}
}

func (indexBuild *IndexBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	message.FinalizeRuntimeFilter(indexBuild.RuntimeFilterSpec, pipelineFailed, err, proc.GetMessageBoard())
	if indexBuild.ctr.buf != nil {
		indexBuild.ctr.buf.Clean(proc.Mp())
	}
}
