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

package source

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Source)

const (
	retrieve = 0
	end      = 1
)

type container struct {
	status int
	buf    *batch.Batch
}
type Source struct {
	ctr    *container
	TblDef *plan.TableDef
	Offset int64
	Limit  int64

	// end     bool
	attrs   []string
	types   []types.Type
	Configs map[string]interface{}

	vm.OperatorBase
	colexec.Projection
}

func (source *Source) GetOperatorBase() *vm.OperatorBase {
	return &source.OperatorBase
}

func init() {
	reuse.CreatePool[Source](
		func() *Source {
			return &Source{}
		},
		func(a *Source) {
			*a = Source{}
		},
		reuse.DefaultOptions[Source]().
			WithEnableChecker(),
	)
}

func (source Source) TypeName() string {
	return opName
}

func NewArgument() *Source {
	return reuse.Alloc[Source](nil)
}

func (source *Source) Release() {
	if source != nil {
		reuse.Free[Source](source, nil)
	}
}

func (source *Source) Reset(proc *process.Process, pipelineFailed bool, err error) {
	source.Free(proc, pipelineFailed, err)
}

func (source *Source) Free(proc *process.Process, pipelineFailed bool, err error) {
	if source.ctr != nil {
		if source.ctr.buf != nil {
			source.ctr.buf.Clean(proc.Mp())
			source.ctr.buf = nil
		}
		source.ctr = nil
	}
	if source.ProjectList != nil {
		anal := proc.GetAnalyze(source.GetIdx(), source.GetParallelIdx(), source.GetParallelMajor())
		anal.Alloc(source.ProjectAllocSize)
		source.FreeProjection(proc)
	}
}
