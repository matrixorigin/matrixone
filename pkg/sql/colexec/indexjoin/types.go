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

package indexjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(IndexJoin)

const (
	Probe = iota
	End
)

type container struct {
	state int
	buf   *batch.Batch
}

type IndexJoin struct {
	ctr                *container
	Result             []int32
	Typs               []types.Type
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	vm.OperatorBase
}

func (indexJoin *IndexJoin) GetOperatorBase() *vm.OperatorBase {
	return &indexJoin.OperatorBase
}

func init() {
	reuse.CreatePool[IndexJoin](
		func() *IndexJoin {
			return &IndexJoin{}
		},
		func(a *IndexJoin) {
			*a = IndexJoin{}
		},
		reuse.DefaultOptions[IndexJoin]().
			WithEnableChecker(),
	)
}

func (indexJoin IndexJoin) TypeName() string {
	return opName
}

func NewArgument() *IndexJoin {
	return reuse.Alloc[IndexJoin](nil)
}

func (indexJoin *IndexJoin) Release() {
	if indexJoin != nil {
		reuse.Free[IndexJoin](indexJoin, nil)
	}
}

func (indexJoin *IndexJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	indexJoin.Free(proc, pipelineFailed, err)
}

func (indexJoin *IndexJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := indexJoin.ctr
	if ctr != nil {
		if indexJoin.ctr.buf != nil {
			indexJoin.ctr.buf.Clean(proc.Mp())
			indexJoin.ctr.buf = nil
		}
		indexJoin.ctr = nil
	}

}
