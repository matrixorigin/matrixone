// Copyright 2021-2023 Matrix Origin
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

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

type Argument struct {
	Reader engine.Reader
	Attrs  []string

	info     *vm.OperatorInfo
	children []vm.Operator

	buf    *batch.Batch
	tmpBuf *batch.Batch

	maxAllocSize int
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
		arg.buf = nil
	}

	if arg.tmpBuf != nil {
		arg.tmpBuf.Clean(proc.Mp())
		arg.tmpBuf = nil
	}
	if arg.info != nil {
		anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
		anal.Alloc(int64(arg.maxAllocSize))
	}
}
