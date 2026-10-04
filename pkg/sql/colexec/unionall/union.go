// Copyright 2024 Matrix Origin
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

package unionall

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "unionall"

func (unionall *UnionAll) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": union all ")
}

func (unionall *UnionAll) OpType() vm.OpType {
	return vm.UnionAll
}

func (unionall *UnionAll) Prepare(proc *process.Process) error {
	if unionall.OpAnalyzer == nil {
		unionall.OpAnalyzer = process.NewAnalyzer(unionall.GetIdx(), unionall.IsFirst, unionall.IsLast, "unionall")
	} else {
		unionall.OpAnalyzer.Reset()
	}
	unionall.currentBranch = 0

	return nil
}

func (unionall *UnionAll) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := unionall.OpAnalyzer
	for {
		result, err := vm.ChildrenCall(unionall.GetChildren(0), proc, analyzer)
		if err != nil || result.Status != vm.ExecStop ||
			unionall.SequentialBranches <= 1 ||
			unionall.currentBranch+1 >= unionall.SequentialBranches {
			return result, err
		}

		next := unionall.currentBranch + 1
		if cause := context.Cause(proc.Ctx); cause != nil {
			return vm.CancelResult, cause
		}
		if unionall.startBranch == nil {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx(
				"sequential union all branch starter is not installed")
		}
		mergeOp, ok := unionall.GetChildren(0).(*merge.Merge)
		if !ok {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx(
				"sequential union all requires a merge child")
		}
		// Listen to the next receiver before starting its producer. If producer
		// submission fails, pipeline cleanup can still drain its terminal error.
		if err = mergeOp.ActivateReceiverRange(proc, int32(next), int32(next+1)); err != nil {
			return vm.CancelResult, err
		}
		unionall.currentBranch = next
		if err = unionall.startBranch(next); err != nil {
			return vm.CancelResult, err
		}
	}
}
