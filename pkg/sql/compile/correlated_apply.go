// Copyright 2026 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type correlatedApplyRunner struct {
	compile *Compile
	scopes  []*Scope

	outputBatches []*batch.Batch
	outputIdx     int
}

var _ apply.SubqueryRunner = (*correlatedApplyRunner)(nil)

func newCorrelatedApplyRunner(parent *Compile, step int32, rightNodeID int32) (*correlatedApplyRunner, error) {
	copiedQuery := plan2.DeepCopyQuery(parent.anal.qry)
	forceSubqueryOnCurrentCN(rightNodeID, copiedQuery.Nodes)

	childProc := parent.proc.NewNoContextChildProc(0)
	childCompile := allocateNewCompile(childProc)
	*childCompile = *parent
	childCompile.proc = childProc
	childCompile.scopes = nil
	childCompile.isPrepare = true

	childCompile.anal = newAnalyzeModule()
	childCompile.anal.qry = copiedQuery
	childCompile.anal.curNodeIdx = int(rightNodeID)
	childCompile.anal.isFirst = true

	runner := &correlatedApplyRunner{compile: childCompile}
	childCompile.fill = runner.captureBatch

	scopes, err := childCompile.compilePlanScope(step, rightNodeID, copiedQuery.Nodes)
	if err != nil {
		childCompile.anal.release()
		reuse.Free[Compile](childCompile, nil)
		return nil, err
	}
	for _, scope := range scopes {
		op := output.NewArgument().WithFunc(runner.captureBatch)
		op.SetAnalyzeControl(childCompile.anal.curNodeIdx, false)
		scope.setRootOperator(op)
	}

	runner.scopes = scopes
	childCompile.scopes = scopes
	return runner, nil
}

func forceSubqueryOnCurrentCN(nodeID int32, nodes []*plan.Node) {
	visited := make(map[int32]struct{})
	var walk func(int32)
	walk = func(id int32) {
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}

		node := nodes[id]
		if node.Stats != nil {
			node.Stats.ForceOneCN = true
			node.Stats.Dop = 1
		}
		for _, child := range node.Children {
			walk(child)
		}
	}
	walk(nodeID)
}

func (r *correlatedApplyRunner) Prepare(proc *process.Process) error {
	return nil
}

func (r *correlatedApplyRunner) Start(input *batch.Batch, row int, proc *process.Process, analyzer process.Analyzer) error {
	r.cleanOutput(proc)

	parentCtx := colexec.WithCorrelatedBatches(proc.Ctx, []*batch.Batch{input}, row)
	for _, scope := range r.scopes {
		if err := scope.Reset(r.compile); err != nil {
			return err
		}
		scope.buildContextFromParentCtx(parentCtx)

		var err error
		switch scope.Magic {
		case Normal:
			err = scope.Run(r.compile)
		case Merge, MergeInsert:
			err = scope.MergeRun(r.compile)
		case Remote:
			err = scope.RemoteRun(r.compile)
		default:
			err = moerr.NewNYI(proc.Ctx, "unsupported scope magic for correlated apply")
		}
		if err != nil {
			return err
		}
	}

	r.outputIdx = 0
	return nil
}

func (r *correlatedApplyRunner) Call(proc *process.Process) (vm.CallResult, error) {
	if r.outputIdx >= len(r.outputBatches) {
		return vm.CallResult{Batch: batch.EmptyBatch}, nil
	}
	result := vm.NewCallResult()
	result.Batch = r.outputBatches[r.outputIdx]
	r.outputIdx++
	return result, nil
}

func (r *correlatedApplyRunner) End(proc *process.Process) error {
	return nil
}

func (r *correlatedApplyRunner) Reset(proc *process.Process, pipelineFailed bool, err error) {
	r.cleanOutput(proc)
}

func (r *correlatedApplyRunner) Free(proc *process.Process, pipelineFailed bool, err error) {
	r.cleanOutput(proc)
	if r.compile != nil {
		if r.compile.anal != nil {
			r.compile.anal.release()
			r.compile.anal = nil
		}
		reuse.Free[Compile](r.compile, nil)
		r.compile = nil
	}
	r.scopes = nil
}

func (r *correlatedApplyRunner) captureBatch(bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	dup, err := bat.Dup(r.compile.proc.Mp())
	if err != nil {
		return err
	}
	r.outputBatches = append(r.outputBatches, dup)
	return nil
}

func (r *correlatedApplyRunner) cleanOutput(proc *process.Process) {
	for _, bat := range r.outputBatches {
		bat.Clean(proc.Mp())
	}
	r.outputBatches = r.outputBatches[:0]
	r.outputIdx = 0
}
