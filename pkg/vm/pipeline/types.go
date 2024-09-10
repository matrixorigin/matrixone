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

package pipeline

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
)

type Pipeline struct {
	tableID uint64
	// attrs, column list.
	attrs []string
	// orders to be executed
	// instructions vm.Instructions
	rootOp vm.Operator
}

func (p *Pipeline) isCtePipelineAtLoop() (isMergeCte bool, atLoop bool) {
	// required:
	// 1. it is a linked tree.
	// 2. it holds `merge-cte` or `merge-recursive`.
	next := p.rootOp
	if next.OpType() != vm.Dispatch {
		return false, false
	}

	for next != nil {
		opt := next.OpType()
		if opt == vm.MergeCTE {
			return true, true
		}
		if opt == vm.MergeRecursive {
			return false, true
		}

		cds := next.GetOperatorBase().Children
		if len(cds) != 1 {
			break
		}
		next = cds[0]
	}
	return false, false
}

// CleanRootOperator only do free or reset work for the last operator.
// this is just used for RemoteRun because we kept the root operator of remote-pipeline at local.
func (p *Pipeline) CleanRootOperator(proc *process.Process, pipelineFailed bool, isPrepare bool, err error) {
	if p.rootOp == nil {
		return
	}
	p.rootOp.Reset(proc, pipelineFailed, err)
	if !isPrepare {
		p.rootOp.Free(proc, pipelineFailed, err)
	}
}

// Cleanup do memory release work for whole pipeline.
// we deliver the error because some operator may need to know what the error it is.
func (p *Pipeline) Cleanup(proc *process.Process, pipelineFailed bool, isPrepare bool, err error) {
	// cancel the context to stop its pre-pipelines.
	proc.Cancel()

	// do special cleanup for the pipeline at a loop.
	if isMergeCte, isSpecial := p.isCtePipelineAtLoop(); isSpecial {
		if proc.Base.GetContextBase().DoSpecialCleanUp(isMergeCte) {
			p.cleanupLoopPipeline(proc, pipelineFailed, isPrepare, err)
			return
		}
	}

	p.cleanupInOrder(proc, pipelineFailed, isPrepare, err)
}

// cleanupInOrder call reset and free methods of operator from first index to the last.
func (p *Pipeline) cleanupInOrder(proc *process.Process, pipelineFailed bool, isPrepare bool, err error) {
	if root := p.rootOp; root != nil {
		_ = vm.HandleAllOp(p.rootOp, func(aprentOp vm.Operator, op vm.Operator) error {
			op.Reset(proc, pipelineFailed, err)
			return nil
		})

		if !isPrepare {
			_ = vm.HandleAllOp(p.rootOp, func(aprentOp vm.Operator, op vm.Operator) error {
				op.Free(proc, pipelineFailed, err)
				return nil
			})
		}
	}
}

// cleanupLoopPipeline cleanup pipeline in a special order to avoid pipeline-loop deadlock.
//
// pipeline-loop, for example,
// pipelineA send data to pipelineB and pipelineC, pipelineB send data to pipelineA.
// pipelineA and pipelineB is a pipeline-loop.
func (p *Pipeline) cleanupLoopPipeline(proc *process.Process, pipelineFailed bool, isPrepare bool, err error) {

	// get Merge and Dispatch operators from pipeline.
	var mergeOperator *merge.Merge
	var dispatchOperator *dispatch.Dispatch
	{
		dispatchOperator = p.rootOp.(*dispatch.Dispatch)

		root := p.rootOp
		for len(root.GetOperatorBase().Children) > 0 {
			root = root.GetOperatorBase().Children[0]
		}
		mergeOperator = root.(*merge.Merge)
	}

	// listen to Dispatch and Merge at the same time.
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		mergeOperator.Reset(proc, pipelineFailed, err)
		if !isPrepare {
			mergeOperator.Free(proc, pipelineFailed, err)
		}
		wg.Done()
	}()

	dispatchOperator.Reset(proc, pipelineFailed, err)
	if !isPrepare {
		dispatchOperator.Free(proc, pipelineFailed, err)
	}
	wg.Wait()

	// from first to last to clean up the left operators.

	for _, child := range p.rootOp.GetOperatorBase().Children {
		_ = vm.HandleAllOp(child, func(_ vm.Operator, op vm.Operator) error {
			op.Reset(proc, pipelineFailed, err)
			return nil
		})
		if !isPrepare {
			_ = vm.HandleAllOp(child, func(_ vm.Operator, op vm.Operator) error {
				op.Free(proc, pipelineFailed, err)
				return nil
			})
		}
	}
}
