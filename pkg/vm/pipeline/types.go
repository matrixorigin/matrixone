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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Pipeline contains the information associated with a pipeline in a query execution plan.
// A query execution plan may contains one or more pipelines.
// As an example:
//
//	 CREATE TABLE order
//	 (
//	       order_id    INT,
//	       uid          INT,
//	       item_id      INT,
//	       year         INT,
//	       nation       VARCHAR(100)
//	 );
//
//	 CREATE TABLE customer
//	 (
//	       uid          INT,
//	       nation       VARCHAR(100),
//	       city         VARCHAR(100)
//	 );
//
//	 CREATE TABLE supplier
//	 (
//	       item_id      INT,
//	       nation       VARCHAR(100),
//	       city         VARCHAR(100)
//	 );
//
//		SELECT c.city, s.city, sum(o.revenue) AS revenue
//	 FROM customer c, order o, supplier s
//	 WHERE o.uid = c.uid
//	 AND o.item_id = s.item_id
//	 AND c.nation = 'CHINA'
//	 AND s.nation = 'CHINA'
//	 AND o.year >= 1992 and o.year <= 1997
//	 GROUP BY c.city, s.city, o.year
//	 ORDER BY o.year asc, revenue desc;
//
//	 AST PLAN:
//	    order
//	      |
//	    group
//	      |
//	    filter
//	      |
//	    join
//	    /  \
//	   s   join
//	       /  \
//	      l   c
//
// In this example, a possible pipeline is as follows:
//
// pipeline:
// o ⨝ c ⨝ s
//
//	-> σ(c.nation = 'CHINA' ∧  o.year >= 1992 ∧  o.year <= 1997 ∧  s.nation = 'CHINA')
//	-> γ([c.city, s.city, o.year, sum(o.revenue) as revenue], c.city, s.city, o.year)
//	-> τ(o.year asc, revenue desc)
//	-> π(c.city, s.city, revenue)
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

	// If the last operator of this pipeline is a RecSink Operator.
	// It means this was a pipeline at the `pipeline cycle` (like: a send data to b, and b send data to a)
	// for deal the deadlock of clean-up,
	// we need to do a special clean logic for this one.
	// clean from first operator (data-source operator) to last operator (sender operator).
	if isMergeCte, isSpecial := p.isCtePipelineAtLoop(); isSpecial {
		if proc.Base.GetContextBase().DoSpecialCleanUp(isMergeCte) {

			p.rootOp.Reset(proc, pipelineFailed, err)
			if !isPrepare {
				p.rootOp.Free(proc, pipelineFailed, err)
			}

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

			return
		}
	}

	// for every pipeline, we clean the last operator first.
	// and then clean from 0 to n-1.
	if root := p.rootOp; root != nil {

		// clean operator hold memory.
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

		//p.rootOp.Reset(proc, pipelineFailed, err)
		//if !isPrepare {
		//	p.rootOp.Free(proc, pipelineFailed, err)
		//}
		//
		//for _, child := range p.rootOp.GetOperatorBase().Children {
		//	_ = vm.HandleAllOp(child, func(_ vm.Operator, op vm.Operator) error {
		//		op.Reset(proc, pipelineFailed, err)
		//		return nil
		//	})
		//	if !isPrepare {
		//		_ = vm.HandleAllOp(child, func(_ vm.Operator, op vm.Operator) error {
		//			op.Free(proc, pipelineFailed, err)
		//			return nil
		//		})
		//	}
		//}
	}
}
