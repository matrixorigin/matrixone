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
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	// attrs, column list.
	attrs []string
	// orders to be executed
	instructions vm.Instructions
	reg          *process.WaitRegister
}

// cleanup do memory release work for whole pipeline.
func (p *Pipeline) cleanup(proc *process.Process, pipelineFailed bool) {
	// clean all the coming batches.
	if pipelineFailed {
		bat := proc.InputBatch()
		if bat != nil {
			bat.Clean(proc.Mp())
		}
		proc.SetInputBatch(nil)
	}
	// clean operator hold memory.
	for i := range p.instructions {
		p.instructions[i].Arg.Free(proc, pipelineFailed)
	}

	// select all merge receivers
	listeners, alive := newSelectListener(proc.Reg.MergeReceivers)
	for alive != 0 {
		chosen, value, ok := reflect.Select(listeners)
		if !ok {
			break
		}
		pointer := value.UnsafePointer()
		bat := (*batch.Batch)(pointer)
		if bat == nil {
			alive--
			listeners = append(listeners[:chosen], listeners[chosen+1:]...)
			continue
		}
		bat.Clean(proc.Mp())
	}
}

func newSelectListener(wrs []*process.WaitRegister) ([]reflect.SelectCase, int) {
	listener := make([]reflect.SelectCase, len(wrs))
	for i, mr := range wrs {
		listener[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(mr.Ch),
		}
	}
	return listener, len(wrs)
}
