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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"runtime"
)

func (group *Group) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
	return r.Batch, err
}

// GroupOperatorReturnIntermediateResult
// if there is a mergeGroup operator behinds this group operator,
// the group operator has no need to return the final result but only aggregation's middle result.
//
// for this situation,
// we do not engage in any blocking actions.
// once a batch is received, the corresponding aggregation's intermediate result will be returned.
func (group *Group) GroupOperatorReturnIntermediateResult(proc *process.Process) (*batch.Batch, error) {
	group.cleanLastOutput(proc)

	for {
		r, err := group.getInputBatch(proc)
		if err != nil {
			return nil, err
		}

		if r == nil {
			// if there is a query without group-by.
			// we cannot return `empty set` even its datasource was empty.
			if len(group.Exprs) == 0 {
				res, errRes := group.setInitialBatch(proc)
				if errRes != nil {
					return nil, errRes
				}
				res.SetRowCount(1)
				return res, nil
			}
			return nil, nil
		}

		if r.IsEmpty() {
			continue
		}

		// calculate the group-by columns, and probe the hashtable.
		if err = group.calculateAggColumnsAndGroupByColumns(proc, r); err != nil {
			return nil, err
		}

		// get a suitable hashmap.
		hah, itr, err := group.getNewHashTable()
		if err != nil {
			return nil, err
		}

		// get the result batch.
		res, errRes := group.setInitialBatch(proc)
		if errRes != nil {
			return nil, errRes
		}

		// probe.

		count := r.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			if i%(hashmap.UnitLimit*32) == 0 {
				runtime.Gosched()
			}

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vals, _, er := itr.Insert(i, n, group.ctr.groupVecs.Vec)
			if er != nil {
				return nil, er
			}

		}

	}

}

func (group *Group) cleanLastOutput(proc *process.Process) {
	if group.ctr.bat != nil {
		group.ctr.bat.Clean(proc.Mp())
		group.ctr.bat = nil
	}
}

type groupHashRelated struct {
	hah hashmap.HashMap
	itr hashmap.Iterator
}

func (group *Group) getNewHashTable() (hashmap.HashMap, hashmap.Iterator, error) {
	if group.ctr.hret.hah != nil {
		group.ctr.hret.hah.Free()
		group.ctr.hret.hah = nil
	}

	if group.ctr.typ == H8 {
		h, err := hashmap.NewIntHashMap(group.ctr.groupVecsNullable)
		if err != nil {
			return nil, nil, err
		}

		if group.ctr.hret.itr == nil {
			group.ctr.hret.itr = h.NewIterator()
		}
		group.ctr.hret.hah = h
		return h, group.ctr.hret.itr, nil
	}

	h, err := hashmap.NewStrMap(group.ctr.groupVecsNullable)
	if err != nil {
		return nil, nil, err
	}
	if group.ctr.hret.itr == nil {
		group.ctr.hret.itr = h.NewIterator()
	}
	group.ctr.hret.hah = h
	return h, group.ctr.hret.itr, nil
}

func (group *Group) getLastHashTable() (hashmap.HashMap, hashmap.Iterator, error) {
	if group.ctr.hret.hah != nil {
		return group.ctr.hret.hah, group.ctr.hret.itr, nil
	}

	if group.ctr.typ == H8 {
		h, err := hashmap.NewStrMap(group.ctr.groupVecsNullable)
		if err != nil {
			return nil, nil, err
		}
		group.ctr.hret.itr = h.NewIterator()
		group.ctr.hret.hah = h

		if group.PreAllocSize > 0 {
			if err = h.PreAlloc(group.PreAllocSize); err != nil {
				return nil, nil, err
			}
		}
		return h, group.ctr.hret.itr, nil
	}

	h, err := hashmap.NewStrMap(group.ctr.groupVecsNullable)
	if err != nil {
		return nil, nil, err
	}
	if group.ctr.hret.itr == nil {
		group.ctr.hret.itr = h.NewIterator()
	}

	if group.PreAllocSize > 0 {
		if err = h.PreAlloc(group.PreAllocSize); err != nil {
			return nil, nil, err
		}
	}
	return h, group.ctr.hret.itr, nil
}

func (group *Group) setInitialBatch(proc *process.Process) (*batch.Batch, error) {
	aggPart, errPart := group.generateAggStructure(proc, false)
	if errPart != nil {
		return nil, errPart
	}
	group.ctr.bat = batch.NewOffHeapEmpty()
	group.ctr.bat.Aggs = aggPart
	return group.ctr.bat, nil
}

// calculateAggColumnsAndGroupByColumns calculate the x and y of `select(x) group by y`.
func (group *Group) calculateAggColumnsAndGroupByColumns(
	proc *process.Process, bat *batch.Batch) (err error) {
	batList := []*batch.Batch{bat}

	for i := range group.ctr.aggVecs {
		for j := range group.ctr.aggVecs[i].Vec {
			group.ctr.aggVecs[i].Vec[j], err = group.ctr.aggVecs[i].Executor[j].Eval(proc, batList, nil)
			if err != nil {
				return err
			}
		}
	}

	for i := range group.ctr.groupVecs.Vec {
		if group.ctr.groupVecs.Vec[i], err = group.ctr.groupVecs.Executor[i].Eval(proc, batList, nil); err != nil {
			return err
		}
	}

	return nil
}

func (group *Group) generateAggStructure(proc *process.Process, isForFinalResult bool) ([]aggexec.AggFuncExec, error) {
	var err error
	execs := make([]aggexec.AggFuncExec, len(group.Aggs))
	defer func() {
		if err != nil {
			for _, exec := range execs {
				if exec != nil {
					exec.Free()
				}
			}
		}
	}()

	for i, ag := range group.Aggs {
		execs[i] = aggexec.MakeAgg(
			proc, ag.GetAggID(), ag.IsDistinct(), group.ctr.aggVecs[i].Typ...)

		if config := ag.GetExtraConfig(); config != nil {
			if err = execs[i].SetExtraInformation(config, 0); err != nil {
				return nil, err
			}
		}
	}

	if isForFinalResult {
		if requireSize := int(group.PreAllocSize); requireSize > 0 {
			for _, exec := range execs {
				if err = exec.PreAllocateGroups(requireSize); err != nil {
					return nil, err
				}
			}
		}
	}
	return execs, nil
}
