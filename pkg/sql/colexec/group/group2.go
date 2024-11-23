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

// noneBlockCall
// if there is a mergeGroup operator behinds this group operator,
// the group operator has no need to return the final result but only aggregation's intermediate result.
// the behind mergeGroup operator will merge all the intermediate and flush its final result.
//
// for this situation,
// we do not engage in any blocking actions at this operator,
// once a batch is received, the corresponding aggregation's intermediate result will be returned.
func (group *Group) noneBlockCall(proc *process.Process) (*batch.Batch, error) {
	group.cleanLastOutput(proc)
	if group.ctr.state == vm.End {
		return nil, nil
	}

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

				group.ctr.state = vm.End
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
		for i, count := 0, r.RowCount(); i < count; i += hashmap.UnitLimit {
			if i%(hashmap.UnitLimit*32) == 0 {
				runtime.Gosched()
			}

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroup := hah.GroupCount()
			vals, _, er := itr.Insert(i, n, group.ctr.groupVecs.Vec)
			if er != nil {
				return nil, er
			}

			if err = group.updateBatch(proc, res, vals[:n], originGroup, i); err != nil {
				return nil, err
			}
		}

		return res, nil
	}
}

// blockCall
// if this group operator should return the final result, we should block here until all data were received.
// and flush the result for return.
func (group *Group) blockCall(proc *process.Process) (*batch.Batch, error) {
	group.cleanLastOutput(proc)
	if group.ctr.state == vm.End {
		return nil, nil
	}

	for {
		if group.ctr.state == vm.Eval {
			if group.ctr.blocking.IsEmpty() {
				group.ctr.state = vm.End
				return nil, nil
			}
			return group.ctr.blocking.PopResult(proc.Mp())
		}

		b, err := group.getInputBatch(proc)
		if err != nil {
			return nil, err
		}
		if b == nil {
			group.ctr.state = vm.Eval
			continue
		}

		if len(group.Exprs) == 0 {
			err = group.consumeInputBatchOnlyAgg(b)
		} else {
			err = group.consumeInputBatchOnlyAgg(b)
		}
		if err != nil {
			return nil, err
		}
	}
}

func (group *Group) consumeInputBatchOnlyAgg(
	bat *batch.Batch) error {
	return nil
}

func (group *Group) consumeInputBatch(
	proc *process.Process, bat *batch.Batch) error {
	return nil
}

func (group *Group) cleanLastOutput(proc *process.Process) {
	if group.ctr.bat != nil {
		group.ctr.bat.Clean(proc.Mp())
		group.ctr.bat = nil
	}
}

func (group *Group) getNewHashTable() (hashmap.HashMap, hashmap.Iterator, error) {
	if err := group.ctr.hashr.BuildHashTable(
		true, group.ctr.typ == HStr, group.ctr.groupVecsNullable, 0); err != nil {
		return nil, nil, err
	}
	return group.ctr.hashr.Hash, group.ctr.hashr.Itr, nil
}

func (group *Group) getLastHashTable() (hashmap.HashMap, hashmap.Iterator, error) {
	if err := group.ctr.hashr.BuildHashTable(
		false, group.ctr.typ == HStr, group.ctr.groupVecsNullable, group.PreAllocSize); err != nil {
		return nil, nil, err
	}
	return group.ctr.hashr.Hash, group.ctr.hashr.Itr, nil
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

// updateBatch append new group to dst and update all the group.
func (group *Group) updateBatch(
	proc *process.Process,
	dst *batch.Batch,
	groupList []uint64, originGroup uint64, offset int) error {

	insertList, howMuchNew := group.ctr.hashr.GetNewList(groupList, originGroup)

	if howMuchNew != 0 {
		for j, vec := range dst.Vecs {
			if err := vec.UnionBatch(group.ctr.groupVecs.Vec[j], int64(offset), len(insertList), insertList, proc.Mp()); err != nil {
				return err
			}
		}

		for _, ag := range dst.Aggs {
			if err := ag.GroupGrow(int(howMuchNew)); err != nil {
				return err
			}
		}
	}

	for j, ag := range dst.Aggs {
		if err := ag.BatchFill(offset, groupList, group.ctr.aggVecs[j].Vec); err != nil {
			return err
		}
	}
	return nil
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

// generateAggStructure init and return the specified type aggregators.
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
