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

package mergegroup

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

var makeInitialAggListFromList = aggexec.MakeInitialAggListFromList

func (mergeGroup *MergeGroup) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperatorName)
}

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	mergeGroup.ctr.state = vm.Build
	mergeGroup.prepareAnalyzer()
	return mergeGroup.prepareProjection(proc)
}

func (mergeGroup *MergeGroup) prepareAnalyzer() {
	if mergeGroup.OpAnalyzer != nil {
		mergeGroup.OpAnalyzer.Reset()
		return
	}
	mergeGroup.OpAnalyzer = process.NewAnalyzer(mergeGroup.GetIdx(), mergeGroup.IsFirst, mergeGroup.IsLast, "merge_group")
}

func (mergeGroup *MergeGroup) prepareProjection(proc *process.Process) error {
	if mergeGroup.ProjectList != nil {
		err := mergeGroup.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mergeGroup *MergeGroup) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}
	mergeGroup.OpAnalyzer.Start()
	defer mergeGroup.OpAnalyzer.Stop()

	for {
		switch mergeGroup.ctr.state {
		case vm.Build:
			// receive data and merge.
			for {
				b, err := mergeGroup.getInputBatch(proc)
				if err != nil {
					return vm.CancelResult, err
				}
				if b == nil {
					break
				}
				if b.IsEmpty() {
					continue
				}

				if err = mergeGroup.consumeBatch(proc, b); err != nil {
					return vm.CancelResult, err
				}
			}
			if err := mergeGroup.ctr.result.DealPartialResult(mergeGroup.PartialResults); err != nil {
				return vm.CancelResult, err
			}
			mergeGroup.ctr.state = vm.Eval

		case vm.Eval:
			// output result.
			mergeGroup.ctr.result.CleanLastPopped(proc.Mp())

			if mergeGroup.ctr.result.IsEmpty() {
				mergeGroup.ctr.state = vm.End
				continue
			}

			b, err := mergeGroup.ctr.result.PopResult(proc.Mp())
			if err != nil {
				return vm.CancelResult, err
			}
			result := vm.NewCallResult()
			result.Batch = b
			mergeGroup.OpAnalyzer.Output(b)
			return result, nil

		default:
			// END status.
			result := vm.NewCallResult()
			result.Batch, result.Status = nil, vm.ExecStop
			return result, nil
		}
	}
}

func (mergeGroup *MergeGroup) getInputBatch(proc *process.Process) (*batch.Batch, error) {
	r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
	return r.Batch, err
}

func (mergeGroup *MergeGroup) consumeBatch(proc *process.Process, b *batch.Batch) error {
	// merge intermedia results with only Aggregation.
	if len(b.Vecs) == 0 {

		if mergeGroup.ctr.result.IsEmpty() {
			mergeGroup.ctr.result.InitOnlyAgg(math.MaxInt32, makeInitialAggListFromList(proc, b.Aggs))
			mergeGroup.ctr.result.ToPopped[0].SetRowCount(1)
			for i := range mergeGroup.ctr.result.AggList {
				if err := mergeGroup.ctr.result.AggList[i].GroupGrow(1); err != nil {
					return err
				}
			}
		}

		for i, input := range b.Aggs {
			if err := mergeGroup.ctr.result.AggList[i].Merge(input, 0, 0); err != nil {
				return err
			}
		}
		return nil
	}

	// merge intermedia results with group-by columns.
	if mergeGroup.ctr.hr.IsEmpty() {
		// calculate key width and build the hash map.
		keyWidth, keyNullable := 0, false
		for _, vec := range b.Vecs {
			keyNullable = keyNullable || (!vec.GetType().GetNotNull())
		}
		for _, vec := range b.Vecs {
			typ := vec.GetType()
			keyWidth += group.GetKeyWidth(typ.Oid, typ.Width, keyNullable)
		}

		if err := mergeGroup.ctr.hr.BuildHashTable(false, keyWidth > 8, keyNullable, 0); err != nil {
			return err
		}
	}

	if mergeGroup.ctr.result.IsEmpty() {
		mergeGroup.ctr.result.InitWithBatch(aggexec.GetMinAggregatorsChunkSize(b.Vecs, b.Aggs), makeInitialAggListFromList(proc, b.Aggs), b)
	}

	for i, count := 0, b.RowCount(); i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		origin := mergeGroup.ctr.hr.Hash.GroupCount()
		vals, _, err := mergeGroup.ctr.hr.Itr.Insert(i, n, b.Vecs)
		if err != nil {
			return err
		}
		insertList, _ := mergeGroup.ctr.hr.GetBinaryInsertList(vals, origin)

		more, err := mergeGroup.ctr.result.AppendBatch(proc.Mp(), b.Vecs, i, insertList)
		if err != nil {
			return err
		}

		if len(b.Aggs) == 0 {
			continue
		}
		if err = mergeGroup.updateAggPart(b, i, len(insertList), more, vals); err != nil {
			return err
		}
	}

	return nil
}

func (mergeGroup *MergeGroup) updateAggPart(
	b *batch.Batch,
	offset int, length int, newGroupCount int,
	groupList []uint64) error {
	var err error

	for i := range mergeGroup.ctr.result.AggList {
		if err = mergeGroup.ctr.result.AggList[i].GroupGrow(newGroupCount); err != nil {
			return err
		}
	}

	for i := range mergeGroup.ctr.result.AggList {
		if err = mergeGroup.ctr.result.AggList[i].BatchMerge(b.Aggs[i], offset, groupList[:length]); err != nil {
			return err
		}
	}
	return nil
}
