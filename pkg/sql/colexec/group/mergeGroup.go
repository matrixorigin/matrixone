// Copyright 2025 Matrix Origin
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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	mergeGroup.ctr.state = vm.Build
	mergeGroup.ctr.mp = mpool.MustNew("merge_group_mpool")

	if mergeGroup.OpAnalyzer != nil {
		mergeGroup.OpAnalyzer.Reset()
	}
	mergeGroup.OpAnalyzer = process.NewAnalyzer(mergeGroup.GetIdx(), mergeGroup.IsFirst, mergeGroup.IsLast, "merge_group")

	err := mergeGroup.PrepareProjection(proc)
	if err != nil {
		return err
	}

	mergeGroup.ctr.setSpillMem(mergeGroup.SpillMem, mergeGroup.Aggs)

	return nil
}

func (mergeGroup *MergeGroup) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	mergeGroup.OpAnalyzer.Start()
	defer mergeGroup.OpAnalyzer.Stop()

	switch mergeGroup.ctr.state {
	case vm.Build:
		// receive data and merge.
		for !mergeGroup.ctr.inputDone {
			r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			// all handled, going to eval mode.
			//
			// XXX: Note that this test, r.Batch == nil is treated as ExecStop.
			// if r.Status == vm.ExecStop || r.Batch == nil {
			if r.Batch == nil {
				mergeGroup.ctr.state = vm.Eval
				mergeGroup.ctr.inputDone = true
			}

			// empty batch, skip.
			if r.Batch == nil || r.Batch.IsEmpty() {
				continue
			}

			needSpill, err := mergeGroup.buildOneBatch(proc, r.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if needSpill {
				if err := mergeGroup.ctr.spillDataToDisk(proc, nil); err != nil {
					return vm.CancelResult, err
				}
			}
		}

		// has partial results, merge them.
		if mergeGroup.PartialResults != nil {
			for i, ag := range mergeGroup.ctr.aggList {
				if len(mergeGroup.PartialResults) > i && mergeGroup.PartialResults[i] != nil {
					if err := ag.SetExtraInformation(mergeGroup.PartialResults[i], 0); err != nil {
						return vm.CancelResult, err
					}
				}
			}
		}

		if mergeGroup.ctr.isSpilling() {
			if err := mergeGroup.ctr.spillDataToDisk(proc, nil); err != nil {
				return vm.CancelResult, err
			}
			if _, err := mergeGroup.ctr.loadSpilledData(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs); err != nil {
				return vm.CancelResult, err
			}
		}

		// output the final result.
		return mergeGroup.ctr.outputOneBatchFinal(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs)

	case vm.Eval:
		return mergeGroup.ctr.outputOneBatchFinal(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs)
	case vm.End:
		return vm.CancelResult, nil
	}
	return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "bug: unknown merge group state")
}

func (mergeGroup *MergeGroup) buildOneBatch(proc *process.Process, bat *batch.Batch) (bool, error) {
	var err error

	defer func() {
		if err != nil {
			mergeGroup.ctr.freeSpillAggList()
		}
	}()

	mergeGroup.ctr.spillAggList, err = mergeGroup.ctr.makeAggList(mergeGroup.Aggs)
	if err != nil {
		return false, err
	}

	// deserialize extra buf2.
	if len(bat.ExtraBuf) != 0 {
		reader := bytes.NewReader(bat.ExtraBuf)

		// XXX: Here, the mtyp is critical.  It will affect how later we unmarshal and merge.
		if mergeGroup.ctr.mtyp, err = types.ReadInt32(reader); err != nil {
			return false, err
		}

		if mergeGroup.ctr.mtyp == H0 {
			if len(mergeGroup.ctr.groupByBatches) == 0 {
				gb := mergeGroup.ctr.createNewGroupByBatch(bat.Vecs, 1)
				gb.SetRowCount(1)
				mergeGroup.ctr.groupByBatches = append(mergeGroup.ctr.groupByBatches, gb)
			}
		}

		var nAggs int32
		nAggs, err := types.ReadInt32(reader)
		if err != nil {
			return false, err
		}

		if len(mergeGroup.ctr.aggList) != len(mergeGroup.Aggs) {
			mergeGroup.ctr.aggList, err = mergeGroup.ctr.makeAggList(mergeGroup.Aggs)
			if err != nil {
				return false, err
			}
		}

		if int(nAggs) != len(mergeGroup.ctr.spillAggList) {
			return false, moerr.NewInternalError(proc.Ctx, "nAggs != len(mergeGroup.ctr.spillAggList)")
		}

		for i := int32(0); i < nAggs; i++ {
			ag := mergeGroup.ctr.spillAggList[i]
			if err := ag.UnmarshalFromReader(reader, mergeGroup.ctr.mp); err != nil {
				return false, err
			}
		}
	}

	// merge intermediate results with only Aggregation.
	if len(bat.Vecs) == 0 {
		// no group by columns, group grow 1 for each agg.
		for i := range mergeGroup.ctr.aggList {
			if err := mergeGroup.ctr.aggList[i].Merge(mergeGroup.ctr.spillAggList[i], 0, 0); err != nil {
				return false, err
			}
		}
	} else {
		if mergeGroup.ctr.hr.IsEmpty() {
			if err := mergeGroup.ctr.buildHashTable(proc.Ctx); err != nil {
				return false, err
			}
		}

		rowCount := bat.RowCount()
		for i := 0; i < rowCount; i += hashmap.UnitLimit {
			n := min(rowCount-i, hashmap.UnitLimit)

			mergeGroup.ctr.sanityCheck()
			originGroupCount := mergeGroup.ctr.hr.Hash.GroupCount()
			vals, _, err := mergeGroup.ctr.hr.Itr.Insert(i, n, bat.Vecs)
			if err != nil {
				return false, err
			}
			insertList, _ := mergeGroup.ctr.hr.GetBinaryInsertList(vals, originGroupCount)
			more, err := mergeGroup.ctr.appendGroupByBatch(bat.Vecs, i, insertList)
			if err != nil {
				return false, err
			}

			if len(mergeGroup.ctr.aggList) == 0 {
				continue
			}

			if more > 0 {
				for j := range mergeGroup.ctr.aggList {
					if err := mergeGroup.ctr.aggList[j].GroupGrow(more); err != nil {
						return false, err
					}
				}
			}
			for j, ag := range mergeGroup.ctr.aggList {
				if err := ag.BatchMerge(mergeGroup.ctr.spillAggList[j], i, vals[:len(insertList)]); err != nil {
					return false, err
				}
			}

			mergeGroup.ctr.sanityCheck()
		}
	}

	return mergeGroup.ctr.needSpill(mergeGroup.OpAnalyzer), nil
}
