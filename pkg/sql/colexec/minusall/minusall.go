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

package minusall

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "minus_all"

func (minusAll *MinusAll) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": minus all ")
}

func (minusAll *MinusAll) OpType() vm.OpType { return vm.MinusAll }

func (minusAll *MinusAll) Prepare(proc *process.Process) error {
	var err error
	if minusAll.OpAnalyzer == nil {
		minusAll.OpAnalyzer = process.NewAnalyzer(minusAll.GetIdx(), minusAll.IsFirst, minusAll.IsLast, "minusAll")
	} else {
		minusAll.OpAnalyzer.Reset()
	}
	minusAll.ctr.hashTable, err = minusAll.NewHashMap(proc)
	if err != nil {
		return err
	}
	if err = minusAll.ctr.keyEvaluator.Prepare(proc, minusAll.KeyExprs, minusAll.ExpressionAllocation()); err != nil {
		minusAll.ctr.cleanHashMap()
		return err
	}
	minusAll.ctr.iterator = minusAll.ctr.hashTable.NewIterator()
	return nil
}

func (minusAll *MinusAll) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := minusAll.OpAnalyzer
	for {
		switch minusAll.ctr.state {
		case buildingHashMap:
			done, err := minusAll.build(proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}
			if !done {
				return vm.CallResult{Status: vm.ExecYield}, nil
			}
			analyzer.Alloc(minusAll.ctr.hashTable.Size())
			minusAll.ctr.state = probingHashMap
		case probingHashMap:
			result := vm.NewCallResult()
			last, err := minusAll.probe(proc, analyzer, &result)
			if err != nil {
				return result, err
			}
			if last {
				minusAll.ctr.state = operatorEnd
				continue
			}
			return result, nil
		case operatorEnd:
			return vm.CancelResult, nil
		}
	}
}

func (minusAll *MinusAll) build(proc *process.Process, analyzer process.Analyzer) (bool, error) {
	ctr := &minusAll.ctr
	child, err := vm.GetChild(minusAll, 1)
	if err != nil {
		return false, err
	}
	for {
		input, err := vm.ChildrenCall(child, proc, analyzer)
		if err != nil {
			return false, err
		}
		if input.Status == vm.ExecYield {
			return false, nil
		}
		if input.Batch == nil {
			return true, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
		if err != nil {
			return false, err
		}
		itr := ctr.iterator
		count := input.Batch.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			values, _, err := itr.Insert(i, n, keyVecs)
			if err != nil {
				return false, err
			}
			if err = ctr.ensureRemaining(int(ctr.hashTable.GroupCount()), proc.Mp(), &minusAll.SetOperationAllocation); err != nil {
				return false, err
			}
			for _, value := range values {
				if value == 0 {
					continue
				}
				idx := value - 1
				if ctr.remaining[idx] == math.MaxUint64 {
					return false, moerr.NewInternalErrorNoCtx("EXCEPT ALL multiplicity overflow")
				}
				ctr.remaining[idx]++
			}
		}
	}
}

func (minusAll *MinusAll) probe(proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) (bool, error) {
	ctr := &minusAll.ctr
	child, err := vm.GetChild(minusAll, 0)
	if err != nil {
		return false, err
	}
	for {
		input, err := vm.ChildrenCall(child, proc, analyzer)
		if err != nil {
			return false, err
		}
		if input.Status == vm.ExecYield {
			*result = input
			return false, nil
		}
		if input.Batch == nil {
			return true, nil
		}
		if input.Batch.Last() {
			result.Batch = input.Batch
			return false, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		if ctr.bat == nil {
			ctr.bat, err = minusAll.NewBatch(proc, input.Batch)
			if err != nil {
				return false, err
			}
		}
		ctr.bat.CleanOnlyData()
		keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
		if err != nil {
			return false, err
		}
		itr := ctr.iterator
		count := input.Batch.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			selection, zeroes := minusAll.Scratch()
			copy(selection[:n], zeroes[:n])
			values, _, err := itr.Find(i, n, keyVecs)
			if err != nil {
				return false, err
			}
			selected := 0
			for row, value := range values {
				if value != 0 && ctr.remaining[value-1] > 0 {
					ctr.remaining[value-1]--
					continue
				}
				selection[row] = 1
				selected++
			}
			ctr.bat.AddRowCount(selected)
			if selected > 0 {
				for col := range input.Batch.Vecs {
					if err := ctr.bat.Vecs[col].UnionBatch(input.Batch.Vecs[col], int64(i), selected, selection[:n], proc.Mp()); err != nil {
						return false, err
					}
				}
			}
		}
		if ctr.bat.IsEmpty() {
			continue
		}
		analyzer.Alloc(int64(ctr.bat.Size()))
		result.Batch = ctr.bat
		return false, nil
	}
}

func (ctr *container) ensureRemaining(length int, mp *mpool.MPool, allocation *colexec.SetOperationAllocation) error {
	var err error
	ctr.remaining, err = colexec.GrowSetOperationSlice(ctr.remaining, length, mp, allocation)
	return err
}
