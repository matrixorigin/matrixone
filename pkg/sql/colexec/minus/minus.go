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

package minus

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "minus"

func (minus *Minus) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": minus ")
}

func (minus *Minus) OpType() vm.OpType {
	return vm.Minus
}

func (minus *Minus) Prepare(proc *process.Process) error {
	var err error
	if minus.OpAnalyzer == nil {
		minus.OpAnalyzer = process.NewAnalyzer(minus.GetIdx(), minus.IsFirst, minus.IsLast, "minus")
	} else {
		minus.OpAnalyzer.Reset()
	}

	minus.ctr.hashTable, err = hashmap.NewStrMap(true, proc.Mp())
	if err != nil {
		return err
	}
	return nil
}

// Call is the execute method of minus operator
// it built a hash table for right relation first.
// use values from left relation to probe and update the hash table.
// and preserve values that do not exist in the hash table.
func (minus *Minus) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := minus.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	var err error
	for {
		switch minus.ctr.state {
		case buildingHashMap:
			// step 1: build the hash table by all right batches.
			if err = minus.buildHashTable(proc, analyzer, 1); err != nil {
				return vm.CancelResult, err
			}
			if minus.ctr.hashTable != nil {
				analyzer.Alloc(minus.ctr.hashTable.Size())
			}
			minus.ctr.state = probingHashMap

		case probingHashMap:
			// step 2: use left batches to probe and update the hash table.
			//
			// only one batch is processed during each loop, and the batch will be sent to
			// next operator immediately after successful processing.
			last := false
			result := vm.NewCallResult()
			last, err = minus.probeHashTable(proc, analyzer, 0, &result)
			if err != nil {
				return vm.CancelResult, err
			}
			if last {
				minus.ctr.state = operatorEnd
				continue
			}
			analyzer.Output(result.Batch)
			return result, nil

		case operatorEnd:
			// operator over.
			return vm.CancelResult, nil
		}
	}
}

// buildHashTable use all batches from proc.Reg.MergeReceiver[index] to build the hash map.
func (minus *Minus) buildHashTable(proc *process.Process, analyzer process.Analyzer, index int) error {
	ctr := &minus.ctr
	for {
		//input, err := minus.GetChildren(index).Call(proc)
		input, err := vm.ChildrenCall(minus.GetChildren(index), proc, analyzer)
		if err != nil {
			return err
		}

		// the last batch of pipeline.
		if input.Batch == nil {
			break
		}

		// just an empty batch.
		if input.Batch.IsEmpty() {
			continue
		}
		//ana.Input(input.Batch, isFirst)

		itr := ctr.hashTable.NewIterator()
		count := input.Batch.Vecs[0].Length()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			_, _, err := itr.Insert(i, n, input.Batch.Vecs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// probeHashTable use a batch from proc.Reg.MergeReceivers[index] to probe and update the hash map.
// If a row of data never appears in the hash table, add it into hath table and send it to the next operator.
// if batch is the last one, return true, else return false.
func (minus *Minus) probeHashTable(proc *process.Process, analyzer process.Analyzer, index int, result *vm.CallResult) (bool, error) {
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		//input, err := minus.GetChildren(index).Call(proc)
		input, err := vm.ChildrenCall(minus.GetChildren(index), proc, analyzer)
		if err != nil {
			return false, err
		}

		// the last batch of block.
		if input.Batch == nil {
			return true, nil
		}
		if input.Batch.Last() {
			result.Batch = input.Batch
			return false, nil
		}
		// just an empty batch.
		if input.Batch.IsEmpty() {
			continue
		}
		//ana.Input(input.Batch, isFirst)

		if minus.ctr.bat == nil {
			minus.ctr.bat = batch.NewWithSize(len(input.Batch.Vecs))
			for i := range input.Batch.Vecs {
				minus.ctr.bat.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
			}
		}
		minus.ctr.bat.CleanOnlyData()

		count := input.Batch.Vecs[0].Length()
		itr := minus.ctr.hashTable.NewIterator()
		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := minus.ctr.hashTable.GroupCount()

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, _, err := itr.Insert(i, n, input.Batch.Vecs)
			if err != nil {
				return false, err
			}
			copy(inserted[:n], restoreInserted[:n])
			rows := oldHashGroup
			for j, v := range vs {
				if v > rows {
					// ensure that the same value will only be inserted once.
					rows++
					inserted[j] = 1
				}
			}
			minus.ctr.bat.AddRowCount(int(rows - oldHashGroup))

			newHashGroup := minus.ctr.hashTable.GroupCount()
			insertCount := int(newHashGroup - oldHashGroup)
			if insertCount > 0 {
				for pos := range input.Batch.Vecs {
					if err := minus.ctr.bat.Vecs[pos].UnionBatch(input.Batch.Vecs[pos], int64(i), insertCount, inserted[:n], proc.Mp()); err != nil {
						return false, err
					}
				}
			}
		}
		//ana.Alloc(int64(minus.ctr.bat.Size()))
		//ana.Output(minus.ctr.bat, isLast)
		analyzer.Alloc(int64(minus.ctr.bat.Size()))
		result.Batch = minus.ctr.bat
		return false, nil
	}
}
