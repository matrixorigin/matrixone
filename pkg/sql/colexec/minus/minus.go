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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "minus"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": minus ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error
	{
		arg.ctr = new(container)
		arg.ctr.InitReceiver(proc, false)
		arg.ctr.bat = nil
		arg.ctr.hashTable, err = hashmap.NewStrMap(true, proc.Mp())
		if err != nil {
			return err
		}
	}
	return nil
}

// Call is the execute method of minus operator
// it built a hash table for right relation first.
// use values from left relation to probe and update the hash table.
// and preserve values that do not exist in the hash table.
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	var err error
	// prepare the analysis work.
	analyze := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()
	result := vm.NewCallResult()

	for {
		switch arg.ctr.state {
		case buildingHashMap:
			// step 1: build the hash table by all right batches.
			if err = arg.ctr.buildHashTable(proc, analyze, 1, arg.GetIsFirst()); err != nil {
				return result, err
			}
			if arg.ctr.hashTable != nil {
				analyze.Alloc(arg.ctr.hashTable.Size())
			}
			arg.ctr.state = probingHashMap

		case probingHashMap:
			// step 2: use left batches to probe and update the hash table.
			//
			// only one batch is processed during each loop, and the batch will be sent to
			// next operator immediately after successful processing.
			last := false
			last, err = arg.ctr.probeHashTable(proc, analyze, 0, arg.GetIsFirst(), arg.GetIsLast(), &result)
			if err != nil {
				return result, err
			}
			if last {
				arg.ctr.state = operatorEnd
				continue
			}
			return result, nil

		case operatorEnd:
			// operator over.
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// buildHashTable use all batches from proc.Reg.MergeReceiver[index] to build the hash map.
func (ctr *container) buildHashTable(proc *process.Process, ana process.Analyze, index int, isFirst bool) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(index, ana)
		if err != nil {
			return err
		}

		// the last batch of pipeline.
		if bat == nil {
			break
		}

		// just an empty batch.
		if bat.IsEmpty() {
			proc.PutBatch(bat)
			continue
		}
		ana.Input(bat, isFirst)

		itr := ctr.hashTable.NewIterator()
		count := bat.Vecs[0].Length()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			_, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.Clean(proc.Mp())
				return err
			}
		}
		proc.PutBatch(bat)
	}
	return nil
}

// probeHashTable use a batch from proc.Reg.MergeReceivers[index] to probe and update the hash map.
// If a row of data never appears in the hash table, add it into hath table and send it to the next operator.
// if batch is the last one, return true, else return false.
func (ctr *container) probeHashTable(proc *process.Process, ana process.Analyze, index int, isFirst bool, isLast bool, result *vm.CallResult) (bool, error) {
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		bat, _, err := ctr.ReceiveFromSingleReg(index, ana)
		if err != nil {
			return false, err
		}

		// the last batch of block.
		if bat == nil {
			return true, nil
		}
		if bat.Last() {
			ctr.bat = bat
			result.Batch = ctr.bat
			return false, nil
		}
		// just an empty batch.
		if bat.IsEmpty() {
			proc.PutBatch(bat)
			continue
		}
		ana.Input(bat, isFirst)

		if ctr.bat != nil {
			proc.PutBatch(ctr.bat)
			ctr.bat = nil
		}
		ctr.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range bat.Vecs {
			ctr.bat.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
		}

		count := bat.Vecs[0].Length()
		itr := ctr.hashTable.NewIterator()
		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := ctr.hashTable.GroupCount()

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.Clean(proc.Mp())
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
			ctr.bat.AddRowCount(int(rows - oldHashGroup))

			newHashGroup := ctr.hashTable.GroupCount()
			insertCount := int(newHashGroup - oldHashGroup)
			if insertCount > 0 {
				for pos := range bat.Vecs {
					if err := ctr.bat.Vecs[pos].UnionBatch(bat.Vecs[pos], int64(i), insertCount, inserted[:n], proc.Mp()); err != nil {
						bat.Clean(proc.Mp())
						return false, err
					}
				}
			}
		}
		ana.Output(ctr.bat, isLast)
		result.Batch = ctr.bat
		proc.PutBatch(bat)
		return false, nil
	}
}
