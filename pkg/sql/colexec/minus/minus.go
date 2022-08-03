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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" minus ")
}

func Prepare(proc *process.Process, argument any) error {
	var err error
	arg := argument.(*Argument)
	{
		arg.ctr.bat = nil
		arg.ctr.hashTable, err = hashmap.NewStrMap(true, arg.IBucket, arg.NBucket, proc.Mp)
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
func Call(idx int, proc *process.Process, argument any) (bool, error) {
	var err error
	arg := argument.(*Argument)

	// prepare the analysis work.
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()

	for {
		switch arg.ctr.state {
		case buildingHashMap:
			// step 1: build the hash table by all right batches.
			if err = arg.ctr.buildHashTable(proc, analyze, 1); err != nil {
				arg.ctr.hashTable.Free()
				arg.ctr.state = operatorEnd
				return true, err
			}
			arg.ctr.state = probingHashMap
		case probingHashMap:
			// step 2: use left batches to probe and update the hash table.
			//
			// only one batch is processed during each loop, and the batch will be sent to
			// next operator immediately after successful processing.
			last := false
			last, err = arg.ctr.probeHashTable(proc, analyze, 0)
			if err != nil {
				arg.ctr.bat.Clean(proc.Mp)
				arg.ctr.hashTable.Free()
				arg.ctr.state = operatorEnd
				return true, err
			}
			if last {
				arg.ctr.state = operatorEnd
				continue
			}
			return false, nil
		case operatorEnd:
			// operator over.
			arg.ctr.hashTable.Free()
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// buildHashTable use all batches from proc.Reg.MergeReceiver[index] to build the hash map.
func (ctr *container) buildHashTable(proc *process.Process, ana process.Analyze, index int) error {
	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		// the last batch of block.
		if bat == nil {
			break
		}
		// just an empty batch.
		if len(bat.Zs) == 0 {
			continue
		}
		ana.Input(bat)

		itr := ctr.hashTable.NewIterator()
		count := vector.Length(bat.Vecs[0])
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.Clean(proc.Mp)
				return err
			}
			for _, v := range vs {
				if v > ctr.hashTable.GroupCount() {
					ctr.hashTable.AddGroup()
				}
			}
		}
		bat.Clean(proc.Mp)
	}
	return nil
}

// probeHashTable use a batch from proc.Reg.MergeReceivers[index] to probe and update the hash map.
// If a row of data never appears in the hash table, add it into hath table and send it to the next operator.
// if batch is the last one, return true, else return false.
func (ctr *container) probeHashTable(proc *process.Process, ana process.Analyze, index int) (bool, error) {
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		// the last batch of block.
		if bat == nil {
			return true, nil
		}
		// just an empty batch.
		if len(bat.Zs) == 0 {
			continue
		}
		ana.Input(bat)

		ctr.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(bat.Vecs[i].Typ)
		}

		count := vector.Length(bat.Vecs[0])
		itr := ctr.hashTable.NewIterator()
		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := ctr.hashTable.GroupCount()

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, _, err := itr.Insert(i, n, bat.Vecs)
			if err != nil {
				bat.Clean(proc.Mp)
				return false, err
			}
			copy(inserted[:n], restoreInserted[:n])
			for j, v := range vs {
				if v > ctr.hashTable.GroupCount() {
					ctr.hashTable.AddGroup()
					inserted[j] = 1
					ctr.bat.Zs = append(ctr.bat.Zs, 1)
				}
			}

			newHashGroup := ctr.hashTable.GroupCount()
			insertCount := int(newHashGroup - oldHashGroup)
			if insertCount > 0 {
				for pos := range bat.Vecs {
					if err := vector.UnionBatch(ctr.bat.Vecs[pos], bat.Vecs[pos], int64(i), insertCount, inserted[:n], proc.Mp); err != nil {
						bat.Clean(proc.Mp)
						return false, err
					}
				}
			}
		}
		ana.Output(ctr.bat)
		proc.SetInputBatch(ctr.bat)
		bat.Clean(proc.Mp)
		return false, nil
	}
}
