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

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" minus ")
}

func Prepare(_ *process.Process, argument interface{}) error {
	arg := argument.(*Argument)
	{
		arg.ctr.bat = nil
		arg.ctr.hashTable = hashmap.NewStrMap(true)
	}
	return nil
}

// Call is the execute method of minus operator
// it built a hash table for right relation
// use values from left relation to probe the hash table
// and preserve values that do not exist in the hash table.
func Call(idx int, proc *process.Process, argument interface{}) (bool, error) {
	var err error
	arg := argument.(*Argument)

	// prepare the analysis work.
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()

	for {
		switch arg.ctr.state {
		case buildingHashMap:
			// build hash table by right batches.
			if err = arg.ctr.buildHashTable(arg, proc, analyze, 1); err != nil {
				arg.ctr.state = operatorEnd
				return true, err
			}
			arg.ctr.state = probingHashMap
		case probingHashMap:
			// probe hash table using left batches.
			last := false
			last, err = arg.ctr.probeHashTable(arg, proc, analyze, 0)
			if err != nil {
				arg.ctr.state = operatorEnd
				return true, err
			}
			if last {
				arg.ctr.state = operatorEnd
				continue
			}
			return false, nil
		case operatorEnd:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// use all batches from proc.Reg.MergeReceiver[index] to build the hash map.
func (ctr *container) buildHashTable(arg *Argument, proc *process.Process, ana process.Analyze, index int) error {
	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		if bat == nil {
			break
		}
		if len(bat.Zs) == 0 {
			continue
		}
		ana.Input(bat)

		itr := ctr.hashTable.NewIterator(arg.IBucket, arg.NBucket)
		count := vector.Length(bat.Vecs[0])
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, _ := itr.Insert(i, n, bat.Vecs)
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

// use a batch from proc.Reg.MergeReceivers[index] to probe the hash map and update the ctr.bat
// if batch is the last one, return true
// else return false.
func (ctr *container) probeHashTable(arg *Argument, proc *process.Process, ana process.Analyze, index int) (bool, error) {
	var err error
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		if bat == nil {
			return true, nil
		}
		if len(bat.Zs) == 0 {
			continue
		}
		ana.Input(bat)

		ctr.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(bat.Vecs[i].Typ)
		}

		count := vector.Length(bat.Vecs[0])
		itr := ctr.hashTable.NewIterator(arg.IBucket, arg.NBucket)
		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := ctr.hashTable.GroupCount()

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}
			vs, _ := itr.Insert(i, n, bat.Vecs)
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
					if err = vector.UnionBatch(ctr.bat.Vecs[pos], bat.Vecs[pos], int64(i), insertCount, inserted[:n], proc.Mp); err != nil {
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
