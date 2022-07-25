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

package union

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" union ")
}

func Prepare(_ *process.Process, argument interface{}) error {
	arg := argument.(*Argument)
	{
		arg.ctr.bat = nil
		arg.ctr.hashTable = hashmap.NewStrMap(true)
	}
	return nil
}

func Call(idx int, proc *process.Process, argument interface{}) (bool, error) {
	var err error
	arg := argument.(*Argument)
	// we make this assertion here for now, the real situation of table size
	// should be noted by the execution plan
	smallTableIndex, bigTableIndex := 1, 0

	// prepare the analysis work
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()

	// step1: deal the small table. if new row, put into bat.
	if err = arg.ctr.insert(proc, analyze, smallTableIndex); err != nil {
		return false, err
	}
	// step2: deal the big table. if new row, put into bat.
	if err = arg.ctr.insert(proc, analyze, bigTableIndex); err != nil {
		return false, err
	}
	// step3: return
	analyze.Output(arg.ctr.bat)
	proc.Reg.InputBatch = arg.ctr.bat
	return true, nil
}

// insert function use Table[index] to probe the HashTable.
// if row data doesn't in HashTable, append it to bat and update the HashTable.
func (ctr *Container) insert(proc *process.Process, analyze process.Analyze, index int) error {
	var err error
	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		if bat == nil {
			return nil
		}
		if len(bat.Zs) == 0 {
			continue
		}
		if ctr.bat == nil {
			ctr.bat = batch.NewWithSize(len(bat.Vecs))
			for i := range bat.Vecs {
				ctr.bat.Vecs[i] = vector.New(bat.Vecs[i].Typ)
			}
		}

		analyze.Input(bat)
		count := len(bat.Zs)

		scales := make([]int32, len(bat.Vecs))
		for i := range scales {
			scales[i] = bat.Vecs[i].Typ.Scale
		}
		for i := 0; i < count; i += hashmap.UnitLimit {
			insertCount := 0
			iterator := ctr.hashTable.NewIterator()

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, _ := iterator.Insert(i, n, bat.Vecs, scales)
			copy(inserted[:n], restoreInserted[:n])
			for j, v := range vs {
				if v > ctr.hashTable.GroupCount() {
					insertCount++
					inserted[j] = 1
					ctr.bat.Zs = append(ctr.bat.Zs, 1)
				}
			}

			if insertCount > 0 {
				for pos := range bat.Vecs {
					if err = vector.UnionBatch(ctr.bat.Vecs[pos], bat.Vecs[pos], int64(i), insertCount, inserted[:n], proc.Mp); err != nil {
						return err
					}
				}
			}
		}
		bat.Clean(proc.Mp)
	}
}
