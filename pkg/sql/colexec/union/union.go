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
	arg.ctr = new(container)
	arg.ctr.hashTable = hashmap.NewStrMap(true)
	return nil
}

func Call(idx int, proc *process.Process, argument interface{}) (bool, error) {
	ap := argument.(*Argument)
	ctr := ap.ctr
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()
	for {
		switch ctr.state {
		case Build:
			end, err := ctr.insert(ap, proc, analyze, 1)
			if err != nil {
				ctr.state = End
				return true, err
			}
			if end {
				ctr.state = Probe
				continue
			}
			return false, nil
		case Probe:
			end, err := ctr.insert(ap, proc, analyze, 0)
			if err != nil {
				ctr.state = End
				return true, err
			}
			if end {
				ctr.state = End
			}
			return end, nil
		default:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// insert function use Table[index] to probe the HashTable.
// if row data doesn't in HashTable, append it to bat and update the HashTable.
func (ctr *container) insert(ap *Argument, proc *process.Process, analyze process.Analyze, index int) (bool, error) {
	var err error

	inserted := make([]uint8, hashmap.UnitLimit)
	restoreInserted := make([]uint8, hashmap.UnitLimit)

	for {
		bat := <-proc.Reg.MergeReceivers[index].Ch
		if bat == nil {
			return true, nil
		}
		if bat.Length() == 0 {
			continue
		}
		defer bat.Clean(proc.Mp)
		ctr.bat = batch.NewWithSize(len(bat.Vecs))
		for i := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(bat.Vecs[i].Typ)
		}

		analyze.Input(bat)
		count := len(bat.Zs)

		for i := 0; i < count; i += hashmap.UnitLimit {
			oldHashGroup := ctr.hashTable.GroupCount()
			iterator := ctr.hashTable.NewIterator(ap.Ibucket, ap.Nbucket)

			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, _ := iterator.Insert(i, n, bat.Vecs)
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
						ctr.bat.Clean(proc.Mp)
						return false, err
					}
				}
			}
		}
		analyze.Output(ctr.bat)
		proc.SetInputBatch(ctr.bat)
		return false, nil
	}
}
