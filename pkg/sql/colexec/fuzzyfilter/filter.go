// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"bytes"
	// "fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxCheckDupCount = 2000

/*
This operator is used to implement a way to ensure primary keys/unique keys are not duplicate in `INSERT` and `LOAD` statements

the BIG idea is to store
    pk columns to be loaded
    pk columns already exist
both in a bitmap-like data structure, let's say bloom filter below

if the final bloom filter claim that
    case 1: have no duplicate keys
        pass duplicate constraint directly
    case 2: Not sure if there are duplicate keys because of hash collision
        start a background SQL to double check

Note:
1. backgroud SQL may slow, so some optimizations could be applied
    manually check whether collision keys duplicate or not,
        if duplicate, then return error timely
2. there is a corner case that no need to run background SQL
    on duplicate key update
*/

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" fuzzy check duplicate constraint")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)
	rowCount := int64(arg.N)
	if rowCount < 10000 {
		rowCount = 10000
	}

	arg.useRoaring = IfCanUseRoaringFilter(arg.PkTyp.Oid)

	if arg.useRoaring {
		arg.roaringFilter = newroaringFilter(arg.PkTyp.Oid)
	} else {
		//@see https://hur.st/bloomfilter/
		var probability float64
		if rowCount < 10001 {
			probability = 0.0001
		} else if rowCount < 100001 {
			probability = 0.00001
		} else if rowCount < 1000001 {
			probability = 0.000003
		} else if rowCount < 10000001 {
			probability = 0.000001
		} else if rowCount < 100000001 {
			probability = 0.0000005
		} else if rowCount < 1000000001 {
			probability = 0.0000002
		} else {
			probability = 0.0000001
		}
		arg.bloomFilter = bloomfilter.New(rowCount, probability)
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()

	if arg.useRoaring {
		return arg.filterByRoaring(proc, anal)
	} else {
		return arg.filterByBloom(proc, anal)
	}
}

func (arg *Argument) filterByBloom(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	result := vm.NewCallResult()
	for {
		switch arg.ctr.state {
		case Build:

			bat, _, err := arg.ctr.ReceiveFromSingleReg(1, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				if err := generateRbat(proc, arg); err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
				arg.ctr.state = Probe
				continue
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)
			arg.bloomFilter.Add(pkCol)
			proc.PutBatch(bat)
			continue

		case Probe:

			bat, _, err := arg.ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				// this will happen in such case:create unique index from a table that unique col have no data
				if arg.rbat == nil || arg.collisionCnt == 0 {
					result.Status = vm.ExecStop
					return result, nil
				}

				// send collisionKeys to output operator to run background SQL
				arg.rbat.SetRowCount(arg.rbat.Vecs[0].Length())
				result.Batch = arg.rbat
				result.Status = vm.ExecStop
				arg.ctr.state = End
				return result, nil
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)

			arg.bloomFilter.TestAndAdd(pkCol, func(exist bool, i int) {
				if exist {
					if arg.collisionCnt < maxCheckDupCount {
						appendCollisionKey(proc, arg, i, bat)
						arg.collisionCnt++
					}
				}
			})
			proc.PutBatch(bat)
			continue
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (arg *Argument) filterByRoaring(proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	result := vm.NewCallResult()
	for {
		switch arg.ctr.state {
		case Build:

			bat, _, err := arg.ctr.ReceiveFromSingleReg(1, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				if err := generateRbat(proc, arg); err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
				arg.ctr.state = Probe
				continue
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)
			arg.roaringFilter.addFunc(arg.roaringFilter, pkCol)
			proc.PutBatch(bat)
			continue

		case Probe:

			bat, _, err := arg.ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				// this will happen in such case:create unique index from a table that unique col have no data
				if arg.rbat == nil || arg.collisionCnt == 0 {
					result.Status = vm.ExecStop
					return result, nil
				}

				// send collisionKeys to output operator to run background SQL
				arg.rbat.SetRowCount(arg.rbat.Vecs[0].Length())
				result.Batch = arg.rbat
				result.Status = vm.ExecStop
				arg.ctr.state = End
				return result, nil
			}

			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			pkCol := bat.GetVector(0)

			idx, dupVal := arg.roaringFilter.testAndAddFunc(arg.roaringFilter, pkCol)
			proc.PutBatch(bat)

			if idx == -1 {
				continue
			} else {
				return result, moerr.NewDuplicateEntry(proc.Ctx, valueToString(dupVal), arg.PkName)
			}
		case End:
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// appendCollisionKey will append collision key into rbat
func appendCollisionKey(proc *process.Process, arg *Argument, idx int, bat *batch.Batch) {
	pkCol := bat.GetVector(0)
	arg.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
}

// rbat will contain the keys that have hash collisions
func generateRbat(proc *process.Process, arg *Argument) error {
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, proc.GetVector(arg.PkTyp))
	arg.rbat = rbat
	return nil
}
