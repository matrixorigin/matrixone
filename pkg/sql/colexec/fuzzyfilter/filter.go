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
	rowCount := int64(arg.N)
	if rowCount < 10000 {
		rowCount = 10000
	}
	//@see https://hur.st/bloomfilter/
	var probability float64
	if rowCount < 10001 {
		probability = 0.0001
	} else if rowCount < 100001 {
		probability = 0.00001
	} else if rowCount < 1000001 {
		probability = 0.000005
	} else if rowCount < 10000001 {
		probability = 0.000001
	} else if rowCount < 100000001 {
		probability = 0.0000005
	} else if rowCount < 1000000001 {
		probability = 0.0000002
	} else {
		probability = 0.0000001
	}
	arg.filter = bloomfilter.New(rowCount, probability)

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()

	result, err := arg.children[0].Call(proc)
	if err != nil {
		result.Status = vm.ExecStop
		return result, err
	}
	bat := result.Batch

	if bat == nil {
		// this will happen in such case:create unique index from a table that unique col have no data
		if arg.rbat == nil || arg.collisionCnt == 0 {
			result.Status = vm.ExecStop
			return result, nil
		}

		// fmt.Printf("Estimated row count is %f, collisionCnt is %d, fp is %f\n", ap.N, ap.collisionCnt, float64(ap.collisionCnt)/float64(ap.N))
		// send collisionKeys to output operator to run background SQL
		arg.rbat.SetRowCount(arg.rbat.Vecs[0].Length())
		result.Batch = arg.rbat
		arg.collisionCnt = 0
		result.Status = vm.ExecStop
		return result, nil
	}

	anal.Input(bat, arg.info.IsFirst)

	rowCnt := bat.RowCount()
	if rowCnt == 0 {
		return result, nil
	}

	if arg.rbat == nil {
		if err := generateRbat(proc, arg, bat); err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
	}

	pkCol := bat.GetVector(0)
	arg.filter.TestAndAdd(pkCol, func(exist bool, i int) {
		if exist {
			if arg.collisionCnt < maxCheckDupCount {
				appendCollisionKey(proc, arg, i, bat)
				arg.collisionCnt++
			}
		}
	})

	result.Batch = batch.EmptyBatch
	return result, nil
}

// appendCollisionKey will append collision key into rbat
func appendCollisionKey(proc *process.Process, arg *Argument, idx int, bat *batch.Batch) {
	pkCol := bat.GetVector(0)
	arg.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
}

// rbat will contain the keys that have hash collisions
func generateRbat(proc *process.Process, arg *Argument, bat *batch.Batch) error {
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, proc.GetVector(*bat.GetVector(0).GetType()))
	arg.rbat = rbat
	return nil
}
