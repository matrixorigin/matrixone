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

	"github.com/bits-and-blooms/bloom"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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
    1. manually check whether collision keys duplicate or not,
        if duplicate, then return error timely
2. there is a corner case that no need to run background SQL
    on duplicate key update

*/

const (
	// false positives rate
	fp = 0.00001
	hashCnt = 3
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" fuzzy check duplicate constraint")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)

	ap.filter = bloom.NewWithEstimates(ap.EstimatedRowCnt, fp)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	ap := arg.(*Argument)
	bat := proc.InputBatch()
	anal.Input(bat, isFirst)

	if bat == nil {
		// this will happen in such case:create unique index from a table that unique col have no data
		if ap.rbat == nil {
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}

		ap.rbat.SetRowCount(ap.collisionCnt)
		if ap.collisionCnt == 0 {
			// case 1: pass duplicate constraint
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		} else {
			// case 2: send collisionKeys to output operator to run background SQL
			proc.SetInputBatch(ap.rbat)
			return process.ExecStop, nil
		}
	}

	rowCnt := bat.RowCount()
	if rowCnt == 0 {
		proc.PutBatch(bat)
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}

	if ap.rbat == nil {
		if err := generateRbat(proc, ap, bat); err != nil {
			return process.ExecStop, err
		}
	}

	pkCol := bat.GetVector(0)
	for i := 0; i < rowCnt; i++ {
		var bytes = pkCol.GetRawBytesAt(i)
		if ap.filter.Test(bytes) {
			appendCollisionKey(proc, ap, i, bat)
			ap.collisionCnt++
		} else {
			ap.filter.Add(bytes)
		}
	}

	proc.SetInputBatch(batch.EmptyBatch)
	return process.ExecNext, nil
}

// appendCollisionKey will append collision key into rbat
func appendCollisionKey(proc *process.Process, arg *Argument, idx int, bat *batch.Batch) {
	pkCol := bat.GetVector(0)
	arg.rbat.GetVector(0).UnionOne(pkCol, int64(idx), proc.GetMPool())
}

// rbat will contain the keys that have hash collisions
func generateRbat(proc *process.Process, arg *Argument, bat *batch.Batch) error {
	rbat := batch.NewWithSize(1)
	rbat.SetVector(0, vector.NewVec(*bat.GetVector(0).GetType()))
	arg.rbat = rbat
	return nil
}
