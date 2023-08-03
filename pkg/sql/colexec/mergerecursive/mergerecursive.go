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

package mergerecursive

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" merge recursive ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, colexec.MergeReceiver)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	var sb *batch.Batch

	bat, end, err := ctr.ReceiveFromSingleRegNonBlock(1, anal)
	if err != nil {
		return process.ExecStop, err
	}
	if end {
		proc.SetInputBatch(nil)
		return process.ExecStop, nil
	}
	if bat != nil {
		b, err := bat.Dup(proc.GetMPool())
		if err != nil {
			return process.ExecStop, err
		}
		b.Recursive = bat.Recursive
		ctr.bats = append(ctr.bats, b)
		if b.Last() {
			ctr.last = true
		}
	}

	switch ctr.status {
	case SendA:
		bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			return process.ExecStop, err
		}
		if bat == nil {
			ctr.status = SendLastBatch
		} else {
			sb, err = bat.Dup(proc.Mp())
			if err != nil {
				return process.ExecStop, err
			}
		}
		fallthrough
	case SendLastBatch:
		if ctr.status == SendLastBatch {
			ctr.status = SendB
			sb = makeRecursiveBatch(proc)
		}
	case SendB:
		for !ctr.last {
			bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
			if err != nil {
				return process.ExecStop, err
			}
			if bat == nil {
				proc.SetInputBatch(nil)
				return process.ExecStop, nil
			}
			b, err := bat.Dup(proc.Mp())
			if err != nil {
				return process.ExecStop, err
			}
			b.Recursive = bat.Recursive
			ctr.bats = append(ctr.bats, b)
			if b.Last() {
				ctr.last = true
			}
		}
		sb = ctr.bats[0]
		ctr.bats = ctr.bats[1:]
	}

	if sb.Last() {
		ctr.last = false
	}

	if sb.End() {
		sb.Clean(proc.Mp())
		proc.SetInputBatch(nil)
		return process.ExecStop, nil
	}

	anal.Input(sb, isFirst)
	anal.Output(sb, isLast)
	proc.SetInputBatch(sb)
	return process.ExecNext, nil
}

func makeRecursiveBatch(proc *process.Process) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	batch.SetLength(b, 1)
	b.SetLast()
	return b
}
