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

package mergecte

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" merge cte ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	ap.ctr.nodeCnt = int32(len(proc.Reg.MergeReceivers)) - 1
	ap.ctr.curNodeCnt = ap.ctr.nodeCnt
	ap.ctr.status = sendInitial
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	var sb *batch.Batch
	var end bool
	var err error

	switch ctr.status {
	case sendInitial:
		sb, _, err = ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			return process.ExecStop, err
		}
		if sb == nil {
			ctr.status = sendLastTag
		}
		fallthrough
	case sendLastTag:
		if ctr.status == sendLastTag {
			ctr.status = sendRecursive
			sb = makeRecursiveBatch(proc)
			ctr.RemoveChosen(1)
		}
	case sendRecursive:
		for {
			sb, end, _ = ctr.ReceiveFromAllRegs(anal)
			if sb == nil || end {
				proc.SetInputBatch(nil)
				return process.ExecStop, nil
			}
			if !sb.Last() {
				break
			}

			sb.SetLast()
			ap.ctr.curNodeCnt--
			if ap.ctr.curNodeCnt == 0 {
				ap.ctr.curNodeCnt = ap.ctr.nodeCnt
				break
			} else {
				proc.PutBatch(sb)
			}
		}
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
