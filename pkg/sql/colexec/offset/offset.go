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

package offset

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("offset(%v)", n.Offset))
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return process.ExecStop, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)

	if ap.Seen > ap.Offset {
		return process.ExecNext, nil
	}
	length := bat.RowCount()
	if ap.Seen+uint64(length) > ap.Offset {
		sels := newSels(int64(ap.Offset-ap.Seen), int64(length)-int64(ap.Offset-ap.Seen), proc)
		ap.Seen += uint64(length)
		bat.Shrink(sels)
		proc.Mp().PutSels(sels)
		proc.SetInputBatch(bat)
		return process.ExecNext, nil
	}
	ap.Seen += uint64(length)
	proc.PutBatch(bat)
	proc.SetInputBatch(batch.EmptyBatch)
	return process.ExecNext, nil
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
