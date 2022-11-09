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

package mergeoffset

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeOffset(%d)", ap.Offset))
}

func Prepare(_ *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	defer anal.Stop()

	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		reg := proc.Reg.MergeReceivers[i]
		bat := <-reg.Ch
		// 1. the last batch at this receiver
		if bat == nil {
			proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
			i--
			continue
		}
		// 2. an empty batch
		if bat.Length() == 0 {
			i--
			continue
		}
		anal.Input(bat)
		if ap.ctr.seen > ap.Offset {
			anal.Output(bat)
			proc.SetInputBatch(bat)
			return false, nil
		}
		length := len(bat.Zs)
		// bat = PartOne + PartTwo, and PartTwo is required.
		if ap.ctr.seen+uint64(length) > ap.Offset {
			sels := newSels(int64(ap.Offset-ap.ctr.seen), int64(length)-int64(ap.Offset-ap.ctr.seen), proc)
			ap.ctr.seen += uint64(length)
			bat.Shrink(sels)
			proc.Mp().PutSels(sels)
			anal.Output(bat)
			proc.SetInputBatch(bat)
			return false, nil
		}
		ap.ctr.seen += uint64(length)
		bat.Clean(proc.Mp())
		i--
	}
	proc.SetInputBatch(nil)
	ap.Free(proc, false)
	return true, nil
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
