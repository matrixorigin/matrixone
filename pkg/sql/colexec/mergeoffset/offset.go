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

	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("mergeOffset(%d)", arg.Offset))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	ap.ctr.seen = 0
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	ap := arg
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	ctr := ap.ctr
	result := vm.NewCallResult()
	for {
		bat, end, err := ctr.ReceiveFromAllRegs(anal)
		if err != nil {
			// WTF, nil?
			result.Status = vm.ExecStop
			return result, nil
		}

		if end {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}

		anal.Input(bat, arg.info.IsFirst)
		if ap.ctr.seen > ap.Offset {
			anal.Output(bat, arg.info.IsLast)
			result.Batch = bat
			return result, nil
		}
		length := bat.RowCount()
		// bat = PartOne + PartTwo, and PartTwo is required.
		if ap.ctr.seen+uint64(length) > ap.Offset {
			sels := newSels(int64(ap.Offset-ap.ctr.seen), int64(length)-int64(ap.Offset-ap.ctr.seen), proc)
			ap.ctr.seen += uint64(length)
			bat.Shrink(sels)
			proc.Mp().PutSels(sels)
			anal.Output(bat, arg.info.IsLast)
			result.Batch = bat
			return result, nil
		}
		ap.ctr.seen += uint64(length)
		proc.PutBatch(bat)
	}
}

func newSels(start, count int64, proc *process.Process) []int64 {
	sels := proc.Mp().GetSels()
	for i := int64(0); i < count; i++ {
		sels = append(sels, start+i)
	}
	return sels[:count]
}
