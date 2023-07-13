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

package mergelimit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", ap.Limit))
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ctr := ap.ctr

	for {
		bat, end, err := ctr.ReceiveFromAllRegs(anal)
		if err != nil {
			return true, nil
		}
		bat.FixedForRemoveZs()
		if end {
			proc.SetInputBatch(nil)
			return true, nil
		}

		anal.Input(bat, isFirst)
		if ap.ctr.seen >= ap.Limit {
			proc.PutBatch(bat)
			continue
		}
		newSeen := ap.ctr.seen + uint64(bat.Length())
		if newSeen < ap.Limit {
			ap.ctr.seen = newSeen
			anal.Output(bat, isLast)

			bat.CheckForRemoveZs("merge limit")
			proc.SetInputBatch(bat)
			return false, nil
		} else {
			num := int(newSeen - ap.Limit)
			batch.SetLength(bat, bat.Length()-num)
			ap.ctr.seen = newSeen
			anal.Output(bat, isLast)

			bat.CheckForRemoveZs("merge limit")
			proc.SetInputBatch(bat)
			return false, nil
		}
	}
}
