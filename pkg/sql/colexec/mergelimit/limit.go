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

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", n.Limit))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr.seen = 0
	return nil
}

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		reg := proc.Reg.MergeReceivers[i]
		bat := <-reg.Ch

		// deal special case for bat
		{
			// 1. the last batch at this receiver
			if bat == nil {
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i--
				continue
			}
			// 2. an empty batch
			if len(bat.Zs) == 0 {
				i--
				continue
			}
		}

		if n.ctr.seen >= n.Limit {
			proc.Reg.InputBatch = nil
			bat.Clean(proc.Mp)
			return true, nil
		}
		newSeen := n.ctr.seen + uint64(len(bat.Zs))
		if newSeen < n.Limit {
			n.ctr.seen = newSeen
			proc.Reg.InputBatch = bat
			return false, nil
		} else {
			num := int(newSeen - n.Limit)
			batch.SetLength(bat, len(bat.Zs)-num)
			n.ctr.seen = newSeen
			proc.Reg.InputBatch = bat
			return false, nil
		}
	}
	return true, nil
}
