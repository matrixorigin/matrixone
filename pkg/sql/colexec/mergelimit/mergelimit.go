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
	argument := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", argument.Limit))
}

func Prepare(_ *process.Process, arg interface{}) error {
	argument := arg.(*Argument)
	argument.ctr.seen = 0
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	argument := arg.(*Argument)

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

		if argument.ctr.seen >= argument.Limit {
			proc.Reg.InputBatch = nil
			batch.Clean(bat, proc.Mp)
			return true, nil
		}
		newSeen := argument.ctr.seen + uint64(len(bat.Zs))
		if newSeen < argument.Limit {
			argument.ctr.seen = newSeen
			proc.Reg.InputBatch = bat
			return true, nil
		} else {
			num := int(newSeen - argument.Limit)
			batch.SetLength(bat, len(bat.Zs)-num)
			argument.ctr.seen = newSeen
			proc.Reg.InputBatch = bat
			return true, nil
		}
		i--
	}
	return true, nil
}
