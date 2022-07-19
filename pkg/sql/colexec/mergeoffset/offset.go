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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeOffset(%d)", n.Offset))
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

		if n.ctr.seen > n.Offset {
			bat.Clean(proc.Mp)
			return false, nil
		}
		length := len(bat.Zs)
		// bat = PartOne + PartTwo, and PartTwo is required.
		if n.ctr.seen+uint64(length) > n.Offset {
			sels := newSels(int64(n.Offset-n.ctr.seen), int64(length)-int64(n.Offset-n.ctr.seen))
			n.ctr.seen += uint64(length)
			batch.Shrink(bat, sels)
			proc.Reg.InputBatch = bat
			return false, nil
		}
		n.ctr.seen += uint64(length)
		bat.Clean(proc.Mp)
		proc.Reg.InputBatch = nil
		i--
	}
	return true, nil
}

func newSels(start, count int64) []int64 {
	sels := make([]int64, count)
	for i := int64(0); i < count; i++ {
		sels[i] = start + i
	}
	return sels[:count]
}
