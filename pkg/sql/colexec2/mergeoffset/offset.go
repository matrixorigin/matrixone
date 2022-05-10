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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	argument := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeOffset(%d)", argument.Offset))
}

func Prepare(_ *process.Process, arg interface{}) error {
	argument := arg.(*Argument)
	argument.ctr.seen = 0
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	argument := arg.(*Argument)

	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		rec := proc.Reg.MergeReceivers[i]
		bat := <-rec.Ch
		// deal special case for bat
		{
			// 1. the last batch at this receiver
			if bat == nil {
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i++
				continue
			}
			// 2. an empty batch
			if len(bat.Zs) == 0 {
				i--
				continue
			}
		}

		if argument.ctr.seen > argument.Offset {
			return false, nil
		}
		length := len(bat.Zs)
		// bat = PartOne + PartTwo, and PartTwo is required.
		if argument.ctr.seen+uint64(length) > argument.Offset {
			data, sels, err := newSels(int64(argument.Offset-argument.ctr.seen), int64(length)-int64(argument.Offset-argument.ctr.seen), proc.Mp)
			if err != nil {
				batch.Clean(bat, proc.Mp)
				return false, err
			}
			argument.ctr.seen += uint64(length)
			batch.Shrink(bat, sels)
			mheap.Free(proc.Mp, data)
			proc.Reg.InputBatch = bat
			return false, nil
		}

		argument.ctr.seen += uint64(length)
		batch.Clean(bat, proc.Mp)
		proc.Reg.InputBatch = nil
		i--
	}

	return true, nil
}

func newSels(start, count int64, mp *mheap.Mheap) ([]byte, []int64, error) {
	data, err := mheap.Alloc(mp, count*8)
	if err != nil {
		return nil, nil, err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i := int64(0); i < count; i++ {
		sels[i] = start + i
	}
	return data, sels[:count], nil
}
