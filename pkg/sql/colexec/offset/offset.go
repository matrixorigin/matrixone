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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mheap"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("offset(%v)", n.Offset))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	if n.Seen > n.Offset {
		return false, nil
	}
	length := len(bat.Zs)
	if n.Seen+uint64(length) > n.Offset {
		data, sels, err := newSels(int64(n.Offset-n.Seen), int64(length)-int64(n.Offset-n.Seen), proc.Mp)
		if err != nil {
			batch.Clean(bat, proc.Mp)
			return false, err
		}
		n.Seen += uint64(length)
		batch.Shrink(bat, sels)
		mheap.Free(proc.Mp, data)
		proc.Reg.InputBatch = bat
		return false, nil
	}
	n.Seen += uint64(length)
	batch.Clean(bat, proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}
	return false, nil
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
