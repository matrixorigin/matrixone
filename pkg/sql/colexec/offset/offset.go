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
	"matrixone/pkg/vm/mempool"
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
	n := arg.(*Argument)
	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	if n.Seen > n.Offset {
		proc.Reg.Ax = bat
		return false, nil
	}
	if length := uint64(len(bat.Sels)); length > 0 {
		if n.Seen+length > n.Offset {
			bat.Sels = bat.Sels[n.Offset-n.Seen:]
			proc.Reg.Ax = bat
			n.Seen += length
			return false, nil
		}
		n.Seen += length
		bat.Clean(proc)
		proc.Reg.Ax = batch.New(true, nil)
		return false, nil
	}
	length := bat.Length(proc)
	if n.Seen+uint64(length) > n.Offset {
		data, sels, err := newSels(int64(n.Offset-n.Seen), int64(length)-int64(n.Offset-n.Seen), proc)
		if err != nil {
			bat.Clean(proc)
			return false, err
		}
		n.Seen += uint64(length)
		bat.Sels = sels
		bat.SelsData = data
		proc.Reg.Ax = bat
		return false, nil
	}
	n.Seen += uint64(length)
	bat.Clean(proc)
	proc.Reg.Ax = batch.New(true, nil)
	return false, nil
}

func newSels(start, count int64, proc *process.Process) ([]byte, []int64, error) {
	data, err := proc.Alloc(count * 8)
	if err != nil {
		return nil, nil, err
	}
	sels := encoding.DecodeInt64Slice(data[mempool.CountSize:])
	for i := int64(0); i < count; i++ {
		sels[i] = start + i
	}
	return data, sels[:count], nil
}
