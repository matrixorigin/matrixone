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

package top

import (
	"bytes"
	"container/heap"
	"fmt"
	"matrixone/pkg/compare"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("], %v)", n.Limit))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	{
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
		}
	}
	ctr.n = len(n.Fs)
	ctr.sels = make([]int64, n.Limit)
	ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	if proc.Reg.Ax == nil {
		return false, nil
	}
	bat := proc.Reg.Ax.(*batch.Batch)
	if bat == nil || bat.Attrs == nil {
		return false, nil
	}
	n := arg.(*Argument)
	ctr := &n.Ctr
	bat.Reorder(ctr.attrs)
	{
		for i := int64(0); i < n.Limit; i++ {
			ctr.sels[i] = i
		}
		if ctr.cmps[0] == nil {
			for i, f := range n.Fs {
				ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
			}
		}
	}
	if len(bat.Sels) > 0 {
		bat.Shuffle(proc)
	}
	ctr.processBatch(n.Limit, bat)
	data, err := proc.Alloc(int64(len(ctr.sels) * 8))
	if err != nil {
		bat.Clean(proc)
		return false, err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	bat.Sels = sels
	bat.SelsData = data
	bat.Shuffle(proc)
	proc.Reg.Ax = bat
	return false, nil
}

func (ctr *Container) processBatch(limit int64, bat *batch.Batch) {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat.Vecs[i])
		cmp.Set(1, bat.Vecs[i])
	}
	length := int64(bat.Vecs[0].Length())
	if length < limit {
		ctr.sels = ctr.sels[:length]
		heap.Init(ctr)
		return
	}
	heap.Init(ctr)
	for i, j := limit, length; i < j; i++ {
		if ctr.compare(i, ctr.sels[0]) < 0 {
			ctr.sels[0] = i
		}
		heap.Fix(ctr, 0)
	}
}
