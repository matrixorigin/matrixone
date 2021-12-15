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
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	n.ctr = new(Container)
	{
		n.ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			n.ctr.attrs[i] = f.Attr
		}
	}
	n.ctr.n = len(n.Fs)
	n.ctr.sels = make([]int64, n.Limit)
	n.ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	return n.ctr.process(n, bat, proc)
}

func (ctr *Container) process(n *Argument, bat *batch.Batch, proc *process.Process) (bool, error) {
	batch.Reorder(bat, ctr.attrs)
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
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat.Vecs[i])
		cmp.Set(1, bat.Vecs[i])
	}
	length := int64(len(bat.Zs))
	if length < n.Limit {
		ctr.sels = ctr.sels[:length]
		heap.Init(ctr)
	} else {
		heap.Init(ctr)
		for i, j := n.Limit, length; i < j; i++ {
			if ctr.compare(i, ctr.sels[0]) < 0 {
				ctr.sels[0] = i
			}
			heap.Fix(ctr, 0)
		}
	}
	data, err := mheap.Alloc(proc.Mp, int64(len(ctr.sels))*8)
	if err != nil {
		batch.Clean(bat, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	bat.Sels = sels
	bat.SelsData = data
	batch.Shuffle(bat, proc.Mp)
	return false, nil
}
