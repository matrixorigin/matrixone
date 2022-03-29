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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	n.ctr.sels = make([]int64, 0, n.Limit)
	n.ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil { // begin eval
		n := arg.(*Argument)
		if n.ctr.bat != nil {
			if err := n.ctr.eval(n.Limit, proc); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	return n.ctr.process(n, bat, proc)
}

func (ctr *Container) process(n *Argument, bat *batch.Batch, proc *process.Process) (bool, error) {
	if ctr.bat == nil {
		batch.Reorder(bat, ctr.attrs)
		ctr.bat = batch.New(true, bat.Attrs)
		for i, vec := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
		}
		for i, f := range n.Fs {
			ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
		}
	} else {
		batch.Reorder(bat, ctr.bat.Attrs)
	}
	proc.Reg.InputBatch = &batch.Batch{}
	defer batch.Clean(bat, proc.Mp)
	if err := ctr.processBatch(n.Limit, bat, proc); err != nil {
		batch.Clean(ctr.bat, proc.Mp)
		ctr.bat = nil
		return false, err
	}
	return false, nil
}

func (ctr *Container) eval(limit int64, proc *process.Process) error {
	if int64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	data, err := mheap.Alloc(proc.Mp, int64(len(ctr.sels))*8)
	if err != nil {
		batch.Clean(ctr.bat, proc.Mp)
		ctr.bat = nil
		return err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	ctr.bat.Sels = sels
	ctr.bat.SelsData = data
	if err := batch.Shuffle(ctr.bat, proc.Mp); err != nil {
		batch.Clean(ctr.bat, proc.Mp)
		ctr.bat = nil
	}
	proc.Reg.InputBatch = ctr.bat
	ctr.bat = nil
	return nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *Container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}

func (ctr *Container) processBatch(limit int64, b *batch.Batch, proc *process.Process) error {
	var start int64

	bLength := int64(vector.Length(b.Vecs[0]))
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > bLength {
			start = bLength
		}

		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionOne(vec, b.Vecs[j], i, proc.Mp); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, n)
			ctr.bat.Zs = append(ctr.bat.Zs, b.Zs[i])
			n++
		}

		if n == limit {
			ctr.sort()
		}
	}
	if start == bLength {
		return nil
	}

	// b is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, b.Vecs[i])
	}
	for i, j := start, bLength; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
				ctr.bat.Zs[0] = b.Zs[i]
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}
