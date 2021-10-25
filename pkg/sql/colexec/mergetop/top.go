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

package mergetop

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
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

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	{
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
		}
	}
	ctr.n = len(n.Fs)
	ctr.sels = make([]int64, 0, n.Limit)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if err := ctr.Eval(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.MergeReceivers) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
			reg := proc.Reg.MergeReceivers[i]
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(ctr.attrs)
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
				ctr.cmps = make([]compare.Compare, len(bat.Attrs))
				for i, f := range n.Fs {
					n.Ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
				}
				for i, j := len(n.Fs), len(bat.Attrs); i < j; i++ {
					n.Ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, false)
				}
			} else {
				bat.Reorder(ctr.bat.Attrs)
			}
			if len(bat.Sels) > 0 {
				bat.Shuffle(proc)
			}
			if err := ctr.processBatch(n.Limit, bat, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) Eval(n *Argument, proc *process.Process) error {
	if int64(len(ctr.sels)) < n.Limit {
		for i, cmp := range ctr.cmps {
			cmp.Set(0, ctr.bat.Vecs[i])
		}
		heap.Init(ctr)
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	data, err := proc.Alloc(int64(len(ctr.sels) * 8))
	if err != nil {
		return err
	}
	sels := encoding.DecodeInt64Slice(data)
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	ctr.bat.Sels = sels
	ctr.bat.SelsData = data
	if !n.Flg {
		ctr.bat.Reduce(ctr.attrs, proc)
	}
	proc.Reg.InputBatch = ctr.bat
	ctr.bat = nil
	ctr.data = nil
	return nil
}

func (ctr *Container) processBatch(limit int64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(bat.Vecs[0].Length())
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vec.UnionOne(bat.Vecs[j], int64(i), proc); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, n)
			n++
		}
		if n == limit {
			for i, cmp := range ctr.cmps {
				cmp.Set(0, ctr.bat.Vecs[i])
			}
			heap.Init(ctr)
		}
	}
	if start == length {
		return nil
	}
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	{
		for _, reg := range proc.Reg.MergeReceivers {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
