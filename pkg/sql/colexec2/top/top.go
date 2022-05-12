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

	compare "github.com/matrixorigin/matrixone/pkg/compare2"
	batch "github.com/matrixorigin/matrixone/pkg/container/batch2"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	process "github.com/matrixorigin/matrixone/pkg/vm/process2"
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
		n.ctr.poses = make([]int32, len(n.Fs))
		for i, f := range n.Fs {
			n.ctr.poses[i] = f.Pos
		}
	}
	n.ctr.n = len(n.Fs)
	n.ctr.sels = make([]int64, 0, n.Limit)
	n.ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := n.ctr
	for {
		switch ctr.state {
		case Build:
			bat := proc.Reg.InputBatch
			if bat == nil {
				ctr.state = Eval
				continue
			}
			if len(bat.Zs) == 0 {
				return false, nil
			}
			return false, ctr.build(n, bat, proc)
		case Eval:
			ctr.state = End
			return true, ctr.eval(n.Limit, proc)
		default:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, bat *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		batch.Reorder(bat, ctr.poses)
		ctr.bat = batch.New(len(bat.Vecs))
		for i, vec := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
		}
		for i, f := range n.Fs {
			ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
		}
		for i := len(n.Fs); i < len(bat.Vecs); i++ {
			ctr.cmps = append(ctr.cmps, compare.New(bat.Vecs[i].Typ.Oid, true))
		}
	} else {
		batch.Reorder(bat, ctr.poses)
	}
	defer batch.Clean(bat, proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}
	return ctr.processBatch(n.Limit, bat, proc)
}

func (ctr *Container) processBatch(limit int64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(len(bat.Zs))
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionOne(vec, bat.Vecs[j], i, proc.Mp); err != nil {
					batch.Clean(ctr.bat, proc.Mp)
					return err
				}
			}
			ctr.sels = append(ctr.sels, n)
			ctr.bat.Zs = append(ctr.bat.Zs, bat.Zs[i])
			n++
		}
		if n == limit {
			ctr.sort()
		}
	}
	if start == length {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					batch.Clean(ctr.bat, proc.Mp)
					return err
				}
				ctr.bat.Zs[0] = bat.Zs[i]
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *Container) eval(limit int64, proc *process.Process) error {
	if int64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	sels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	if err := batch.Shuffle(ctr.bat, sels, proc.Mp); err != nil {
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
