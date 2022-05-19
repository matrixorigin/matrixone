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
	top "github.com/matrixorigin/matrixone/pkg/sql/colexec2/top"
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
		n.ctr.poses = make([]int32, len(n.Fs))
		for i, f := range n.Fs {
			n.ctr.poses[i] = f.Pos
		}
	}
	n.ctr.sels = make([]int64, 0, n.Limit)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := n.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			ctr.state = End
			return true, ctr.eval(n.Limit, proc)
		default:
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
			bat := <-reg.Ch
			if bat == nil {
				proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
				i--
				continue
			}
			if len(bat.Zs) == 0 {
				i--
				continue
			}
			if ctr.bat == nil {
				mp := make(map[int]int)
				for i, pos := range ctr.poses {
					mp[int(pos)] = i
				}
				ctr.bat = batch.NewWithSize(len(bat.Vecs))
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
				ctr.cmps = make([]compare.Compare, len(bat.Vecs))
				for i := range ctr.cmps {
					if pos, ok := mp[i]; ok {
						ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, n.Fs[pos].Type == top.Descending)
					} else {
						ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, true)
					}
				}
			}
			if err := ctr.processBatch(n.Limit, bat, proc); err != nil {
				bat.Clean(proc.Mp)
				return err
			}
			bat.Clean(proc.Mp)
		}
	}
	return nil
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
	if err := ctr.bat.Shuffle(sels, proc.Mp); err != nil {
		ctr.bat.Clean(proc.Mp)
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
