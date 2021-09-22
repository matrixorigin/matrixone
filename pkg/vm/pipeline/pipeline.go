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

package pipeline

import (
	"bytes"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		cs:    cs,
		ins:   ins,
		attrs: attrs,
	}
}

func NewMerge(ins vm.Instructions) *Pipeline {
	return &Pipeline{
		ins: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.ins, &buf)
	return buf.String()
}

func (p *Pipeline) Run(segs []engine.Segment, proc *process.Process) (bool, error) {
	var end bool
	var err error

	proc.Mp = mempool.New()
	defer func() {
		proc.Reg.Ax = nil
		vm.Run(p.ins, proc)
		proc.Mp = nil
	}()
	if err = vm.Prepare(p.ins, proc); err != nil {
		return false, err
	}
	q := p.prefetch(segs, proc)
	p.cds, p.dds = make([]*bytes.Buffer, len(p.cs)), make([]*bytes.Buffer, len(p.cs))
	{
		for i := range p.cs {
			p.cds[i] = bytes.NewBuffer(make([]byte, 0, 8))
		}
		for i := range p.cs {
			p.dds[i] = bytes.NewBuffer(make([]byte, 0, 8))
		}
	}
	for i, j := 0, len(q.bs); i < j; i++ {
		if err := q.prefetch(p.attrs); err != nil {
			return false, err
		}
		bat, err := q.bs[i].blk.Read(p.cs, p.attrs, p.cds, p.dds)
		if err != nil {
			return false, err
		}
		proc.Reg.Ax = bat
		if end, err = vm.Run(p.ins, proc); err != nil {
			return end, err
		}
		if end {
			break
		}
	}
	return end, err
}

func (p *Pipeline) RunMerge(proc *process.Process) (bool, error) {
	proc.Mp = mempool.New()
	defer func() {
		proc.Reg.Ax = nil
		vm.Run(p.ins, proc)
		proc.Mp = nil
	}()
	if err := vm.Prepare(p.ins, proc); err != nil {
		vm.Clean(p.ins, proc)
		return false, err
	}
	for {
		proc.Reg.Ax = nil
		if end, err := vm.Run(p.ins, proc); err != nil || end {
			return end, err
		}
	}
}

func (p *Pipeline) prefetch(segs []engine.Segment, proc *process.Process) *queue {
	q := new(queue)
	q.bs = make([]block, 0, 8) // prefetch block list
	{
		for _, seg := range segs {
			ids := seg.Blocks()
			for _, id := range ids {
				q.bs = append(q.bs, block{blk: seg.Block(id, proc)})
			}
		}
	}
	return q
}

func (q *queue) prefetch(attrs []string) error {
	if q.pi == len(q.bs) {
		return nil
	}
	start := q.pi
	for i, j := q.pi, len(q.bs); i < j; i++ {
		if i > PrefetchNum+start {
			break
		}
		q.bs[i].blk.Prefetch(attrs)
		q.pi = i + 1
	}
	return nil
}
