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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		refCount:     cs,
		instructions: ins,
		attrs:        attrs,
	}
}

func NewMerge(ins vm.Instructions) *Pipeline {
	return &Pipeline{
		instructions: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.instructions, &buf)
	return buf.String()
}

func (p *Pipeline) Run(segs []engine.Segment, proc *process.Process) (bool, error) {
	var end bool //exit flag
	var err error

	proc.Mp = mempool.New()
	defer func() {
		proc.Reg.InputBatch = nil
		vm.Run(p.instructions, proc)
		proc.Mp = nil
	}()
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	q := p.prefetch(segs, proc)
	p.compressedBytes, p.decompressedBytes = make([]*bytes.Buffer, len(p.refCount)), make([]*bytes.Buffer, len(p.refCount))
	{
		for i := range p.refCount {
			p.compressedBytes[i] = bytes.NewBuffer(make([]byte, 0, 8))
		}
		for i := range p.refCount {
			p.decompressedBytes[i] = bytes.NewBuffer(make([]byte, 0, 8))
		}
	}
	for i, j := 0, len(q.blocks); i < j; i++ {
		if err := q.prefetch(p.attrs); err != nil {
			return false, err
		}
		bat, err := q.blocks[i].blk.Read(p.refCount, p.attrs, p.compressedBytes, p.decompressedBytes)
		if err != nil {
			return false, err
		}
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil {
			return end, err
		}
		if end {
			break
		}
	}
	return end, err
}

func (p *Pipeline) RunMerge(proc *process.Process) (bool, error) {
	var end bool
	var err error

	proc.Mp = mempool.New()
	defer func() {
		proc.Reg.InputBatch = nil
		vm.Run(p.instructions, proc)
		proc.Mp = nil
	}()
	if err := vm.Prepare(p.instructions, proc); err != nil {
		vm.Clean(p.instructions, proc)
		return false, err
	}
	for {
		proc.Reg.InputBatch = nil
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			{
				fmt.Printf("+++%p begin clean\n", p)
			}
			p.clean(proc)
			return end, err
		}
	}
}

// prefetch generates a prefetch queue
func (p *Pipeline) prefetch(segs []engine.Segment, proc *process.Process) *queue {
	q := new(queue)
	q.blocks = make([]block, 0, 8) // prefetch block list
	{
		for _, seg := range segs {
			ids := seg.Blocks()
			for _, id := range ids {
				q.blocks = append(q.blocks, block{blk: seg.Block(id, proc)})
			}
		}
	}
	return q
}

func (p *Pipeline) clean(proc *process.Process) {
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

// prefetch
func (q *queue) prefetch(attrs []string) error {
	if q.prefetchIndex == len(q.blocks) {
		return nil
	}
	start := q.prefetchIndex
	for i, j := q.prefetchIndex, len(q.blocks); i < j; i++ {
		if i > PrefetchNum+start {
			break
		}
		q.blocks[i].blk.Prefetch(attrs)
		q.prefetchIndex = i + 1
	}
	return nil
}
