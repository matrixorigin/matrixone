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

package process

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

// New creates a new Process.
// A process stores the execution context.
func New(gm *guest.Mmu) *Process {
	return &Process{
		Gm: gm,
	}
}

// Size returns
func (p *Process) Size() int64 {
	return p.Gm.Size()
}

func (p *Process) HostSize() int64 {
	return p.Gm.HostSize()
}

func (p *Process) Free(data []byte) {
	p.Mp.Free(data)
	p.Gm.Free(int64(cap(data)))
}

func (p *Process) Alloc(size int64) ([]byte, error) {
	data := p.Mp.Alloc(int(size))
	if err := p.Gm.Alloc(int64(cap(data))); err != nil {
		p.Mp.Free(data)
		return nil, err
	}
	return data[:size], nil
}

func (p *Process) Grow(old []byte, size int64) ([]byte, error) {
	data, err := p.Alloc(mempool.Realloc(old, size))
	if err != nil {
		return nil, err
	}
	copy(data, old)
	return data[:size], nil
}
