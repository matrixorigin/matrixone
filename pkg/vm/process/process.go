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

func New(gm *guest.Mmu, mp *mempool.Mempool) *Process {
	return &Process{
		Gm: gm,
		Mp: mp,
	}
}

func (p *Process) Size() int64 {
	return p.Gm.Size()
}

func (p *Process) HostSize() int64 {
	return p.Gm.HostSize()
}

func (p *Process) Free(data []byte) bool {
	end := p.Mp.Free(data)
	if end {
		p.Gm.Free(int64(cap(data)))
	}
	return end
}

func (p *Process) Alloc(size int64) ([]byte, error) {
	data := p.Mp.Alloc(int(size))
	if err := p.Gm.Alloc(int64(cap(data))); err != nil {
		p.Mp.Free(data)
		return nil, err
	}
	return data[:mempool.CountSize+size], nil
}

func (p *Process) Grow(old []byte, size int64) ([]byte, error) {
	n := int64(cap(old))
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if n < 1024 {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	data, err := p.Alloc(newcap * 8)
	if err != nil {
		return nil, err
	}
	copy(data[mempool.CountSize:], old)
	return data[:mempool.CountSize+size], nil
}
