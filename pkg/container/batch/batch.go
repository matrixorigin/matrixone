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

package batch

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func New(n int) *Batch {
	return &Batch{
		Cnt:  1,
		Vecs: make([]vector.AnyVector, n),
	}
}

func Length(bat *Batch) int {
	return len(bat.Zs)
}

func (bat *Batch) SetLength(n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.Zs = bat.Zs[:n]
}

func (bat *Batch) Free(m *mheap.Mheap) {
	if atomic.AddInt64(&bat.Cnt, -1) != 0 {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(m)
		}
	}
	bat.Vecs = nil
	bat.Zs = nil
}

// InitZsOne init Batch.Zs and values are all 1
func (bat *Batch) InitZsOne(len int) {
	bat.Zs = make([]int64, len)
	for i := range bat.Zs {
		bat.Zs[i]++
	}
}
