// Copyright 2021-2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// BatchFreeList is a simple LIFO batch pool that implements IBatchBuffer.
// Unlike OneSchemaBatchBuffer, it never evicts batches based on size,
// ensuring pre-allocated vector capacity is always preserved for reuse.
// It is released when the owning sinker calls Close.
type BatchFreeList struct {
	offHeap    bool
	attrs      []string
	typs       []types.Type
	pool       []*batch.Batch
	reuseBytes int
	reuseCount int
}

func NewBatchFreeList(
	attrs []string,
	typs []types.Type,
	offHeap bool,
) *BatchFreeList {
	return &BatchFreeList{
		offHeap: offHeap,
		attrs:   attrs,
		typs:    typs,
	}
}

func (fl *BatchFreeList) Len() int {
	return len(fl.pool)
}

func (fl *BatchFreeList) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	if len(fl.pool) > 0 {
		bat := fl.pool[len(fl.pool)-1]
		fl.pool = fl.pool[:len(fl.pool)-1]
		fl.reuseBytes += bat.Allocated()
		fl.reuseCount++
		return bat
	}
	return batch.NewWithSchema(fl.offHeap, fl.attrs, fl.typs)
}

func (fl *BatchFreeList) Fetch() *batch.Batch {
	return fl.FetchWithSchema(fl.attrs, fl.typs)
}

func (fl *BatchFreeList) Putback(bat *batch.Batch, _ *mpool.MPool) {
	if bat == nil || bat.Vecs == nil {
		return
	}
	bat.CleanOnlyData()
	fl.pool = append(fl.pool, bat)
}

func (fl *BatchFreeList) Close(mp *mpool.MPool) {
	for i := range fl.pool {
		if fl.pool[i] != nil {
			fl.pool[i].Clean(mp)
			fl.pool[i] = nil
		}
	}
	fl.pool = nil
}

func (fl *BatchFreeList) Usage() (int, int, int, int) {
	var currSize int
	for _, bat := range fl.pool {
		currSize += bat.Allocated()
	}
	return currSize, 0, fl.reuseBytes, fl.reuseCount
}
