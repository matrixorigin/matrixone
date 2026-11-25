// Copyright 2024 Matrix Origin
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

package pSpool

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// batchCache keeps the batches receivers have released, so the next copy
// reuses their batch shell and the vectors (with their backing) in the same
// column positions. Every cached vector is owned by mp.
//
// The number of batches is bounded by the spool's slot count: a batch is only
// created when the cache is empty, and every live batch occupies a slot.
//
// free closes the cache. It may be called at any time: the cache only holds
// batches nobody references, and after it is closed released batches are
// freed directly instead of cached.
type batchCache struct {
	mp *mpool.MPool

	mu     sync.Mutex
	closed bool
	bats   []*batch.Batch
}

func newBatchCache(mp *mpool.MPool, capacity uint32) *batchCache {
	return &batchCache{
		mp:   mp,
		bats: make([]*batch.Batch, 0, capacity),
	}
}

func (c *batchCache) get() *batch.Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.bats)
	if n == 0 {
		return nil
	}
	bat := c.bats[n-1]
	c.bats[n-1] = nil
	c.bats = c.bats[:n-1]
	return bat
}

// put takes back a batch produced by copyBatch. aliased reports whether
// copyBatch made several columns share one vector.
func (c *batchCache) put(bat *batch.Batch, aliased bool) {
	if aliased {
		// Keep each vector once, at its first position, so a later copy can
		// reuse every cached vector independently.
		dedupVectors(bat.Vecs)
	}
	bat.DropExtraBuffer()
	bat.ReleaseLeases()
	bat.SetRowCount(0)

	c.mu.Lock()
	if !c.closed {
		c.bats = append(c.bats, bat)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	bat.Clean(c.mp)
}

func (c *batchCache) free() {
	c.mu.Lock()
	bats := c.bats
	c.bats = nil
	c.closed = true
	c.mu.Unlock()
	for _, bat := range bats {
		bat.Clean(c.mp)
	}
}

// reclaim frees the cached batches but keeps the cache open. It returns
// whether it freed anything.
func (c *batchCache) reclaim() bool {
	c.mu.Lock()
	bats := c.bats
	c.bats = c.bats[:0:0]
	c.mu.Unlock()
	for _, bat := range bats {
		bat.Clean(c.mp)
	}
	return len(bats) > 0
}

// dedupVectors clears every entry of vecs that repeats an earlier one, so
// each shared vector is kept, and freed, once.
func dedupVectors(vecs []*vector.Vector) {
	for i := 1; i < len(vecs); i++ {
		for j := 0; j < i; j++ {
			if vecs[i] != nil && vecs[j] == vecs[i] {
				vecs[i] = nil
				break
			}
		}
	}
}

// copyBatch returns a deep copy of src whose vectors are owned by the cache's
// pool, whether the returned batch is owned by the spool (and must be given
// back with put), and whether several of its columns share one vector.
//
// The special marker batches are never released, so they are returned as is.
//
// Every source vector is copied, borrowed ones included: a borrowed vector is
// valid only while its lender (the producer's batch) holds the lent backing,
// which ends at the producer's next call, before consumers read this copy.
// A vector shared by several columns of src is copied once and shared by the
// same columns of the copy.
//
// Cached vectors keep the backing sized for the column at their position in
// an earlier batch, so a copy into them can be refused by a memory limit that
// a fresh copy fits in (for example when two columns swap sizes). When a copy
// that reused cached memory fails, all idle cached memory is freed and the
// copy is retried once into fresh vectors.
func (c *batchCache) copyBatch(src *batch.Batch) (dst *batch.Batch, owned, aliased bool, err error) {
	if src == nil || src == batch.EmptyBatch || src == batch.CteEndBatch {
		return src, false, false, nil
	}

	cached := c.get()
	dst, aliased, err = c.copyInto(cached, src)
	if err != nil {
		reclaimed := c.reclaim()
		if cached == nil && !reclaimed {
			return nil, false, false, err
		}
		dst, aliased, err = c.copyInto(nil, src)
		if err != nil {
			return nil, false, false, err
		}
	}

	dst.MoveExtraBufferFrom(src)
	dst.SetRowCount(src.RowCount())
	return dst, true, aliased, nil
}

// copyInto copies the vectors of src into dst, a cached batch or nil for a
// fresh one. On error it frees dst and returns nil.
func (c *batchCache) copyInto(dst, src *batch.Batch) (_ *batch.Batch, aliased bool, err error) {
	if dst == nil {
		dst = batch.NewWithSize(len(src.Vecs))
	} else {
		dst.Vecs = c.reshape(dst.Vecs, len(src.Vecs))
	}
	dst.Recursive = src.Recursive
	dst.ShuffleIDX = src.ShuffleIDX
	dst.Attrs = append(dst.Attrs[:0], src.Attrs...)

copyVectors:
	for i, vec := range src.Vecs {
		target := dst.Vecs[i]
		if vec == nil {
			if target != nil {
				target.Free(c.mp)
				dst.Vecs[i] = nil
			}
			continue
		}
		for j := 0; j < i; j++ {
			if src.Vecs[j] == vec {
				if target != nil {
					target.Free(c.mp)
				}
				dst.Vecs[i] = dst.Vecs[j]
				aliased = true
				continue copyVectors
			}
		}

		typ := *vec.GetType()
		if target == nil {
			target = vector.NewVec(typ, c.mp)
			dst.Vecs[i] = target
		} else {
			target.Reset(typ)
		}
		if vec.IsConst() {
			err = vector.GetConstSetFunction(typ, c.mp)(target, vec, 0, vec.Length())
		} else {
			err = vector.GetUnionAllFunction(typ, c.mp)(target, vec)
			target.SetSorted(vec.GetSorted())
		}
		if err != nil {
			// Aliases are only made to earlier columns, so dst is consistent
			// up to here; the uncopied suffix still holds cached vectors.
			if aliased {
				dedupVectors(dst.Vecs)
			}
			dst.Clean(c.mp)
			return nil, false, err
		}
	}
	return dst, aliased, nil
}

// reshape resizes a cached batch's vector slice to n columns, freeing the
// vectors of dropped columns. Entries past len(vecs) are always nil.
func (c *batchCache) reshape(vecs []*vector.Vector, n int) []*vector.Vector {
	for i := n; i < len(vecs); i++ {
		if vecs[i] != nil {
			vecs[i].Free(c.mp)
			vecs[i] = nil
		}
	}
	if n <= cap(vecs) {
		return vecs[:n]
	}
	grown := make([]*vector.Vector, n)
	copy(grown, vecs)
	return grown
}
