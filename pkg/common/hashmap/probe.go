// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashmap

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// FindWithMPool uses the existing key encoding and lookup, but charges string
// scratch to the actual probe pool instead of growing unaccounted Go buffers.
// Like Find, count is at most UnitLimit and the iterator is worker-local.
// Returned IDs/null flags belong to the iterator, not to the temporary arena.
func FindWithMPool(iter Iterator, start, count int, vecs []*vector.Vector, pool *mpool.MPool) ([]uint64, []int64, error) {
	itr, ok := iter.(*strHashmapIterator)
	if !ok {
		values, nonnull := iter.Find(start, count, vecs)
		return values, nonnull, nil
	}
	clear(itr.keys)
	itr.keyCapacity = 0
	if count == 0 {
		return itr.values[:0], itr.zValues[:0], nil
	}
	var sizes [UnitLimit]int
	total := 0
	for row := 0; row < count; row++ {
		size := 0
		for _, vec := range vecs {
			index := start + row
			if vec.IsConst() {
				index = 0
			}
			n := 1
			if !vec.IsGrouping() && !vec.IsConstNull() && !vec.GetNulls().Contains(uint64(index)) {
				if vec.GetType().IsFixedLen() {
					n += vec.GetType().TypeSize()
				} else {
					// Two length bytes plus the optional NULL/grouping tag.
					payload := len(vec.GetBytesAt(index))
					if payload > mpool.CapLimit-3 {
						return nil, nil, moerr.NewOOMNoCtx()
					}
					n = payload + 3
				}
			}
			if n > mpool.CapLimit-size {
				return nil, nil, moerr.NewOOMNoCtx()
			}
			size += n
		}
		size = max(size, 16)
		if size > mpool.CapLimit-total {
			return nil, nil, moerr.NewOOMNoCtx()
		}
		sizes[row] = size
		total += size
	}
	// Only off-heap allocations participate in MPool's atomic cap checks.
	// Avoid a known refusal first; Alloc still arbitrates concurrent workers.
	if int64(total) > min(pool.Cap()-pool.CurrNB(), mpool.GlobalCap()-mpool.GlobalStats().NumCurrBytes.Load()) {
		return nil, nil, moerr.NewOOMNoCtx()
	}
	arena, err := pool.Alloc(total, true)
	if err != nil {
		// The allocator includes its private header in the single-allocation
		// limit. For this checked nonnegative size, that rejection is also
		// an optional scratch budget failure, not an expression error.
		if moerr.IsMoErrCode(err, moerr.ErrInternal) && strings.HasPrefix(err.Error(), "internal error: mpool memory allocation exceed limit with requested size ") {
			err = moerr.NewOOMNoCtx()
		}
		return nil, nil, err
	}
	// The encoder's append operations stay inside these pre-sized slices. Do
	// not let a later lookup retain a reference to memory returned to the pool.
	defer func() {
		clear(itr.keys)
		itr.keyCapacity = 0
		pool.Free(arena)
	}()
	offset := 0
	for row := 0; row < count; row++ {
		end := offset + sizes[row]
		itr.keys[row] = arena[offset:offset:end]
		offset = end
	}
	copy(itr.zValues[:count], OneInt64s[:count])
	copy(itr.values[:count], zeroUint64[:count])
	itr.encodeHashKeys(vecs, start, count)
	itr.mp.hashMap.FindStringBatch(itr.strHashStates, itr.keys[:count], itr.values)
	return itr.values[:count], itr.zValues[:count], nil
}
