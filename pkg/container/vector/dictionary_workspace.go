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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"sync/atomic"
)

// DictionaryVectorSlot holds at most one private retired header. Free seals the
// slot before releasing its spare, so a late borrower cannot resurrect an old
// execution generation. No payload is retained by snapshot slots.
type DictionaryVectorSlot struct{ spare atomic.Pointer[Vector] }

// A sentinel seals only the metadata slot; it owns no resources.
var closedDictionaryVectorSlot Vector

func (s *DictionaryVectorSlot) take() *Vector {
	if s == nil {
		return nil
	}
	for {
		v := s.spare.Load()
		if v == nil || v == &closedDictionaryVectorSlot {
			return nil
		}
		if s.spare.CompareAndSwap(v, nil) {
			return v
		}
	}
}
func (s *DictionaryVectorSlot) put(v *Vector, mp *mpool.MPool) {
	if s == nil || !s.spare.CompareAndSwap(nil, v) {
		v.Free(mp)
	}
}
func (s *DictionaryVectorSlot) Free(mp *mpool.MPool) {
	if s == nil {
		return
	}
	v := s.spare.Swap(&closedDictionaryVectorSlot)
	if v != nil && v != &closedDictionaryVectorSlot {
		v.Free(mp)
	}
}

// TryFreezeVectorReuse recycles only private snapshot metadata after its final
// immutable borrower releases it. Payload pinning and admission are unchanged.
func TryFreezeVectorReuse(source *Vector, mp *mpool.MPool, account *AllocationAccountSelection, slot *DictionaryVectorSlot) (*DictionaryBase, bool, error) {
	return tryFreezeVector(source, mp, account, slot)
}
