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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// TrySelectDictionaryVectors transactionally fills initially empty destination
// slots. All columns in the same row domain share their immutable mapping.
// A normal decline leaves every destination nil and preserves source values.
func TrySelectDictionaryVectors(sources, destinations []*Vector, selected []int64, domain int, mp *mpool.MPool, account *AllocationAccountSelection) (retained bool, retErr error) {
	return TrySelectDictionaryVectorsReuse(sources, destinations, selected, domain, mp, account, nil)
}

// TrySelectDictionaryVectorsReuse preserves transactional destination publication
// and shares mappings while taking eligible private headers from reusable slots.
func TrySelectDictionaryVectorsReuse(sources, destinations []*Vector, selected []int64, domain int, mp *mpool.MPool, account *AllocationAccountSelection, reuse []*Vector) (retained bool, retErr error) {
	return TrySelectDictionaryVectorsWithWorkspace(sources, destinations, selected, domain, mp, account, reuse, nil, nil)
}

// TrySelectDictionaryVectorsWithWorkspace reuses cleared publication headers,
// private immutable snapshots and one selection bank owned by the execution lane.
func TrySelectDictionaryVectorsWithWorkspace(sources, destinations []*Vector, selected []int64, domain int, mp *mpool.MPool, account *AllocationAccountSelection, reuse []*Vector, snapshots []*DictionaryVectorSlot, selectionSlot *DictionaryVectorSlot) (retained bool, retErr error) {
	if snapshots != nil && len(snapshots) != len(sources) {
		return false, moerr.NewInvalidInputNoCtx("invalid dictionary snapshot slots")
	}
	if reuse != nil && len(reuse) != len(sources) {
		return false, moerr.NewInvalidInputNoCtx("invalid reusable dictionary slots")
	}

	if len(sources) != len(destinations) || mp == nil {
		return false, moerr.NewInvalidInputNoCtx("invalid dictionary vector destinations")
	}
	for i, source := range sources {
		if destinations[i] != nil {
			return false, moerr.NewInvalidInputNoCtx("dictionary destination must be empty")
		}
		if source == nil || source.typ.TypeSize() <= 0 {
			return false, nil
		}
		if !source.CoversLogicalRows(0, domain) {
			return false, moerr.NewInvalidInputNoCtx("dictionary input row domain mismatch")
		}
		if source.allocationAccount != nil && !dictionaryAccountsMatch(source.allocationAccount, account) {
			return false, nil
		}
	}
	defer func() {
		if !retained {
			for i, dst := range destinations {
				if dst != nil {
					dst.Free(mp)
					destinations[i] = nil
				}
			}
		}
		if errors.Is(retErr, mpool.ErrAllocationAccountCapacity) {
			retErr = nil
		}
	}()
	rows, err := NewRowSelectionReuse(selected, domain, mp, account, selectionSlot)
	if err != nil {
		return false, err
	}
	defer rows.Release()
	var mappings map[*RowSelection]*RowSelection
	var bases map[*Vector]*DictionaryBase
	defer func() {
		for _, mapping := range mappings {
			mapping.Release()
		}
		for _, base := range bases {
			base.Release()
		}
	}()
	for i, source := range sources {
		var spare **Vector
		if reuse != nil {
			spare = &reuse[i]
		}
		if source.IsDictionary() {
			backing := source.dictionaryState()
			mapping := mappings[backing.rows]
			if mapping == nil {
				mapping, err = composeDictionaryRows(backing.rows, selected, mp, account)
				if err != nil {
					return false, err
				}
				if mappings == nil {
					mappings = make(map[*RowSelection]*RowSelection)
				}
				mappings[backing.rows] = mapping
			}
			destinations[i], err = newDictionaryHeaderReuse(backing.base, mapping, source, selected, mp, account, spare)
		} else {
			base := bases[source]
			if base == nil {
				var admitted bool
				var slot *DictionaryVectorSlot
				if snapshots != nil {
					slot = snapshots[i]
				}
				base, admitted, err = TryFreezeVectorReuse(source, mp, account, slot)
				if err != nil {
					return false, err
				}
				if !admitted {
					return false, nil
				}
				if bases == nil {
					bases = make(map[*Vector]*DictionaryBase, len(sources))
				}
				bases[source] = base
			}
			destinations[i], err = NewDictionaryVectorReuse(base, rows, mp, account, spare)

		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}
