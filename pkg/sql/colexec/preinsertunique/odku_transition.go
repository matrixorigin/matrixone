// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preinsertunique

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// odkuKeyChange borrows the old and proposed physical key for one constraint.
// old is nil only for a newly inserted logical row; next may contain SQL NULL.
// Each logical row owns at most one key per constraint. The action owner must
// have already registered every non-NULL old key and checked the proposed keys
// against the statement's prior-data view. Neither vector may alias index-owned
// storage, which can be reallocated while a transition is being published.
type odkuKeyChange struct {
	old, next       *vector.Vector
	oldRow, nextRow int
	nextOwned       bool // Scratch from this call's validation phase only.
}

// transitionODKUKeys checks all current statement-owned keys before publishing
// any key change. A conflict returns its constraint and current owner without
// modifying logical ownership. A conflict or any error makes the entire action
// attempt terminal: its caller must Reset before reuse. Keys found only in the
// prior-data view are the caller's responsibility.
func (preInsertUnique *PreInsertUnique) transitionODKUKeys(proc *process.Process, owner int64, changes []odkuKeyChange) (int, int64, error) {
	completed := false
	defer func() {
		if !completed {
			preInsertUnique.ctr.failed = true
		}
	}()
	if preInsertUnique.ctr.failed || owner < 0 || len(changes) == 0 || len(changes) != len(preInsertUnique.ctr.acceptedKeys) {
		return -1, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU key transition")
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return -1, 0, err
	}
	for slot := range changes {
		change := &changes[slot]
		change.nextOwned = false
		key := &preInsertUnique.ctr.acceptedKeys[slot]
		if key.index == nil || key.iterator == nil || key.rows == nil ||
			change.next == nil || change.next == key.fixedKeys || change.next == key.rows ||
			(change.old != nil && (change.old == key.fixedKeys || change.old == key.rows)) ||
			change.nextRow < 0 || change.nextRow >= change.next.Length() ||
			!odkuKeyTypeEqual(key.keyType, *change.next.GetType()) ||
			(change.old != nil && (change.oldRow < 0 || change.oldRow >= change.old.Length() || !odkuKeyTypeEqual(key.keyType, *change.old.GetType()))) {
			return -1, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU transition key")
		}
		oldPresent := change.old != nil && !change.old.IsNull(uint64(change.oldRow))
		if oldPresent {
			current, found, e := preInsertUnique.lookupODKUKeyVector(proc, slot, change.old, change.oldRow)
			if e != nil {
				return -1, 0, e
			}
			if !found || current != owner {
				return -1, 0, moerr.NewInternalError(proc.Ctx, "ODKU transition old key owner mismatch")
			}
		}
		if change.next.IsNull(uint64(change.nextRow)) {
			continue
		}
		current, found, e := preInsertUnique.lookupODKUKeyVector(proc, slot, change.next, change.nextRow)
		if e != nil {
			return -1, 0, e
		}
		if found && current != owner {
			return slot, current, nil
		}
		if found && !oldPresent {
			return -1, 0, moerr.NewInternalError(proc.Ctx, "ODKU transition owner already has a key")
		}
		change.nextOwned = found
	}
	for slot, change := range changes {
		if err := context.Cause(proc.Ctx); err != nil {
			return -1, 0, err
		}
		if change.nextOwned {
			continue
		}
		if change.old != nil && !change.old.IsNull(uint64(change.oldRow)) {
			if e := preInsertUnique.mutateODKUKeyVector(proc, slot, change.old, change.oldRow, owner, false); e != nil {
				return -1, 0, e
			}
		}
		if !change.next.IsNull(uint64(change.nextRow)) {
			if e := preInsertUnique.mutateODKUKeyVector(proc, slot, change.next, change.nextRow, owner, true); e != nil {
				return -1, 0, e
			}
		}
	}
	completed = true
	return -1, 0, nil
}

func (preInsertUnique *PreInsertUnique) lookupODKUKeyVector(proc *process.Process, slot int, keys *vector.Vector, row int) (int64, bool, error) {
	key := &preInsertUnique.ctr.acceptedKeys[slot]
	previous := key.key[0]
	key.key[0] = keys
	defer func() { key.key[0] = previous }()
	return preInsertUnique.lookupODKUKeyIndex(proc, key, row)
}

func (preInsertUnique *PreInsertUnique) mutateODKUKeyVector(proc *process.Process, slot int, keys *vector.Vector, row int, owner int64, claim bool) error {
	key := &preInsertUnique.ctr.acceptedKeys[slot]
	previous := key.key[0]
	key.key[0] = keys
	defer func() { key.key[0] = previous }()
	if claim {
		return preInsertUnique.claimODKUKey(proc, slot, row, owner)
	}
	return preInsertUnique.releaseODKUKey(proc, slot, row, owner)
}
