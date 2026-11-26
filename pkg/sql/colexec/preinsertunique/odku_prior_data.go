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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (preInsertUnique *PreInsertUnique) resolveODKUKey(proc *process.Process, slot int, key *vector.Vector, row int, basePK *vector.Vector, baseRow int) (int64, bool, error) {
	owner, found, err := preInsertUnique.lookupODKUKeyVector(proc, slot, key, row)
	if err != nil || found || key.IsNull(uint64(row)) {
		return owner, found, err
	}
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	constraint := c.Constraints[slot]
	tableID := uint64(constraint.LookupRef.Obj)
	lookupKey, lookupRow := key, row
	if constraint.LookupRef.Obj == c.PriorData.Ref.Obj {
		lookupKey, lookupRow = basePK, baseRow
	}
	prior, err := preInsertUnique.PriorDataView.Lookup(proc.Ctx, tableID, lookupKey, lookupRow)
	if err != nil || prior == nil {
		return 0, false, err
	}
	defer prior.Clean(proc.Mp())
	original := prior
	if constraint.LookupRef.Obj != c.PriorData.Ref.Obj {
		p := slices.Index(prior.Attrs, catalog.IndexTablePrimaryColName)
		if p < 0 || p >= len(prior.Vecs) || prior.RowCount() != 1 || prior.Vecs[p] == nil || prior.Vecs[p].Length() != 1 ||
			!odkuKeyTypeEqual(*a.old.Vecs[c.PkColumn].GetType(), *prior.Vecs[p].GetType()) {
			return 0, false, moerr.NewInternalError(proc.Ctx, "invalid ODKU prior unique row")
		}
		original, err = preInsertUnique.PriorDataView.Lookup(proc.Ctx, uint64(c.PriorData.Ref.Obj), prior.Vecs[p], 0)
		if err != nil {
			return 0, false, err
		}
		if original == nil {
			return 0, false, moerr.NewInternalError(proc.Ctx, "ODKU prior index is missing its base row")
		}
		defer original.Clean(proc.Mp())
	}
	if original.RowCount() != 1 {
		return 0, false, moerr.NewInternalError(proc.Ctx, "invalid ODKU prior base row")
	}
	// The view is an engine boundary, unlike retained action images. Validate
	// the required prefix once before any indexed use; extra columns are allowed.
	for i, dst := range a.old.Vecs {
		if i >= len(original.Vecs) || original.Vecs[i] == nil || original.Vecs[i].Length() != 1 ||
			!odkuKeyTypeEqual(*dst.GetType(), *original.Vecs[i].GetType()) {
			return 0, false, moerr.NewInternalError(proc.Ctx, "invalid ODKU input vector")
		}
	}
	if original.Vecs[c.RowIdColumn].IsNull(0) || original.Vecs[c.PkColumn].IsNull(0) {
		return 0, false, moerr.NewInternalError(proc.Ctx, "ODKU prior row is missing its identity")
	}
	owner, found, err = preInsertUnique.lookupODKUOrigin(proc, original.Vecs[c.RowIdColumn], 0)
	if err != nil {
		return 0, false, err
	}
	if found {
		// The snapshot can still return an original key that an earlier action
		// vacated. Its RowID remains known, but it is no longer a conflict claim.
		matches := sameODKUKey(preInsertUnique.ctr.actionRows.keys.Vecs[slot], int(owner), key, row)
		return owner, matches, nil
	}
	// Only a newly discovered original needs owned scratch. Known RowIDs,
	// including vacated keys, already have immutable originals and current images.
	if err = preInsertUnique.copyODKURow(proc, a.old, original, 0, nil); err != nil {
		return 0, false, err
	}
	copy(a.priorImage.Vecs, a.old.Vecs)
	copy(a.priorProposed.Vecs, a.incoming.Vecs)
	for i, p := range c.InputColumns {
		if p >= 0 {
			a.priorProposed.Vecs[p] = a.old.Vecs[i]
		}
	}
	for i, exec := range a.priorKeyExecs {
		value, e := exec.Eval(proc, []*batch.Batch{a.priorImage, a.incoming}, nil)
		if e != nil {
			return 0, false, e
		}
		a.priorKeys.Vecs[i] = value
	}
	owner = int64(preInsertUnique.ctr.actionRows.images.RowCount())
	preInsertUnique.snapshotODKUKeyGroups()
	conflict, _, err := preInsertUnique.publishODKUAction(proc, owner, a.priorProposed, a.priorKeys, a.old)
	if err != nil {
		return 0, false, err
	}
	if conflict >= 0 {
		return 0, false, preInsertUnique.odkuDuplicate(proc, conflict, a.priorKeys.Vecs[conflict], 0)
	}
	if err = preInsertUnique.retainODKULockKeys(proc, a.priorKeys, a.old.Vecs[c.PkColumn]); err != nil {
		return 0, false, err
	}
	return owner, true, nil
}

func (preInsertUnique *PreInsertUnique) snapshotODKUKeyGroups() {
	a := preInsertUnique.ctr.actions
	for i := range a.groupCounts {
		a.groupCounts[i] = preInsertUnique.ctr.acceptedKeys[i].index.GroupCount()
	}
}

// Existing key groups retain every old/new/transient claim. Only a fresh group
// needs another typed lock value; cycles and repeated payload actions retain no
// duplicate history. Base-table locks use the raw PK, not its comparison codec.
func (preInsertUnique *PreInsertUnique) retainODKULockKeys(proc *process.Process, keys *batch.Batch, basePK *vector.Vector) error {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	for slot, keep := range a.touchedKeys {
		if preInsertUnique.ctr.acceptedKeys[slot].index.GroupCount() == a.groupCounts[slot] {
			continue
		}
		source := keys.Vecs[slot]
		if c.Constraints[slot].LookupRef.Obj == c.PriorData.Ref.Obj {
			source = basePK
		}
		if !source.IsNull(0) {
			if err := keep.UnionOne(source, 0, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) validateODKUPriorData(proc *process.Process) error {
	a := preInsertUnique.ctr.actions
	if a.priorValidated || preInsertUnique.PriorDataView == nil {
		return nil
	}
	for slot, keys := range a.touchedKeys {
		if keys.Length() != 0 {
			if err := preInsertUnique.LockKeys(proc, slot, keys); err != nil {
				return err
			}
		}
	}
	if err := preInsertUnique.PriorDataView.Validate(proc.Ctx); err != nil {
		return err
	}
	a.priorValidated = true
	return nil
}
