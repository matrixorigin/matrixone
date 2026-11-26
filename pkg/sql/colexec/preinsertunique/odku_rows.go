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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// odkuActionRows owns current typed images and their physical constraint keys.
// The row position is the stable logical owner used by acceptedKeys. Origins
// preserve the writer's original physical identity; originKey maps a frozen
// RowID hit back to that same owner after its current keys have moved.
type odkuActionRows struct {
	images, keys, origins      *batch.Batch
	changes                    []odkuKeyChange
	mutable                    []int32
	imageColumns, keyColumns   []int32
	deadImageArea, deadKeyArea []int
	originRowIDColumn          int32
	originKey                  odkuKeyIndex
}

const odkuDeadAreaSlack = 64 << 10

// initODKUActionRows initializes an empty action state using the actual typed
// expression results. The origin layout contains the writer's old physical
// identities and a canonical RowID at originRowIDColumn. The
// caller must have prepared acceptedKeys for this execution attempt.
func (preInsertUnique *PreInsertUnique) initODKUActionRows(proc *process.Process, image, keys, origins *batch.Batch, originRowIDColumn int32, mutable []int32) (err error) {
	completed := false
	defer func() {
		if !completed {
			preInsertUnique.ctr.failed = true
		}
	}()
	if preInsertUnique.ctr.failed || preInsertUnique.ctr.actionRows != nil || image == nil || keys == nil || origins == nil ||
		len(image.Vecs) == 0 || len(keys.Vecs) == 0 || len(keys.Vecs) != len(preInsertUnique.ctr.acceptedKeys) ||
		originRowIDColumn < 0 || int(originRowIDColumn) >= len(origins.Vecs) || origins.Vecs[originRowIDColumn] == nil || origins.Vecs[originRowIDColumn].GetType().Oid != types.T_Rowid || preInsertUnique.retainedAllocation == nil {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU action state")
	}
	for _, key := range preInsertUnique.ctr.acceptedKeys {
		if key.index != nil || key.rows != nil {
			return moerr.NewInternalError(proc.Ctx, "ODKU action state already initialized")
		}
	}
	if err = context.Cause(proc.Ctx); err != nil {
		return err
	}
	rows := &odkuActionRows{changes: make([]odkuKeyChange, len(keys.Vecs)), mutable: make([]int32, 0, len(mutable)), imageColumns: make([]int32, 0, len(image.Vecs)), deadImageArea: make([]int, len(image.Vecs)), originRowIDColumn: originRowIDColumn}
	preInsertUnique.ctr.actionRows = rows // Reset owns every successfully constructed vector, including on failure.
	seen := make([]bool, len(image.Vecs))
	for _, col := range mutable {
		if col < 0 || int(col) >= len(seen) {
			return moerr.NewInternalError(proc.Ctx, "invalid ODKU mutable column")
		}
		if !seen[col] {
			seen[col] = true
			rows.mutable = append(rows.mutable, col)
		}
	}
	newBatch := func(template *batch.Batch) (*batch.Batch, error) {
		b := batch.NewWithSize(len(template.Vecs))
		for i, v := range template.Vecs {
			if v == nil {
				b.Clean(proc.Mp())
				return nil, moerr.NewInternalError(proc.Ctx, "nil ODKU action vector")
			}
			var e error
			b.Vecs[i], e = vector.NewOffHeapVecWithTypeAndAllocation(*v.GetType(), preInsertUnique.retainedAllocation)
			if e != nil {
				b.Clean(proc.Mp())
				return nil, e
			}
		}
		return b, nil
	}
	if rows.images, err = newBatch(image); err != nil {
		return err
	}
	if preInsertUnique.ctr.actions == nil || !preInsertUnique.ctr.actions.staticKeys {
		rows.keyColumns, rows.deadKeyArea = make([]int32, len(keys.Vecs)), make([]int, len(keys.Vecs))
		for i := range rows.keyColumns {
			rows.keyColumns[i] = int32(i)
		}
		if rows.keys, err = newBatch(keys); err != nil {
			return err
		}
	}
	if rows.origins, err = newBatch(origins); err != nil {
		return err
	}
	// INSERT-only statements do not need an original-identity map. Keep its
	// type for lookups, and allocate the map only when an original is recorded.
	rows.originKey.keyType = *rows.origins.Vecs[originRowIDColumn].GetType()
	for slot := range keys.Vecs {
		preInsertUnique.ctr.acceptedKeys[slot].key[0] = keys.Vecs[slot]
		err = preInsertUnique.initODKUKey(proc, slot)
		preInsertUnique.ctr.acceptedKeys[slot].key[0] = nil
		if err != nil {
			return err
		}
	}
	completed = true
	return nil
}

// publishODKUAction borrows stable one-row execution scratch until it returns.
// Inputs must not alias retained rows or index storage. The execution owner
// copies retained values before evaluating assignments or prior keys. owner is
// either an existing row position or exactly RowCount for a new logical row.
// A newly registered snapshot row supplies its original physical identity;
// a newly inserted row has a nil origin. Once registered, an origin is fixed.
// A conflict, error, or panic makes the attempt terminal; no partial state may
// be emitted, and the owner must Reset before reuse.
func (preInsertUnique *PreInsertUnique) publishODKUAction(proc *process.Process, owner int64, image, keys, origin *batch.Batch) (slot int, other int64, err error) {
	completed := false
	defer func() {
		if !completed {
			preInsertUnique.ctr.failed = true
		}
	}()
	rows := preInsertUnique.ctr.actionRows
	if preInsertUnique.ctr.failed || rows == nil || owner < 0 || owner > int64(rows.images.RowCount()) {
		return -1, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU action owner")
	}
	if err = context.Cause(proc.Ctx); err != nil {
		return -1, 0, err
	}
	inserting := owner == int64(rows.images.RowCount())
	if !inserting && origin != nil {
		return -1, 0, moerr.NewInternalError(proc.Ctx, "ODKU original identity is immutable")
	}
	if inserting && origin != nil {
		if err = validateODKUActionRow(proc, rows.origins, origin); err != nil {
			return -1, 0, err
		}
		originalKey := origin.Vecs[rows.originRowIDColumn]
		if originalKey.IsNull(0) {
			return -1, 0, moerr.NewInternalError(proc.Ctx, "NULL ODKU original identity")
		}
		// All production callers have just resolved this RowID through the
		// sole owner. The native claim below remains the integrity check.
	}
	if err = validateODKUActionRow(proc, rows.images, image); err != nil {
		return -1, 0, err
	}
	// Reuse the bounded column scratch. Always refill it for a new owner;
	// updates leave immutable table columns and identical values untouched.
	rows.imageColumns = rows.imageColumns[:0]
	if inserting {
		for i := range image.Vecs {
			rows.imageColumns = append(rows.imageColumns, int32(i))
		}
	} else {
		for _, column := range rows.mutable {
			current, proposed := rows.images.Vecs[column], image.Vecs[column]
			if current.GetGrouping().Contains(uint64(owner)) != proposed.GetGrouping().Contains(0) ||
				!sameODKUKey(current, int(owner), proposed, 0) ||
				(proposed.GetType().IsVarlen() && proposed.IsNull(0)) {
				rows.imageColumns = append(rows.imageColumns, column)
			}
		}
	}
	imageColumns := rows.imageColumns
	if err = preInsertUnique.validateODKUActionKeys(proc, keys); err != nil {
		return -1, 0, err
	}
	// Static snapshot keys already belong to storage; retain only their RowID
	// identity and current image. Insert claims still use the normal native map.
	snapshot := rows.keys == nil && (origin != nil || (!inserting && !rows.origins.Vecs[rows.originRowIDColumn].IsNull(uint64(owner))))
	keysUnchanged := snapshot || !inserting
	if !inserting && !snapshot {
		for i, proposed := range keys.Vecs {
			index, current := &preInsertUnique.ctr.acceptedKeys[i], preInsertUnique.retainedODKUActionKey(i)
			if index.index == nil || index.iterator == nil || index.rows == nil {
				return -1, 0, moerr.NewInternalError(proc.Ctx, "invalid ODKU action key")
			}
			// The sole owner already claims every retained key. Identical
			// physical keys require no index transition, history or copying.
			// GROUPING has its own hash grammar and must use the full path.
			keysUnchanged = keysUnchanged && !current.HasGrouping() && !proposed.HasGrouping() &&
				sameODKUKey(current, int(owner), proposed, 0)
		}
	}
	if !keysUnchanged {
		defer clear(rows.changes) // Do not retain borrowed expression results, even on failure.
		for i := range rows.changes {
			rows.changes[i] = odkuKeyChange{next: keys.Vecs[i], nextRow: 0}
			if !inserting {
				rows.changes[i].old = preInsertUnique.retainedODKUActionKey(i)
				rows.changes[i].oldRow = int(owner)
			}
		}
		slot, other, err = preInsertUnique.transitionODKUKeys(proc, owner, rows.changes)
		if err != nil || slot >= 0 {
			return slot, other, err
		}
	}
	if err = context.Cause(proc.Ctx); err != nil {
		return -1, 0, err
	}
	if err = rows.copyInto(proc, rows.images, image, int(owner), inserting, rows.deadImageArea, imageColumns, preInsertUnique); err != nil {
		return -1, 0, err
	}
	if !keysUnchanged && rows.keys != nil {
		if err = rows.copyInto(proc, rows.keys, keys, int(owner), inserting, rows.deadKeyArea, rows.keyColumns, preInsertUnique); err != nil {
			return -1, 0, err
		}
	}
	if inserting {
		for i, dst := range rows.origins.Vecs {
			if err = context.Cause(proc.Ctx); err != nil {
				return -1, 0, err
			}
			if origin == nil {
				err = dst.UnionNull(proc.Mp())
			} else {
				err = dst.UnionOne(origin.Vecs[i], 0, proc.Mp())
			}
			if err != nil {
				return -1, 0, err
			}
		}
		rows.origins.AddRowCount(1)
		if origin != nil {
			key := &rows.originKey
			err = func() error {
				previous := key.key[0]
				key.key[0] = origin.Vecs[rows.originRowIDColumn]
				defer func() { key.key[0] = previous }()
				if key.index == nil {
					if err := preInsertUnique.initODKUKeyIndex(proc, key); err != nil {
						return err
					}
				}
				return preInsertUnique.claimODKUKeyIndex(proc, key, 0, owner)
			}()
			if err != nil {
				return -1, 0, err
			}
		}
	}
	for i := range preInsertUnique.ctr.acceptedKeys {
		// Mutable-key attempts retain the old groups until their distinct
		// transient keys have been locked; the same groups deduplicate that history.
		if rows.keys != nil && preInsertUnique.PreInsertCtx.GetOdkuActions().GetPriorData() == nil && preInsertUnique.ctr.acceptedKeys[i].shouldRebuild() {
			// Logical owners are dense positions in keys; nil avoids an
			// additional row-sized, separately-accounted owners array.
			if err = preInsertUnique.rebuildODKUKeyIndex(proc, i, rows.keys.Vecs[i], nil); err != nil {
				return -1, 0, err
			}
		}
	}
	completed = true
	return -1, 0, nil
}

func (preInsertUnique *PreInsertUnique) retainedODKUActionKey(slot int) *vector.Vector {
	rows := preInsertUnique.ctr.actionRows
	if rows.keys != nil {
		return rows.keys.Vecs[slot]
	}
	c := preInsertUnique.PreInsertCtx.OdkuActions
	return rows.images.Vecs[c.InputColumns[c.Constraints[slot].CurrentKey.GetCol().ColPos]]
}

func (preInsertUnique *PreInsertUnique) validateODKUActionKeys(proc *process.Process, keys *batch.Batch) error {
	if keys == nil || keys.RowCount() != 1 || len(keys.Vecs) != len(preInsertUnique.ctr.acceptedKeys) {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU action keys")
	}
	for i, key := range keys.Vecs {
		if key == nil || key.Length() != 1 || !odkuKeyTypeEqual(*key.GetType(), preInsertUnique.ctr.acceptedKeys[i].keyType) {
			return moerr.NewInternalError(proc.Ctx, "invalid ODKU action key column")
		}
	}
	return nil
}

// lookupODKUOrigin uses the immutable statement-start physical RowID,
// not a current constraint key. Its owner survives key moves and key reuse.
func (preInsertUnique *PreInsertUnique) lookupODKUOrigin(proc *process.Process, source *vector.Vector, row int) (owner int64, found bool, err error) {
	completed := false
	defer func() {
		if !completed {
			preInsertUnique.ctr.failed = true
		}
	}()
	rows := preInsertUnique.ctr.actionRows
	if preInsertUnique.ctr.failed || rows == nil || source == nil || row < 0 || row >= source.Length() ||
		source == rows.originKey.fixedKeys || source == rows.originKey.rows ||
		!odkuKeyTypeEqual(*source.GetType(), rows.originKey.keyType) {
		return 0, false, moerr.NewInternalError(proc.Ctx, "invalid ODKU original identity lookup")
	}
	if err = context.Cause(proc.Ctx); err != nil {
		return 0, false, err
	}
	key := &rows.originKey
	previous := key.key[0]
	key.key[0] = source
	defer func() { key.key[0] = previous }()
	owner, found, err = preInsertUnique.lookupODKUKeyIndex(proc, key, row)
	if err != nil {
		return 0, false, err
	}
	completed = true
	return owner, found, nil
}

func validateODKUActionRow(proc *process.Process, dst, src *batch.Batch) error {
	if src == nil || src.RowCount() != 1 || len(dst.Vecs) != len(src.Vecs) {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU action image")
	}
	for i, v := range src.Vecs {
		if v == nil || v.Length() != 1 || !odkuKeyTypeEqual(*dst.Vecs[i].GetType(), *v.GetType()) {
			return moerr.NewInternalError(proc.Ctx, "invalid ODKU action column")
		}
	}
	return nil
}

func (rows *odkuActionRows) copyInto(proc *process.Process, dst, src *batch.Batch, owner int, inserting bool, dead []int, columns []int32, pre *PreInsertUnique) error {
	for _, column := range columns {
		i := int(column)
		v := dst.Vecs[i]
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		oldBytes := 0
		if !inserting && v.GetType().IsVarlen() && !v.IsNull(uint64(owner)) {
			n := len(v.GetBytesAt(owner))
			if n > types.VarlenaInlineSize {
				oldBytes = n
			}
		}
		var err error
		if inserting {
			err = v.UnionOne(src.Vecs[i], 0, proc.Mp())
		} else {
			err = v.Copy(src.Vecs[i], int64(owner), 0, proc.Mp())
		}
		if err != nil {
			return err
		}
		dead[i] += oldBytes
		if dead[i] >= odkuDeadAreaSlack && dead[i] >= len(v.GetArea())/2 {
			compact, e := v.CloneToFlatCompactWithAllocation(proc.Mp(), pre.retainedAllocation)
			if e != nil {
				return e
			}
			dst.Vecs[i] = compact
			v.Free(proc.Mp())
			dead[i] = 0
		}
	}
	if inserting {
		dst.AddRowCount(1)
	}
	return nil
}

func (rows *odkuActionRows) free(proc *process.Process) {
	rows.originKey.free(proc)
	for _, b := range []*batch.Batch{rows.images, rows.keys, rows.origins} {
		if b != nil {
			b.Clean(proc.Mp())
		}
	}
	*rows = odkuActionRows{}
}
