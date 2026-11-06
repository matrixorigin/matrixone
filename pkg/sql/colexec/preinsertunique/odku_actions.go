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
	"bytes"
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/odkueval"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// odkuActions owns execution scratch. Retained current images, constraint keys
// and original identities have exactly one owner, ctr.actionRows.
type odkuActions struct {
	assignments, keyExecs, priorKeyExecs []colexec.ExpressionExecutor
	changeExecs                          []colexec.ExpressionExecutor
	insertChecks, updateChecks           []odkueval.Check
	fkChanges                            [][]colexec.ExpressionExecutor
	incoming, current, old, nullImage    *batch.Batch
	image, before, proposed, inputImage  *batch.Batch
	updateInputs                         [2]*batch.Batch
	keys, lastFKEligibility              *batch.Batch
	eligible                             []bool
	priorImage, priorKeys, priorProposed *batch.Batch
	touchedKeys                          []*vector.Vector
	groupCounts                          []uint64
	priorValidated                       bool
	staticKeys                           bool

	source              *batch.Batch
	sourceRow, finalRow int
	inputDone           bool
	targetReady         bool
	target              int64
	logicalAffected     uint64
	countPublished      bool
}

func odkuType(t plan.Type) types.Type {
	return types.New(types.T(t.Id), t.Width, t.Scale)
}

// Retained table columns change only through ordered assignments. Non-table
// source values still follow the latest action, including materialized RHSs.
func odkuMutableColumns(c *plan.OdkuActionCtx) []int32 {
	table := make([]bool, len(c.SourceColumns))
	all := false
	for _, p := range c.InputColumns {
		if p >= 0 {
			all = all || table[p]
			table[p] = true
		}
	}
	mutable := make([]bool, len(table))
	for i := range mutable {
		mutable[i] = !table[i]
	}
	for _, column := range c.UpdateColumns {
		if p := c.InputColumns[column]; p >= 0 {
			mutable[p] = true
		} else {
			all = true
		}
	}
	for _, fk := range c.ForeignKeyChecks {
		mutable[fk.Eligibility.GetCol().ColPos] = true
	}
	if c.ActionFinalColumn >= 0 {
		mutable[c.ActionFinalColumn] = true
	}
	columns := make([]int32, 0, len(mutable))
	for i, source := range c.SourceColumns {
		if all || mutable[i] || source.GetCol().ColPos == c.SourceOrdinal.GetCol().ColPos {
			columns = append(columns, int32(i))
		}
	}
	return columns
}

// With plain immutable keys and no prior-data/FK suspension, snapshot indexes
// own original keys and the RowID map owns their current images. Only successful
// inserts need statement key claims. Without a final-FK selector the owner
// consumes every source action before emitting its first row, so writes cannot
// change a later snapshot lookup within this statement.
func hasStaticODKUKeys(c *plan.OdkuActionCtx) bool {
	if c.PriorData != nil || len(c.ForeignKeyChecks) != 0 || c.ActionFinalColumn >= 0 {
		return false
	}
	seen := make([]bool, len(c.SourceColumns))
	for _, position := range c.InputColumns {
		if position >= 0 {
			if seen[position] {
				return false
			}
			seen[position] = true
		}
	}
	for _, key := range c.Constraints {
		column := key.CurrentKey.GetCol()
		if column == nil || column.RelPos != 0 || c.InputColumns[column.ColPos] < 0 {
			return false
		}
		for _, updated := range c.UpdateColumns {
			if updated == column.ColPos {
				return false
			}
		}
	}
	return true
}

func (preInsertUnique *PreInsertUnique) prepareODKUActions(proc *process.Process) error {
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	c := preInsertUnique.PreInsertCtx.OdkuActions
	left, right := make([]types.Type, len(c.OldColumns)), make([]types.Type, len(c.SourceColumns))
	for i, e := range c.OldColumns {
		left[i] = odkuType(e.Typ)
	}
	for i, e := range c.SourceColumns {
		right[i] = odkuType(e.Typ)
	}
	keys := make([]*plan.Expr, len(c.Constraints))
	for i, key := range c.Constraints {
		keys[i] = key.CurrentKey
	}
	a := &odkuActions{eligible: make([]bool, len(c.ForeignKeyChecks)), staticKeys: hasStaticODKUKeys(c)}
	if c.PriorData != nil && (preInsertUnique.PriorDataView == nil || preInsertUnique.LockKeys == nil) {
		return moerr.NewInternalError(proc.Ctx, "ODKU prior-data view is not bound to the coordinator")
	}
	preInsertUnique.ctr.actions = a // Reset owns partial preparation too.
	preInsertUnique.ctr.acceptedKeys = make([]odkuKeyIndex, len(keys))
	var err error
	if a.assignments, err = hashbuild.NewExpressionExecutors(proc, c.UpdateExprs, preInsertUnique.allocationAccount); err != nil {
		return err
	}
	if a.changeExecs, err = odkueval.CompileUpdateChanges(c.ChangeCheckColumns, left, proc, preInsertUnique.allocationAccount); err != nil {
		return err
	}
	if !a.staticKeys {
		if a.keyExecs, err = hashbuild.NewExpressionExecutors(proc, keys, preInsertUnique.allocationAccount); err != nil {
			return err
		}
	}
	if a.insertChecks, err = odkueval.CompileChecks(c.InsertChecks, left, right, proc, preInsertUnique.allocationAccount); err != nil {
		return err
	}
	if a.updateChecks, err = odkueval.CompileChecks(c.UpdateChecks, left, right, proc, preInsertUnique.allocationAccount); err != nil {
		return err
	}
	if a.fkChanges, err = odkueval.CompileFKChanges(c.ForeignKeyChecks, left, right, proc, preInsertUnique.allocationAccount); err != nil {
		return err
	}
	newBatch := func(ts []types.Type) (*batch.Batch, error) {
		b := batch.NewWithSize(len(ts))
		for i, t := range ts {
			b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(t, preInsertUnique.retainedAllocation)
			if err != nil {
				b.Clean(proc.Mp())
				return nil, err
			}
		}
		return b, nil
	}
	for _, dst := range []**batch.Batch{&a.incoming, &a.current} {
		if *dst, err = newBatch(right); err != nil {
			return err
		}
	}
	for _, dst := range []**batch.Batch{&a.old, &a.nullImage} {
		if *dst, err = newBatch(left); err != nil {
			return err
		}
	}
	if c.ActionFinalColumn >= 0 {
		ts := make([]types.Type, len(c.ForeignKeyChecks))
		for i := range ts {
			ts[i] = types.T_bool.ToType()
		}
		if a.lastFKEligibility, err = newBatch(ts); err != nil {
			return err
		}
	}
	if c.PriorData != nil {
		if a.priorKeyExecs, err = hashbuild.NewExpressionExecutors(proc, keys, preInsertUnique.allocationAccount); err != nil {
			return err
		}
		a.priorImage = batch.NewWithSize(len(left))
		a.priorImage.SetRowCount(1)
		a.priorKeys = batch.NewWithSize(len(keys))
		a.priorKeys.SetRowCount(1)
		a.priorProposed = batch.NewWithSize(len(right))
		a.priorProposed.SetRowCount(1)
		a.groupCounts = make([]uint64, len(keys))
		a.touchedKeys = make([]*vector.Vector, len(keys))
		for i, key := range c.Constraints {
			typ := odkuType(key.CurrentKey.Typ)
			if key.LookupRef.Obj == c.PriorData.Ref.Obj {
				typ = left[c.PkColumn]
			}
			if a.touchedKeys[i], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, preInsertUnique.retainedAllocation); err != nil {
				return err
			}
		}
	}
	for _, v := range a.nullImage.Vecs {
		if err = v.UnionNull(proc.Mp()); err != nil {
			return err
		}
	}
	a.nullImage.SetRowCount(1)
	a.image, a.before, a.inputImage = batch.NewWithSize(len(left)), batch.NewWithSize(len(left)), batch.NewWithSize(len(left))
	a.proposed, a.keys = batch.NewWithSize(len(right)), batch.NewWithSize(len(keys))
	for _, b := range []*batch.Batch{a.image, a.before, a.inputImage, a.proposed, a.keys} {
		b.SetRowCount(1)
	}
	a.updateInputs = [2]*batch.Batch{a.image, a.incoming}
	return nil
}

// Validate the complete prefetched layout before indexed reads. In particular,
// key and ordinal columns need not be part of the copied incoming image.
func (preInsertUnique *PreInsertUnique) validateODKUSource(proc *process.Process, b *batch.Batch) error {
	c := preInsertUnique.PreInsertCtx.OdkuActions
	validate := func(e *plan.Expr) error {
		p := e.GetCol().ColPos
		if p < 0 || int(p) >= len(b.Vecs) || b.Vecs[p] == nil || b.Vecs[p].Length() != b.RowCount() ||
			!odkuKeyTypeEqual(*b.Vecs[p].GetType(), odkuType(e.Typ)) {
			return moerr.NewInternalError(proc.Ctx, "invalid ODKU source vector layout")
		}
		return nil
	}
	for _, list := range [][]*plan.Expr{c.SourceColumns, c.OldColumns} {
		for _, e := range list {
			if err := validate(e); err != nil {
				return err
			}
		}
	}
	for _, key := range c.Constraints {
		if err := validate(key.InputKey); err != nil {
			return err
		}
		if err := validate(key.PriorTarget); err != nil {
			return err
		}
	}
	return validate(c.SourceOrdinal)
}

// Child batches and prior-view rows are validated at their input boundaries;
// retained rows are published only after action-layout validation succeeds.
func (preInsertUnique *PreInsertUnique) copyODKURow(proc *process.Process, dst, src *batch.Batch, row int, positions []*plan.Expr) error {
	for i, v := range dst.Vecs {
		p := i
		if positions != nil {
			p = int(positions[i].GetCol().ColPos)
		}
		// This scratch is flat and owned. Fixed-width replacement reuses its
		// row and bitmaps; varlen replacement must discard dead payload.
		if v.Length() == 1 && v.GetType().IsFixedLen() {
			v.SetSorted(false)
			if err := v.Copy(src.Vecs[p], 0, int64(row), proc.Mp()); err != nil {
				return err
			}
		} else {
			v.CleanOnlyData()
			if err := v.UnionOne(src.Vecs[p], int64(row), proc.Mp()); err != nil {
				return err
			}
		}
	}
	dst.SetRowCount(1)
	return nil
}

func (preInsertUnique *PreInsertUnique) evalODKUKeys(proc *process.Process) error {
	a := preInsertUnique.ctr.actions
	if a.staticKeys {
		for i, key := range preInsertUnique.PreInsertCtx.OdkuActions.Constraints {
			a.keys.Vecs[i] = a.image.Vecs[key.CurrentKey.GetCol().ColPos]
		}
		return nil
	}
	for i, exec := range a.keyExecs {
		v, err := exec.Eval(proc, []*batch.Batch{a.image, a.incoming}, nil)
		if err != nil {
			return err
		}
		a.keys.Vecs[i] = v
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) odkuDuplicate(proc *process.Process, slot int, key *vector.Vector, row int) error {
	c := preInsertUnique.PreInsertCtx.OdkuActions.Constraints[slot]
	value, err := colexec.FormatDedupKey(key, row, c.Types)
	if err != nil {
		return err
	}
	return moerr.NewDuplicateEntry(proc.Ctx, value, c.Name)
}

func sameODKUKey(a *vector.Vector, ar int, b *vector.Vector, br int) bool {
	an, bn := a.IsNull(uint64(ar)), b.IsNull(uint64(br))
	return an == bn && (an || bytes.Equal(a.GetRawBytesAt(ar), b.GetRawBytesAt(br)))
}

func (preInsertUnique *PreInsertUnique) registerODKUOriginal(proc *process.Process, slot int) (int64, error) {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	prior := a.source.Vecs[c.Constraints[slot].PriorTarget.GetCol().ColPos]
	// The source is a frozen snapshot lookup. A previously retained original
	// already owns its current image, so identify it before copying the payload.
	pk := a.source.Vecs[c.OldColumns[c.PkColumn].GetCol().ColPos]
	rowID := a.source.Vecs[c.OldColumns[c.RowIdColumn].GetCol().ColPos]
	if rowID.IsNull(uint64(a.sourceRow)) || !sameODKUKey(prior, a.sourceRow, pk, a.sourceRow) {
		return 0, moerr.NewInternalError(proc.Ctx, "missing prefetched ODKU original row")
	}
	owner, found, err := preInsertUnique.lookupODKUOrigin(proc, rowID, a.sourceRow)
	if err != nil || found {
		return owner, err
	}
	if err := preInsertUnique.copyODKURow(proc, a.old, a.source, a.sourceRow, c.OldColumns); err != nil {
		return 0, err
	}
	copy(a.image.Vecs, a.old.Vecs)
	copy(a.proposed.Vecs, a.incoming.Vecs)
	for i, p := range c.InputColumns {
		if p >= 0 {
			a.proposed.Vecs[p] = a.image.Vecs[i]
		}
	}
	if a.staticKeys {
		rows := preInsertUnique.ctr.actionRows
		for i, key := range c.Constraints {
			a.keys.Vecs[i] = a.old.Vecs[key.CurrentKey.GetCol().ColPos]
		}
		// Preserve the eager registration's validation/conflict boundary before
		// input checks or volatile RHSs. Only publication is delayed.
		for _, layout := range [][2]*batch.Batch{{rows.origins, a.old}, {rows.images, a.proposed}} {
			if err := validateODKUActionRow(proc, layout[0], layout[1]); err != nil {
				return 0, err
			}
		}
		if err := preInsertUnique.validateODKUActionKeys(proc, a.keys); err != nil {
			return 0, err
		}
		for i, key := range a.keys.Vecs {
			_, found, err := preInsertUnique.lookupODKUKeyVector(proc, i, key, 0)
			if err != nil {
				return 0, err
			}
			if found {
				return 0, preInsertUnique.odkuDuplicate(proc, i, key, 0)
			}
		}
		return int64(rows.images.RowCount()), nil
	}
	if err := preInsertUnique.evalODKUKeys(proc); err != nil {
		return 0, err
	}
	owner = int64(preInsertUnique.ctr.actionRows.images.RowCount())
	conflict, _, err := preInsertUnique.publishODKUAction(proc, owner, a.proposed, a.keys, a.old)
	if err != nil {
		return 0, err
	}
	if conflict >= 0 {
		return 0, preInsertUnique.odkuDuplicate(proc, conflict, a.keys.Vecs[conflict], 0)
	}
	return owner, nil
}

// Keep the most recent image pending until it is superseded or emitted as a
// final row. Only superseded images need transient FK validation; final images
// must reach the existing parent-lock/retry path before a missing-parent error.
func (preInsertUnique *PreInsertUnique) prepareODKUTarget(proc *process.Process) error {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	ordinal := a.source.Vecs[c.SourceOrdinal.GetCol().ColPos]
	ord := vector.GetFixedAtNoTypeCheck[int64](ordinal, a.sourceRow)
	if ordinal.IsNull(uint64(a.sourceRow)) || ord <= preInsertUnique.ctr.lastOrdinal {
		return moerr.NewInternalError(proc.Ctx, "ODKU source ordinals must be positive and strictly increasing")
	}
	if err := preInsertUnique.copyODKURow(proc, a.incoming, a.source, a.sourceRow, c.SourceColumns); err != nil {
		return err
	}
	owner := int64(-1)
	priorRows := preInsertUnique.ctr.actionRows.images.RowCount()
	for slot, key := range c.Constraints {
		if !key.TargetEligible {
			continue
		}
		input := a.source.Vecs[key.InputKey.GetCol().ColPos]
		if c.PriorData != nil {
			foundOwner, found, err := preInsertUnique.resolveODKUKey(proc, slot, input, a.sourceRow, a.incoming.Vecs[c.InputColumns[c.PkColumn]], 0)
			if err != nil {
				return err
			}
			if found {
				owner = foundOwner
				break
			}
			continue
		}
		foundOwner, found, err := preInsertUnique.lookupODKUKeyVector(proc, slot, input, a.sourceRow)
		if err != nil {
			return err
		}
		if found {
			owner = foundOwner
			break
		}
		if !a.source.Vecs[key.PriorTarget.GetCol().ColPos].IsNull(uint64(a.sourceRow)) {
			owner, err = preInsertUnique.registerODKUOriginal(proc, slot)
			if err != nil {
				return err
			}
			break
		}
	}
	if owner >= 0 && owner < int64(priorRows) && c.ActionFinalColumn >= 0 {
		validate := false
		for i, v := range a.lastFKEligibility.Vecs {
			a.eligible[i] = vector.GetFixedAtNoTypeCheck[bool](v, int(owner))
			validate = validate || a.eligible[i]
		}
		if validate {
			if err := preInsertUnique.appendODKUOutput(proc, int(owner), 1, false); err != nil {
				return err
			}
		}
	}
	a.target, a.targetReady = owner, true
	return nil
}

func (preInsertUnique *PreInsertUnique) applyODKUAction(proc *process.Process) error {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	owner := a.target
	freshOriginal := a.staticKeys && owner >= 0 && owner == int64(preInsertUnique.ctr.actionRows.images.RowCount())
	for i, p := range c.InputColumns {
		a.inputImage.Vecs[i] = a.nullImage.Vecs[i]
		if p >= 0 {
			a.inputImage.Vecs[i] = a.incoming.Vecs[p]
		}
	}
	copy(a.image.Vecs, a.inputImage.Vecs)
	insert := owner < 0
	changed := insert
	if insert {
		owner = int64(preInsertUnique.ctr.actionRows.images.RowCount())
		if err := odkueval.EvaluateChecks(a.insertChecks, a.image, a.incoming, proc); err != nil {
			return err
		}
		for i := range a.eligible {
			a.eligible[i] = true
		}
		for slot, key := range c.Constraints {
			if c.PriorData == nil && !a.source.Vecs[key.PriorTarget.GetCol().ColPos].IsNull(uint64(a.sourceRow)) {
				return preInsertUnique.odkuDuplicate(proc, slot, a.source.Vecs[key.InputKey.GetCol().ColPos], a.sourceRow)
			}
		}
	} else {
		if err := odkueval.EvaluateChecks(a.insertChecks[:c.InsertCheckConstraintCount], a.inputImage, a.incoming, proc); err != nil {
			return err
		}
		if !freshOriginal {
			if err := preInsertUnique.copyODKURow(proc, a.current, preInsertUnique.ctr.actionRows.images, int(owner), nil); err != nil {
				return err
			}
		}
		for i, p := range c.InputColumns {
			a.image.Vecs[i] = a.nullImage.Vecs[i]
			if p >= 0 {
				if freshOriginal {
					a.image.Vecs[i] = a.old.Vecs[i]
				} else {
					a.image.Vecs[i] = a.current.Vecs[p]
				}
			}
		}
		copy(a.before.Vecs, a.image.Vecs)
		prefix := int(c.ChangeCheckAssignmentCount)
		if err := odkueval.EvaluateUpdate(a.assignments[:prefix], c.UpdateColumns[:prefix], nil, a.updateInputs[:], proc); err != nil {
			return err
		}
		var err error
		changed, err = odkueval.UpdateChanged(a.changeExecs, c.ChangeCheckColumns, a.before, a.image, proc)
		if err != nil {
			return err
		}
		remaining := prefix
		if !changed {
			remaining += int(c.AutoUpdateAssignmentCount)
		}
		if err := odkueval.EvaluateUpdate(a.assignments[remaining:], c.UpdateColumns[remaining:], a.updateChecks, a.updateInputs[:], proc); err != nil {
			return err
		}
		if !changed {
			// SQL-equal assignments can have different representations. A no-op
			// retains the entire effective image for later actions and key lookup.
			copy(a.image.Vecs, a.before.Vecs)
		}
		if err := odkueval.EvaluateFKChanges(c.ForeignKeyChecks, a.fkChanges, a.before, a.image, a.eligible, proc); err != nil {
			return err
		}
	}
	if err := preInsertUnique.evalODKUKeys(proc); err != nil {
		return err
	}
	if c.PriorData != nil {
		for slot, key := range a.keys.Vecs {
			foundOwner, found, err := preInsertUnique.resolveODKUKey(proc, slot, key, 0, a.image.Vecs[c.PkColumn], 0)
			if err != nil {
				return err
			}
			if found && (insert || foundOwner != owner) {
				return preInsertUnique.odkuDuplicate(proc, slot, key, 0)
			}
		}
	} else if !insert {
		for slot, key := range a.keys.Vecs {
			original, row := preInsertUnique.retainedODKUActionKey(slot), int(owner)
			if freshOriginal {
				original, row = a.old.Vecs[c.Constraints[slot].CurrentKey.GetCol().ColPos], 0
			}
			if !sameODKUKey(original, row, key, 0) || original.GetGrouping().Contains(uint64(row)) != key.GetGrouping().Contains(0) {
				return moerr.NewInternalError(proc.Ctx, "ODKU changing key is missing prior-data metadata")
			}
		}
	}

	copy(a.proposed.Vecs, a.incoming.Vecs)
	for i, p := range c.InputColumns {
		if p >= 0 {
			a.proposed.Vecs[p] = a.image.Vecs[i]
		}
	}
	for i, fk := range c.ForeignKeyChecks {
		p := fk.Eligibility.GetCol().ColPos
		v := a.incoming.Vecs[p]
		v.CleanOnlyData()
		eligible := a.eligible[i] || (!insert && vector.GetFixedAtNoTypeCheck[bool](a.current.Vecs[p], 0))
		if err := vector.AppendFixed(v, eligible, false, proc.Mp()); err != nil {
			return err
		}
	}
	preInsertUnique.snapshotODKUKeyGroups()
	var origin *batch.Batch
	if freshOriginal {
		origin = a.old
	}
	conflict, _, err := preInsertUnique.publishODKUAction(proc, owner, a.proposed, a.keys, origin)
	if err != nil {
		return err
	}
	if conflict >= 0 {
		return preInsertUnique.odkuDuplicate(proc, conflict, a.keys.Vecs[conflict], 0)
	}
	if err := preInsertUnique.retainODKULockKeys(proc, a.keys, a.image.Vecs[c.PkColumn]); err != nil {
		return err
	}
	if a.lastFKEligibility != nil {
		for i, v := range a.lastFKEligibility.Vecs {
			if int(owner) == v.Length() {
				if err := vector.AppendFixed(v, a.eligible[i], false, proc.Mp()); err != nil {
					return err
				}
			} else {
				vector.SetFixedAtNoTypeCheck(v, int(owner), a.eligible[i])
			}
		}
		a.lastFKEligibility.SetRowCount(preInsertUnique.ctr.actionRows.images.RowCount())
	}
	preInsertUnique.ctr.lastOrdinal = vector.GetFixedAtNoTypeCheck[int64](a.source.Vecs[c.SourceOrdinal.GetCol().ColPos], a.sourceRow)
	weight := uint64(0)
	if insert {
		weight = 1
	} else if changed {
		weight = 2
	}
	if a.logicalAffected > math.MaxUint64-weight {
		return moerr.NewInternalError(proc.Ctx, "ODKU affected row count overflow")
	}
	a.logicalAffected += weight
	return nil
}

// Immutable columns are retained by construction. Compare the complete mutable
// stored-column closure and raw PK against the immutable original, excluding
// ordinals, RowIDs and FK/source metadata. SQL equality is insufficient here.
func (preInsertUnique *PreInsertUnique) physicalODKUChange(row int) bool {
	c, rows := preInsertUnique.PreInsertCtx.OdkuActions, preInsertUnique.ctr.actionRows
	if rows.origins.Vecs[c.RowIdColumn].IsNull(uint64(row)) {
		return true
	}
	for _, column := range c.UpdateColumns {
		position := c.InputColumns[column]
		if position < 0 || !sameODKUKey(rows.origins.Vecs[column], row, rows.images.Vecs[position], row) {
			return true
		}
	}
	position := c.InputColumns[c.PkColumn]
	return position < 0 || !sameODKUKey(rows.origins.Vecs[c.PkColumn], row, rows.images.Vecs[position], row)
}

// odkuOutputVector maps the writer layout to the sole retained row owner.
func (preInsertUnique *PreInsertUnique) odkuOutputVector(column int) *vector.Vector {
	c, rows := preInsertUnique.PreInsertCtx.OdkuActions, preInsertUnique.ctr.actionRows
	if column >= len(c.SourceColumns) {
		return rows.origins.Vecs[column-len(c.SourceColumns)]
	}
	return rows.images.Vecs[column]
}

// finalODKUOutputCount stops at the existing row/byte quota, including the
// crossing row. Inspect only live varlen payload, never retained dead area.
func (preInsertUnique *PreInsertUnique) finalODKUOutputCount() int {
	a, c, rows, out := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions, preInsertUnique.ctr.actionRows, preInsertUnique.ctr.buf
	count := min(rows.images.RowCount()-a.finalRow, colexec.DefaultBatchSize-out.RowCount())
	if c.PhysicalChangedColumn == 0 {
		for n := 0; n < count; n++ {
			if !preInsertUnique.physicalODKUChange(a.finalRow + n) {
				count = n
				break
			}
		}
	}
	remaining, fixed := max(0, (1<<20)-out.Size()), 0
	hasVarlen := false
	for _, v := range out.Vecs {
		fixed += v.GetType().TypeSize()
		hasVarlen = hasVarlen || v.GetType().IsVarlen()
	}
	if !hasVarlen {
		if fixed > 0 {
			count = min(count, max(1, (remaining+fixed-1)/fixed))
		}
		return count
	}
	for n := 0; n < count; n++ {
		remaining = max(0, remaining-fixed)
		for col, v := range out.Vecs {
			if !v.GetType().IsVarlen() || col == int(c.ActionFinalColumn) {
				continue
			}
			src := preInsertUnique.odkuOutputVector(col)
			if !src.IsNull(uint64(a.finalRow + n)) {
				payload := src.GetBytesAt(a.finalRow + n)
				if len(payload) > types.VarlenaInlineSize {
					remaining = max(0, remaining-len(payload))
				}
			}
		}
		if remaining == 0 {
			return n + 1
		}
	}
	return count
}

func (preInsertUnique *PreInsertUnique) appendODKUOutput(proc *process.Process, owner, count int, final bool) error {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	for i, dst := range preInsertUnique.ctr.buf.Vecs {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		if i == int(c.ActionFinalColumn) {
			if err := vector.AppendMultiFixed(dst, final, false, count, proc.Mp()); err != nil {
				return err
			}
			continue
		}
		if c.PhysicalChangedColumn != 0 && i == int(c.PhysicalChangedColumn-1) {
			for row := owner; row < owner+count; row++ {
				if err := vector.AppendFixed(dst, final && preInsertUnique.physicalODKUChange(row), false, proc.Mp()); err != nil {
					return err
				}
			}
			continue
		}
		if !final {
			fkColumn := false
			for j, fk := range c.ForeignKeyChecks {
				if i == int(fk.Eligibility.GetCol().ColPos) {
					if err := vector.AppendFixed(dst, a.eligible[j], false, proc.Mp()); err != nil {
						return err
					}
					fkColumn = true
					break
				}
			}
			if fkColumn {
				continue
			}
		}
		src := preInsertUnique.odkuOutputVector(i)
		if dst.GetType().IsFixedLen() {
			if err := dst.UnionBatch(src, int64(owner), count, nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			for row := owner; row < owner+count; row++ {
				if err := dst.UnionOne(src, int64(row), proc.Mp()); err != nil {
					return err
				}
			}
		}
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	preInsertUnique.ctr.buf.AddRowCount(count)
	return nil
}

func (preInsertUnique *PreInsertUnique) callODKUActions(proc *process.Process) (vm.CallResult, error) {
	a, c := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions
	result := vm.NewCallResult()
	if preInsertUnique.ctr.buf == nil {
		ts := make([]types.Type, 0, len(c.SourceColumns)+len(c.OldColumns))
		for _, e := range c.SourceColumns {
			ts = append(ts, odkuType(e.Typ))
		}
		for _, e := range c.OldColumns {
			ts = append(ts, odkuType(e.Typ))
		}
		preInsertUnique.ctr.buf = batch.NewWithSize(len(ts))
		for i, typ := range ts {
			v, err := vector.NewOffHeapVecWithTypeAndAllocation(typ, preInsertUnique.retainedAllocation)
			if err != nil {
				return vm.CancelResult, err
			}
			preInsertUnique.ctr.buf.Vecs[i] = v
		}
	} else {
		preInsertUnique.ctr.buf.CleanOnlyData()
	}
	for {
		if err := context.Cause(proc.Ctx); err != nil {
			return vm.CancelResult, err
		}
		if !a.inputDone {
			if a.source == nil || a.sourceRow == a.source.RowCount() {
				child, err := vm.ChildrenCall(preInsertUnique.GetChildren(0), proc, preInsertUnique.OpAnalyzer)
				if err != nil {
					return vm.CancelResult, err
				}
				if child.Batch == nil || child.Batch.Last() {
					a.source, a.inputDone = nil, true
					continue
				}
				if child.Batch.IsEmpty() {
					continue
				}
				if err := preInsertUnique.validateODKUSource(proc, child.Batch); err != nil {
					return vm.CancelResult, err
				}
				a.source, a.sourceRow = child.Batch, 0
				if preInsertUnique.ctr.actionRows == nil {
					for i, key := range c.Constraints {
						a.keys.Vecs[i] = child.Batch.Vecs[key.InputKey.GetCol().ColPos]
					}
					if err := preInsertUnique.initODKUActionRows(proc, a.incoming, a.keys, a.old, c.RowIdColumn, odkuMutableColumns(c)); err != nil {
						return vm.CancelResult, err
					}
				}
			}
			if !a.targetReady {
				if err := preInsertUnique.prepareODKUTarget(proc); err != nil {
					return vm.CancelResult, err
				}
				if !preInsertUnique.ctr.buf.IsEmpty() && (preInsertUnique.ctr.buf.RowCount() >= colexec.DefaultBatchSize || preInsertUnique.ctr.buf.Size() >= 1<<20) {
					result.Batch = preInsertUnique.ctr.buf
					return result, nil
				}
			}
			if err := preInsertUnique.applyODKUAction(proc); err != nil {
				return vm.CancelResult, err
			}
			a.targetReady = false
			a.sourceRow++
		} else {
			if err := preInsertUnique.validateODKUPriorData(proc); err != nil {
				return vm.CancelResult, err
			}
			rows := preInsertUnique.ctr.actionRows
			if rows == nil || a.finalRow == rows.images.RowCount() {
				if preInsertUnique.ctr.buf.IsEmpty() {
					if !a.countPublished {
						if preInsertUnique.PublishAffectedRows != nil {
							if err := preInsertUnique.PublishAffectedRows(a.logicalAffected); err != nil {
								return vm.CancelResult, err
							}
						}
						a.countPublished = true
					}
					result.Status = vm.ExecStop
					return result, nil
				}
				result.Batch = preInsertUnique.ctr.buf
				return result, nil
			}
			if c.PhysicalChangedColumn == 0 && !preInsertUnique.physicalODKUChange(a.finalRow) {
				a.finalRow++
				continue
			}
			count := preInsertUnique.finalODKUOutputCount()
			if err := preInsertUnique.appendODKUOutput(proc, a.finalRow, count, true); err != nil {
				return vm.CancelResult, err
			}
			a.finalRow += count
			if preInsertUnique.ctr.buf.RowCount() >= colexec.DefaultBatchSize || preInsertUnique.ctr.buf.Size() >= 1<<20 {
				result.Batch = preInsertUnique.ctr.buf
				return result, nil
			}
		}
	}
}

func (a *odkuActions) free(proc *process.Process) {
	for _, list := range [][]colexec.ExpressionExecutor{a.assignments, a.keyExecs, a.priorKeyExecs, a.changeExecs} {
		for _, exec := range list {
			if exec != nil {
				exec.Free()
			}
		}
	}
	odkueval.FreeChecks(a.insertChecks)
	odkueval.FreeChecks(a.updateChecks)
	odkueval.FreeFKChanges(a.fkChanges)
	for _, owned := range []*batch.Batch{a.incoming, a.current, a.old, a.nullImage, a.lastFKEligibility} {
		if owned != nil {
			owned.Clean(proc.Mp())
		}
	}
	for _, keys := range a.touchedKeys {
		if keys != nil {
			keys.Free(proc.Mp())
		}
	}
	*a = odkuActions{}
}
