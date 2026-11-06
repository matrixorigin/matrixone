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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/odkueval"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// directNoopOrdinal is a preparation-time proof, not layout validation. Every
// unknown shape keeps the normal action path. No RHS is evaluated here.
func directNoopOrdinal(c *plan.OdkuActionCtx, a *odkuActions) int32 {
	if !a.staticKeys || c.PhysicalChangedColumn != 0 || c.AutoUpdateAssignmentCount != 0 ||
		int(c.ChangeCheckAssignmentCount) != len(c.UpdateExprs) || len(c.UpdateExprs) != len(c.UpdateColumns) ||
		len(a.assignments) != len(c.UpdateExprs) || !odkueval.DirectNotNullChecks(a.updateChecks, c.InputColumns) {
		return -1
	}
	for i, e := range c.UpdateExprs {
		column := e.GetCol()
		exec, ok := a.assignments[i].(*colexec.ColumnExpressionExecutor)
		if !ok || column == nil || column.RelPos != 1 || column.ColPos < 0 || int(column.ColPos) >= len(c.SourceColumns) ||
			exec.GetRelIndex() != 1 || exec.GetColIndex() != int(column.ColPos) ||
			c.UpdateColumns[i] < 0 || int(c.UpdateColumns[i]) >= len(c.InputColumns) || c.InputColumns[c.UpdateColumns[i]] < 0 {
			return -1
		}
	}
	for _, compared := range c.ChangeCheckColumns {
		covered := false
		for _, assigned := range c.UpdateColumns {
			covered = covered || compared == assigned
		}
		if !covered {
			return -1
		}
	}
	covered := make([]bool, len(c.SourceColumns))
	for _, position := range c.InputColumns {
		if position >= 0 {
			if int(position) >= len(covered) || covered[position] {
				return -1
			}
			covered[position] = true
		}
	}
	ordinal := int32(-1)
	ref := c.SourceOrdinal.GetCol()
	if ref == nil {
		return -1
	}
	for i, table := range covered {
		if table {
			continue
		}
		source := c.SourceColumns[i].GetCol()
		if ordinal >= 0 || source == nil || source.RelPos != ref.RelPos || source.ColPos != ref.ColPos ||
			c.SourceColumns[i].Typ.Id != int32(types.T_int64) || c.SourceOrdinal.Typ.Id != int32(types.T_int64) {
			return -1
		}
		ordinal = int32(i)
	}
	return ordinal
}

// tryDirectNoop preserves the sole action owner. Its read-only proof either
// declines before executing any RHS, or every verified RHS executes once and
// only the latest source ordinal needs publication. Original images/keys stay
// authoritative; a fresh original still takes normal registration/checks.
func (preInsertUnique *PreInsertUnique) tryDirectNoop(proc *process.Process, owner int64) (bool, error) {
	a, c, rows := preInsertUnique.ctr.actions, preInsertUnique.PreInsertCtx.OdkuActions, preInsertUnique.ctr.actionRows
	if a.noopOrdinal < 0 || rows == nil || owner < 0 || owner >= int64(rows.images.RowCount()) {
		return false, nil
	}
	for i, expr := range c.UpdateExprs {
		incoming := a.incoming.Vecs[expr.GetCol().ColPos]
		if incoming.IsConst() || incoming.Length() != 1 {
			return false, nil
		}
		current := rows.images.Vecs[c.InputColumns[c.UpdateColumns[i]]]
		equal, known := odkueval.IdenticalUpdateValueAt(current, int(owner), incoming, 0)
		if !known || !equal {
			return false, nil
		}
	}
	for _, exec := range a.assignments {
		if _, err := exec.Eval(proc, a.updateInputs[:], nil); err != nil {
			return true, err
		}
	}
	if err := odkueval.EvaluateDirectNotNullChecksAt(a.updateChecks, rows.images, c.InputColumns, int(owner), proc); err != nil {
		return true, err
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return true, err
	}
	if err := rows.copyInto(proc, rows.images, a.incoming, int(owner), false, rows.deadImageArea, []int32{a.noopOrdinal}, preInsertUnique); err != nil {
		return true, err
	}
	return true, preInsertUnique.finishODKUAction(proc, 0)
}
