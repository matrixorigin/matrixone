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

package preinsertunique

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type orderedForeignKeyResult struct {
	value         uint64
	valid, exists bool
	parents       *hashmap.StrHashMap
	iterator      hashmap.Iterator
	tuple         []*vector.Vector
}

type orderedAutoIncrementState struct {
	foreignKeyResults []orderedForeignKeyResult
	foreignKeysLocked bool
	started           bool
	remaining         uint64
	nextBlock         uint64
	oldest            timestamp.Timestamp
	hasTimestamp      bool
	manual            *vector.Vector
	manualVecs        []*vector.Vector
}

func (p *PreInsertUnique) orderedContext(proc *process.Process) context.Context {
	spec := p.PreInsertCtx.OrderedAutoIncrement
	ctx := incrservice.WithAutoIDCachePolicy(proc.Ctx, spec.TableDef.TblId, spec.TableDef.AutoIdCache)
	return incrservice.WithAutoIncrementOptions(ctx, proc.GetSessionInfo().AutoIncrementIncrement, proc.GetSessionInfo().AutoIncrementOffset)
}

func (p *PreInsertUnique) orderedCandidate(proc *process.Process) (uint64, error) {
	state := &p.ctr.ordered
	queue := &p.ctr.autoIncrementCandidates
	spec := p.PreInsertCtx.OrderedAutoIncrement
	if queue.runIndex == len(queue.runs) {
		count := state.remaining
		if !state.started {
			count = spec.ValuesRows
		}
		if count == 0 {
			count = max(uint64(1), state.nextBlock)
		}
		col := spec.TableDef.Cols[spec.ColumnIndex]
		typ := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)
		request := incrservice.AutoIncrementReservation{ColumnIndex: int(spec.ColumnIndex), Type: typ, Count: count}
		ctx := incrservice.WithAutoIncrementReservation(p.orderedContext(proc), &request)
		_, err := proc.GetIncrService().InsertValues(ctx, spec.TableDef.TblId, spec.TableDef.AutoIncrEpoch, proc.Base.TxnOperator, nil, 0, 0)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				return 0, moerr.NewTxnNeedRetryWithDefChanged(proc.Ctx)
			}
			return 0, err
		}
		if len(request.Runs) == 0 {
			return 0, moerr.NewInternalError(proc.Ctx, "empty AUTO_INCREMENT reservation")
		}
		for _, run := range request.Runs {
			if err := queue.appendRun(typ, run.First, run.Count, run.Step, proc.Mp()); err != nil {
				return 0, err
			}
			if !state.hasTimestamp || run.AllocatedAt.Less(state.oldest) {
				state.oldest = run.AllocatedAt
				state.hasTimestamp = true
			}
		}
		state.remaining = count
		state.started = true
		state.nextBlock = min(max(uint64(1), state.nextBlock)*2, uint64(65535))
	}
	if queue.runIndex >= len(queue.runs) {
		return 0, moerr.NewInternalError(proc.Ctx, "AUTO_INCREMENT reservation below accepted explicit value")
	}
	return queue.valueAt(queue.runs[queue.runIndex], queue.runOffset)
}

func (p *PreInsertUnique) finishOrderedRow() {
	if p.ctr.ordered.started && p.ctr.ordered.remaining > 0 {
		p.ctr.ordered.remaining--
	}
}

func (p *PreInsertUnique) acceptOrderedExplicit(proc *process.Process, vec *vector.Vector, row int, value uint64) error {
	queue := &p.ctr.autoIncrementCandidates
	advance := !queue.hasLowerBound || value > queue.lowerBound
	if queue.runIndex < len(queue.runs) {
		next, err := queue.valueAt(queue.runs[queue.runIndex], queue.runOffset)
		if err != nil {
			return err
		}
		if value < next {
			advance = false
		}
	}
	if advance {
		state := &p.ctr.ordered
		spec := p.PreInsertCtx.OrderedAutoIncrement
		if state.manual == nil {
			state.manual = vector.NewVec(*vec.GetType())
			state.manualVecs = make([]*vector.Vector, len(spec.TableDef.Cols))
			state.manualVecs[spec.ColumnIndex] = state.manual
		}
		state.manual.CleanOnlyData()
		if err := state.manual.UnionOne(vec, int64(vectorRowIndex(vec, row)), proc.Mp()); err != nil {
			return err
		}
		if _, err := proc.GetIncrService().InsertValues(p.orderedContext(proc), spec.TableDef.TblId, spec.TableDef.AutoIncrEpoch,
			proc.Base.TxnOperator, state.manualVecs, 1, 0); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				return moerr.NewTxnNeedRetryWithDefChanged(proc.Ctx)
			}
			return err
		}
	}
	return queue.discardThrough(value)
}

func (queue *autoIncrementCandidateStream) appendRun(typ types.Type, first, count, step uint64, mp *mpool.MPool) error {
	if count == 0 {
		return moerr.NewInvalidInputNoCtx("empty AUTO_INCREMENT run")
	}
	if queue.hasLowerBound && first <= queue.lowerBound {
		return moerr.NewInternalErrorNoCtx("AUTO_INCREMENT reservation below explicit value")
	}
	if !queue.initialized {
		queue.typ = typ
		queue.initialized = true
	} else if !queue.typ.Eq(typ) {
		return moerr.NewInvalidInputNoCtx("AUTO_INCREMENT type changed")
	}
	if queue.runIndex == len(queue.runs) {
		queue.runs = queue.runs[:0]
		queue.runIndex = 0
		queue.runOffset = 0
	}
	if len(queue.runs) == cap(queue.runs) {
		grown, err := mpool.MakeSlice[autoIncrementCandidateRun](max(8, 2*cap(queue.runs)), mp, true)
		if err != nil {
			return err
		}
		copy(grown, queue.runs)
		mpool.FreeSlice(mp, queue.runs)
		queue.runs = grown[:len(queue.runs)]
	}
	queue.runs = append(queue.runs, autoIncrementCandidateRun{start: first, count: count, step: step})
	return nil
}

// Reuse the ordinary PRE_INSERT lease-timestamp guard on the final batch. A
// concurrent explicit insert may have occupied an older CN lease even though
// its generated values have never been published by this statement.
func (p *PreInsertUnique) checkOrderedPrimaryKeys(proc *process.Process, output *batch.Batch) error {
	spec := p.PreInsertCtx.OrderedAutoIncrement
	if spec == nil || !p.ctr.ordered.hasTimestamp || spec.TableDef.IsTemporary {
		return nil
	}
	eng := proc.Base.SessionInfo.StorageEngine
	if eng == nil || proc.Base.TxnOperator == nil {
		return moerr.NewInternalError(proc.Ctx, "missing ordered AUTO_INCREMENT transaction")
	}
	if err := p.ensureOrderedWorkspace(proc); err != nil {
		return err
	}
	_, _, rel, err := eng.GetRelationById(proc.Ctx, proc.Base.TxnOperator, spec.TableDef.TblId)
	if err != nil {
		return err
	}
	changed, err := rel.PrimaryKeysMayBeUpserted(proc.Ctx, types.TimestampToTS(p.ctr.ordered.oldest),
		types.TimestampToTS(proc.Base.TxnOperator.SnapshotTS()), output, p.PreInsertCtx.AutoIncrementOutputColumn)
	if err != nil {
		return err
	}
	if changed {
		return moerr.NewTxnNeedRetry(proc.Ctx)
	}
	return nil
}

// Remote pipelines have a transaction snapshot but may not have a local
// workspace yet. Use the same read-only workspace admission as PRE_INSERT.
func (p *PreInsertUnique) ensureOrderedWorkspace(proc *process.Process) error {
	op := proc.GetTxnOperator()
	if op == nil {
		return moerr.NewInternalError(proc.Ctx, "missing ordered AUTO_INCREMENT transaction")
	}
	if op.GetWorkspace() != nil {
		return nil
	}
	eng, ok := proc.Base.SessionInfo.StorageEngine.(*disttae.Engine)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "missing ordered AUTO_INCREMENT workspace")
	}
	ws := disttae.NewTxnWorkSpace(eng, proc)
	op.AddWorkspace(ws)
	ws.BindTxnOp(op)
	return nil
}

func (p *PreInsertUnique) validateOrderedAutoIncrement(proc *process.Process) error {
	ctx := p.PreInsertCtx
	spec := ctx.OrderedAutoIncrement
	if spec == nil {
		return nil
	}
	if !ctx.InsertIgnoreMultiDedup || !ctx.AutoIncrementReorder || spec.TableDef == nil || spec.ColumnIndex < 0 || int(spec.ColumnIndex) >= len(spec.TableDef.Cols) {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ordered AUTO_INCREMENT context")
	}
	col := spec.TableDef.Cols[spec.ColumnIndex]
	if col == nil || !col.Typ.AutoIncr || !types.T(col.Typ.Id).ToType().IsIntOrUint() {
		return moerr.NewInvalidInput(proc.Ctx, "invalid ordered AUTO_INCREMENT column")
	}
	for _, fk := range spec.ForeignKeys {
		if fk == nil || fk.TableDef == nil || fk.TableDef.Pkey == nil || fk.Predicate == nil || len(fk.ChildColumns) == 0 || len(fk.ParentColumns) < len(fk.ChildColumns) {
			return moerr.NewInvalidInput(proc.Ctx, "invalid ordered foreign-key context")
		}
		if len(fk.SelfParentColumns) > 0 && len(fk.SelfParentColumns) != len(fk.ChildColumns) {
			return moerr.NewInvalidInput(proc.Ctx, "invalid self-referencing foreign-key context")
		}
		for _, child := range append(fk.ChildColumns[:len(fk.ChildColumns):len(fk.ChildColumns)], fk.SelfParentColumns...) {
			if child == nil || child.ColPos < 0 {
				return moerr.NewInvalidInput(proc.Ctx, "invalid ordered foreign-key child column")
			}
		}
		for _, pos := range fk.ParentColumns {
			if pos < 0 || int(pos) >= len(fk.TableDef.Cols) || fk.TableDef.Cols[pos] == nil {
				return moerr.NewInvalidInput(proc.Ctx, "invalid ordered foreign-key parent column")
			}
		}
	}
	return nil
}
