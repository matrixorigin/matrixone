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

// Package updateunique admits the contributions of one alias in a repeated
// UPDATE target. Its input must contain exactly one row per physical target.
package updateunique

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Key identifies the input columns for one constraint. SnapshotHit says if
// the proposed key existed at the statement snapshot, including keys owned by
// other participating rows. The planner must use the writer's key encoding.
type Key struct {
	Original, Current, Proposed, SnapshotHit int
}

// Value identifies one current/proposed value pair selected by admission.
type Value struct{ Current, Proposed int }

// Check identifies a candidate column that must satisfy the normal write
// constraints for each accepted row.
type Check struct {
	Column  int
	Name    string
	NotNull bool
}

// UpdateUnique appends an accepted boolean to each input row. The producer
// supplies original and current keys for every participant, including rows
// whose Active flag is false. Only active rows can change key ownership.
//
// It first consumes the complete input and initializes all owners. It then
// decides all rows before publishing output, so a late child or allocation
// error cannot expose partial decisions. The replay uses a bounded source and
// the decision vector and key ledgers are charged to the statement account.
type UpdateUnique struct {
	RowID, Active      int
	Keys               []Key
	Values             []Value
	Checks             []Check
	RejectZeroTemporal bool
	SpillConfig        materialized.SpillConfig

	account *mpool.AllocationAccount
	alloc   *updateKeyLedgerAllocations
	ctr     container
	vm.OperatorBase
}

type container struct {
	source    *materialized.Source
	ledgers   []*updateKeyLedger
	decisions *vector.Vector
	output    *batch.Batch
	position  int
	offset    int64
	prepared  bool
	decided   bool
	done      bool
}

var _ vm.Operator = (*UpdateUnique)(nil)

func (op *UpdateUnique) GetOperatorBase() *vm.OperatorBase { return &op.OperatorBase }
func (op *UpdateUnique) OpType() vm.OpType                 { return vm.UpdateUnique }
func (op *UpdateUnique) TypeName() string                  { return "update_unique" }
func (op *UpdateUnique) String(buf *bytes.Buffer)          { buf.WriteString("update_unique") }
func (op *UpdateUnique) Release()                          {}
func (op *UpdateUnique) SetRejectZeroTemporal(reject bool) { op.RejectZeroTemporal = reject }
func (op *UpdateUnique) ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (op *UpdateUnique) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if op.account != nil && op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if op.account == account {
		return nil
	}
	alloc, err := newUpdateKeyLedgerAllocations(account)
	if err != nil {
		return err
	}
	op.account, op.alloc = account, alloc
	return nil
}

func (op *UpdateUnique) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if op.account == nil {
		return nil
	}
	if op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if op.ctr.source != nil || len(op.ctr.ledgers) != 0 || op.ctr.decisions != nil || op.ctr.output != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	op.account, op.alloc = nil, nil
	return nil
}

func (op *UpdateUnique) Prepare(proc *process.Process) error {
	if op.alloc == nil || op.ctr.prepared {
		return mpool.ErrAllocationAccountInvalid
	}
	if op.OpAnalyzer == nil {
		op.OpAnalyzer = process.NewAnalyzer(op.GetIdx(), op.IsFirst, op.IsLast, "update_unique")
	} else {
		op.OpAnalyzer.Reset()
	}
	if len(op.Keys) == 0 {
		op.ctr.prepared = true
		return nil
	}
	op.ctr.source = materialized.NewSource(2)
	if err := op.ctr.source.Begin(proc.Mp(), op.SpillConfig); err != nil {
		op.cleanup(proc)
		return err
	}
	var err error
	op.ctr.decisions, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_bool.ToType(), op.alloc.vector)
	if err != nil {
		op.cleanup(proc)
		return err
	}
	for range op.Keys {
		ledger, err := newUpdateKeyLedger(proc.Mp(), op.alloc)
		if err != nil {
			op.cleanup(proc)
			return err
		}
		op.ctr.ledgers = append(op.ctr.ledgers, ledger)
	}
	op.ctr.prepared = true
	return nil
}

func (op *UpdateUnique) Call(proc *process.Process) (vm.CallResult, error) {
	if !op.ctr.prepared {
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("update unique operator is not prepared")
	}
	if op.ctr.output != nil {
		op.ctr.output.Clean(proc.Mp())
		op.ctr.output = nil
	}
	if op.ctr.done {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	if len(op.Keys) == 0 {
		return op.callWithoutKeys(proc)
	}
	if !op.ctr.decided {
		if err := op.collectAndDecide(proc); err != nil {
			op.cleanup(proc)
			return vm.CancelResult, err
		}
	}
	bat, end, err := op.ctr.source.Next(proc.Ctx, 1, op.ctr.position)
	if err != nil {
		op.cleanup(proc)
		return vm.CancelResult, err
	}
	if end {
		op.ctr.source.ReleaseReader(1)
		op.ctr.source.Close()
		op.ctr.source = nil
		op.ctr.decisions.Free(proc.Mp())
		op.ctr.decisions = nil
		op.ctr.done = true
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	op.ctr.position++
	accepted, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_bool.ToType(), op.alloc.vector)
	if err == nil {
		err = accepted.UnionBatch(op.ctr.decisions, op.ctr.offset, bat.RowCount(), nil, proc.Mp())
	}
	if err != nil {
		if accepted != nil {
			accepted.Free(proc.Mp())
		}
		bat.Clean(proc.Mp())
		op.cleanup(proc)
		return vm.CancelResult, err
	}
	op.ctr.offset += int64(bat.RowCount())
	if err = op.appendSelection(proc, bat, accepted); err != nil {
		bat.Clean(proc.Mp())
		op.cleanup(proc)
		return vm.CancelResult, err
	}
	op.ctr.output = bat
	return vm.CallResult{Batch: bat, Status: vm.ExecNext}, nil
}

// Without changed constraints each present role is accepted. This path can
// stream instead of retaining and replaying all participants.
func (op *UpdateUnique) callWithoutKeys(proc *process.Process) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(op.GetChildren(0), proc, op.OpAnalyzer)
	if err != nil {
		return vm.CancelResult, err
	}
	if input.Batch == nil {
		if err = context.Cause(proc.Ctx); err != nil {
			return vm.CancelResult, err
		}
		op.ctr.done = true
		return input, nil
	}
	if input.Batch.Last() {
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("unexpected recursive batch in update unique")
	}
	if input.Batch.IsEmpty() {
		return input, nil
	}
	if err = op.validate(input.Batch); err != nil {
		return vm.CancelResult, err
	}
	active := input.Batch.Vecs[op.Active]
	for row := 0; row < input.Batch.RowCount(); row++ {
		if row%256 == 0 {
			if err = context.Cause(proc.Ctx); err != nil {
				return vm.CancelResult, err
			}
		}
		if input.Batch.Vecs[op.RowID].IsNull(uint64(row)) {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx("null repeated UPDATE participant")
		}
		if active.IsNull(uint64(row)) {
			return vm.CancelResult, moerr.NewInternalErrorNoCtx("null repeated UPDATE activity")
		}
	}
	if err = op.validateChecks(proc, input.Batch, active, 0); err != nil {
		return vm.CancelResult, err
	}
	bat, err := input.Batch.Dup(proc.Mp())
	if err != nil {
		return vm.CancelResult, err
	}
	accepted, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_bool.ToType(), op.alloc.vector)
	if err == nil {
		err = accepted.UnionBatch(active, 0, bat.RowCount(), nil, proc.Mp())
	}
	if err != nil {
		if accepted != nil {
			accepted.Free(proc.Mp())
		}
		bat.Clean(proc.Mp())
		return vm.CancelResult, err
	}
	if err = op.appendSelection(proc, bat, accepted); err != nil {
		bat.Clean(proc.Mp())
		return vm.CancelResult, err
	}
	op.ctr.output = bat
	return vm.CallResult{Batch: bat, Status: vm.ExecNext}, nil
}

func (op *UpdateUnique) appendSelection(proc *process.Process, bat *batch.Batch, accepted *vector.Vector) error {
	if len(bat.Attrs) == len(bat.Vecs) {
		bat.Attrs = append(bat.Attrs, "__mo_update_accepted")
	}
	bat.Vecs = append(bat.Vecs, accepted)
	for _, pair := range op.Values {
		current, proposed := bat.Vecs[pair.Current], bat.Vecs[pair.Proposed]
		value, selectErr := vector.NewOffHeapVecWithTypeAndAllocation(*current.GetType(), op.alloc.vector)
		if selectErr == nil {
			for row := 0; row < bat.RowCount(); row++ {
				if row%256 == 0 {
					if selectErr = context.Cause(proc.Ctx); selectErr != nil {
						break
					}
				}
				selected := current
				if vector.GetFixedAtNoTypeCheck[bool](accepted, row) {
					selected = proposed
				}
				if selectErr = value.UnionOne(selected, int64(row), proc.Mp()); selectErr != nil {
					break
				}
			}
		}
		if selectErr != nil {
			if value != nil {
				value.Free(proc.Mp())
			}
			return selectErr
		}
		if len(bat.Attrs) == len(bat.Vecs) {
			bat.Attrs = append(bat.Attrs, "__mo_update_value")
		}
		bat.Vecs = append(bat.Vecs, value)
	}
	return nil
}

func (op *UpdateUnique) collectAndDecide(proc *process.Process) error {
	for {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		input, err := vm.ChildrenCall(op.GetChildren(0), proc, op.OpAnalyzer)
		if err != nil {
			return err
		}
		if input.Batch == nil {
			break
		}
		if input.Batch.Last() {
			return moerr.NewInternalErrorNoCtx("unexpected recursive batch in update unique")
		}
		if input.Batch.IsEmpty() {
			continue
		}
		if err = op.validate(input.Batch); err != nil {
			return err
		}
		for row := 0; row < input.Batch.RowCount(); row++ {
			if row%256 == 0 {
				if err = context.Cause(proc.Ctx); err != nil {
					return err
				}
			}
			idVec := input.Batch.Vecs[op.RowID]
			if idVec.IsNull(uint64(row)) {
				return moerr.NewInternalErrorNoCtx("null repeated UPDATE participant")
			}
			owner := vector.GetFixedAtNoTypeCheck[types.Rowid](idVec, row)
			for i, key := range op.Keys {
				if err = op.ctr.ledgers[i].addParticipant(proc.Mp(), input.Batch.Vecs[key.Original], input.Batch.Vecs[key.Current], row, owner); err != nil {
					return err
				}
			}
		}
		if err = op.ctr.source.Append(input.Batch); err != nil {
			return err
		}
	}
	op.ctr.source.Finish(nil)
	for pos := 0; ; pos++ {
		bat, end, err := op.ctr.source.Next(proc.Ctx, 0, pos)
		if err != nil {
			return err
		}
		if end {
			break
		}
		err = op.decideBatch(proc, bat)
		bat.Clean(proc.Mp())
		if err != nil {
			return err
		}
	}
	op.ctr.source.ReleaseReader(0)
	for _, ledger := range op.ctr.ledgers {
		ledger.close(proc.Mp())
	}
	op.ctr.ledgers = nil
	op.ctr.decided = true
	return nil
}

func (op *UpdateUnique) decideBatch(proc *process.Process, bat *batch.Batch) error {
	candidates := make([]updateKeyCandidate, len(op.Keys))
	offset := op.ctr.decisions.Length()
	for row := 0; row < bat.RowCount(); row++ {
		if row%256 == 0 {
			if err := context.Cause(proc.Ctx); err != nil {
				return err
			}
		}
		active := bat.Vecs[op.Active]
		if active.IsNull(uint64(row)) {
			return moerr.NewInternalErrorNoCtx("null repeated UPDATE activity")
		}
		accepted := false
		if vector.GetFixedAtNoTypeCheck[bool](active, row) {
			for i, key := range op.Keys {
				hit := bat.Vecs[key.SnapshotHit]
				if hit.IsNull(uint64(row)) {
					return moerr.NewInternalErrorNoCtx("null repeated UPDATE snapshot hit")
				}
				candidates[i] = updateKeyCandidate{
					ledger: op.ctr.ledgers[i], current: bat.Vecs[key.Current], proposed: bat.Vecs[key.Proposed],
					row: row, snapshotHit: vector.GetFixedAtNoTypeCheck[bool](hit, row),
				}
			}
			owner := vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[op.RowID], row)
			var err error
			accepted, err = acceptUpdateKeys(proc.Mp(), owner, candidates)
			if err != nil {
				return err
			}
		}
		if err := vector.AppendFixed(op.ctr.decisions, accepted, false, proc.Mp()); err != nil {
			return err
		}
	}
	return op.validateChecks(proc, bat, op.ctr.decisions, offset)
}

func (op *UpdateUnique) validateChecks(proc *process.Process, bat *batch.Batch, accepted *vector.Vector, offset int) error {
	for _, check := range op.Checks {
		if err := colexec.CheckZeroTemporal(proc.Ctx, bat.Vecs[check.Column], accepted, offset, op.RejectZeroTemporal); err != nil {
			return err
		}
	}
	for _, check := range op.Checks {
		if check.NotNull {
			if err := colexec.CheckNotNull(proc.Ctx, bat.Vecs[check.Column], accepted, offset, check.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *UpdateUnique) validate(bat *batch.Batch) error {
	check := func(pos int, oid types.T) bool {
		return pos >= 0 && pos < len(bat.Vecs) && bat.Vecs[pos] != nil &&
			(oid == types.T_any || bat.Vecs[pos].GetType().Oid == oid) && bat.Vecs[pos].Length() >= bat.RowCount()
	}
	if !check(op.RowID, types.T_Rowid) || !check(op.Active, types.T_bool) {
		return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE participant columns")
	}
	for _, key := range op.Keys {
		if !check(key.Original, types.T_any) || !check(key.Current, types.T_any) ||
			!check(key.Proposed, types.T_any) || !check(key.SnapshotHit, types.T_bool) {
			return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE key columns")
		}
	}
	for _, pair := range op.Values {
		if !check(pair.Current, types.T_any) || !check(pair.Proposed, types.T_any) ||
			!bat.Vecs[pair.Current].GetType().Eq(*bat.Vecs[pair.Proposed].GetType()) {
			return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE value columns")
		}
	}
	for _, column := range op.Checks {
		if !check(column.Column, types.T_any) {
			return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE constraint column")
		}
	}
	return nil
}

func (op *UpdateUnique) cleanup(proc *process.Process) {
	if op.ctr.output != nil {
		op.ctr.output.Clean(proc.Mp())
	}
	if op.ctr.source != nil {
		op.ctr.source.Close()
	}
	if op.ctr.decisions != nil {
		op.ctr.decisions.Free(proc.Mp())
	}
	for _, ledger := range op.ctr.ledgers {
		ledger.close(proc.Mp())
	}
	op.ctr = container{}
}

func (op *UpdateUnique) Reset(proc *process.Process, _ bool, _ error) { op.cleanup(proc) }
func (op *UpdateUnique) Free(proc *process.Process, _ bool, _ error)  { op.cleanup(proc) }
