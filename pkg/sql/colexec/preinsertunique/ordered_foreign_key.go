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
	"cmp"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Only ID-dependent or self-referencing foreign keys reach this path. Their parent tables must
// remain stable while final IDs are selected, rather than locking a provisional
// value. Shared table locks are statement/transaction owned and acquired once
// in table-ID order. Ordinary foreign keys retain the vectorized join path.
func (preInsertUnique *PreInsertUnique) orderedForeignKeys() []*plan.OrderedForeignKey {
	if spec := preInsertUnique.PreInsertCtx.OrderedAutoIncrement; spec != nil {
		return spec.ForeignKeys
	}
	return preInsertUnique.PreInsertCtx.OrderedForeignKeys
}

func (preInsertUnique *PreInsertUnique) lockOrderedForeignKeys(proc *process.Process) error {
	if preInsertUnique.ctr.ordered.foreignKeysLocked {
		return nil
	}
	checks := slices.Clone(preInsertUnique.orderedForeignKeys())
	slices.SortFunc(checks, func(a, b *plan.OrderedForeignKey) int { return cmp.Compare(a.TableDef.TblId, b.TableDef.TblId) })
	var previous uint64
	for _, check := range checks {
		table := check.TableDef
		if table.TblId == previous {
			continue
		}
		if proc.GetTxnOperator().Txn().IsPessimistic() {
			pk, ok := table.Name2ColIndex[table.Pkey.PkeyColName]
			if !ok {
				return moerr.NewInternalError(proc.Ctx, "missing ordered foreign-key parent primary key")
			}
			if err := lockop.LockTableWithMode(proc.Base.SessionInfo.StorageEngine, proc, table.TblId,
				types.MustTypeFromPlan(table.Cols[pk].Typ), lockpb.LockMode_Shared, false); err != nil {
				return err
			}
		}
		previous = table.TblId
	}
	preInsertUnique.ctr.ordered.foreignKeysLocked = true
	return nil
}

func (preInsertUnique *PreInsertUnique) checkOrderedForeignKeys(proc *process.Process, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	foreignKeys := preInsertUnique.orderedForeignKeys()
	if len(foreignKeys) == 0 {
		return true, nil
	}
	if err := preInsertUnique.ensureOrderedWorkspace(proc); err != nil {
		return false, err
	}
	if err := preInsertUnique.lockOrderedForeignKeys(proc); err != nil {
		return false, err
	}
	if preInsertUnique.ctr.ordered.foreignKeyResults == nil {
		preInsertUnique.ctr.ordered.foreignKeyResults = make([]orderedForeignKeyResult, len(foreignKeys))
	}
	for i, check := range foreignKeys {
		// A rejected generated value can recur arbitrarily often. For a
		// single-column auto-ID FK retain just its last point lookup, bounded
		// by the number of constraints and discarded on Reset.
		cached := &preInsertUnique.ctr.ordered.foreignKeyResults[i]
		if len(check.SelfParentColumns) > 0 {
			valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, cached, check.ChildColumns, input, row, generated, value)
			if err != nil {
				return false, err
			}
			if !valid {
				continue // MATCH SIMPLE: a NULL child tuple needs no parent.
			}
			matches, err := preInsertUnique.currentRowSatisfiesForeignKey(proc, cached, check, input, row, generated, value)
			if err != nil {
				return false, err
			}
			if matches {
				continue
			}
			if cached.parents != nil {
				ids, zs, err := cached.iterator.Find(0, 1, cached.tuple)
				if err != nil {
					return false, err
				}
				if zs[0] != 0 && ids[0] != 0 {
					continue
				}
			}
		}
		reusable := len(check.SelfParentColumns) == 0 && generated && len(check.ChildColumns) == 1 && check.ChildColumns[0].ColPos == preInsertUnique.PreInsertCtx.AutoIncrementColumn
		if reusable && cached.valid && cached.value == value {
			if !cached.exists {
				return false, nil
			}
			continue
		}
		ok, err := preInsertUnique.checkOrderedForeignKey(proc, check, input, row, generated, value)
		if reusable && err == nil {
			*cached = orderedForeignKeyResult{value: value, valid: true, exists: ok}
		}
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func (preInsertUnique *PreInsertUnique) checkOrderedForeignKey(proc *process.Process, check *plan.OrderedForeignKey, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	if err := proc.Ctx.Err(); err != nil {
		return false, err
	}
	literals := make([]*plan.Expr, len(check.ChildColumns))
	for i, col := range check.ChildColumns {
		if col.ColPos < 0 || int(col.ColPos) >= len(input.Vecs) {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid ordered foreign-key input column")
		}
		vec := input.Vecs[col.ColPos]
		index := vectorRowIndex(vec, row)
		var literal *plan.Literal
		if generated && col.ColPos == preInsertUnique.PreInsertCtx.AutoIncrementColumn {
			literal = generatedAutoIncrementLiteral(vec.GetType().Oid, value)
		} else {
			if vec.IsNull(uint64(index)) {
				return true, nil
			} // MATCH SIMPLE
			literal = rule.GetConstantValue(vec, true, uint64(index))
		}
		if literal == nil {
			return false, moerr.NewInvalidInput(proc.Ctx, "unsupported ordered foreign-key value")
		}
		literals[i] = &plan.Expr{Typ: vec.GetType().PlanType(), Expr: &plan.Expr_Lit{Lit: literal}}
	}
	predicate := proto.Clone(check.Predicate).(*plan.Expr)
	var bind func(*plan.Expr) error
	bind = func(expr *plan.Expr) error {
		if col := expr.GetCol(); col != nil && col.RelPos == 1 {
			if col.ColPos < 0 || int(col.ColPos) >= len(literals) {
				return moerr.NewInvalidInput(proc.Ctx, "invalid ordered foreign-key predicate")
			}
			*expr = *literals[col.ColPos]
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if err := bind(arg); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := bind(predicate); err != nil {
		return false, err
	}
	predicate, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, predicate, proc, true, true)
	if err != nil {
		return false, err
	}
	_, _, rel, err := proc.Base.SessionInfo.StorageEngine.GetRelationById(proc.Ctx, proc.GetTxnOperator(), check.TableDef.TblId)
	if err != nil {
		return false, err
	}
	rel = engine.NewRelationHandle(rel)
	params := engine.DefaultRangesParam
	params.BlockFilters = []*plan.Expr{predicate}
	params.TxnOffset = proc.GetTxnOperator().GetWorkspace().GetSnapshotWriteOffset()
	data, err := rel.Ranges(proc.Ctx, params)
	if err != nil {
		return false, err
	}
	readers, err := rel.BuildReaders(proc.Ctx, proc, predicate, data, 1, params.TxnOffset, false, engine.Policy_CheckAll, engine.FilterHint{})
	if err != nil {
		return false, err
	}
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()

	// Reader pushdown is only a pruning hint. Evaluate the complete FK equality
	// over the returned rows before accepting a candidate.
	evalPredicate := proto.Clone(predicate).(*plan.Expr)
	positions := make(map[int32]int32, len(check.ParentColumns))
	names := make([]string, len(check.ParentColumns))
	output := batch.NewWithSize(len(check.ParentColumns))
	output.Attrs = names
	defer output.Clean(proc.Mp())
	for i, pos := range check.ParentColumns {
		positions[pos] = int32(i)
		col := check.TableDef.Cols[pos]
		names[i] = col.Name
		output.Vecs[i] = vector.NewVec(types.MustTypeFromPlan(col.Typ))
	}
	var remap func(*plan.Expr)
	remap = func(expr *plan.Expr) {
		if col := expr.GetCol(); col != nil {
			col.ColPos = positions[col.ColPos]
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				remap(arg)
			}
		}
	}
	remap(evalPredicate)
	executor, err := colexec.NewExpressionExecutor(proc, evalPredicate)
	if err != nil {
		return false, err
	}
	defer executor.Free()
	for _, reader := range readers {
		for {
			if err := proc.Ctx.Err(); err != nil {
				return false, err
			}
			output.CleanOnlyData()
			end, err := reader.Read(proc.Ctx, names, nil, proc.Mp(), output)
			if err != nil {
				return false, err
			}
			if output.RowCount() > 0 {
				result, err := executor.Eval(proc, []*batch.Batch{output}, nil)
				if err != nil {
					return false, err
				}
				for j := 0; j < output.RowCount(); j++ {
					k := vectorRowIndex(result, j)
					if !result.IsNull(uint64(k)) && vector.GetFixedAtNoTypeCheck[bool](result, k) {
						return true, nil
					}
				}
			}
			if end {
				break
			}
		}
	}
	return false, nil
}

func (preInsertUnique *PreInsertUnique) currentRowSatisfiesForeignKey(proc *process.Process, state *orderedForeignKeyResult, check *plan.OrderedForeignKey, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	parentState := orderedForeignKeyResult{tuple: state.selfTuple}
	valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, &parentState, check.SelfParentColumns, input, row, generated, value)
	state.selfTuple = parentState.tuple // Keep partial allocations owned on failure too.
	if err != nil || !valid {
		return false, err
	}
	if state.selfExecutor == nil {
		var predicate *plan.Expr
		for i := range state.tuple {
			equal, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
				{Typ: state.selfTuple[i].GetType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}}},
				{Typ: state.tuple[i].GetType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: int32(i)}}},
			})
			if err != nil {
				return false, err
			}
			if predicate == nil {
				predicate = equal
			} else {
				predicate, err = plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "and", []*plan.Expr{predicate, equal})
				if err != nil {
					return false, err
				}
			}
		}
		state.selfExecutor, err = colexec.NewExpressionExecutor(proc, predicate)
		if err != nil {
			return false, err
		}
	}
	parent := batch.Batch{Vecs: state.selfTuple}
	child := batch.Batch{Vecs: state.tuple}
	parent.SetRowCount(1)
	child.SetRowCount(1)
	result, err := state.selfExecutor.Eval(proc, []*batch.Batch{&parent, &child}, nil)
	if err != nil {
		return false, err
	}
	return !result.IsNull(0) && vector.GetFixedAtNoTypeCheck[bool](result, 0), nil
}

func generatedAutoIncrementLiteral(typ types.T, value uint64) *plan.Literal {
	switch typ {
	case types.T_int8:
		return &plan.Literal{Value: &plan.Literal_I8Val{I8Val: int32(value)}}
	case types.T_int16:
		return &plan.Literal{Value: &plan.Literal_I16Val{I16Val: int32(value)}}
	case types.T_int32:
		return &plan.Literal{Value: &plan.Literal_I32Val{I32Val: int32(value)}}
	case types.T_int64:
		return &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(value)}}
	case types.T_uint8:
		return &plan.Literal{Value: &plan.Literal_U8Val{U8Val: uint32(value)}}
	case types.T_uint16:
		return &plan.Literal{Value: &plan.Literal_U16Val{U16Val: uint32(value)}}
	case types.T_uint32:
		return &plan.Literal{Value: &plan.Literal_U32Val{U32Val: uint32(value)}}
	case types.T_uint64:
		return &plan.Literal{Value: &plan.Literal_U64Val{U64Val: value}}
	default:
		return nil
	}
}

// Publish referenced parent keys only after every constraint has passed.
// Current-row self matching is local and never publishes a speculative key.
func (preInsertUnique *PreInsertUnique) recordOrderedForeignKeyParents(proc *process.Process, input *batch.Batch, row int, generated bool, value uint64) error {
	foreignKeys := preInsertUnique.orderedForeignKeys()
	if len(foreignKeys) == 0 {
		return nil
	}
	if preInsertUnique.ctr.ordered.foreignKeyResults == nil {
		preInsertUnique.ctr.ordered.foreignKeyResults = make([]orderedForeignKeyResult, len(foreignKeys))
	}
	for i, check := range foreignKeys {
		if len(check.SelfParentColumns) == 0 {
			continue
		}
		state := &preInsertUnique.ctr.ordered.foreignKeyResults[i]
		valid, err := preInsertUnique.fillOrderedForeignKeyTuple(proc, state, check.SelfParentColumns, input, row, generated, value)
		if err != nil {
			return err
		}
		if !valid {
			continue
		}
		if state.parents == nil {
			state.parents, err = hashmap.NewStrHashMapWithAllocation(false, proc.Mp(), preInsertUnique.hashAllocation)
			if err != nil {
				return err
			}
			state.iterator = state.parents.NewIterator()
		}
		if _, err := state.iterator.DetectDup(state.tuple, 0); err != nil {
			return err
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) fillOrderedForeignKeyTuple(proc *process.Process, state *orderedForeignKeyResult, columns []*plan.ColRef, input *batch.Batch, row int, generated bool, value uint64) (bool, error) {
	if state.tuple == nil {
		state.tuple = make([]*vector.Vector, len(columns))
	}
	for i, col := range columns {
		if col.ColPos < 0 || int(col.ColPos) >= len(input.Vecs) {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid self-referencing foreign-key column")
		}
		src := input.Vecs[col.ColPos]
		if state.tuple[i] == nil {
			state.tuple[i] = vector.NewVec(*src.GetType())
		}
		dst := state.tuple[i]
		dst.CleanOnlyData()
		if generated && col.ColPos == preInsertUnique.PreInsertCtx.AutoIncrementColumn {
			if err := appendAutoIncrementValue(proc, dst, *src.GetType(), value); err != nil {
				return false, err
			}
		} else {
			index := vectorRowIndex(src, row)
			if src.IsNull(uint64(index)) {
				return false, nil
			}
			if err := dst.UnionOne(src, int64(index), proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}
