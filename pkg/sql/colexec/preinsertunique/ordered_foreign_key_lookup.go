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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type orderedFKProjection struct {
	positions map[int32]int32
	names     []string
	types     []types.Type
}

func newOrderedFKProjection(def *plan.TableDef, ordinals []int32) (orderedFKProjection, error) {
	p := orderedFKProjection{positions: make(map[int32]int32, len(ordinals))}
	for _, ordinal := range ordinals {
		if _, exists := p.positions[ordinal]; exists {
			continue
		}
		if ordinal < 0 || int(ordinal) >= len(def.Cols) || def.Cols[ordinal] == nil {
			return p, moerr.NewInvalidInputNoCtx("invalid foreign-key projection ordinal")
		}
		p.positions[ordinal] = int32(len(p.names))
		p.names = append(p.names, def.Cols[ordinal].Name)
		p.types = append(p.types, types.MustTypeFromPlan(def.Cols[ordinal].Typ))
	}
	return p, nil
}

func (p *orderedFKProjection) newBatch() *batch.Batch {
	b := batch.NewWithSize(len(p.names))
	b.Attrs = p.names
	for i, typ := range p.types {
		b.Vecs[i] = vector.NewVec(typ)
	}
	return b
}

func remapOrderedFKExpr(expr *plan.Expr, parentPositions map[int32]int32, childToInput bool) (*plan.Expr, error) {
	copy := plan2.DeepCopyExpr(expr)
	var visit func(*plan.Expr) error
	visit = func(e *plan.Expr) error {
		if e == nil {
			return nil
		}
		if col := e.GetCol(); col != nil {
			if col.RelPos == 0 && parentPositions != nil {
				pos, ok := parentPositions[col.ColPos]
				if !ok {
					return moerr.NewInvalidInputNoCtx("foreign-key projection misses expression column")
				}
				col.ColPos = pos
			} else if col.RelPos == 1 && childToInput {
				col.RelPos = 0
			}
		}
		if f := e.GetF(); f != nil {
			for _, arg := range f.Args {
				if err := visit(arg); err != nil {
					return err
				}
			}
		}
		if list := e.GetList(); list != nil {
			for _, arg := range list.List {
				if err := visit(arg); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return copy, visit(copy)
}

func orderedFKParentOrdinals(expr *plan.Expr, into []int32) []int32 {
	if expr == nil {
		return into
	}
	if col := expr.GetCol(); col != nil && col.RelPos == 0 {
		return append(into, col.ColPos)
	}
	if f := expr.GetF(); f != nil {
		for _, arg := range f.Args {
			into = orderedFKParentOrdinals(arg, into)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			into = orderedFKParentOrdinals(arg, into)
		}
	}
	return into
}

type orderedFKLookup struct {
	projection              orderedFKProjection
	parentKeys              []*plan.Expr
	probeExecutors          []colexec.ExpressionExecutor
	selfExecutors           []colexec.ExpressionExecutor
	probeValues, selfValues []*vector.Vector // borrowed from the corresponding executors
	predicate, lockKey      colexec.ExpressionExecutor
	row                     *batch.Batch
	base, access            engine.Relation
	ready                   bool
	parents                 *message.JoinMap
	iterator                hashmap.Iterator
	witness                 *vector.Vector
	needsLock               bool
}

func (l *orderedFKLookup) free(proc *process.Process) {
	hashmap.IteratorClearOwner(l.iterator)
	l.iterator = nil
	if l.parents != nil {
		l.parents.Free()
		l.parents = nil
	}
	for _, executors := range [][]colexec.ExpressionExecutor{l.probeExecutors, l.selfExecutors} {
		for _, executor := range executors {
			if executor != nil {
				executor.Free()
			}
		}
	}
	if l.predicate != nil {
		l.predicate.Free()
	}
	if l.lockKey != nil {
		l.lockKey.Free()
	}
	if l.row != nil {
		l.row.Clean(proc.Mp())
	}
	if l.witness != nil {
		l.witness.Free(proc.Mp())
	}
}

func (preInsertUnique *PreInsertUnique) prepareOrderedFKLookup(proc *process.Process, check *plan.OrderedForeignKey, state *orderedForeignKeyResult) (err error) {
	if state.lookup != nil {
		return nil
	}
	l := &orderedFKLookup{}
	defer func() {
		if err != nil {
			l.free(proc)
		}
	}()
	if check.ParentAccess == nil {
		return moerr.NewInvalidInput(proc.Ctx, "missing ordered foreign-key access")
	}
	ordinals := append([]int32(nil), check.ParentColumns...)
	ordinals = orderedFKParentOrdinals(check.ParentAccess.WitnessLockKey, ordinals)
	l.projection, err = newOrderedFKProjection(check.TableDef, ordinals)
	if err != nil {
		return err
	}
	parentKeys, probeKeys, err := plan2.OrderedForeignKeyEqualityKeys(proc.Ctx, check)
	if err != nil {
		return err
	}
	l.parentKeys = make([]*plan.Expr, len(parentKeys))
	l.probeValues = make([]*vector.Vector, len(probeKeys))
	for i, key := range parentKeys {
		l.parentKeys[i], err = remapOrderedFKExpr(key, l.projection.positions, false)
		if err != nil {
			return err
		}
		probe, err := remapOrderedFKExpr(probeKeys[i], nil, true)
		if err != nil {
			return err
		}
		executor, err := colexec.NewExpressionExecutor(proc, probe)
		if err != nil {
			return err
		}
		l.probeExecutors = append(l.probeExecutors, executor)
	}
	if len(check.SelfParentColumns) > 0 {
		selfPositions := make(map[int32]int32, len(check.ParentColumns))
		for i, pos := range check.ParentColumns {
			if _, ok := selfPositions[pos]; !ok {
				selfPositions[pos] = int32(i)
			}
		}
		l.selfValues = make([]*vector.Vector, len(parentKeys))
		for _, key := range parentKeys {
			expr, err := remapOrderedFKExpr(key, selfPositions, false)
			if err != nil {
				return err
			}
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return err
			}
			l.selfExecutors = append(l.selfExecutors, executor)
		}
	}
	predicate, err := remapOrderedFKExpr(check.Predicate, l.projection.positions, false)
	if err != nil {
		return err
	}
	l.predicate, err = colexec.NewExpressionExecutor(proc, predicate)
	if err != nil {
		return err
	}
	lockKey, err := remapOrderedFKExpr(check.ParentAccess.WitnessLockKey, l.projection.positions, false)
	if err != nil {
		return err
	}
	l.lockKey, err = colexec.NewExpressionExecutor(proc, lockKey)
	if err != nil {
		return err
	}
	l.row = l.projection.newBatch()
	l.witness = vector.NewVec(types.MustTypeFromPlan(check.ParentAccess.LockTarget.PrimaryColTyp))
	state.lookup = l
	return nil
}

func evalOrderedFKKeys(proc *process.Process, executors []colexec.ExpressionExecutor, vectors, values []*vector.Vector) error {
	b := &batch.Batch{Vecs: vectors}
	b.SetRowCount(1)
	for i, executor := range executors {
		value, err := executor.Eval(proc, []*batch.Batch{b}, nil)
		if err != nil {
			return err
		}
		values[i] = value
	}
	return nil
}

func orderedFKRelation(proc *process.Process, tableID uint64) (engine.Relation, error) {
	_, _, rel, err := proc.Base.SessionInfo.StorageEngine.GetRelationById(proc.Ctx, proc.GetTxnOperator(), tableID)
	if err != nil {
		return nil, err
	}
	return engine.NewRelationHandle(rel), nil
}

// scanOrderedFK owns readers and their transient output even when BuildReaders
// returns a partial reader list together with an error. Every caller uses the
// fixed statement view; current-statement self visibility lives in the arbiter.
func (preInsertUnique *PreInsertUnique) scanOrderedFK(proc *process.Process, rel engine.Relation, filter *plan.Expr, projection *orderedFKProjection, visit func(*batch.Batch) (bool, error)) (err error) {
	if err = proc.Ctx.Err(); err != nil {
		return err
	}
	params := engine.DefaultRangesParam
	if filter != nil {
		params.BlockFilters = []*plan.Expr{filter}
	}
	params.TxnOffset = preInsertUnique.TxnOffset
	data, err := rel.Ranges(proc.Ctx, params)
	if err != nil {
		return err
	}
	readers, err := rel.BuildReaders(proc.Ctx, proc, filter, data, 1, params.TxnOffset, false, engine.Policy_CheckAll, engine.FilterHint{})
	defer func() {
		for _, reader := range readers {
			if reader != nil {
				if closeErr := reader.Close(); err == nil {
					err = closeErr
				}
			}
		}
	}()
	if err != nil {
		return err
	}
	output := projection.newBatch()
	defer output.Clean(proc.Mp())
	for _, reader := range readers {
		if reader == nil {
			return moerr.NewInvalidInput(proc.Ctx, "nil foreign-key reader")
		}
		for {
			if err = proc.Ctx.Err(); err != nil {
				return err
			}
			output.CleanOnlyData()
			end, readErr := reader.Read(proc.Ctx, projection.names, nil, proc.Mp(), output)
			if readErr != nil {
				return readErr
			}
			if output.RowCount() > 0 {
				stop, visitErr := visit(output)
				if visitErr != nil {
					return visitErr
				}
				if stop {
					return nil
				}
			}
			if end {
				break
			}
		}
	}
	return nil
}

func (preInsertUnique *PreInsertUnique) buildOrderedFKParents(proc *process.Process, check *plan.OrderedForeignKey, lookup *orderedFKLookup) (err error) {
	if lookup.ready {
		return nil
	}
	var builder hashbuild.HashmapBuilder
	defer builder.Free(proc)
	if err = builder.SetAllocationAccount(preInsertUnique.allocationAccount); err != nil {
		return err
	}
	if err = builder.Prepare(lookup.parentKeys, -1, -1, nil, proc); err != nil {
		return err
	}
	if lookup.base == nil {
		lookup.base, err = orderedFKRelation(proc, check.TableDef.TblId)
		if err != nil {
			return err
		}
	}
	err = preInsertUnique.scanOrderedFK(proc, lookup.base, nil, &lookup.projection, func(b *batch.Batch) (bool, error) {
		return false, builder.AppendResidentBatch(b, proc)
	})
	if err != nil {
		return err
	}
	parents, err := builder.FinishResidentBuild(proc)
	if err != nil {
		return err
	}
	lookup.parents, lookup.ready = parents, true
	if parents != nil {
		lookup.iterator = parents.NewIterator()
	}
	return nil
}

func (l *orderedFKLookup) captureWitness(proc *process.Process, source *batch.Batch, row int, tuple []*vector.Vector) (bool, error) {
	l.row.CleanOnlyData()
	for i, src := range source.Vecs {
		if err := l.row.Vecs[i].UnionOne(src, int64(vectorRowIndex(src, row)), proc.Mp()); err != nil {
			return false, err
		}
	}
	l.row.SetRowCount(1)
	child := &batch.Batch{Vecs: tuple}
	child.SetRowCount(1)
	matches, err := l.predicate.Eval(proc, []*batch.Batch{l.row, child}, nil)
	if err != nil {
		return false, err
	}
	if matches.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](matches, 0) {
		return false, nil
	}
	key, err := l.lockKey.Eval(proc, []*batch.Batch{l.row}, nil)
	if err != nil {
		return false, err
	}
	if key.IsNull(0) {
		return false, moerr.NewInvalidInput(proc.Ctx, "foreign-key witness has NULL physical key")
	}
	l.witness.CleanOnlyData()
	if err := l.witness.UnionOne(key, int64(vectorRowIndex(key, 0)), proc.Mp()); err != nil {
		return false, err
	}
	l.needsLock = true
	return true, nil
}

func bindOrderedFKTuple(proc *process.Process, expr *plan.Expr, tuple []*vector.Vector) (*plan.Expr, error) {
	bound := plan2.DeepCopyExpr(expr)
	var visit func(*plan.Expr) error
	visit = func(e *plan.Expr) error {
		if e == nil {
			return nil
		}
		if col := e.GetCol(); col != nil && col.RelPos == 1 {
			if col.ColPos < 0 || int(col.ColPos) >= len(tuple) || tuple[col.ColPos] == nil {
				return moerr.NewInvalidInput(proc.Ctx, "invalid foreign-key child tuple slot")
			}
			v := tuple[col.ColPos]
			literal := rule.GetConstantValue(v, true, uint64(vectorRowIndex(v, 0)))
			if literal == nil {
				return moerr.NewInvalidInput(proc.Ctx, "unsupported foreign-key child value")
			}
			*e = plan.Expr{Typ: v.GetType().PlanType(), Expr: &plan.Expr_Lit{Lit: literal}}
		}
		if f := e.GetF(); f != nil {
			for _, arg := range f.Args {
				if err := visit(arg); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(bound); err != nil {
		return nil, err
	}
	return plan2.ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, true, true)
}

func prepareOrderedFKFilter(proc *process.Process, expr *plan.Expr) (*plan.Expr, []colexec.ExpressionExecutor, error) {
	filters, executors, err := plan2.BuildFoldedFilterExprs(proc, []*plan.Expr{expr}, nil, true)
	if err != nil {
		return nil, nil, err
	}
	return filters[0], executors, nil
}

func freeOrderedFKExecutors(executors []colexec.ExpressionExecutor) {
	for _, executor := range executors {
		executor.Free()
	}
}

func (preInsertUnique *PreInsertUnique) pointOrderedFKParent(proc *process.Process, check *plan.OrderedForeignKey, state *orderedForeignKeyResult) (found bool, err error) {
	l, access := state.lookup, check.ParentAccess
	if l.base == nil {
		l.base, err = orderedFKRelation(proc, check.TableDef.TblId)
		if err != nil {
			return false, err
		}
	}
	bound, err := bindOrderedFKTuple(proc, access.LookupFilter, state.tuple)
	if err != nil {
		return false, err
	}
	filter, executors, err := prepareOrderedFKFilter(proc, bound)
	if err != nil {
		return false, err
	}
	defer freeOrderedFKExecutors(executors)
	visitParent := func(b *batch.Batch) (bool, error) {
		for row := 0; row < b.RowCount(); row++ {
			ok, err := l.captureWitness(proc, b, row, state.tuple)
			if err != nil || ok {
				found = ok
				return ok, err
			}
		}
		return false, nil
	}
	if access.Kind == plan.OrderedForeignKeyAccess_PK_POINT {
		err = preInsertUnique.scanOrderedFK(proc, l.base, filter, &l.projection, visitParent)
		return found, err
	}
	if l.access == nil {
		l.access, err = orderedFKRelation(proc, access.LookupTableDef.TblId)
		if err != nil {
			return false, err
		}
	}
	ordinals := orderedFKParentOrdinals(access.LookupFilter, nil)
	ordinals = orderedFKParentOrdinals(access.LookupBasePk, ordinals)
	projection, err := newOrderedFKProjection(access.LookupTableDef, ordinals)
	if err != nil {
		return false, err
	}
	// Storage Fold payloads are pruning bytes, not executable SQL expressions.
	predicate, err := remapOrderedFKExpr(bound, projection.positions, false)
	if err != nil {
		return false, err
	}
	predicateExecutor, err := colexec.NewExpressionExecutor(proc, predicate)
	if err != nil {
		return false, err
	}
	defer predicateExecutor.Free()
	keyExpr, err := remapOrderedFKExpr(access.LookupBasePk, projection.positions, false)
	if err != nil {
		return false, err
	}
	keyExecutor, err := colexec.NewExpressionExecutor(proc, keyExpr)
	if err != nil {
		return false, err
	}
	defer keyExecutor.Free()
	err = preInsertUnique.scanOrderedFK(proc, l.access, filter, &projection, func(indexRows *batch.Batch) (bool, error) {
		matches, err := predicateExecutor.Eval(proc, []*batch.Batch{indexRows}, nil)
		if err != nil {
			return false, err
		}
		keys, err := keyExecutor.Eval(proc, []*batch.Batch{indexRows}, nil)
		if err != nil {
			return false, err
		}
		for row := 0; row < indexRows.RowCount(); row++ {
			i := vectorRowIndex(matches, row)
			if matches.IsNull(uint64(i)) || !vector.GetFixedAtNoTypeCheck[bool](matches, i) {
				continue
			}
			key := rule.GetConstantValue(keys, true, uint64(vectorRowIndex(keys, row)))
			if key == nil {
				return false, moerr.NewInvalidInput(proc.Ctx, "invalid foreign-key base-row locator")
			}
			pkPos := check.TableDef.Name2ColIndex[check.TableDef.Pkey.PkeyColName]
			pkType := check.TableDef.Cols[pkPos].Typ
			expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
				{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pkPos, Name: check.TableDef.Pkey.PkeyColName}}},
				{Typ: keys.GetType().PlanType(), Expr: &plan.Expr_Lit{Lit: key}},
			})
			if err != nil {
				return false, err
			}
			expr, err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, true, true)
			if err != nil {
				return false, err
			}
			baseFilter, exes, err := prepareOrderedFKFilter(proc, expr)
			if err != nil {
				return false, err
			}
			err = preInsertUnique.scanOrderedFK(proc, l.base, baseFilter, &l.projection, visitParent)
			freeOrderedFKExecutors(exes)
			if err != nil || found {
				return found, err
			}
		}
		return false, nil
	})
	return found, err
}

// Standard GroupSels ordinals address the builder's DefaultBatchSize chunks.
// Hashing the complete SQL equality key makes any member a valid witness.
func (preInsertUnique *PreInsertUnique) residentOrderedFKParent(proc *process.Process, check *plan.OrderedForeignKey, state *orderedForeignKeyResult) (bool, error) {
	l := state.lookup
	if err := preInsertUnique.buildOrderedFKParents(proc, check, l); err != nil {
		return false, err
	}
	if l.parents == nil {
		return false, nil
	}
	ids, zs, err := l.iterator.Find(0, 1, l.probeValues)
	if err != nil {
		return false, err
	}
	if zs[0] == 0 || ids[0] == 0 {
		return false, nil
	}
	ordinal := int(ids[0] - 1)
	if !l.parents.HashOnUnique() {
		sels := l.parents.GetSels(ids[0] - 1)
		if len(sels) == 0 {
			return false, moerr.NewInternalError(proc.Ctx, "foreign-key group has no witness")
		}
		ordinal = int(sels[0])
	}
	batches := l.parents.GetBatches()
	return l.captureWitness(proc, batches[ordinal/colexec.DefaultBatchSize], ordinal%colexec.DefaultBatchSize, state.tuple)
}
