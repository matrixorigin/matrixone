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

package plan

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// numericValueKindLowerer moves the selected-branch kind beside the value at
// the plan owner that produces it. The kind is an ordinary BOOL expression;
// no vector or batch metadata crosses a relational boundary.
type numericValueKindLowerer struct {
	builder        *QueryBuilder
	pairs          map[[2]int32]*Expr
	active         map[[2]int32]bool
	parents        [][]int32
	distinctKeys   map[int32][]*Expr
	programs       map[int64]numericValueProgramRebinding
	recursive      map[int32]*recursiveNumericKindLayout
	slotKinds      map[[2]uint32]uint32
	slotKindActive map[[2]uint32]bool
}

type numericValueProgramRebinding struct {
	source *Expr
	bound  *Expr
}

// sameNumericValueProgramMemoExpr allows cost-based selectivity estimates to
// differ between copies of one lexical source. It only ignores Selectivity
// after cycle-safe retained-graph validation; all other protobuf fields still
// have to match exactly.
func sameNumericValueProgramMemoExpr(ctx context.Context, a, b *Expr) (bool, error) {
	if err := walkPersistedExprRoots(ctx, []*Expr{a, b}, func(*Expr) error { return nil }); err != nil {
		return false, err
	}
	if proto.Equal(a, b) {
		return true, nil
	}
	if a == nil || b == nil {
		return false, nil
	}
	aCopy, bCopy := proto.Clone(a).(*Expr), proto.Clone(b).(*Expr)
	if err := walkPersistedExprRoots(ctx, []*Expr{aCopy, bCopy}, func(expr *Expr) error {
		expr.Selectivity = 0
		return nil
	}); err != nil {
		return false, err
	}
	return proto.Equal(aCopy, bCopy), nil
}

type recursiveNumericKindLayout struct {
	visibleColumns int
	kindColumns    map[int32]int32
}

type numericValueLoweringRule struct {
	lowerer *numericValueKindLowerer
}

func (numericValueLoweringRule) MatchNode(*Node) bool { return false }
func (numericValueLoweringRule) IsApplyExpr() bool    { return true }
func (numericValueLoweringRule) ApplyNode(*Node) error {
	return nil
}

func (rule numericValueLoweringRule) ApplyExpr(expr *Expr) (*Expr, error) {
	if err := rule.lowerer.rewriteExpression(&expr); err != nil {
		return nil, err
	}
	return expr, nil
}

func (builder *QueryBuilder) lowerNumericValueKinds() error {
	l := &numericValueKindLowerer{
		builder:        builder,
		pairs:          make(map[[2]int32]*Expr),
		active:         make(map[[2]int32]bool),
		parents:        make([][]int32, len(builder.qry.Nodes)),
		distinctKeys:   make(map[int32][]*Expr),
		recursive:      make(map[int32]*recursiveNumericKindLayout),
		slotKinds:      make(map[[2]uint32]uint32),
		slotKindActive: make(map[[2]uint32]bool),
	}
	for nodeID, node := range builder.qry.Nodes {
		if node == nil {
			continue
		}
		for _, childID := range node.Children {
			if childID >= 0 && int(childID) < len(l.parents) {
				l.parents[childID] = append(l.parents[childID], int32(nodeID))
			}
		}
		if node.NodeType == planpb.Node_DISTINCT && len(node.Children) > 0 {
			childID := node.Children[0]
			if childID >= 0 && int(childID) < len(builder.qry.Nodes) {
				l.distinctKeys[int32(nodeID)] = distinctVisibleOutputRefs(builder.qry.Nodes[childID])
			}
		}
	}
	visitor := &VisitPlan{isUpdatePlan: builder.qry.StmtType == planpb.Query_UPDATE}
	rule := numericValueLoweringRule{lowerer: l}
	for nodeID, node := range builder.qry.Nodes {
		if node == nil {
			continue
		}
		if err := visitor.exploreNode(builder.GetContext(), rule, node, int32(nodeID)); err != nil {
			return err
		}
	}
	if builder.intoUserVariableKindDemand && len(builder.qry.IntoUserVariableKindColumns) == 0 {
		if err := builder.lowerSelectIntoResultKinds(l); err != nil {
			return err
		}
	}
	return nil
}

// NumericValueProgramOutputsForScalar binds selected string leaves for an
// internal scalar consumer and returns the raw value plus its same-evaluation
// BOOL kind. It is for local frontend evaluation of SET expressions, not a
// substitute for lowering relational columns in a query plan.
func NumericValueProgramOutputsForScalar(
	ctx CompilerContext, source *Expr,
) (raw, kind *Expr, paired bool, err error) {
	if ctx == nil || source == nil {
		return nil, nil, false, moerr.NewInvalidInputNoCtx("scalar numeric value source is nil")
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, true)
	lowerer := &numericValueKindLowerer{builder: builder}
	bound, err := lowerer.bindNumericValueLeaves(source)
	if err != nil {
		return nil, nil, false, err
	}
	if existingKind, ok := scalarNumericProgramKindRef(bound); ok {
		return bound, existingKind, true, nil
	}
	raw, kind, err = makeNumericValueProgramOutputs(builder.GetContext(), bound)
	if err != nil {
		return nil, nil, false, err
	}
	return raw, kind, !isFalseBoolLiteral(kind), nil
}

// lowerSelectIntoResultKinds appends ordinary BOOL passengers to the active
// top-level result projection. The collector reads only the visible prefix;
// keeping passengers in the plan lets prepared/retried execution use the
// exact source choice made by the same projection evaluation.
func (builder *QueryBuilder) lowerSelectIntoResultKinds(l *numericValueKindLowerer) error {
	q := builder.qry
	if q == nil || q.StmtType != planpb.Query_SELECT || q.HasReturning || len(q.Headings) == 0 {
		return nil
	}
	step := q.MainOutputStep()
	if step < 0 || int(step) >= len(q.Steps) {
		return moerr.NewInternalError(builder.GetContext(), "SELECT INTO has no main output step")
	}
	nodeID := q.Steps[step]
	if nodeID < 0 || int(nodeID) >= len(q.Nodes) || q.Nodes[nodeID] == nil {
		return moerr.NewInternalError(builder.GetContext(), "SELECT INTO has an invalid main output node")
	}
	node := q.Nodes[nodeID]
	visible := len(q.Headings)
	if visible > len(node.ProjectList) {
		return moerr.NewInternalError(builder.GetContext(), "SELECT INTO headings exceed its result projection")
	}
	layout := make([]int32, visible)
	for i := range layout {
		layout[i] = -1
	}
	added := false
	for i := 0; i < visible; i++ {
		expr := node.ProjectList[i]
		if expr == nil {
			return moerr.NewInternalError(builder.GetContext(), "SELECT INTO has a nil result expression")
		}
		typ := types.T(expr.Typ.Id)
		if !typ.IsMySQLString() && typ != types.T_any {
			continue
		}

		var raw, kind *Expr
		var err error
		switch {
		case scalarNumericProgramKindRefExists(expr):
			raw = DeepCopyExpr(expr)
			kind, _ = scalarNumericProgramKindRef(expr)
		case expr.GetCol() != nil:
			raw = DeepCopyExpr(expr)
			col := expr.GetCol()
			kind, err = l.kindForGlobalColumn(col.RelPos, col.ColPos)
		case expr.GetLit() != nil:
			raw = DeepCopyExpr(expr)
			lit := expr.GetLit()
			kind = makePlan2BoolConstExprWithType(!lit.Isnull && lit.GetIsBin())
		default:
			bound, bindErr := l.bindNumericValueLeaves(expr)
			if bindErr != nil {
				return bindErr
			}
			raw, kind, err = makeNumericValueProgramOutputs(builder.GetContext(), bound)
		}
		if err != nil {
			return err
		}
		if raw == nil || kind == nil || kind.Typ.Id != int32(types.T_bool) {
			return moerr.NewInternalError(builder.GetContext(), "SELECT INTO numeric kind lowering produced an invalid BOOL passenger")
		}
		node.ProjectList[i] = raw
		if isFalseBoolLiteral(kind) {
			continue
		}
		layout[i] = int32(len(node.ProjectList))
		node.ProjectList = append(node.ProjectList, kind)
		added = true
	}
	if added {
		q.IntoUserVariableKindColumns = layout
	}
	return nil
}

func scalarNumericProgramKindRefExists(value *Expr) bool {
	_, ok := scalarNumericProgramKindRef(value)
	return ok
}

// freezeDistinctParents captures a DISTINCT node's visible equality tuple
// immediately before a producer below it grows a BOOL passenger. Seeding every
// DISTINCT node changes plans that do not need a passenger and can turn an
// ordinary projection into an invalid extra grouping key.
func (l *numericValueKindLowerer) freezeDistinctParents(nodeID int32) {
	if nodeID < 0 || int(nodeID) >= len(l.parents) {
		return
	}
	seen := make(map[int32]struct{})
	queue := append([]int32(nil), l.parents[nodeID]...)
	for len(queue) != 0 {
		parentID := queue[0]
		queue = queue[1:]
		if _, ok := seen[parentID]; ok {
			continue
		}
		seen[parentID] = struct{}{}
		if parentID < 0 || int(parentID) >= len(l.builder.qry.Nodes) {
			continue
		}
		parent := l.builder.qry.Nodes[parentID]
		if parent == nil {
			continue
		}
		if parent.NodeType == planpb.Node_DISTINCT && len(parent.Children) > 0 &&
			len(parent.PhysicalEqualityKeyList) == 0 {
			if keys := l.distinctKeys[parentID]; len(keys) > 0 {
				parent.PhysicalEqualityKeyList = DeepCopyExprList(keys)
			}
		}
		queue = append(queue, l.parents[parentID]...)
	}
}

func (l *numericValueKindLowerer) rewriteExpression(expr **Expr) error {
	return l.rewriteExpressionScoped(expr, false, false)
}

func (l *numericValueKindLowerer) rewriteExpressionScoped(
	expr **Expr, preserveOrdinaryIntegerCast, allowFrozenPreparedTextKind bool,
) error {
	if expr == nil || *expr == nil {
		return nil
	}
	if f := (*expr).GetF(); f != nil {
		if f.Func != nil {
			fid, oid := function.DecodeOverloadID(f.Func.Obj)
			if fid == function.SCALAR_PROGRAM_REF && numericProgramDefinitionMatchesRef(*expr) {
				if raw, ok := numericProgramRawValue(*expr); ok {
					definition := f.Args[0].GetF()
					if definition != nil && len(definition.Args) == 2 {
						// Rebind the existing lexical owner in place. Its attached
						// REF0 and sibling REF1 must keep the same scope; wrapping the
						// raw selector in another program would replay it and recurse
						// through a fresh two-argument definition on every pass.
						scope, _ := numericProgramRefScope(*expr)
						if l.programs == nil {
							l.programs = make(map[int64]numericValueProgramRebinding)
						}
						program, exists := l.programs[scope]
						if exists {
							sameSource, err := sameNumericValueProgramMemoExpr(l.builder.GetContext(), program.source, raw)
							if err != nil {
								return err
							}
							sameBound, err := sameNumericValueProgramMemoExpr(l.builder.GetContext(), program.bound, raw)
							if err != nil {
								return err
							}
							if !sameSource && !sameBound {
								return moerr.NewInternalError(l.builder.GetContext(), "numeric value program scope has conflicting definitions")
							}
						}
						bound := program.bound
						if !exists {
							var err error
							bound, err = l.bindNumericValueLeaves(raw)
							if err != nil {
								return err
							}
							l.programs[scope] = numericValueProgramRebinding{
								source: DeepCopyExpr(raw),
								bound:  DeepCopyExpr(bound),
							}
						}
						updated, err := l.replaceNumericProgramRawSource(*expr, bound)
						if err != nil {
							return err
						}
						*expr = updated
						f = (*expr).GetF()
					}
				}
			}
			if allowFrozenPreparedTextKind && fid == function.CAST &&
				oid == function.NumericValueCastOverload && !f.SyntaxExplicitCast {
				restored, changed, err := l.restoreFrozenTextComparisonCast(*expr)
				if err != nil {
					return err
				}
				if changed {
					*expr = restored
					return l.rewriteExpressionScoped(expr, preserveOrdinaryIntegerCast, false)
				}
			}
			if fid == function.IFF && len(f.Args) == 3 &&
				(types.T(f.Args[0].Typ.Id).IsMySQLString() || types.T(f.Args[0].Typ.Id) == types.T_any) {
				raw, kind, err := l.numericArgumentProgramOutputs(f.Args[0], false)
				if err != nil {
					return err
				}
				if !isFalseBoolLiteral(kind) {
					f.Args[0] = raw
					f.Args = append(f.Args, kind)
				}
			}
			if !preserveOrdinaryIntegerCast && fid == function.CAST && oid >= 0 && oid <= 3 && len(f.Args) == 2 &&
				(types.T(f.Args[0].Typ.Id).IsMySQLString() || types.T(f.Args[0].Typ.Id) == types.T_any) {
				target := types.T(f.Args[1].Typ.Id)
				if target.ToType().IsNumeric() {
					raw, kind, err := l.numericArgumentProgramOutputs(
						f.Args[0], allowFrozenPreparedTextKind && !f.SyntaxExplicitCast,
					)
					if err != nil {
						return err
					}
					if !isFalseBoolLiteral(kind) {
						policies := [...]int32{
							function.NumericValueCastPolicyOrdinary,
							function.NumericValueCastPolicyExplicit,
							function.NumericValueCastPolicyComparison,
							function.NumericValueCastPolicySetOperation,
						}
						converted, err := appendNumericValueCastWithKind(
							l.builder.GetContext(), raw, kind, f.Args[1].Typ, policies[oid])
						if err != nil {
							return err
						}
						// Keep metadata attached to this original CAST occurrence on
						// the owning replacement. The program refs are only execution
						// inputs; prepared source identity and SQL CAST syntax belong
						// to the converted expression itself.
						converted.PreparedNumeric = copyPreparedNumericMetadata((*expr).PreparedNumeric)
						converted.GetF().SyntaxExplicitCast = f.SyntaxExplicitCast
						*expr = converted
						return l.rewriteExpression(expr)
					}
				} else if isTemporalNumericKindTarget(target) {
					raw, kind, err := l.numericArgumentProgramOutputs(f.Args[0], false)
					if err != nil {
						return err
					}
					if !isFalseBoolLiteral(kind) {
						f.Args[0] = raw
						f.Args = append(f.Args, kind)
					}
				}
			}
			if (fid == function.CAST_STRICT || fid == function.CAST_ASSIGN || fid == function.CAST_IGNORE) && len(f.Args) == 2 &&
				(types.T(f.Args[0].Typ.Id).IsMySQLString() || types.T(f.Args[0].Typ.Id) == types.T_any) &&
				(types.T(f.Args[1].Typ.Id).ToType().IsNumeric() || isTemporalNumericKindTarget(types.T(f.Args[1].Typ.Id))) {
				raw, kind, err := l.numericArgumentProgramOutputs(f.Args[0], false)
				if err != nil {
					return err
				}
				if !isFalseBoolLiteral(kind) {
					f.Args[0] = raw
					f.Args = append(f.Args, kind)
				}
			}
			if fid == function.CAST && oid == function.NumericValueCastOverload && len(f.Args) == 4 {
				raw, ok := numericProgramRawValue(f.Args[0])
				if ok {
					if col := raw.GetCol(); col != nil && col.RelPos >= 0 {
						kind, err := l.kindForGlobalColumn(col.RelPos, col.ColPos)
						if err != nil {
							return err
						}
						f.Args[0] = DeepCopyExpr(raw)
						f.Args[2] = kind
					}
				}
			}
			if fid == function.CAST && function.IsIntegerArgumentCastOverload(oid) && len(f.Args) == 2 &&
				types.T(f.Args[0].Typ.Id).IsMySQLString() {
				raw, kind, err := l.numericArgumentProgramOutputs(f.Args[0], false)
				if err != nil {
					return err
				}
				if !isFalseBoolLiteral(kind) {
					f.Args[0] = raw
					f.Args = append(f.Args, kind)
				}
			}
			if positions, baseArity := function.NumericValueKindArgumentPositions(f.Func.ObjName); len(positions) > 0 {
				// Persisted OCT overloads keep their legacy calendar/string
				// conversion contract; only the current string executor consumes
				// a numeric-literal kind companion.
				if fid != function.OCT || oid >= function.OctStringOverloadStart {
					if baseArity == 0 {
						return moerr.NewInternalError(l.builder.GetContext(), "numeric-value kind registry is missing function arity")
					}
					if len(f.Args) == baseArity {
						companions := make([]*Expr, len(positions))
						rawValues := make([]*Expr, len(positions))
						needsKind := false
						for i, position := range positions {
							if position < 0 || position >= baseArity {
								return moerr.NewInternalError(l.builder.GetContext(), "numeric-value kind registry position exceeds function arity")
							}
							source := f.Args[position]
							sourceType := types.T(source.Typ.Id)
							if sourceType.IsMySQLString() || sourceType == types.T_any {
								raw, kind, err := l.numericArgumentProgramOutputs(source, false)
								if err != nil {
									return err
								}
								rawValues[i], companions[i] = raw, kind
								needsKind = needsKind || !isFalseBoolLiteral(kind)
							} else {
								companions[i] = makePlan2BoolConstExprWithType(false)
							}
						}
						if needsKind {
							for i, position := range positions {
								if rawValues[i] != nil {
									f.Args[position] = rawValues[i]
								}
							}
							f.Args = append(f.Args, companions...)
						}
					} else if len(f.Args) > baseArity {
						// A previous pre- or post-optimization pass already
						// installed these companions.
						if len(f.Args) != baseArity+len(positions) {
							return moerr.NewInternalError(l.builder.GetContext(), "numeric-value kind companion arity is inconsistent")
						}
					}
				}
			}
		}
		for i := range f.Args {
			_, preserveIntegerCast := function.IntegerArgumentOrdinaryCastTarget(f.Func.ObjName, i)
			allowFrozenKind := isNumericComparisonCastOperand(f.Func.ObjName, f.Args[i])
			if f.Func.ObjName == "in" || f.Func.ObjName == "not_in" {
				if i == 1 && f.Args[i].GetList() != nil {
					for j := range f.Args[i].GetList().List {
						item := f.Args[i].GetList().List[j]
						if err := l.rewriteExpressionScoped(&f.Args[i].GetList().List[j], false,
							isNumericComparisonCastOperand(f.Func.ObjName, item)); err != nil {
							return err
						}
					}
					continue
				}
			}
			if f.Func.ObjName == "cast" && allowFrozenPreparedTextKind && !f.SyntaxExplicitCast &&
				isNumericConversionCast(f.Args[i]) {
				allowFrozenKind = true
			}
			if err := l.rewriteExpressionScoped(&f.Args[i], preserveIntegerCast, allowFrozenKind); err != nil {
				return err
			}
		}
		return nil
	}
	if list := (*expr).GetList(); list != nil {
		for i := range list.List {
			if err := l.rewriteExpressionScoped(&list.List[i], false, false); err != nil {
				return err
			}
		}
		return nil
	}
	if sub := (*expr).GetSub(); sub != nil {
		return l.rewriteExpressionScoped(&sub.Child, false, false)
	}
	if (*expr).GetW() != nil {
		_, err := applyWindowExpr(*expr, func(child *Expr) (*Expr, error) {
			if err := l.rewriteExpressionScoped(&child, false, false); err != nil {
				return nil, err
			}
			return child, nil
		})
		return err
	}
	return nil
}

func isNumericConversionCast(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	fid, overload := function.DecodeOverloadID(fn.Func.Obj)
	return fid == function.CAST && (overload >= function.NumericValueCastPolicyOrdinary &&
		overload <= function.NumericValueCastPolicySetOperation || overload == function.NumericValueCastOverload)
}

func (l *numericValueKindLowerer) restoreFrozenTextComparisonCast(expr *Expr) (*Expr, bool, error) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 4 {
		return nil, false, nil
	}
	source, policy, ok := numericValueCastAnalysisSource(expr)
	if !ok || source == nil || source.GetP() == nil {
		return nil, false, nil
	}
	valueScope, valueSlot, valueOK := numericProgramRefIdentity(fn.Args[0])
	kindScope, kindSlot, kindOK := numericProgramRefIdentity(fn.Args[2])
	if !valueOK || !kindOK || valueSlot != 0 || kindSlot != 1 || valueScope != kindScope ||
		!l.preparedParamKindFrozenFalse(source) {
		return nil, false, nil
	}
	target := DeepCopyExpr(fn.Args[1]).Typ
	restored, err := appendCastBeforeExprWithOverload(l.builder.GetContext(), source, target, policy)
	if err != nil {
		return nil, false, err
	}
	restored.PreparedNumeric = copyPreparedNumericMetadata(expr.PreparedNumeric)
	if restored.GetF() != nil {
		restored.GetF().SyntaxExplicitCast = fn.SyntaxExplicitCast
	}
	return restored, true, nil
}

func isNumericComparisonCastOperand(parentName string, operand *Expr) bool {
	switch parentName {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=", "between", "in", "not_in":
	default:
		return false
	}
	fn := operand.GetF()
	if fn == nil || fn.Func == nil || fn.SyntaxExplicitCast || len(fn.Args) != 2 {
		return false
	}
	fid, overload := function.DecodeOverloadID(fn.Func.Obj)
	return fid == function.CAST && overload >= function.NumericValueCastPolicyOrdinary &&
		overload <= function.NumericValueCastPolicySetOperation
}

func isTemporalNumericKindTarget(target types.T) bool {
	switch target {
	case types.T_date, types.T_time, types.T_datetime, types.T_timestamp:
		return true
	default:
		return false
	}
}

func (l *numericValueKindLowerer) numericArgumentProgramOutputs(
	source *Expr, allowFrozenPreparedTextKind bool,
) (*Expr, *Expr, error) {
	if slot := source.GetSlot(); slot != nil {
		kind, err := l.kindForCorrelationSlot(slot)
		return DeepCopyExpr(source), kind, err
	}
	if call := source.GetDependent(); call != nil {
		paired, err := l.ensureDependentScalarResultKind(call)
		if err != nil {
			return nil, nil, err
		}
		if !paired {
			return DeepCopyExpr(source), makePlan2BoolConstExprWithType(false), nil
		}
		return makeNumericValueProgramOutputs(l.builder.GetContext(), source)
	}
	if raw, ok := numericProgramRawValue(source); ok {
		if scope, scopeOK := numericProgramRefScope(source); scopeOK {
			definition := source.GetF().Args[0].GetF()
			if definition != nil && len(definition.Args) == 2 {
				bound, err := l.bindNumericValueLeaves(raw)
				if err != nil {
					return nil, nil, err
				}
				pairedValue, err := l.replaceNumericProgramRawSource(source, bound)
				if err != nil {
					return nil, nil, err
				}
				return pairedValue, scalarNumericProgramRef(scope, 1, makeSimplePlan2Type(types.T_bool)), nil
			}
			return DeepCopyExpr(source), scalarNumericProgramRef(scope, 1, makeSimplePlan2Type(types.T_bool)), nil
		}
	}
	if col := source.GetCol(); col != nil && col.RelPos >= 0 {
		kind, err := l.kindForGlobalColumn(col.RelPos, col.ColPos)
		return DeepCopyExpr(source), kind, err
	}
	if literal := source.GetLit(); literal != nil {
		return DeepCopyExpr(source), makePlan2BoolConstExprWithType(!literal.Isnull && literal.GetIsBin()), nil
	}
	if allowFrozenPreparedTextKind && l.preparedParamKindFrozenFalse(source) {
		return DeepCopyExpr(source), makePlan2BoolConstExprWithType(false), nil
	}
	bound, err := l.bindNumericValueLeaves(source)
	if err != nil {
		return nil, nil, err
	}
	return makeNumericValueProgramOutputs(l.builder.GetContext(), bound)
}

func (l *numericValueKindLowerer) kindForCorrelationSlot(slot *planpb.CorrelationSlot) (*Expr, error) {
	if slot == nil || l.builder.qry.Correlation == nil || slot.ProgramId == 0 ||
		int(slot.ProgramId) > len(l.builder.qry.Correlation.Programs) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	key := [2]uint32{slot.ProgramId, slot.Ordinal}
	program := l.builder.qry.Correlation.Programs[slot.ProgramId-1]
	if int(slot.Ordinal) >= len(program.InputTypes) {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "numeric kind demand references an invalid correlation slot")
	}
	if kindOrdinal, ok := l.slotKinds[key]; ok {
		return &Expr{Typ: *DeepCopyType(&program.InputTypes[kindOrdinal]), Expr: &planpb.Expr_Slot{Slot: &planpb.CorrelationSlot{
			ProgramId: slot.ProgramId, Ordinal: kindOrdinal,
		}}}, nil
	}
	if l.slotKindActive[key] {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "cyclic correlation numeric kind demand")
	}
	l.slotKindActive[key] = true
	defer delete(l.slotKindActive, key)

	type inputOwner struct {
		call   *planpb.DependentExpr
		raw    *Expr
		kind   *Expr
		paired bool
	}
	var owners []inputOwner
	seen := make(map[*planpb.DependentExpr]struct{})
	for _, node := range l.builder.qry.Nodes {
		if node == nil {
			continue
		}
		if err := planpb.VisitExpressionsInOwner(node, func(root *Expr) error {
			return planpb.VisitExprTree(root, func(expr *Expr) error {
				call := expr.GetDependent()
				if call == nil || call.ProgramId != slot.ProgramId {
					return nil
				}
				if _, exists := seen[call]; exists {
					return nil
				}
				seen[call] = struct{}{}
				if int(slot.Ordinal) >= len(call.Inputs) || !proto.Equal(&call.Inputs[slot.Ordinal].Typ, &program.InputTypes[slot.Ordinal]) {
					return moerr.NewInvalidInput(l.builder.GetContext(), "numeric kind demand has an inconsistent correlation input")
				}
				raw, kind, err := l.numericArgumentProgramOutputs(call.Inputs[slot.Ordinal], false)
				if err != nil {
					return err
				}
				if raw == nil || !proto.Equal(&raw.Typ, &program.InputTypes[slot.Ordinal]) {
					return moerr.NewInvalidInput(l.builder.GetContext(), "numeric kind demand changes a correlation input type")
				}
				owners = append(owners, inputOwner{call: call, raw: raw, kind: kind, paired: !isFalseBoolLiteral(kind)})
				return nil
			})
		}); err != nil {
			return nil, err
		}
	}
	if len(owners) == 0 {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "numeric kind demand has no correlation call site")
	}
	anyPaired := false
	for _, owner := range owners {
		anyPaired = anyPaired || owner.paired
	}
	if !anyPaired {
		return makePlan2BoolConstExprWithType(false), nil
	}
	if uint64(len(program.InputTypes)) >= uint64(^uint32(0)) {
		return nil, moerr.NewOOM(l.builder.GetContext())
	}
	kindOrdinal := uint32(len(program.InputTypes))
	kindType := makeSimplePlan2Type(types.T_bool)
	for _, owner := range owners {
		kind := owner.kind
		if !owner.paired {
			kind = makePlan2BoolConstExprWithType(false)
		}
		if kind == nil || types.T(kind.Typ.Id) != types.T_bool {
			return nil, moerr.NewInvalidInput(l.builder.GetContext(), "correlation numeric kind input is not BOOL")
		}
		kind = DeepCopyExpr(kind)
		kind.Typ = kindType
		// Publish the transformed value together with its passenger. Leaving
		// the original selector here would make REF1 describe different bytes
		// (and can leave it pointing at an orphaned lexical owner).
		owner.call.Inputs[slot.Ordinal] = DeepCopyExpr(owner.raw)
		owner.call.Inputs = append(owner.call.Inputs, kind)
	}
	program.InputTypes = append(program.InputTypes, kindType)
	l.slotKinds[key] = kindOrdinal
	return &Expr{Typ: kindType, Expr: &planpb.Expr_Slot{Slot: &planpb.CorrelationSlot{
		ProgramId: slot.ProgramId, Ordinal: kindOrdinal,
	}}}, nil
}

func (l *numericValueKindLowerer) ensureDependentScalarResultKind(call *planpb.DependentExpr) (bool, error) {
	if call == nil || call.Reducer != planpb.SubqueryRef_SCALAR || call.Comparison != nil ||
		call.ProgramId == 0 || l.builder.qry.Correlation == nil ||
		int(call.ProgramId) > len(l.builder.qry.Correlation.Programs) {
		return false, nil
	}
	program := l.builder.qry.Correlation.Programs[call.ProgramId-1]
	if program.ScalarResultKindColumn == 1 {
		if len(program.OutputTypes) != 2 || types.T(program.OutputTypes[1].Id) != types.T_bool {
			return false, moerr.NewInvalidInput(l.builder.GetContext(), "invalid dependent scalar numeric kind layout")
		}
		return true, nil
	}
	if program.ScalarResultKindColumn != 0 || len(program.OutputTypes) != 1 {
		return false, moerr.NewInvalidInput(l.builder.GetContext(), "dependent scalar cannot carry a numeric kind")
	}
	if program.RootNode < 0 || int(program.RootNode) >= len(l.builder.qry.Nodes) || l.builder.qry.Nodes[program.RootNode] == nil {
		return false, moerr.NewInvalidInput(l.builder.GetContext(), "dependent scalar has an invalid result owner")
	}
	root := l.builder.qry.Nodes[program.RootNode]
	if len(root.ProjectList) != 1 {
		return false, moerr.NewInvalidInput(l.builder.GetContext(), "dependent scalar result is not one visible column")
	}
	tag := int32(0)
	if len(root.BindingTags) > 0 {
		tag = root.BindingTags[0]
	}
	kind, err := l.kindForNodeOutput(program.RootNode, tag, 0)
	if err != nil || isFalseBoolLiteral(kind) {
		return false, err
	}
	if len(root.ProjectList) == 1 {
		root.ProjectList = append(root.ProjectList, DeepCopyExpr(kind))
	} else if len(root.ProjectList) != 2 || types.T(root.ProjectList[1].Typ.Id) != types.T_bool {
		return false, moerr.NewInvalidInput(l.builder.GetContext(), "dependent scalar result kind changed output arity")
	}
	kindType := makeSimplePlan2Type(types.T_bool)
	root.ProjectList[1].Typ = kindType
	program.OutputTypes = append(program.OutputTypes, kindType)
	program.ScalarResultKindColumn = 1
	return true, nil
}

// bindNumericValueLeaves attaches already-produced BOOL companions only at
// value-preserving selector leaves. Conditions and ordinary allocating
// functions stay in the original tree, so this never re-evaluates a branch or
// carries provenance through a new string value.
func (l *numericValueKindLowerer) bindNumericValueLeaves(source *Expr) (*Expr, error) {
	if source == nil {
		return nil, nil
	}
	if slot := source.GetSlot(); slot != nil &&
		(types.T(source.Typ.Id).IsMySQLString() || types.T(source.Typ.Id) == types.T_any) {
		raw, kind, err := l.numericArgumentProgramOutputs(source, false)
		if err != nil || isFalseBoolLiteral(kind) {
			return raw, err
		}
		paired, _, err := makeNumericValueProgramOutputsWithKind(l.builder.GetContext(), raw, kind)
		return paired, err
	}
	if call := source.GetDependent(); call != nil {
		paired, err := l.ensureDependentScalarResultKind(call)
		if err != nil || !paired {
			return DeepCopyExpr(source), err
		}
		return DeepCopyExpr(source), nil
	}
	if col := source.GetCol(); col != nil && col.RelPos >= 0 &&
		(types.T(source.Typ.Id).IsMySQLString() || types.T(source.Typ.Id) == types.T_any) {
		kind, err := l.kindForGlobalColumn(col.RelPos, col.ColPos)
		if err != nil {
			return nil, err
		}
		if isFalseBoolLiteral(kind) {
			return DeepCopyExpr(source), nil
		}
		value, _, err := makeNumericValueProgramOutputsWithKind(l.builder.GetContext(), source, kind)
		return value, err
	}

	f := source.GetF()
	if f == nil || f.Func == nil {
		return DeepCopyExpr(source), nil
	}
	fid, overload := function.DecodeOverloadID(f.Func.GetObj())
	if fid == function.SCALAR_PROGRAM_REF {
		if raw, ok := numericProgramRawValue(source); ok {
			definition := f.Args[0].GetF()
			if definition != nil && len(definition.Args) == 2 {
				bound, err := l.bindNumericValueLeaves(raw)
				if err != nil {
					return nil, err
				}
				return l.replaceNumericProgramRawSource(source, bound)
			}
		}
		return DeepCopyExpr(source), nil
	}

	name := strings.ToLower(f.Func.GetObjName())
	if fid == function.CAST {
		if isInferredStringDeclarationCast(source) {
			value, kind, err := l.numericArgumentProgramOutputs(f.Args[0], false)
			if err != nil {
				return nil, err
			}
			cast := DeepCopyExpr(source)
			cast.GetF().Args[0] = value
			if isFalseBoolLiteral(kind) {
				return cast, nil
			}
			paired, _, err := makeNumericValueProgramOutputsWithKind(l.builder.GetContext(), cast, kind)
			return paired, err
		}
		if overload != 0 || f.SyntaxExplicitCast || len(f.Args) != 2 || f.Args[0] == nil ||
			!types.T(source.Typ.Id).IsMySQLString() || !types.T(f.Args[0].Typ.Id).IsMySQLString() {
			return DeepCopyExpr(source), nil
		}
		bound, err := l.bindNumericValueLeaves(f.Args[0])
		if err != nil {
			return nil, err
		}
		copy := DeepCopyExpr(source)
		copy.GetF().Args[0] = bound
		return copy, nil
	}

	indexes := controlFlowValueIndexes(name, len(f.Args))
	switch name {
	case "ifnull":
		if len(f.Args) == 2 {
			indexes = []int{0, 1}
		}
	case "nullif":
		if len(f.Args) > 0 {
			indexes = []int{0}
		}
	}
	if len(indexes) == 0 {
		return DeepCopyExpr(source), nil
	}
	copy := DeepCopyExpr(source)
	for _, index := range indexes {
		if index < 0 || index >= len(f.Args) || f.Args[index] == nil {
			return DeepCopyExpr(source), nil
		}
		bound, err := l.bindNumericValueLeaves(f.Args[index])
		if err != nil {
			return nil, err
		}
		copy.GetF().Args[index] = bound
	}
	return copy, nil
}

func (l *numericValueKindLowerer) replaceNumericProgramRawSource(owner, source *Expr) (*Expr, error) {
	if owner == nil || source == nil || owner.GetF() == nil || len(owner.GetF().Args) != 3 {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "numeric value program reference is malformed")
	}
	if _, ok := numericProgramRawValue(owner); !ok {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "numeric value program reference has no validated source")
	}
	copy := DeepCopyExpr(owner)
	definition := copy.GetF().Args[0].GetF()
	if definition == nil || len(definition.Args) != 2 {
		return nil, moerr.NewInvalidInput(l.builder.GetContext(), "numeric value program definition is not a selector owner")
	}
	definition.Args[1] = DeepCopyExpr(source)
	return copy, nil
}

// preparedParamKindFrozenFalse permits the execution-specific plan to use the
// existing ordinary cast path when its live parameter is known to have false
// IsBin provenance. Keep the ParamRef and mark this plan value-dependent so a
// later binary-kind binding is rebound through the paired path.
func (l *numericValueKindLowerer) preparedParamKindFrozenFalse(source *Expr) bool {
	if source == nil || source.GetP() == nil {
		return false
	}
	state := preparedBindingState(l.builder.GetContext())
	if state == nil {
		return false
	}
	position := source.GetP().Pos
	binding, ok := state.bindingForPosition(position)
	if !ok || !binding.Type.Oid.IsMySQLString() ||
		types.StaticStringDomain(binding.Type) != types.StringDomainText ||
		position < 0 || int(position) >= len(state.values) {
		return false
	}
	value, ok := state.values[position].(ParamValue)
	if !ok || value.Value == nil || value.IsBin || value.IsBinaryString ||
		value.RuntimeStringDomain == types.RuntimeStringBinary ||
		(value.HasSourceType && (!value.SourceType.Oid.IsMySQLString() ||
			types.StaticStringDomain(value.SourceType) != types.StringDomainText)) ||
		(value.HasRuntimeType && (!value.RuntimeType.Oid.IsMySQLString() ||
			types.StaticStringDomain(value.RuntimeType) != types.StringDomainText)) {
		return false
	}
	state.valueDependent = true
	return true
}

func numericProgramRawValue(expr *Expr) (*Expr, bool) {
	f := expr.GetF()
	if f == nil || f.Func == nil {
		return nil, false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj)
	if fid != function.SCALAR_PROGRAM_REF || len(f.Args) != 3 {
		return nil, false
	}
	definition := f.Args[0].GetF()
	if definition == nil || definition.Func == nil || (len(definition.Args) != 2 && len(definition.Args) != 3) {
		return nil, false
	}
	defID, _ := function.DecodeOverloadID(definition.Func.Obj)
	refScope, slot, refOK := numericProgramRefIdentity(expr)
	defScope, scopeOK := numericProgramDefinitionScope(f.Args[0])
	if defID != function.SCALAR_PROGRAM || !refOK || !scopeOK || refScope != defScope ||
		slot != 0 || definition.Args[1] == nil {
		return nil, false
	}
	return definition.Args[1], true
}

func (l *numericValueKindLowerer) kindForGlobalColumn(tag, col int32) (*Expr, error) {
	key := [2]int32{tag, col}
	if kind := l.pairs[key]; kind != nil {
		return DeepCopyExpr(kind), nil
	}
	if l.active[key] {
		return makePlan2BoolConstExprWithType(false), nil
	}
	l.active[key] = true
	defer delete(l.active, key)
	if nodeID, ok := l.builder.tag2NodeID[tag]; ok && nodeID >= 0 && int(nodeID) < len(l.builder.qry.Nodes) {
		kind, err := l.kindAtNodeOutput(nodeID, tag, col)
		if err != nil {
			return nil, err
		}
		l.pairs[key] = DeepCopyExpr(kind)
		return kind, nil
	}
	return makePlan2BoolConstExprWithType(false), nil
}

func (l *numericValueKindLowerer) kindAtNodeOutput(nodeID int32, tag, col int32) (*Expr, error) {
	if nodeID < 0 || int(nodeID) >= len(l.builder.qry.Nodes) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	node := l.builder.qry.Nodes[nodeID]
	if node == nil {
		return makePlan2BoolConstExprWithType(false), nil
	}
	if kind := l.pairs[[2]int32{tag, col}]; kind != nil {
		return DeepCopyExpr(kind), nil
	}
	switch node.NodeType {
	case planpb.Node_PROJECT, planpb.Node_MATERIAL:
		if col < 0 || int(col) >= len(node.ProjectList) {
			return makePlan2BoolConstExprWithType(false), nil
		}
		return l.kindForProjectOutput(nodeID, node, int(col))
	case planpb.Node_FILTER, planpb.Node_ASSERT, planpb.Node_SORT, planpb.Node_PARTITION:
		if len(node.Children) == 0 {
			return makePlan2BoolConstExprWithType(false), nil
		}
		return l.kindForNodeOutput(node.Children[0], tag, col)
	case planpb.Node_JOIN:
		return l.kindForJoinOutput(nodeID, node, tag, col)
	case planpb.Node_UNION, planpb.Node_UNION_ALL, planpb.Node_INTERSECT,
		planpb.Node_INTERSECT_ALL, planpb.Node_MINUS, planpb.Node_MINUS_ALL:
		return l.kindForSetOutput(nodeID, node, tag, col)
	case planpb.Node_DISTINCT:
		return l.kindForDistinctOutput(nodeID, node, col)
	case planpb.Node_AGG:
		return l.kindForAggregateOutput(nodeID, node, tag, col)
	case planpb.Node_SINK_SCAN:
		return l.kindForSinkScanOutput(nodeID, node, tag, col)
	case planpb.Node_SINK:
		if node.RecursiveSink {
			return l.kindForRecursiveSinkOutput(nodeID, node, col)
		}
		return l.kindForSinkOutput(nodeID, node, col)
	default:
		// Stored columns and scan outputs have ordinary SQL string semantics.
		return makePlan2BoolConstExprWithType(false), nil
	}
}

func (l *numericValueKindLowerer) kindForNodeOutput(nodeID, tag, col int32) (*Expr, error) {
	if nodeID < 0 || int(nodeID) >= len(l.builder.qry.Nodes) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	node := l.builder.qry.Nodes[nodeID]
	if len(node.BindingTags) > 0 {
		for _, bindingTag := range node.BindingTags {
			if bindingTag == tag {
				return l.kindAtNodeOutput(nodeID, tag, col)
			}
		}
	}
	return l.kindAtNodeOutput(nodeID, tag, col)
}

func (l *numericValueKindLowerer) kindForProjectOutput(nodeID int32, node *planpb.Node, col int) (*Expr, error) {
	var key [2]int32
	if len(node.BindingTags) > 0 {
		key = [2]int32{node.BindingTags[0], int32(col)}
		if kind := l.pairs[key]; kind != nil {
			return DeepCopyExpr(kind), nil
		}
	}
	value := node.ProjectList[col]
	if value == nil || (!types.T(value.Typ.Id).IsMySQLString() && types.T(value.Typ.Id) != types.T_any) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	if _, ok := scalarNumericProgramKindRef(value); ok {
		bound, err := l.bindNumericValueLeaves(value)
		if err != nil {
			return nil, err
		}
		node.ProjectList[col] = bound
		value = bound
		kind, _ := scalarNumericProgramKindRef(value)
		if len(node.BindingTags) == 0 {
			l.pairs[key] = kind
			return DeepCopyExpr(kind), nil
		}
		scope, _ := numericProgramRefScope(value)
		for outputPos, output := range node.ProjectList {
			if existingScope, slot, ok := numericProgramRefIdentity(output); ok && existingScope == scope && slot == 1 {
				kind = GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], int32(outputPos))
				l.pairs[key] = kind
				return DeepCopyExpr(kind), nil
			}
		}
		l.freezeDistinctParents(nodeID)
		kindPos := int32(len(node.ProjectList))
		node.ProjectList = append(node.ProjectList, kind)
		kind = GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], kindPos)
		l.pairs[key] = kind
		return DeepCopyExpr(kind), nil
	}
	if colRef := value.GetCol(); colRef != nil {
		var inputKind *Expr
		var err error
		if colRef.RelPos == 0 && len(node.Children) > 0 {
			inputKind, err = l.kindForNodeOutput(node.Children[0], colRef.RelPos, colRef.ColPos)
		} else if colRef.RelPos >= 0 {
			inputKind, err = l.kindForGlobalColumn(colRef.RelPos, colRef.ColPos)
		} else {
			inputKind = makePlan2BoolConstExprWithType(false)
		}
		if err != nil {
			return nil, err
		}
		if isFalseBoolLiteral(inputKind) {
			l.pairs[key] = inputKind
			return DeepCopyExpr(inputKind), nil
		}
		if len(node.BindingTags) == 0 {
			l.pairs[key] = DeepCopyExpr(inputKind)
			return inputKind, nil
		}
		// Export the child's kind through this PROJECT's own schema. Returning
		// a grandchild reference loses lineage when an identity projection is
		// retained (for example, below LIMIT or beside a non-removable output).
		l.freezeDistinctParents(nodeID)
		kindPos := int32(len(node.ProjectList))
		node.ProjectList = append(node.ProjectList, DeepCopyExpr(inputKind))
		projectedKind := GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], kindPos)
		l.pairs[key] = projectedKind
		return DeepCopyExpr(projectedKind), nil
	}
	if literal := value.GetLit(); literal != nil {
		kind := makePlan2BoolConstExprWithType(!literal.Isnull && literal.GetIsBin())
		l.pairs[key] = kind
		return DeepCopyExpr(kind), nil
	}
	bound, err := l.bindNumericValueLeaves(value)
	if err != nil {
		return nil, err
	}
	valueRef, kindRef, err := makeNumericValueProgramOutputsWithKind(l.builder.GetContext(), bound, nil)
	if err != nil {
		return nil, err
	}
	if isFalseBoolLiteral(kindRef) {
		l.pairs[key] = DeepCopyExpr(kindRef)
		return DeepCopyExpr(kindRef), nil
	}
	node.ProjectList[col] = valueRef
	if len(node.BindingTags) == 0 {
		return kindRef, nil
	}
	l.freezeDistinctParents(nodeID)
	kindPos := int32(len(node.ProjectList))
	node.ProjectList = append(node.ProjectList, kindRef)
	kind := GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], kindPos)
	l.pairs[key] = kind
	_ = nodeID
	return DeepCopyExpr(kind), nil
}

func scalarNumericProgramKindRef(value *Expr) (*Expr, bool) {
	f := value.GetF()
	if f == nil || f.Func == nil {
		return nil, false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj)
	if fid != function.SCALAR_PROGRAM_REF || len(f.Args) != 3 {
		return nil, false
	}
	scope, ok := numericPlanInt64(f.Args[1])
	slot, slotOK := numericPlanInt64(f.Args[2])
	if !ok || !slotOK || slot != 0 {
		return nil, false
	}
	return scalarNumericProgramRef(scope, 1, makeSimplePlan2Type(types.T_bool)), true
}

func numericProgramRefIdentity(value *Expr) (int64, int64, bool) {
	f := value.GetF()
	if f == nil || f.Func == nil {
		return 0, 0, false
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj)
	if fid != function.SCALAR_PROGRAM_REF || (len(f.Args) != 2 && len(f.Args) != 3) {
		return 0, 0, false
	}
	argOffset := 0
	if len(f.Args) == 3 {
		// Slot zero owns the source definition. Slot one is a standalone
		// reference to the selected kind from that same program.
		argOffset = 1
	}
	scope, scopeOK := numericPlanInt64(f.Args[argOffset])
	slot, slotOK := numericPlanInt64(f.Args[argOffset+1])
	return scope, slot, scopeOK && slotOK
}

func numericProgramRefScope(value *Expr) (int64, bool) {
	scope, _, ok := numericProgramRefIdentity(value)
	return scope, ok
}

func isFalseBoolLiteral(expr *Expr) bool {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return false
	}
	value, ok := lit.Value.(*planpb.Literal_Bval)
	return ok && !value.Bval
}

func numericPlanInt64(expr *Expr) (int64, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return 0, false
	}
	value, ok := expr.GetLit().GetValue().(*planpb.Literal_I64Val)
	if !ok || value == nil {
		return 0, false
	}
	return value.I64Val, true
}

func (l *numericValueKindLowerer) kindForJoinOutput(nodeID int32, node *planpb.Node, tag, col int32) (*Expr, error) {
	for _, childID := range node.Children {
		if l.subtreeHasTag(childID, tag) {
			kind, err := l.kindForNodeOutput(childID, tag, col)
			if err != nil || isFalseBoolLiteral(kind) {
				return kind, err
			}
			for _, existing := range node.ProjectList {
				c := existing.GetCol()
				kindCol := kind.GetCol()
				if c != nil && kindCol != nil && c.RelPos == tag && c.ColPos == kindCol.ColPos {
					return DeepCopyExpr(kind), nil
				}
			}
			// A DISTINCT parent hashes only the original tuple. Save its keys
			// before this JOIN output schema receives the BOOL passenger.
			l.freezeDistinctParents(nodeID)
			node.ProjectList = append(node.ProjectList, DeepCopyExpr(kind))
			return DeepCopyExpr(kind), nil
		}
	}
	return makePlan2BoolConstExprWithType(false), nil
}

func (l *numericValueKindLowerer) subtreeHasTag(nodeID, tag int32) bool {
	if nodeID < 0 || int(nodeID) >= len(l.builder.qry.Nodes) {
		return false
	}
	node := l.builder.qry.Nodes[nodeID]
	if node == nil {
		return false
	}
	for _, bindingTag := range node.BindingTags {
		if bindingTag == tag {
			return true
		}
	}
	for _, child := range node.Children {
		if l.subtreeHasTag(child, tag) {
			return true
		}
	}
	return false
}

func (l *numericValueKindLowerer) kindForSetOutput(nodeID int32, node *planpb.Node, tag, col int32) (*Expr, error) {
	if col < 0 || int(col) >= len(node.ProjectList) || len(node.Children) != 2 {
		return makePlan2BoolConstExprWithType(false), nil
	}
	leftKind, err := l.kindForNodeOutput(node.Children[0], tag, col)
	if err != nil {
		return nil, err
	}
	rightKind, err := l.kindForNodeOutput(node.Children[1], tag, col)
	if err != nil {
		return nil, err
	}
	left := l.builder.setOperationInput(node.Children[0])
	right := l.builder.setOperationInput(node.Children[1])
	if len(node.BindingTags) == 0 {
		return makePlan2BoolConstExprWithType(false), nil
	}
	l.freezeDistinctParents(nodeID)
	outputPos := int32(len(node.ProjectList))
	if len(left.ProjectList) == int(outputPos) {
		left.ProjectList = append(left.ProjectList, DeepCopyExpr(leftKind))
	} else if len(left.ProjectList) < int(outputPos) {
		for len(left.ProjectList) < int(outputPos) {
			left.ProjectList = append(left.ProjectList, makePlan2BoolConstExprWithType(false))
		}
		left.ProjectList = append(left.ProjectList, DeepCopyExpr(leftKind))
	}
	if len(right.ProjectList) == int(outputPos) {
		right.ProjectList = append(right.ProjectList, DeepCopyExpr(rightKind))
	} else if len(right.ProjectList) < int(outputPos) {
		for len(right.ProjectList) < int(outputPos) {
			right.ProjectList = append(right.ProjectList, makePlan2BoolConstExprWithType(false))
		}
		right.ProjectList = append(right.ProjectList, DeepCopyExpr(rightKind))
	}
	keyList := make([]*Expr, len(node.ProjectList))
	for i, expr := range node.ProjectList {
		keyList[i] = DeepCopyExpr(expr)
	}
	if node.NodeType != planpb.Node_UNION_ALL && len(node.PhysicalEqualityKeyList) == 0 {
		node.PhysicalEqualityKeyList = keyList
	}
	node.ProjectList = append(node.ProjectList, GetColExpr(makeSimplePlan2Type(types.T_bool), 0, outputPos))
	kind := GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], outputPos)
	l.pairs[[2]int32{node.BindingTags[0], col}] = kind
	return DeepCopyExpr(kind), nil
}

func (l *numericValueKindLowerer) kindForDistinctOutput(nodeID int32, node *planpb.Node, col int32) (*Expr, error) {
	if len(node.Children) == 0 {
		return makePlan2BoolConstExprWithType(false), nil
	}
	childID := node.Children[0]
	if len(l.distinctKeys[nodeID]) > 0 && len(node.PhysicalEqualityKeyList) == 0 {
		// Save references to the original visible tuple. Copying source
		// expressions here could evaluate a volatile producer a second time.
		node.PhysicalEqualityKeyList = DeepCopyExprList(l.distinctKeys[nodeID])
	}
	kind, err := l.kindForNodeOutput(childID, 0, col)
	if err != nil || isFalseBoolLiteral(kind) {
		return kind, err
	}
	// The DISTINCT output is later lowered to an aggregate. Retain the kind as
	// payload while PhysicalEqualityKeyList preserves only SQL-visible keys.
	return kind, nil
}

func distinctVisibleOutputRefs(child *planpb.Node) []*Expr {
	if child == nil {
		return nil
	}
	if len(child.BindingTags) == 0 {
		return DeepCopyExprList(child.ProjectList)
	}
	keys := make([]*Expr, len(child.ProjectList))
	for i, expr := range child.ProjectList {
		if expr == nil {
			continue
		}
		keys[i] = GetColExpr(expr.Typ, child.BindingTags[0], int32(i))
	}
	return keys
}

func (l *numericValueKindLowerer) kindForAggregateOutput(nodeID int32, node *planpb.Node, tag, col int32) (*Expr, error) {
	groupPos := -1
	if col >= 0 && int(col) < len(node.ProjectList) {
		output := node.ProjectList[col].GetCol()
		if output != nil && output.RelPos == -1 && output.ColPos >= 0 && int(output.ColPos) < len(node.GroupBy) {
			groupPos = int(output.ColPos)
		}
	}
	// Before physical remapping, an aggregate may not yet have ProjectList
	// entries. Its first binding tag addresses GroupBy slots directly.
	if groupPos < 0 && len(node.BindingTags) > 0 && tag == node.BindingTags[0] && col >= 0 && int(col) < len(node.GroupBy) {
		groupPos = int(col)
	}
	if groupPos < 0 {
		return makePlan2BoolConstExprWithType(false), nil
	}
	group := node.GroupBy[groupPos]
	var kind *Expr
	var err error
	if c := group.GetCol(); c != nil {
		if c.RelPos == 0 && len(node.Children) > 0 {
			kind, err = l.kindForNodeOutput(node.Children[0], c.RelPos, c.ColPos)
		} else if c.RelPos >= 0 {
			kind, err = l.kindForGlobalColumn(c.RelPos, c.ColPos)
		} else {
			kind = makePlan2BoolConstExprWithType(false)
		}
	} else if types.T(group.Typ.Id).IsMySQLString() {
		bound, bindErr := l.bindNumericValueLeaves(group)
		if bindErr != nil {
			return nil, bindErr
		}
		valueRef, kindRef, makeErr := makeNumericValueProgramOutputs(l.builder.GetContext(), bound)
		if makeErr != nil {
			return nil, makeErr
		}
		node.GroupBy[groupPos] = valueRef
		kind = kindRef
	} else {
		kind = makePlan2BoolConstExprWithType(false)
	}
	if err != nil || isFalseBoolLiteral(kind) {
		return kind, err
	}
	originalGroupCount := len(node.GroupBy)
	if len(node.GroupByHashKey) == 0 {
		node.GroupByHashKey = make([]int32, originalGroupCount)
		for i := range node.GroupByHashKey {
			node.GroupByHashKey[i] = int32(i)
		}
	}
	l.freezeDistinctParents(nodeID)
	groupKindPos := int32(len(node.GroupBy))
	node.GroupBy = append(node.GroupBy, DeepCopyExpr(kind))
	if len(node.GroupingFlag) > 0 {
		// A passenger is evaluated for every grouping row and is excluded from
		// hashing via GroupByHashKey. Mark it active so the grouping-set rollup
		// machinery does not replace the representative value with a sentinel.
		node.GroupingFlag = append(node.GroupingFlag, true)
	}
	// Group binding tags expose the GroupBy slots directly. A downstream
	// consumer's new reference to groupKindPos makes the normal remapper carry
	// this passenger through the aggregate.
	kind = GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], groupKindPos)
	return kind, nil
}

func (l *numericValueKindLowerer) kindForSinkOutput(nodeID int32, node *planpb.Node, col int32) (*Expr, error) {
	if node.ExtraOptions != materialized.CTESinkOption || len(node.Children) == 0 || col < 0 || int(col) >= len(node.ProjectList) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	if len(node.BindingTags) == 0 {
		return makePlan2BoolConstExprWithType(false), nil
	}
	key := [2]int32{node.BindingTags[0], col}
	if kind := l.pairs[key]; kind != nil {
		return DeepCopyExpr(kind), nil
	}
	value := node.ProjectList[col]
	valueCol := value.GetCol()
	if valueCol == nil {
		return makePlan2BoolConstExprWithType(false), nil
	}
	// Resolve the sink output inside its actual producer subtree. The shared
	// CTE tag can also be owned by a reader in tag2NodeID, which would make
	// global lookup follow a reader back into itself and lose the source kind.
	kind, err := l.kindForNodeOutput(node.Children[0], valueCol.RelPos, valueCol.ColPos)
	if err != nil || isFalseBoolLiteral(kind) {
		return kind, err
	}
	l.freezeDistinctParents(nodeID)
	var producerSteps []int32
	for step, root := range l.builder.qry.Steps {
		if root == nodeID {
			producerSteps = append(producerSteps, int32(step))
		}
	}
	if len(producerSteps) == 0 {
		return nil, moerr.NewInternalError(l.builder.GetContext(), "materialized CTE sink is not a query-step root")
	}
	pos := int32(len(node.ProjectList))
	node.ProjectList = append(node.ProjectList, DeepCopyExpr(kind))
	projectedKind := GetColExpr(makeSimplePlan2Type(types.T_bool), node.BindingTags[0], pos)
	l.pairs[key] = projectedKind

	// Readers are separate SINK_SCAN owners, commonly below a PROJECT rather
	// than at a step root. Publish the passenger through every matching reader
	// using that reader's own tag and the exact shared output ordinal.
	for readerID, reader := range l.builder.qry.Nodes {
		if reader == nil || reader.NodeType != planpb.Node_SINK_SCAN || !sourceStepContainsAny(reader.SourceStep, producerSteps) {
			continue
		}
		if len(reader.BindingTags) == 0 || int(col) >= len(reader.ProjectList) {
			return nil, moerr.NewInternalError(l.builder.GetContext(), "materialized CTE reader is missing a demanded output slot")
		}
		l.freezeDistinctParents(int32(readerID))
		readerSource := reader.ProjectList[col].GetCol()
		if readerSource == nil || readerSource.ColPos != valueCol.ColPos {
			return nil, moerr.NewInternalError(l.builder.GetContext(), "materialized CTE reader output does not match its producer slot")
		}
		if len(reader.ProjectList) == int(pos) {
			readerTag := reader.BindingTags[0]
			reader.ProjectList = append(reader.ProjectList, GetColExpr(makeSimplePlan2Type(types.T_bool), readerTag, pos))
		} else if len(reader.ProjectList) <= int(pos) || types.T(reader.ProjectList[pos].Typ.Id) != types.T_bool {
			return nil, moerr.NewInternalError(l.builder.GetContext(), "materialized CTE readers have inconsistent passenger ordinals")
		}
		readerTag := reader.BindingTags[0]
		l.pairs[[2]int32{readerTag, col}] = GetColExpr(makeSimplePlan2Type(types.T_bool), readerTag, pos)
	}
	return DeepCopyExpr(projectedKind), nil
}

func (l *numericValueKindLowerer) kindForSinkScanOutput(nodeID int32, node *planpb.Node, tag, col int32) (*Expr, error) {
	if col < 0 || int(col) >= len(node.ProjectList) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	if kind := l.pairs[[2]int32{tag, col}]; kind != nil {
		return DeepCopyExpr(kind), nil
	}
	for _, step := range node.SourceStep {
		if step < 0 || int(step) >= len(l.builder.qry.Steps) {
			continue
		}
		sinkID := l.builder.qry.Steps[step]
		if sinkID < 0 || int(sinkID) >= len(l.builder.qry.Nodes) {
			continue
		}
		sink := l.builder.qry.Nodes[sinkID]
		if sink == nil || sink.NodeType != planpb.Node_SINK {
			continue
		}
		if sink.RecursiveSink {
			return l.kindForRecursiveSinkScanOutput(nodeID, node, sinkID, sink, col)
		}
		if sink.ExtraOptions != materialized.CTESinkOption {
			continue
		}
		kind, err := l.kindForSinkOutput(sinkID, sink, col)
		if err != nil || isFalseBoolLiteral(kind) {
			return kind, err
		}
		if local := l.pairs[[2]int32{tag, col}]; local != nil {
			return DeepCopyExpr(local), nil
		}
	}
	_ = nodeID
	return makePlan2BoolConstExprWithType(false), nil
}

// kindForRecursiveSinkScanOutput transports the selected kind through the
// recursive CTE's existing row schema. Each branch sink writes one ordinary
// BOOL passenger at the same physical ordinal; recursive scans and the CTE
// expose that ordinal unchanged on every iteration.
func (l *numericValueKindLowerer) kindForRecursiveSinkScanOutput(
	scanID int32, scan *planpb.Node, sinkID int32, sink *planpb.Node, col int32,
) (*Expr, error) {
	if len(sink.Children) != 1 || col < 0 || int(col) >= len(sink.ProjectList) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	cteID := sink.Children[0]
	if cteID < 0 || int(cteID) >= len(l.builder.qry.Nodes) {
		return nil, moerr.NewInternalError(l.builder.GetContext(), "recursive kind sink has an invalid CTE source")
	}
	cte := l.builder.qry.Nodes[cteID]
	if cte == nil || cte.NodeType != planpb.Node_RECURSIVE_CTE {
		return nil, moerr.NewInternalError(l.builder.GetContext(), "recursive kind sink does not read a recursive CTE")
	}
	kindPos, err := l.ensureRecursiveKind(cteID, cte, sinkID, sink, col)
	if err != nil {
		return nil, err
	}
	if len(scan.BindingTags) == 0 {
		return nil, moerr.NewInternalError(l.builder.GetContext(), "recursive kind reader has no output tag")
	}
	if err := appendRecursiveBoolProjection(scan, scan.BindingTags[0], kindPos); err != nil {
		return nil, err
	}
	l.preserveRecursiveScan(scanID)
	kind := GetColExpr(makeSimplePlan2Type(types.T_bool), scan.BindingTags[0], kindPos)
	l.pairs[[2]int32{scan.BindingTags[0], col}] = DeepCopyExpr(kind)
	return kind, nil
}

func (l *numericValueKindLowerer) kindForRecursiveSinkOutput(
	sinkID int32, sink *planpb.Node, col int32,
) (*Expr, error) {
	if len(sink.Children) != 1 || col < 0 || int(col) >= len(sink.ProjectList) {
		return makePlan2BoolConstExprWithType(false), nil
	}
	cteID := sink.Children[0]
	if cteID < 0 || int(cteID) >= len(l.builder.qry.Nodes) {
		return nil, moerr.NewInternalError(l.builder.GetContext(), "recursive kind sink has an invalid CTE source")
	}
	cte := l.builder.qry.Nodes[cteID]
	if cte == nil || cte.NodeType != planpb.Node_RECURSIVE_CTE {
		return makePlan2BoolConstExprWithType(false), nil
	}
	kindPos, err := l.ensureRecursiveKind(cteID, cte, sinkID, sink, col)
	if err != nil {
		return nil, err
	}
	return GetColExpr(makeSimplePlan2Type(types.T_bool), cte.BindingTags[0], kindPos), nil
}

func (l *numericValueKindLowerer) ensureRecursiveKind(
	cteID int32, cte *planpb.Node, outputSinkID int32, outputSink *planpb.Node, col int32,
) (int32, error) {
	if cte == nil || cte.NodeType != planpb.Node_RECURSIVE_CTE || len(cte.BindingTags) == 0 ||
		col < 0 || int(col) >= len(cte.ProjectList) {
		return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive kind request has an invalid output")
	}
	if l.recursive == nil {
		l.recursive = make(map[int32]*recursiveNumericKindLayout)
	}
	layout := l.recursive[cteID]
	if layout == nil {
		layout = &recursiveNumericKindLayout{
			visibleColumns: len(cte.ProjectList),
			kindColumns:    make(map[int32]int32),
		}
		l.recursive[cteID] = layout
	}
	if slot, ok := layout.kindColumns[col]; ok {
		return slot, nil
	}
	if layout.visibleColumns == 0 || len(cte.ProjectList) < layout.visibleColumns {
		return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE visible schema changed during kind lowering")
	}
	kindPos := int32(len(cte.ProjectList))
	layout.kindColumns[col] = kindPos
	cteTag := cte.BindingTags[0]
	// Publish the recursive reader before resolving the member branch. A
	// recursive member that selects its prior value must read this BOOL from the
	// preceding round rather than hit the ordinary cycle fallback.
	l.pairs[[2]int32{cteTag, col}] = GetColExpr(makeSimplePlan2Type(types.T_bool), cteTag, kindPos)

	if cte.RecursiveUnionDistinct && len(cte.PhysicalEqualityKeyList) == 0 {
		keys := make([]*Expr, layout.visibleColumns)
		for i := 0; i < layout.visibleColumns; i++ {
			keys[i] = GetColExpr(cte.ProjectList[i].Typ, 0, int32(i))
		}
		cte.PhysicalEqualityKeyList = keys
	}
	if err := appendRecursiveBoolProjection(cte, cteTag, kindPos); err != nil {
		return 0, err
	}
	l.preserveRecursiveScan(cteID)

	for scanID, scan := range l.builder.qry.Nodes {
		if scan == nil || scan.NodeType != planpb.Node_RECURSIVE_SCAN || !l.recursiveScanReadsCte(scan, cteID) {
			continue
		}
		if len(scan.BindingTags) == 0 {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive scan has no output tag")
		}
		scanTag := scan.BindingTags[0]
		if err := appendRecursiveBoolProjection(scan, scanTag, kindPos); err != nil {
			return 0, err
		}
		l.pairs[[2]int32{scanTag, col}] = GetColExpr(makeSimplePlan2Type(types.T_bool), scanTag, kindPos)
		l.preserveRecursiveScan(int32(scanID))
	}

	for _, step := range cte.SourceStep {
		if step < 0 || int(step) >= len(l.builder.qry.Steps) {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE has an invalid producer step")
		}
		branchSinkID := l.builder.qry.Steps[step]
		if branchSinkID < 0 || int(branchSinkID) >= len(l.builder.qry.Nodes) {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE has an invalid producer sink")
		}
		branchSink := l.builder.qry.Nodes[branchSinkID]
		if branchSink == nil || branchSink.NodeType != planpb.Node_SINK || len(branchSink.Children) != 1 ||
			len(branchSink.ProjectList) != int(kindPos) {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE producer schemas do not align")
		}
		childID := branchSink.Children[0]
		if childID < 0 || int(childID) >= len(l.builder.qry.Nodes) || l.builder.qry.Nodes[childID] == nil {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE producer has an invalid child")
		}
		child := l.builder.qry.Nodes[childID]
		valueCol := branchSink.ProjectList[col].GetCol()
		if valueCol == nil {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE producer output is not a column")
		}
		branchKind, err := l.kindForNodeOutput(childID, valueCol.RelPos, valueCol.ColPos)
		if err != nil {
			return 0, err
		}
		localKind, err := recursiveKindExpressionForInput(branchKind, child)
		if err != nil {
			return 0, err
		}
		projects := make([]*Expr, 0, len(branchSink.ProjectList)+1)
		for _, output := range branchSink.ProjectList {
			colRef := output.GetCol()
			if colRef == nil {
				return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE producer output is not a column")
			}
			projects = append(projects, DeepCopyExpr(output))
		}
		projects = append(projects, localKind)
		var bindCtx *BindContext
		if int(branchSinkID) < len(l.builder.ctxByNode) {
			bindCtx = l.builder.ctxByNode[branchSinkID]
		}
		helperTag := l.builder.genNewBindTag()
		helperID := l.builder.appendNode(&planpb.Node{
			NodeType:    planpb.Node_PROJECT,
			Children:    []int32{childID},
			ProjectList: projects,
			BindingTags: []int32{helperTag},
			Stats:       child.Stats,
		}, bindCtx)
		branchSink.Children[0] = helperID
		branchSink.BindingTags = []int32{cteTag}
		branchSink.ProjectList = make([]*Expr, len(projects))
		for i, project := range projects {
			branchSink.ProjectList[i] = GetColExpr(project.Typ, helperTag, int32(i))
		}
		l.preserveRecursiveSink(branchSinkID)
	}

	// The feedback sink and every final reader carry the same appended column.
	if len(outputSink.ProjectList) != int(kindPos) {
		return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive feedback sink schema does not align")
	}
	outputSink.ProjectList = append(outputSink.ProjectList,
		GetColExpr(makeSimplePlan2Type(types.T_bool), cteTag, kindPos))
	l.preserveRecursiveSink(outputSinkID)
	var outputStep int32 = -1
	for step, root := range l.builder.qry.Steps {
		if root == outputSinkID {
			outputStep = int32(step)
			break
		}
	}
	if outputStep < 0 {
		return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive feedback sink is not a query-step root")
	}
	for readerID, reader := range l.builder.qry.Nodes {
		if reader == nil || reader.NodeType != planpb.Node_SINK_SCAN ||
			!sourceStepContainsAny(reader.SourceStep, []int32{outputStep}) {
			continue
		}
		if len(reader.BindingTags) == 0 || len(reader.ProjectList) != int(kindPos) {
			return 0, moerr.NewInternalError(l.builder.GetContext(), "recursive CTE reader schema does not align")
		}
		readerTag := reader.BindingTags[0]
		reader.ProjectList = append(reader.ProjectList,
			GetColExpr(makeSimplePlan2Type(types.T_bool), readerTag, kindPos))
		l.pairs[[2]int32{readerTag, col}] = GetColExpr(makeSimplePlan2Type(types.T_bool), readerTag, kindPos)
		l.preserveRecursiveScan(int32(readerID))
	}
	return kindPos, nil
}

func (l *numericValueKindLowerer) recursiveScanReadsCte(scan *planpb.Node, cteID int32) bool {
	for _, step := range scan.SourceStep {
		if step < 0 || int(step) >= len(l.builder.qry.Steps) {
			continue
		}
		sinkID := l.builder.qry.Steps[step]
		if sinkID < 0 || int(sinkID) >= len(l.builder.qry.Nodes) {
			continue
		}
		sink := l.builder.qry.Nodes[sinkID]
		if sink != nil && sink.NodeType == planpb.Node_SINK && sink.RecursiveSink &&
			len(sink.Children) == 1 && sink.Children[0] == cteID {
			return true
		}
	}
	return false
}

func recursiveKindExpressionForInput(kind *Expr, child *planpb.Node) (*Expr, error) {
	if kind == nil || types.T(kind.Typ.Id) != types.T_bool || child == nil {
		return nil, moerr.NewInvalidInputNoCtx("recursive CTE kind output must be BOOL")
	}
	local := DeepCopyExpr(kind)
	if local.GetLit() != nil {
		return local, nil
	}
	allowedTags := make(map[int32]struct{}, len(child.BindingTags))
	for _, tag := range child.BindingTags {
		allowedTags[tag] = struct{}{}
	}
	if err := planpb.VisitExprTree(local, func(expr *Expr) error {
		col := expr.GetCol()
		if col == nil {
			return nil
		}
		if _, ok := allowedTags[col.RelPos]; !ok {
			return moerr.NewInvalidInputNoCtx("recursive CTE kind references a nonlocal producer")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return local, nil
}

func appendRecursiveBoolProjection(node *planpb.Node, tag, pos int32) error {
	if node == nil || pos < 0 {
		return moerr.NewInvalidInputNoCtx("recursive kind projection is invalid")
	}
	if int(pos) < len(node.ProjectList) {
		if types.T(node.ProjectList[pos].Typ.Id) != types.T_bool {
			return moerr.NewInvalidInputNoCtx("recursive kind projection has a conflicting slot")
		}
		return nil
	}
	if int(pos) != len(node.ProjectList) {
		return moerr.NewInvalidInputNoCtx("recursive kind projection has a gap")
	}
	node.ProjectList = append(node.ProjectList, GetColExpr(makeSimplePlan2Type(types.T_bool), tag, pos))
	return nil
}

func (l *numericValueKindLowerer) preserveRecursiveScan(nodeID int32) {
	if l.builder.preserveScanProjection == nil {
		l.builder.preserveScanProjection = make(map[int32]struct{})
	}
	l.builder.preserveScanProjection[nodeID] = struct{}{}
}

func (l *numericValueKindLowerer) preserveRecursiveSink(nodeID int32) {
	if l.builder.preserveSinkProjection == nil {
		l.builder.preserveSinkProjection = make(map[int32]struct{})
	}
	l.builder.preserveSinkProjection[nodeID] = struct{}{}
}

func sourceStepContainsAny(values, targets []int32) bool {
	for _, value := range values {
		for _, target := range targets {
			if value == target {
				return true
			}
		}
	}
	return false
}
