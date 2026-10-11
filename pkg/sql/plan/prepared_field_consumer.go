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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type preparedFieldConsumersKey struct{}
type preparedFieldConsumersOption struct {
	domains map[int32]types.Type
	binary  bool
}

// WithPreparedFieldConsumers restores handle-owned facts at the original FIELD
// AST owner, before optimizer folding/remapping. Positive lexical FIELD ids own
// CASE return facts; CASE ids follow all FIELD ids and own declaration defaults.
// Negative ids own FIELD comparison facts. Neither identity
// nor handle state is encoded in an expression, AuxId, or the wire plan.
func WithPreparedFieldConsumers(ctx context.Context, domains map[int32]types.Type, binary bool) context.Context {
	return context.WithValue(ctx, preparedFieldConsumersKey{}, preparedFieldConsumersOption{domains: domains, binary: binary})
}

type preparedFieldDomainBinder struct {
	ctx                            context.Context
	initial, pending               map[int32]types.Type
	copied, valueDependent, binary bool
	bindings                       []PreparedSourceBinding
	values                         []any
	sources                        map[int32]types.Type
	ids                            map[*tree.FuncExpr]int32
	caseIDs                        map[*tree.CaseExpr]int32
	activeCase                     int32
}

func newPreparedFieldDomainBinder(ctx context.Context, domains map[int32]types.Type, bindings []PreparedSourceBinding, values []any, binary bool) *preparedFieldDomainBinder {
	return &preparedFieldDomainBinder{ctx: ctx, initial: domains, pending: domains, bindings: bindings, values: values, binary: binary}
}

func (owner *preparedFieldDomainBinder) record(key int32, typ types.Type) {
	if !owner.copied {
		owner.pending = make(map[int32]types.Type, len(owner.initial)+1)
		for id, old := range owner.initial {
			owner.pending[id] = old
		}
		owner.copied = true
	}
	owner.pending[key] = typ
}

func (state *preparedSourceBindingState) initFieldConsumers(ctx context.Context, stmt tree.Statement) {
	option, ok := ctx.Value(preparedFieldConsumersKey{}).(preparedFieldConsumersOption)
	if !ok {
		return
	}
	owner := newPreparedFieldDomainBinder(ctx, option.domains, state.bindings, state.values, option.binary)
	owner.ids = make(map[*tree.FuncExpr]int32)
	var ordinal int32
	for _, ast := range tree.FunctionExprs(stmt) {
		ref, ok := ast.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok || !strings.EqualFold(ref.ColName(), "field") {
			continue
		}
		if _, exists := owner.ids[ast]; exists {
			continue
		}
		ordinal++
		owner.ids[ast] = ordinal
	}
	owner.caseIDs = make(map[*tree.CaseExpr]int32)
	for _, ast := range tree.CaseExprs(stmt) {
		if _, exists := owner.caseIDs[ast]; !exists {
			ordinal++
			owner.caseIDs[ast] = ordinal
		}
	}
	state.fieldConsumers = owner
}

func (owner *preparedFieldDomainBinder) bind(expr *Expr, ast *tree.FuncExpr) error {
	id, ok := owner.ids[ast]
	if !ok {
		return nil
	} // private probes/cloned ASTs never own published facts
	projected := len(ast.Exprs) > 0 && preparedFieldProjectedReturnAST(ast.Exprs[0]) &&
		expr.GetF() != nil && len(expr.GetF().Args) > 0 && preparedFieldProjectedMarkerValue(expr.GetF().Args[0])
	if err := owner.bindCase(expr, id, projected); err != nil {
		return err
	}
	if len(ast.Exprs) > 0 && preparedFieldDeclaredBinaryReturnAST(ast.Exprs[0]) {
		return owner.bindComparison(expr, -id)
	}
	return nil
}

// A projected marker has the SELECT column's default text declaration even
// when its transport is NULL. Conditions are deliberately not value owners.
func preparedFieldProjectedMarkerValue(expr *Expr) bool {
	if expr == nil || isExplicitPreparedCast(expr) {
		return false
	}
	if expr.GetCol() != nil || expr.GetSub() != nil {
		// A column forwarding a raw marker declares default text. A column
		// forwarding a CASE contract does not redeclare that CASE's domain.
		return preparedFieldCaseConditionMarker(expr) != nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	name := fn.Func.ObjName
	if name == "cast" && len(fn.Args) > 0 {
		return preparedFieldProjectedMarkerValue(fn.Args[0])
	}
	if name != "case" && name != "coalesce" && name != "ifnull" && name != "max" && name != "min" && name != "any_value" && name != "if" && name != "iff" {
		return false
	}
	for index, arg := range fn.Args {
		if name == "case" && !numericFunctionArgKeepsContext(name, index, len(fn.Args)) || (name == "if" || name == "iff") && index == 0 {
			continue
		}
		if preparedFieldProjectedMarkerValue(arg) {
			return true
		}
	}
	return false
}

func preparedFieldProjectedReturnAST(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		return true
	case *tree.ParenExpr:
		return preparedFieldProjectedReturnAST(e.Expr)
	case *tree.CaseExpr:
		for _, when := range e.Whens {
			if preparedFieldProjectedReturnAST(when.Val) {
				return true
			}
		}
		return e.Else != nil && preparedFieldProjectedReturnAST(e.Else)
	case *tree.FuncExpr:
		ref, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return false
		}
		name := strings.ToLower(ref.ColName())
		if name != "coalesce" && name != "ifnull" && name != "max" && name != "min" && name != "any_value" && name != "if" && name != "iff" {
			return false
		}
		for index, arg := range e.Exprs {
			if (name == "if" || name == "iff") && index == 0 {
				continue
			}
			if preparedFieldProjectedReturnAST(arg) {
				return true
			}
		}
	}
	return false
}

// Fixed binary value declarations and numeric FIELD comparison resolution are
// different owners. Retain the latter only here, never on COALESCE's return or
// a marker shared with another consumer. Text-first resolution remains native
// until an actual numeric binding reparses FIELD, then stays numeric for strings.
func preparedFieldDeclaredBinaryReturnAST(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.CastExpr:
		typ, ok := e.Type.(*tree.T)
		return ok && (strings.EqualFold(typ.InternalType.FamilyString, "binary") || strings.EqualFold(typ.InternalType.FamilyString, "varbinary"))
	case *tree.ParenExpr:
		return preparedFieldDeclaredBinaryReturnAST(e.Expr)
	case *tree.CaseExpr:
		for _, when := range e.Whens {
			if preparedFieldDeclaredBinaryReturnAST(when.Val) {
				return true
			}
		}
		return e.Else != nil && preparedFieldDeclaredBinaryReturnAST(e.Else)
	case *tree.FuncExpr:
		ref, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return false
		}
		name := strings.ToLower(ref.ColName())
		if name != "coalesce" && name != "ifnull" && name != "max" && name != "min" && name != "any_value" && name != "if" && name != "iff" {
			return false
		}
		for index, arg := range e.Exprs {
			if (name == "if" || name == "iff") && index == 0 {
				continue
			}
			if preparedFieldDeclaredBinaryReturnAST(arg) {
				return true
			}
		}
	}
	return false
}

func (owner *preparedFieldDomainBinder) bindComparison(expr *Expr, key int32) error {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "field" || len(fn.Args) == 0 || !preparedFieldFixedBinaryReturn(fn.Args[0]) {
		return nil
	}
	resolved, found := owner.pending[key]
	numeric := true
	for _, arg := range fn.Args {
		if !makeTypeByPlan2Expr(arg).IsNumeric() {
			numeric = false
			break
		}
	}
	if !found {
		if !numeric {
			return nil // no handle fact is needed for ordinary native comparison
		}
		resolved = types.T_float64.ToType()
		owner.record(key, resolved)
	}
	if resolved.Oid == types.T_any {
		return nil
	}
	args := append([]*Expr(nil), fn.Args...)
	for index, arg := range args {
		if !makeTypeByPlan2Expr(arg).Oid.IsMySQLString() {
			continue
		}
		text := types.T_text.ToType()
		text.Width = -1
		converted, err := makePlan2CastExpr(owner.ctx, arg, makePlan2Type(&text))
		if err != nil {
			return err
		}
		number := types.T_float64.ToType()
		converted, err = appendCastBeforeExprWithOverload(owner.ctx, converted, makePlan2Type(&number), 2)
		if err != nil {
			return err
		}
		witness := makePlan2Int64ConstExprWithType(0)
		witness.Typ = makePlan2Type(&number)
		ensurePreparedNumericMetadata(converted).StringDomainSource = witness
		args[index] = converted
	}
	rebound, err := BindFuncExprImplByPlanExpr(owner.ctx, "field", args)
	if err != nil {
		return err
	}
	rebound.PreparedNumeric = expr.PreparedNumeric
	*expr = *rebound
	return nil
}

func preparedFieldUntypedNullBinding(pos int32, sources map[int32]types.Type, values []any) bool {
	source, found := sources[pos]
	if !found || pos < 0 || int(pos) >= len(values) || source.Oid != types.T_any {
		return false
	}
	value := values[pos]
	if param, ok := value.(ParamValue); ok {
		value = param.Value
	}
	return value == nil
}

func preparedFieldFixedBinaryReturn(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if isExplicitPreparedCast(expr) {
		return types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainBinary
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Src != nil {
			return preparedFieldFixedBinaryReturn(lit.Src)
		}
		return !lit.Isnull && types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainBinary
	}
	if source := preparedStringDomainDependencyWitness(expr); source != nil {
		return preparedFieldFixedBinaryReturn(source)
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	name := fn.Func.ObjName
	if name == "cast" && len(fn.Args) > 0 {
		return preparedFieldFixedBinaryReturn(fn.Args[0])
	}
	if name != "case" && name != "coalesce" && name != "ifnull" && name != "if" && name != "iff" && name != "max" && name != "min" && name != "any_value" {
		return false
	}
	for index, arg := range fn.Args {
		if name == "case" && !numericFunctionArgKeepsContext(name, index, len(fn.Args)) || (name == "if" || name == "iff") && index == 0 {
			continue
		}
		if preparedFieldFixedBinaryReturn(arg) {
			return true
		}
	}
	return false
}
