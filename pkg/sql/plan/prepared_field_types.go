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
	"maps"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// PreparedFieldTypes is an immutable snapshot owned by one prepared generation.
// It contains declaration types and provenance, never values, AST nodes, vectors,
// or executable expressions.
// A nil snapshot is unresolved; a populated text type is a resolved fact.
type PreparedFieldTypes struct {
	revision uint64
	cases    map[int]preparedFieldCaseType
	params   map[int32]preparedFieldParameter
}

type preparedFieldCaseType struct {
	result         types.Type
	inferredBinary bool
}

type preparedFieldParameter struct {
	typ       types.Type
	inherited bool
}

func (s *PreparedFieldTypes) Revision() uint64 {
	if s == nil {
		return 0
	}
	return s.revision
}

type preparedFieldTypesKey struct{}
type preparedFieldTypesContext struct {
	types *PreparedFieldTypes
	seed  *Plan
}

func WithPreparedFieldTypes(ctx context.Context, state *PreparedFieldTypes, seed ...*Plan) context.Context {
	value := preparedFieldTypesContext{types: state}
	if len(seed) != 0 {
		value.seed = seed[0]
	}
	return context.WithValue(ctx, preparedFieldTypesKey{}, value)
}

type preparedFieldCaseProducer struct {
	ordinal int
	ast     *tree.CaseExpr
}

// All pointer maps are private to a single binder invocation and are discarded
// when it restores the compiler context. Only the immutable snapshot escapes.
type preparedFieldTypeBinding struct {
	current   *PreparedFieldTypes
	ids       map[*tree.CaseExpr]int
	producers map[*Expr]preparedFieldCaseProducer
	params    map[int32]preparedFieldParameter
	collected map[*Expr]bool
	actual    map[int32]types.Type
	dirty     bool
	apply     bool
	needed    map[int]bool
	originals map[*Expr]*Expr
}

func newPreparedFieldTypeBinding(ctx context.Context, stmt tree.Statement) *preparedFieldTypeBinding {
	prior, enabled := ctx.Value(preparedFieldTypesKey{}).(preparedFieldTypesContext)
	if !enabled {
		return nil
	}
	cases := tree.CaseExpressions(stmt)
	if len(cases) == 0 {
		return nil
	}
	b := &preparedFieldTypeBinding{current: prior.types, ids: make(map[*tree.CaseExpr]int), producers: make(map[*Expr]preparedFieldCaseProducer), collected: make(map[*Expr]bool), originals: make(map[*Expr]*Expr)}
	if prior.types != nil {
		b.apply = true
		b.needed = make(map[int]bool, len(prior.types.cases))
		for ordinal := range prior.types.cases {
			b.needed[ordinal] = true
		}
		b.params = prior.types.params
	} else {
		b.params = make(map[int32]preparedFieldParameter)
		for pos := range tree.ParameterCount(stmt) {
			b.params[int32(pos)] = preparedFieldParameter{typ: types.T_text.ToType()}
		}
		b.seedParameters(prior.seed)
	}
	for _, expr := range cases {
		if _, exists := b.ids[expr]; !exists {
			b.ids[expr] = len(b.ids)
			if prior.types == nil && expr.Expr == nil {
				for _, when := range expr.Whens {
					if p, ok := unwrapParenExpr(when.Cond).(*tree.ParamExpr); ok {
						b.params[int32(p.Offset-1)] = preparedFieldParameter{typ: types.T_text.ToType()}
					}
				}
			}
		}
	}
	return b
}

func (b *preparedFieldTypeBinding) result() *PreparedFieldTypes {
	if b == nil {
		return nil
	}
	return b.current
}

func (b *baseBinder) bindPreparedFieldCase(expr *Expr, ast *tree.CaseExpr) (*Expr, error) {
	state := preparedBindingState(b.GetContext())
	if state == nil || state.fieldTypes == nil {
		return expr, nil
	}
	binding := state.fieldTypes
	ordinal, ok := binding.ids[ast]
	if !ok {
		return expr, nil
	}
	producer := preparedFieldCaseProducer{ordinal, ast}
	binding.producers[expr] = producer
	if !binding.apply || !binding.needed[ordinal] {
		return expr, nil
	}
	domain := binding.resolve(newPreparedFieldStringResolver(b), expr, producer)
	typ := fieldDomainType(domain, makeTypeByPlan2Expr(expr))
	if typ.Oid == types.T_any {
		typ = types.T_blob.ToType()
	}
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "case" {
		return expr, nil
	}
	args := append([]*Expr(nil), fn.Args...)
	for i := range args {
		if i%2 == 0 && i != len(args)-1 {
			continue
		}
		var err error
		if typ.IsNumeric() {
			args[i], err = castPreparedFieldNumeric(b.GetContext(), preparedCommonValueSource(args[i]), typ)
		} else {
			args[i], err = appendExplicitCastBeforeExpr(b.GetContext(), preparedCommonValueSource(args[i]), makePlan2Type(&typ))
		}
		if err != nil {
			return nil, err
		}
	}
	resolved, err := BindFuncExprImplByPlanExpr(b.GetContext(), "case", args)
	if err != nil {
		return nil, err
	}
	binding.producers[resolved] = producer
	binding.originals[resolved] = expr
	return resolved, nil
}

func (b *preparedFieldTypeBinding) beginApplication() {
	b.apply = true
	b.needed = make(map[int]bool, len(b.current.cases))
	for ordinal := range b.current.cases {
		b.needed[ordinal] = true
	}
	b.producers = make(map[*Expr]preparedFieldCaseProducer)
}

func fieldDomainType(domain preparedFieldStringDomain, fallback types.Type) types.Type {
	if domain.resolved.Oid != types.T_any {
		return domain.resolved
	}
	switch domain.kind {
	case fieldStringText, fieldStringMarker:
		typ := types.T_text.ToType()
		typ.Charset = types.CharsetUTF8
		return typ
	case fieldStringBinary:
		return types.T_blob.ToType()
	case fieldStringNull:
		return types.T_any.ToType()
	default:
		return fallback
	}
}

func fieldParameterTypeCompatible(resolved, actual types.Type) bool {
	if actual.Oid == types.T_any || actual.Oid.IsMySQLString() {
		return true
	}
	if resolved.IsDecimal() {
		return actual.Oid.IsInteger() || actual.IsDecimal()
	}
	if resolved.Oid.IsFloat() {
		return actual.IsNumeric()
	}
	if resolved.Oid.IsInteger() {
		return actual.Oid.IsInteger() && resolved.Oid.IsUnsignedInt() == actual.Oid.IsUnsignedInt()
	}
	return resolved.Oid == actual.Oid
}

func preparedFieldActualType(state *preparedSourceBindingState, pos int32) (types.Type, bool) {
	if int(pos) >= len(state.values) || pos < 0 {
		return types.Type{}, false
	}
	value := state.values[pos]
	param, wrapped := value.(ParamValue)
	if wrapped {
		value = param.Value
	}
	if value == nil {
		return types.Type{}, false
	}
	var binding PreparedSourceBinding
	var ok bool
	if int(pos) < len(state.bindings) && state.bindings[pos].Position == pos {
		binding, ok = state.bindings[pos], true
	} else {
		binding, ok = state.bindingForPosition(pos)
	}
	if !ok {
		return types.Type{}, false
	}
	typ := binding.Type
	if numeric, ok := preparedNumericReprepareType(typ); ok {
		typ = numeric
	}
	// COM string payloads use the connection charset when MySQL reprepares;
	// SQL user variables retain their declared binary domain.
	if wrapped && param.IsBinaryProtocol && typ.Oid.IsMySQLString() {
		typ = types.T_text.ToType()
		typ.Charset = types.CharsetUTF8
	}
	return typ, true
}

// The original prepare plan supplies contracts outside CASE (including pinned
// explicit casts). CASE supplies its own unresolved return/condition contracts
// below, before testing whether the statement needs a reprepare.
func (b *preparedFieldTypeBinding) seedParameters(seed *Plan) {
	seen := make(map[*Expr]bool)
	var visit func(*Expr) error
	visit = func(expr *Expr) error {
		if expr == nil || seen[expr] {
			return nil
		}
		seen[expr] = true
		if fn := expr.GetF(); fn != nil {
			if fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
				if p := fn.Args[0].GetP(); p != nil {
					b.params[p.Pos] = preparedFieldParameter{typ: makeTypeByPlan2Expr(expr), inherited: isExplicitPreparedCast(expr)}
				}
			}
			for _, arg := range fn.Args {
				_ = visit(arg)
			}
		}
		return nil
	}
	_ = planpb.VisitExpressionsInOwner(seed, visit)
}

// A reprepare belongs to the statement, not the CASE first encountered by a
// consumer. All parameters and producers switch together. There is at most one
// extra bind, on the first execution; subsequent executions check the immutable
// signature before binding. No value or expression graph is retained.
func (b *preparedFieldTypeBinding) reprepare(state *preparedSourceBindingState) bool {
	if b == nil || b.actual != nil {
		return false
	}
	changed := false
	for pos, parameter := range b.params {
		if actual, ok := fieldReprepareActualType(state, pos, parameter.typ); ok && !parameter.inherited && !fieldParameterTypeCompatible(parameter.typ, actual) {
			changed = true
			break
		}
	}
	if !changed {
		return false
	}
	b.actual = make(map[int32]types.Type, len(b.params))
	params := make(map[int32]preparedFieldParameter, len(b.params))
	for pos, parameter := range b.params {
		typ := parameter.typ
		if !parameter.inherited {
			typ, _ = fieldReprepareActualType(state, pos, parameter.typ)
		}
		b.actual[pos] = typ
		params[pos] = preparedFieldParameter{typ: typ, inherited: parameter.inherited}
	}
	b.params = params
	b.current = &PreparedFieldTypes{revision: b.current.Revision() + 1, cases: make(map[int]preparedFieldCaseType), params: params}
	b.dirty = true
	b.producers = make(map[*Expr]preparedFieldCaseProducer)
	return true
}

func (b *preparedFieldTypeBinding) collectParameters(expr *Expr, typ types.Type) {
	if expr == nil || b.collected[expr] {
		return
	}
	b.collected[expr] = true
	if isExplicitPreparedCast(expr) {
		return
	}
	if p := expr.GetP(); p != nil {
		b.params[p.Pos] = preparedFieldParameter{typ: typ}
	} else if fn := expr.GetF(); fn != nil {
		switch fn.Func.ObjName {
		case "cast", "case", "coalesce", "if", "iff", "concat", "concat_ws":
			for _, child := range fn.Args {
				b.collectParameters(child, typ)
			}
		}
	}
}

func (b *preparedFieldTypeBinding) resolve(r *preparedFieldStringResolver, expr *Expr, producer preparedFieldCaseProducer) preparedFieldStringDomain {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "case" {
		return fieldStringStaticDomain(expr)
	}
	fact, exists := preparedFieldCaseType{}, false
	if b.current != nil {
		fact, exists = b.current.cases[producer.ordinal]
	}
	if !exists {
		values := make([]*Expr, 0, len(fn.Args)/2+1)
		for i := 1; i < len(fn.Args); i += 2 {
			values = append(values, fn.Args[i])
		}
		if len(fn.Args)%2 == 1 {
			values = append(values, fn.Args[len(fn.Args)-1])
		}
		resolver := newPreparedFieldStringResolver(r.binder)
		resolver.defaultMarker = r.defaultMarker
		resolver.readOnly = r.readOnly
		resolver.unresolvedParameters = b.actual == nil
		resolver.parameterTypes = b.actual
		domain := resolver.common(values)
		if !domain.parameter {
			return r.conditionalStringDeclaration(expr, r.common(values))
		}
		fact.result = fieldDomainType(domain, makeTypeByPlan2Expr(expr))
		fact.inferredBinary = domain.inferredBinary
		if !r.readOnly {
			if b.actual == nil {
				// The candidate's parameter map is private until result() is published.
				if !b.dirty {
					b.params = maps.Clone(b.params)
				}
				for _, value := range values {
					b.collectParameters(value, fact.result)
				}
				if producer.ast.Expr == nil {
					for _, when := range producer.ast.Whens {
						if p, ok := unwrapParenExpr(when.Cond).(*tree.ParamExpr); ok {
							b.params[int32(p.Offset-1)] = preparedFieldParameter{typ: types.T_text.ToType()}
						}
					}
				}
			}
			if !b.dirty {
				next := &PreparedFieldTypes{revision: b.current.Revision() + 1, cases: make(map[int]preparedFieldCaseType), params: b.params}
				if b.current != nil {
					maps.Copy(next.cases, b.current.cases)
				}
				b.current = next
				b.dirty = true
			}
			b.current.cases[producer.ordinal] = fact
		}
	}
	result := fieldStringStaticDomain(&Expr{Typ: makePlan2Type(&fact.result)})
	if fact.result.Oid == types.T_any {
		result.kind = fieldStringNull
	}
	result.resolved = fact.result
	result.inferredBinary = fact.inferredBinary
	result.parameter = true
	result.history = true
	return result
}

func castPreparedFieldNumeric(ctx context.Context, source *Expr, target types.Type) (*Expr, error) {
	if types.T(source.Typ.Id).IsMySQLString() {
		// A prepared string is a decimal spelling, including a SQL binary variable;
		// it is not a hexadecimal numeric literal. Reuse the existing prefix casts.
		var err error
		source, err = makePlan2CastExpr(ctx, source, makeSimplePlan2Type(types.T_text))
		if err != nil {
			return nil, err
		}
		if target.IsDecimal() {
			target.Charset = 255
			return appendComparisonCastBeforeExpr(ctx, source, makePlan2Type(&target))
		}
	}
	return appendComparisonCastBeforeExpr(ctx, source, makePlan2Type(&target))
}

// FIELD asks an unmaterialized selector for the comparison domain directly.
// Keep that conversion before selection (val_real/val_decimal in MySQL), while
// a relational/aggregate column keeps its materialized producer type.
func (b *baseBinder) fieldNumericValue(expr *Expr, target types.Type) (*Expr, error) {
	if state := preparedBindingState(b.GetContext()); state != nil && state.fieldTypes != nil {
		if original := state.fieldTypes.originals[expr]; original != nil {
			expr = original
		}
	}
	if fn := expr.GetF(); fn != nil {
		switch fn.Func.ObjName {
		case "case", "coalesce", "if", "iff":
			args := append([]*Expr(nil), fn.Args...)
			for _, i := range controlFlowValueIndexes(fn.Func.ObjName, len(args)) {
				var err error
				args[i], err = b.fieldNumericValue(preparedCommonValueSource(args[i]), target)
				if err != nil {
					return nil, err
				}
			}
			return BindFuncExprImplByPlanExpr(b.GetContext(), fn.Func.ObjName, args)
		}
	}
	return castPreparedFieldNumeric(b.GetContext(), expr, target)
}

// NeedsReprepare must also guard cache admission: an out-of-range integer
// spelling can change the resolved type without changing its wire/source type.
func (s *PreparedFieldTypes) NeedsReprepare(bindings []PreparedSourceBinding, values []any) bool {
	if s == nil {
		return false
	}
	state := &preparedSourceBindingState{bindings: bindings, values: values}
	for pos, parameter := range s.params {
		if actual, ok := fieldReprepareActualType(state, pos, parameter.typ); ok && !parameter.inherited && !fieldParameterTypeCompatible(parameter.typ, actual) {
			return true
		}
	}
	return false
}

func fieldReprepareActualType(state *preparedSourceBindingState, pos int32, resolved types.Type) (types.Type, bool) {
	actual, ok := preparedFieldActualType(state, pos)
	if !ok || !resolved.Oid.IsInteger() || !actual.Oid.IsMySQLString() {
		return actual, ok
	}
	value := state.values[pos]
	if param, wrapped := value.(ParamValue); wrapped {
		value = param.Value
	}
	var spelling string
	switch value := value.(type) {
	case string:
		spelling = value
	case []byte:
		spelling = string(value)
	default:
		return actual, ok
	}
	spelling = strings.Trim(spelling, " \t\n\r\v\f")
	prefix, valid := function.GetNumericStringPrefix(spelling)
	if !valid || prefix != spelling {
		return actual, ok
	}
	decimal, err := function.ParseDecimal256CastString(spelling, 65, 0)
	lower, upper := types.Decimal256FromInt64(math.MinInt64), types.Decimal256FromInt64(math.MaxInt64)
	if resolved.Oid.IsUnsignedInt() {
		lower = types.Decimal256{}
		upper = types.Decimal256{B0_63: math.MaxUint64}
	}
	if err != nil || decimal.Compare(lower) < 0 || decimal.Compare(upper) > 0 {
		return mysqlPreparedDecimalReprepareType(), true
	}
	return actual, ok
}
