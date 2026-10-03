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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// A binding fixes a parser ordinal's execution slot and SQL source domain.
// Ordinary parameters remain references throughout reusable planning.
type PreparedSourceBinding struct {
	Position     int32
	Type         types.Type
	NumericType  types.Type
	BitCountType types.Type
}

type preparedSourceBindingsKey struct{}
type preparedUserVariableBindingsKey struct{}

type preparedUserVariableBinding struct {
	typ          Type
	stringDomain uint32
}

// WithPreparedUserVariableBindings retains PREPARE-time user-variable types
// and row domains while execution-time parameter binding replans the statement.
func WithPreparedUserVariableBindings(ctx context.Context, prepared *Plan) context.Context {
	if prepared == nil {
		return ctx
	}
	bindings := make(map[string]preparedUserVariableBinding)
	var visit func(*Expr) error
	visit = func(expr *Expr) error {
		if v := expr.GetV(); v != nil && !v.System {
			bindings[strings.ToLower(v.Name)] = preparedUserVariableBinding{
				typ: expr.Typ, stringDomain: v.BoundStringDomain,
			}
		}
		if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil {
			return plan.VisitExprTree(source, visit)
		}
		return nil
	}
	_ = plan.VisitExpressionsInOwner(prepared, func(root *Expr) error {
		return plan.VisitExprTree(root, visit)
	})
	if len(bindings) == 0 {
		return ctx
	}
	return context.WithValue(ctx, preparedUserVariableBindingsKey{}, bindings)
}

func preparedUserVariable(ctx context.Context, name string) (preparedUserVariableBinding, bool) {
	if ctx == nil {
		return preparedUserVariableBinding{}, false
	}
	bindings, _ := ctx.Value(preparedUserVariableBindingsKey{}).(map[string]preparedUserVariableBinding)
	binding, ok := bindings[strings.ToLower(name)]
	return binding, ok
}

// Values are visible only during this binding. Consumers which require a
// value to determine schema/configuration mark the result value-dependent;
// such a plan must not enter the type-only cache.
type preparedSourceBindingState struct {
	bindings             []PreparedSourceBinding
	values               []any
	valueDependent       bool
	selectStatement      bool
	diagnosticCandidates []*Expr
	diagnosticFree       bool
}

func withPreparedSourceBindings(ctx context.Context, bindings []PreparedSourceBinding, values ...[]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	bindings = append([]PreparedSourceBinding(nil), bindings...)
	state := &preparedSourceBindingState{bindings: bindings, diagnosticFree: true}
	if len(values) > 0 {
		state.values = values[0]
	}
	return context.WithValue(ctx, preparedSourceBindingsKey{}, state)
}

func preparedBindingState(ctx context.Context) *preparedSourceBindingState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(preparedSourceBindingsKey{}).(*preparedSourceBindingState)
	return state
}

// A consumer that asks for exact numeric spelling can choose different
// overloads for values with the same source type, including failed parses.
func preparedExactNumericStringType(ctx context.Context, ordinal int) (types.Type, bool) {
	state := preparedBindingState(ctx)
	if state == nil || ordinal < 0 || ordinal >= len(state.values) {
		return types.Type{}, false
	}
	value := state.values[ordinal]
	if param, ok := value.(ParamValue); ok {
		value = param.Value
	}
	spelling, ok := value.(string)
	if !ok {
		return types.Type{}, false
	}
	state.valueDependent = true
	if !PreparedNumericStringIsComplete(spelling) {
		return types.Type{}, false
	}
	return PreparedRuntimeTypeFromString(spelling)
}

func preparedSourceBindings(ctx context.Context) []PreparedSourceBinding {
	if state := preparedBindingState(ctx); state != nil {
		return state.bindings
	}
	return nil
}

func preparedConfigurationValue(ctx context.Context, expr *Expr) (any, bool) {
	state := preparedBindingState(ctx)
	if state == nil || expr == nil || expr.GetP() == nil {
		return nil, false
	}
	position := expr.GetP().Pos
	if position < 0 || int(position) >= len(state.values) {
		return nil, false
	}
	state.valueDependent = true
	value := state.values[position]
	if param, ok := value.(ParamValue); ok {
		if param.MaterializedValue != "" {
			return param.MaterializedValue, true
		}
		return param.Value, true
	}
	return value, true
}

func preparedBoundDoubleValue(ctx context.Context, expr *Expr) (float64, bool) {
	raw, present := preparedConfigurationValue(ctx, expr)
	if !present {
		return 0, false
	}
	switch value := raw.(type) {
	case float64:
		return value, true
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func preparedNumericValueSpelling(value any) string {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return fmt.Sprint(value)
}

func preparedSourceBindingAt(ctx context.Context, ordinal int) (PreparedSourceBinding, error) {
	bindings := preparedSourceBindings(ctx)
	if ordinal <= 0 || ordinal > len(bindings) {
		return PreparedSourceBinding{}, moerr.NewInternalErrorf(ctx,
			"prepared parameter ordinal %d has no source binding", ordinal)
	}
	return bindings[ordinal-1], nil
}

func bindPreparedSource(ctx context.Context, ordinal int) (*Expr, error) {
	binding, err := preparedSourceBindingAt(ctx, ordinal)
	if err != nil {
		return nil, err
	}
	// Keep the parameter executable even when this execution supplies NULL.
	// Consumers choose a concrete domain; relational materialization handles
	// the remaining domainless projection at its own boundary.
	return &Expr{
		Typ:  makePlan2Type(&binding.Type),
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: binding.Position}},
	}, nil
}

func integerDomainFits(source, target types.T) bool {
	if !source.IsInteger() || !target.IsInteger() {
		return false
	}
	if source.IsUnsignedInt() == target.IsUnsignedInt() {
		return source.TypeLen() <= target.TypeLen()
	}
	return source.IsUnsignedInt() && !target.IsUnsignedInt() && source.TypeLen() < target.TypeLen()
}

// IN owns this admission; scalar comparisons and DML keep their source-domain
// rules. Save the full cast before rewrites can remove the consumer, including
// an IN used only in a projection. The existing per-EXECUTE probe owns reuse.
func bindPreparedIntegerInValue(ctx context.Context, column, source *Expr) (*Expr, bool, error) {
	state := preparedBindingState(ctx)
	if state == nil || !state.selectStatement || column.GetCol() == nil || source.GetP() == nil {
		return source, false, nil
	}
	target := types.T(column.Typ.Id)
	binding, ok := state.bindingForPosition(source.GetP().Pos)
	if !ok || !target.IsSignedInt() || target.TypeLen() > 4 ||
		!binding.Type.Oid.IsSignedInt() || binding.Type.Oid.TypeLen() <= target.TypeLen() {
		return source, false, nil
	}
	wasValueDependent := state.valueDependent
	value, present := preparedConfigurationValue(ctx, source)
	if !present || value == nil {
		return source, false, nil
	}
	_, exact, err := preparedComparisonExactIntegerExpr(ctx, preparedNumericValueSpelling(value), column.Typ)
	if err != nil || !exact {
		// Unsafe fallbacks must not replace a valid cached narrowing plan.
		return source, false, err
	}
	converted, err := makePlan2CastExpr(ctx, source, column.Typ)
	if err != nil {
		return nil, false, err
	}
	state.diagnosticCandidates = append(state.diagnosticCandidates, DeepCopyExpr(converted))
	state.valueDependent = wasValueDependent
	return converted, true, nil
}

// Capture before relational rewrites can remove or duplicate a predicate.
// Each builder owns its proof: a safe child must not authorize an unprobed
// parent. Only immutable expression copies escape into the eventual cache.
func (builder *QueryBuilder) bindPreparedPredicateDiagnostics() error {
	state := preparedBindingState(builder.GetContext())
	if state == nil || state.values == nil || builder.preparedBindingProof != nil {
		return nil
	}
	p := &Plan{Plan: &plan.Plan_Query{Query: builder.qry}}
	candidates := PreparedPlanDiagnosticCandidates(p)
	for i, expr := range candidates {
		candidates[i] = DeepCopyExpr(expr)
	}
	safe, err := ProbePreparedDiagnosticCandidates(builder.compCtx.GetProcess(), candidates)
	if err != nil {
		return err
	}
	builder.preparedBindingProof = &safe
	state.diagnosticFree = state.diagnosticFree && safe
	state.diagnosticCandidates = append(state.diagnosticCandidates, candidates...)
	return nil
}

func (builder *QueryBuilder) preparedParameterDiagnosticsFree() bool {
	if builder.preparedBindingProof != nil {
		return *builder.preparedBindingProof
	}
	return false
}

// Source types are known during EXECUTE, but the resulting plan can still be
// reused. Keep that lifetime separate from PREPARE's unresolved-type mode.
func (builder *QueryBuilder) isReusablePlan() bool {
	return builder.isPrepareStatement || preparedBindingState(builder.GetContext()) != nil
}

// lowerPreparedSourceTransports runs after SQL binding and optimization. Keep
// the physical representation compatible with existing readers and remote CNs
// without letting transport casts participate in semantic type selection.
func lowerPreparedSourceTransports(ctx context.Context, p *Plan) error {
	return plan.VisitExpressionsInOwner(p, func(root *Expr) error {
		return plan.VisitExprTree(root, func(expr *Expr) error {
			if expr.GetP() == nil || types.T(expr.Typ.Id).IsMySQLString() || expr.Typ.Id == int32(types.T_any) {
				return nil
			}
			source := &Expr{
				Typ:  plan.Type{Id: int32(types.T_text)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: expr.GetP().Pos}},
			}
			physical, err := makePlan2CastExpr(ctx, source, expr.Typ)
			if err != nil {
				return err
			}
			*expr = *physical
			return nil
		})
	})
}

// SQL EXECUTE exposes a bare marker as TEXT, even when its source domain is
// numeric or BOOL for expressions that consume it. Apply that presentation
// only at the final result edge, including transparent derived projections;
// inner columns retain their semantic source types.
func presentPreparedSQLResults(ctx context.Context, query *plan.Query) {
	state := preparedBindingState(ctx)
	if state == nil || query == nil || query.StmtType != plan.Query_SELECT {
		return
	}
	textType := types.T_text.ToType()
	for _, step := range query.Steps {
		if step < 0 || int(step) >= len(query.Nodes) {
			continue
		}
		root := query.Nodes[step]
		if root == nil {
			continue
		}
		for i, expr := range root.ProjectList {
			if expr == nil || expr.Typ.Id == int32(types.T_text) {
				continue
			}
			pos, direct := preparedProjectedParamPosition(query, root, expr,
				make(map[preparedSetOperationNullKey]bool), false)
			if !direct || pos < 0 || int(pos) >= len(state.values) {
				continue
			}
			param, ok := state.values[pos].(ParamValue)
			if !ok || param.IsBinaryProtocol {
				continue
			}
			// SQL EXECUTE already transports the original spelling as TEXT.
			// Reuse that source for presentation instead of formatting a
			// semantic BOOL/DECIMAL value back into a different string.
			root.ProjectList[i] = &Expr{
				Typ:  makePlan2Type(&textType),
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
			}
		}
	}
}

// Consumer-specific conversions are selected before their parent's overload
// and before key construction. The witness can determine a conversion domain,
// but only the original parameter reference enters the executable expression.
func bindPreparedConsumerArguments(ctx context.Context, name string, args []*Expr) ([]*Expr, error) {
	state := preparedBindingState(ctx)
	if state == nil {
		return args, nil
	}
	name = strings.ToLower(name)
	args = append([]*Expr(nil), args...)
	for i, source := range args {
		if source == nil {
			continue
		}
		if source.GetP() == nil {
			if len(args) == 1 && types.T(source.Typ.Id).IsMySQLString() &&
				(name == "sum" || name == "avg" || name == "abs" || name == "sign" || name == "sleep") {
				// The source may be a projected marker, scalar subquery, or
				// ordinary string. These numeric consumers use the same text
				// conversion as a direct prepared marker.
				var err error
				args[i], err = makePlan2CastExpr(ctx, source, makeSimplePlan2Type(types.T_float64))
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		binding, ok := state.bindingForPosition(source.GetP().Pos)
		if !ok {
			continue
		}
		if name == "member of" && i == 0 && binding.Type.Oid.IsArrayRelate() {
			// MEMBER OF rejects SQL arrays with an argument-specific runtime
			// error. Keep a direct prepared array in its TEXT transport form so
			// the existing checker can report that error using source metadata.
			args[i] = &Expr{Typ: makeSimplePlan2Type(types.T_text),
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: source.GetP().Pos}}}
			continue
		}
		if len(args) == 2 && isPreparedNumericComparisonContext(name) &&
			args[1-i] != nil && args[1-i].Typ.Id == int32(types.T_float32) &&
			binding.Type.IsNumeric() && int(source.GetP().Pos) < len(state.values) {
			if param, ok := state.values[source.GetP().Pos].(ParamValue); ok && !param.IsBinaryProtocol {
				// A SQL PREPARE marker compared directly with FLOAT inherits
				// that column's storage precision. Comparing DECIMAL transport
				// against FLOAT via DOUBLE would miss the stored rounded value.
				converted, castErr := makePlan2CastExpr(ctx, source, args[1-i].Typ)
				if castErr != nil {
					return nil, castErr
				}
				args[i] = converted
				continue
			}
		}
		if len(args) == 2 && state.selectStatement && isPreparedNumericComparisonContext(name) &&
			binding.Type.Oid == types.T_float64 && args[1-i] != nil && args[1-i].GetCol() != nil &&
			types.T(args[1-i].Typ.Id).IsDecimal() {
			if value, ok := preparedBoundDoubleValue(ctx, source); ok &&
				decimalFloatComparisonHasUniqueValue(value, makeTypeByPlan2Expr(args[1-i])) {
				converted, castErr := makePlan2CastExpr(ctx, source, args[1-i].Typ)
				if castErr != nil {
					return nil, castErr
				}
				args[i] = converted
				continue
			}
		}
		// Binary clients can transmit an INT key as BIGINT. For a direct signed
		// column of at most 32 bits, a guarded narrowing preserves its comparison
		// domain and lets non-SELECT predicates use the existing storage PK filter.
		guardedIntegerKey := !state.selectStatement && len(args) == 2 &&
			args[1-i] != nil && args[1-i].GetCol() != nil &&
			binding.Type.Oid.IsSignedInt() && types.T(args[1-i].Typ.Id).IsSignedInt() &&
			types.T(args[1-i].Typ.Id).TypeLen() <= 4 &&
			binding.Type.Oid.TypeLen() > types.T(args[1-i].Typ.Id).TypeLen()
		if len(args) == 2 && isPreparedNumericComparisonContext(name) && args[1-i] != nil &&
			(types.T(args[1-i].Typ.Id).IsInteger() || args[1-i].Typ.Id == int32(types.T_bit)) &&
			(binding.Type.Oid.IsMySQLString() ||
				(binding.Type.Oid.IsFloat() && types.T(args[1-i].Typ.Id).IsSignedInt()) ||
				((state.selectStatement || guardedIntegerKey) && binding.Type.Oid.IsInteger() &&
					(binding.Type.Oid.TypeLen() > types.T(args[1-i].Typ.Id).TypeLen() ||
						binding.Type.Oid.IsSignedInt() != types.T(args[1-i].Typ.Id).IsSignedInt()))) {
			// A proven integral value can compare in the peer's integer domain
			// without casting the indexed column to a wider domain.
			// Most such proofs are value-dependent. The narrow signed-key case
			// can reuse a plan only while its full cast passes the per-EXECUTE
			// diagnostic guard; overflow falls back to the original wide domain.
			wasValueDependent := state.valueDependent
			if value, present := preparedConfigurationValue(ctx, source); present && value != nil {
				spelling := preparedNumericValueSpelling(value)
				_, exact, proofErr := preparedComparisonExactIntegerExpr(ctx, spelling, args[1-i].Typ)
				if proofErr != nil {
					return nil, proofErr
				}
				if binding.Type.Oid.IsInteger() || binding.Type.Oid.IsFloat() {
					// Some mixed integer comparisons enter an approximate domain.
					// Keep the existing comparison at values
					// whose adjacent integers may collide in DOUBLE.
					normalized, _ := planfunction.NormalizeExactIntegerString(spelling)
					integer, err := strconv.ParseInt(normalized, 10, 54)
					exact = exact && err == nil && integer >= -(1<<53)+1 && integer <= (1<<53)-1
				}
				if exact {
					target := args[1-i].Typ
					if target.Id == int32(types.T_bit) {
						// BIT stores an unsigned integer; a direct string→BIT cast
						// would interpret the source bytes instead of its numeric text.
						unsigned := types.T_uint64.ToType()
						target = makePlan2Type(&unsigned)
					}
					converted := source
					var castErr error
					if binding.Type.Oid.IsMySQLString() {
						// Decimal/scientific text needs an exact intermediate parser;
						// direct string→integer casts reject those spellings.
						decimalType := types.New(types.T_decimal128, 38, 0)
						converted, castErr = makePlan2CastExpr(ctx, source, makePlan2Type(&decimalType))
						if castErr != nil {
							return nil, castErr
						}
					}
					converted, castErr = makePlan2CastExpr(ctx, converted, target)
					if castErr != nil {
						return nil, castErr
					}
					args[i] = converted
					if guardedIntegerKey {
						// Keep the guard even if optimization removes the predicate.
						// Do not clear dependencies belonging to other consumers.
						state.diagnosticCandidates = append(state.diagnosticCandidates, DeepCopyExpr(converted))
						state.valueDependent = wasValueDependent
					}
					continue
				}
			}
		}
		if len(args) == 2 && isPreparedNumericComparisonContext(name) &&
			binding.Type.Oid.IsMySQLString() && args[1-i] != nil {
			peer := types.T(args[1-i].Typ.Id)
			if peer.IsInteger() || peer.IsFloat() {
				// A text marker compared with a numeric peer keeps its text
				// source identity, but comparison uses a fractional domain.
				// Scope this conversion to prepared parameters; ordinary SQL
				// comparisons retain their established coercion contract.
				var castErr error
				args[i], castErr = makePlan2CastExpr(ctx, source,
					makeSimplePlan2Type(types.T_float64))
				if castErr != nil {
					return nil, castErr
				}
				continue
			}
		}
		if len(args) == 2 && isPreparedNumericComparisonContext(name) &&
			binding.Type.IsNumeric() && args[1-i] != nil &&
			types.T(args[1-i].Typ.Id).IsMySQLString() {
			// A numeric marker compared with a text expression uses MySQL's
			// numeric comparison domain. Bind both operands here, where the
			// parameter source is known, instead of changing plain SQL casts.
			var castErr error
			args[i], castErr = makePlan2CastExpr(ctx, source,
				makeSimplePlan2Type(types.T_float64))
			if castErr != nil {
				return nil, castErr
			}
			args[1-i], castErr = makePlan2CastExpr(ctx, args[1-i],
				makeSimplePlan2Type(types.T_float64))
			if castErr != nil {
				return nil, castErr
			}
			continue
		}
		var target types.Type
		switch {
		case name == "bit_count" && len(args) == 1:
			target = binding.BitCountType
			if target.Oid == types.T_any && (binding.Type.Oid.IsMySQLString() || binding.Type.Oid == types.T_any) {
				target = types.T_varbinary.ToType()
			}
		case name == "ntile" && len(args) == 1 && binding.Type.Oid == types.T_any:
			// Keep a NULL bucket count executable so NTILE reports its
			// runtime argument error instead of a binder ANY overload error.
			target = types.T_int64.ToType()
		case len(args) == 1 && binding.Type.Oid.IsMySQLString() &&
			(name == "sum" || name == "avg"):
			// Aggregates consume the numeric prefix of a text marker. The
			// source remains text for every other occurrence of the marker.
			target = types.T_float64.ToType()
		case len(args) == 1 && binding.Type.Oid.IsMySQLString() &&
			(name == "abs" || name == "sign" || name == "sleep"):
			// Prepared string sources can contain fractions. Keep this
			// conversion at the marker consumer; ordinary string expressions
			// retain their established overload selection.
			target = types.T_float64.ToType()
			if name == "abs" {
				if exact, ok := preparedExactNumericStringType(ctx, int(source.GetP().Pos)); ok {
					target = exact
				}
			}
		case binding.NumericType.Oid.IsDecimal() &&
			(isNumericContextFunction(name) || supportsGenericNumericFunctionContext(name) ||
				preparedSQLExecuteNumericResultConsumer(name) || isPreparedNumericComparisonContext(name)):
			target = binding.NumericType
		}
		if target.Oid != types.T_any {
			var err error
			args[i], err = makePlan2CastExpr(ctx, source, makePlan2Type(&target))
			if err != nil {
				return nil, err
			}
		}
		if int(source.GetP().Pos) < len(state.values) {
			if param, ok := state.values[source.GetP().Pos].(ParamValue); ok {
				if target, ok := preparedSQLExecuteTextConsumerType(name, source, param); ok {
					var err error
					args[i], err = preparedSQLExecuteTextConsumerCast(ctx, source, target)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	positions := make(map[int]types.StringConversionKind)
	prefixArgs := make([]bool, len(args))
	prefixKinds := make([]types.StringConversionKind, len(args))
	prefixListArgs := make([][]bool, len(args))
	prefixListKinds := make([][]types.StringConversionKind, len(args))
	eligible := func(source *Expr) (bool, types.StringConversionKind) {
		if source == nil || source.GetP() == nil {
			return false, 0
		}
		pos := int(source.GetP().Pos)
		if pos < 0 || pos >= len(state.values) {
			return false, 0
		}
		param, ok := state.values[pos].(ParamValue)
		if !ok || !param.EnableNumericPrefix {
			return false, 0
		}
		positions[pos] = param.PrepareParamKind
		return true, param.PrepareParamKind
	}
	for i, source := range args {
		if list := source.GetList(); list != nil {
			prefixListArgs[i] = make([]bool, len(list.List))
			prefixListKinds[i] = make([]types.StringConversionKind, len(list.List))
			for j, item := range list.List {
				prefixListArgs[i][j], prefixListKinds[i][j] = eligible(item)
			}
		} else {
			prefixArgs[i], prefixKinds[i] = eligible(source)
		}
	}
	if !preparedNumericPrefixContext(name, args, prefixArgs, prefixKinds, prefixListArgs, prefixListKinds) {
		return args, nil
	}
	witnesses := append([]*Expr(nil), args...)
	witness := func(source *Expr) *Expr {
		if source == nil || !types.T(source.Typ.Id).IsMySQLString() {
			return source
		}
		value, ok := preparedConfigurationValue(ctx, source)
		if !ok {
			return source
		}
		if value == nil {
			return &Expr{Typ: source.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		}
		return makePlan2StringConstExprWithType(fmt.Sprint(value))
	}
	for i, source := range args {
		if list := source.GetList(); list != nil {
			witnesses[i] = DeepCopyExpr(source)
			for j, item := range list.List {
				if prefixListArgs[i][j] {
					witnesses[i].GetList().List[j] = witness(item)
				}
			}
		} else if prefixArgs[i] {
			witnesses[i] = witness(source)
		}
	}
	fixed := preparedCommonValueFixedDecimalPeer(name, args, positions, state.values)
	if isPreparedCommonValueFunction(name) && !fixed {
		return args, nil
	}
	bound, _, err := preparedNumericPrefixArgs(ctx, name, args, witnesses,
		prefixArgs, prefixKinds, prefixListArgs, prefixListKinds, fixed)
	return bound, err
}

func (state *preparedSourceBindingState) bindingForPosition(position int32) (PreparedSourceBinding, bool) {
	for _, binding := range state.bindings {
		if binding.Position == position {
			return binding, true
		}
	}
	return PreparedSourceBinding{}, false
}

// Retained for the legacy parameter-replacement path, which still handles
// prepared DDL and other statements not built through source bindings.
func preparedCharSourceCast(ctx context.Context, source *Expr, value string) (*Expr, error) {
	target, ok := PreparedCharSourceTypeFromString(value)
	if !ok {
		return source, nil
	}
	return makePlan2CastExpr(ctx, source, makePlan2Type(&target))
}

// PreparedExecutionPlan carries the immutable logical plan and the original
// predicate candidates that must be proved again for every cache hit.
type PreparedExecutionPlan struct {
	Plan                 *Plan
	DiagnosticCandidates []*Expr
	DiagnosticFree       bool
	ValueDependent       bool
}

// EXPLAIN must profile the same binding semantics as its underlying statement.
// Keep the wrapper for planning/authorization, and never classify explained DML
// as SELECT merely because it is wrapped in a diagnostic statement.
func preparedUnderlyingSelect(stmt tree.Statement) bool {
	for stmt != nil {
		switch wrapped := stmt.(type) {
		case *tree.ExplainStmt:
			stmt = wrapped.Statement
		case *tree.ExplainAnalyze:
			stmt = wrapped.Statement
		case *tree.ExplainPhyPlan:
			stmt = wrapped.Statement
		default:
			return stmt.GetQueryType() == tree.QueryTypeDQL
		}
	}
	return false
}

// BuildPreparedExecutionPlan binds SQL source domains before optimization.
// The caller installs the current Process parameter vector before invoking it.
func BuildPreparedExecutionPlan(ctx CompilerContext, stmt tree.Statement,
	bindings []PreparedSourceBinding, values []any) (*PreparedExecutionPlan, error) {
	previous := ctx.GetContext()
	if tree.ParameterCount(stmt) != len(bindings) {
		return nil, moerr.NewInvalidInput(previous, "Incorrect arguments to EXECUTE")
	}
	planning := withPreparedSourceBindings(previous, bindings, values)
	preparedBindingState(planning).selectStatement = preparedUnderlyingSelect(stmt)
	ctx.SetContext(planning)
	defer ctx.SetContext(previous)
	query, err := NewPrepareOptimizer(ctx).Optimize(stmt, false)
	if err != nil {
		return nil, err
	}
	p := &Plan{Plan: &plan.Plan_Query{Query: query}, IsPrepare: true}
	presentPreparedSQLResults(planning, query)
	if err = lowerPreparedSourceTransports(planning, p); err != nil {
		return nil, err
	}
	state := preparedBindingState(planning)
	return &PreparedExecutionPlan{Plan: p, DiagnosticCandidates: state.diagnosticCandidates,
		DiagnosticFree: state.diagnosticFree, ValueDependent: state.valueDependent}, nil
}

func (state *preparedSourceBindingState) stringDomainParamLookup(pos int) (any, types.Type, bool) {
	binding, found := state.bindingForPosition(int32(pos))
	if !found {
		return nil, types.Type{}, false
	}
	if pos >= 0 && pos < len(state.values) {
		return state.values[pos], binding.Type, true
	}
	// Binder-only callers may omit values; use the supplied type metadata.
	return ParamValue{Value: "", SourceType: binding.Type, HasSourceType: true}, binding.Type, true
}
