// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// BindPreparedFieldCaseDomains applies SQL EXECUTE's resolved FIELD NULL-only
// CASE domain to a newly owned bound plan, including numeric re-resolution.
// Snapshots are copy-on-write and published after binding/metadata succeeds.
// The boolean marks value-derived decimal precision that must not be cached
// under a source-type-only key. Source domains and cached plans stay immutable.
func BindPreparedFieldCaseDomains(ctx context.Context, p *Plan, domains map[int32]types.Type, bindings []PreparedSourceBinding, values []any) (map[int32]types.Type, bool, error) {
	return bindPreparedFieldCaseDomains(ctx, p, domains, bindings, values, false)
}

// BindPreparedFieldNullFirstCaseDomains admits only an initial untyped NULL
// for COM_STMT. Ordinary typed protocol executions keep their existing rules;
// once admitted, numeric re-resolution uses the same owned CASE contract.
func BindPreparedFieldNullFirstCaseDomains(ctx context.Context, p *Plan, domains map[int32]types.Type, bindings []PreparedSourceBinding, values []any) (map[int32]types.Type, bool, error) {
	return bindPreparedFieldCaseDomains(ctx, p, domains, bindings, values, true)
}

func bindPreparedFieldCaseDomains(ctx context.Context, p *Plan, domains map[int32]types.Type, bindings []PreparedSourceBinding, values []any, nullAdmissionOnly bool) (map[int32]types.Type, bool, error) {
	valueDependent := false
	var sources map[int32]types.Type
	pending := domains
	copied := false
	err := planpb.VisitExpressionsInOwner(p, func(root *Expr) error {
		return planpb.VisitExprTree(root, func(expr *Expr) error {
			fn := expr.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != "field" || len(fn.Args) == 0 {
				return nil
			}
			subject := fn.Args[0]
			// FIELD may have inserted a comparison cast around a numeric CASE.
			// It is not SQL-authored and does not own CASE's resolved type.
			for isPreparedCaseComparisonCast(subject) {
				subject = subject.GetF().Args[0]
			}
			if subject == nil || isExplicitPreparedCast(subject) || !preparedFieldNullCaseHasDynamicCondition(subject) {
				return nil
			}
			collector := stringDomainWitnessCollector{seen: make(map[string]struct{})}
			markerValue, nullValue, other := preparedFieldBoundCaseValues(subject, &collector)
			if !markerValue || !nullValue || other {
				return nil
			}
			key := int32(-1)
			for _, marker := range collector.args {
				if param := marker.GetP(); param != nil && param.Pos >= 0 && (key < 0 || param.Pos < key) {
					key = param.Pos
				}
			}
			if key < 0 {
				return nil
			}
			target, found := pending[key]
			// T_any records a completed native protocol resolution, without a
			// consumer override. Absence alone cannot mean first execution.
			if found && target.Oid == types.T_any {
				return nil
			}
			current := makeTypeByPlan2Expr(subject)
			// String-origin changes are assignable to an already resolved type.
			// A numeric binding can instead reprepare a string CASE or widen an
			// existing numeric CASE (integer -> decimal -> floating point).
			if sources == nil {
				sources = make(map[int32]types.Type, len(bindings))
				for _, binding := range bindings {
					sources[binding.Position] = binding.Type
				}
			}
			numericSource := false
			nullFirst := !found
			for _, marker := range collector.args {
				param := marker.GetP()
				if sources[param.Pos].IsNumeric() {
					numericSource = true
				}
				pos := int(param.Pos)
				if sources[param.Pos].Oid != types.T_any || pos < 0 || pos >= len(values) {
					nullFirst = false
				} else {
					value := values[pos]
					if paramValue, ok := value.(ParamValue); ok {
						value = paramValue.Value
					}
					if value != nil {
						nullFirst = false
					}
				}
			}
			if nullAdmissionOnly && !found && !nullFirst {
				for _, marker := range collector.args {
					pos := int(marker.GetP().Pos)
					if pos < 0 || pos >= len(values) {
						return nil
					}
				}
				if !copied {
					pending = make(map[int32]types.Type, len(domains)+1)
					for pos, typ := range domains {
						pending[pos] = typ
					}
					copied = true
				}
				pending[key] = types.T_any.ToType()
				return nil
			}
			reparseNumeric := numericSource && current.IsNumeric() && (!found || target.Oid.IsMySQLString() ||
				preparedCaseNumericRank(current) > preparedCaseNumericRank(target) ||
				current.Oid.IsInteger() && target.Oid.IsInteger() && current.Oid != target.Oid)
			if !found || reparseNumeric {
				for _, marker := range collector.args {
					if param := marker.GetP(); param != nil && !sources[param.Pos].Oid.IsMySQLString() && !numericSource && !nullFirst {
						// Without NULL-first evidence, non-string value roles do not
						// resolve a string domain merely because FIELD casts them.
						return nil
					}
				}
				if nullFirst {
					// An untyped NULL first execution resolves the default binary
					// string CASE domain; later strings are assignable to it.
					target = types.T_blob.ToType()
				} else if current.IsNumeric() && numericSource {
					target = current
				} else if !current.Oid.IsMySQLString() {
					return nil
				} else if types.StaticStringDomain(current) == types.StringDomainBinary {
					target = types.T_blob.ToType()
				} else {
					target = types.T_text.ToType()
					target.Width = -1
				}
				target.Charset, target.CollationVersion = current.Charset, current.CollationVersion
				if nullFirst {
					target.Charset, target.CollationVersion = types.CharsetBinary, types.CollationVersionLegacy
				}
				if !copied {
					pending = make(map[int32]types.Type, len(domains)+1)
					for pos, typ := range domains {
						pending[pos] = typ
					}
					copied = true
				}
				pending[key] = target
			}
			// DECIMAL's resolved family does not freeze the first value's
			// scale. Keep fresh numeric precision, or infer a string prefix's
			// exact envelope; a value-derived envelope is never type-key cached.
			consumption := target
			if target.IsDecimal() {
				if current.IsDecimal() {
					consumption = current
				} else if current.IsNumeric() {
					common, err := planfunction.GetFunctionByName(ctx, "coalesce", []types.Type{target, current})
					if err != nil {
						return err
					}
					consumption = common.GetReturnType()
				} else if current.Oid.IsMySQLString() {
					valueDependent = true
					for _, marker := range collector.args {
						pos := int(marker.GetP().Pos)
						if pos < 0 || pos >= len(values) {
							return moerr.NewInvalidInput(ctx, "missing prepared CASE value for decimal inference")
						}
						param, ok := values[pos].(ParamValue)
						if !ok || param.Value == nil {
							continue
						}
						inferred := PreparedNumericPrefixTypeFromString(preparedParamValueText(param))
						common, err := planfunction.GetFunctionByName(ctx, "coalesce", []types.Type{consumption, inferred})
						if err != nil {
							return err
						}
						consumption = common.GetReturnType()
					}
					if consumption.IsDecimal() {
						// Existing decimal-prefix CAST contract; this is numeric
						// parsing metadata, not a character collation identity.
						consumption.Charset = 255
					}
				}
			}
			var converted *Expr
			var err error
			if target.IsNumeric() && makeTypeByPlan2Expr(subject).Oid.IsMySQLString() {
				// A binary SQL variable is a character representation here, not
				// an integer byte payload. Use the existing comparison-prefix
				// contract after a byte-preserving, unbounded text view.
				text := types.T_text.ToType()
				text.Width = -1
				converted, err = makePlan2CastExpr(ctx, subject, makePlan2Type(&text))
				if err == nil {
					converted, err = appendCastBeforeExprWithOverload(ctx, converted, makePlan2Type(&consumption), 2)
				}
			} else {
				converted, err = makePlan2CastExpr(ctx, subject, makePlan2Type(&consumption))
			}
			if err != nil {
				return err
			}
			if converted == subject {
				converted = DeepCopyExpr(subject)
			}
			// A resolved domain is a one-node metadata contract, not the
			// current execution's CASE graph. Keep reset/probes from looking
			// through this consumer-local boundary and re-inferring its domain.
			witness := makePlan2StringConstExprWithType("")
			if target.IsNumeric() {
				witness = makePlan2Int64ConstExprWithType(0)
			}
			witness.Typ = makePlan2Type(&consumption)
			if types.StaticStringDomain(target) == types.StringDomainBinary {
				witness.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
			}
			ensurePreparedNumericMetadata(converted).StringDomainSource = witness
			args := append([]*Expr(nil), fn.Args...)
			args[0] = converted
			if target.IsNumeric() {
				for index := 1; index < len(args); index++ {
					// Remove FIELD's old implicit DOUBLE promotion, not the
					// underlying prepared source transport or SQL-authored cast.
					if isPreparedCaseComparisonCast(args[index]) && makeTypeByPlan2Expr(args[index]).IsFloat() {
						child := args[index].GetF().Args[0]
						if makeTypeByPlan2Expr(child).Oid.IsMySQLString() || makeTypeByPlan2Expr(child).IsDecimal() {
							args[index] = child
						}
					}
				}
				for index := 1; index < len(args); index++ {
					if !makeTypeByPlan2Expr(args[index]).Oid.IsMySQLString() {
						continue
					}
					text := types.T_text.ToType()
					text.Width = -1
					args[index], err = makePlan2CastExpr(ctx, args[index], makePlan2Type(&text))
					if err == nil {
						number := types.T_float64.ToType()
						args[index], err = appendCastBeforeExprWithOverload(ctx, args[index], makePlan2Type(&number), 2)
					}
					if err != nil {
						return err
					}
				}
			}
			// A later binding can have numeric candidates. Re-resolve FIELD's
			// comparison overload after restoring the subject's owned domain;
			// retaining a numeric kernel with a string input is not safe.
			rebound, err := BindFuncExprImplByPlanExpr(ctx, "field", args)
			if err != nil {
				return err
			}
			rebound.PreparedNumeric = expr.PreparedNumeric
			*expr = *rebound
			return nil
		})
	})
	if err != nil {
		return nil, false, err
	}
	return pending, valueDependent, nil
}

func preparedCaseNumericRank(typ types.Type) int {
	if typ.IsFloat() {
		return 3
	}
	if typ.IsDecimal() {
		return 2
	}
	if typ.Oid.IsInteger() {
		return 1
	}
	return 0
}

func isPreparedCaseComparisonCast(expr *Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || fn.SyntaxExplicitCast || len(fn.Args) != 2 {
		return false
	}
	_, overload := planfunction.DecodeOverloadID(fn.Func.Obj)
	return (overload == 0 || overload == 2) && makeTypeByPlan2Expr(expr).IsNumeric()
}

// Bound plans have already typed SQL NULL leaves to the CASE result type.
// Accept inherited NULL typing here, but not SQL-authored numeric typed NULL
// or explicit casts; unresolved-template witness classification stays stricter.
func preparedFieldBoundCaseValues(expr *Expr, collector *stringDomainWitnessCollector) (marker, nullValue, other bool) {
	if expr == nil || isExplicitPreparedCast(expr) {
		return false, false, true
	}
	if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil {
		return preparedFieldBoundCaseValues(source, collector)
	}
	if expr.GetP() != nil {
		collector.addMarker(expr)
		return true, false, false
	}
	if lit := expr.GetLit(); lit != nil {
		isNull := lit.Isnull && (expr.Typ.Id == int32(types.T_any) || types.StaticStringDomain(makeTypeByPlan2Expr(expr)) != types.StringDomainNone || lit.Src == nil)
		return false, isNull, !isNull
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false, false, true
	}
	name := fn.Func.ObjName
	if (name == "cast" || name == "max" || name == "min" || name == "any_value") && len(fn.Args) > 0 {
		return preparedFieldBoundCaseValues(fn.Args[0], collector)
	}
	if name != "case" && name != "coalesce" && name != "ifnull" && name != "if" && name != "iff" {
		return false, false, true
	}
	for index, arg := range fn.Args {
		if (name == "if" || name == "iff") && index == 0 || name == "case" && !numericFunctionArgKeepsContext(name, index, len(fn.Args)) {
			continue
		}
		m, n, o := preparedFieldBoundCaseValues(arg, collector)
		marker, nullValue, other = marker || m, nullValue || n, other || o
	}
	return
}

// CASE resolution ownership survives a numeric common type chosen for an
// all-NULL binding. Preserve only returned markers, one NULL and one control
// marker; no executable producer or relational graph is retained.
func preparedFieldBoundCaseContractWitness(source *Expr) *Expr {
	condition := preparedFieldNullCaseCondition(source)
	if condition == nil {
		return nil
	}
	values := stringDomainWitnessCollector{seen: make(map[string]struct{})}
	marker, nullValue, other := preparedFieldBoundCaseValues(source, &values)
	if !marker || !nullValue || other {
		return nil
	}
	controls := stringDomainWitnessCollector{seen: make(map[string]struct{})}
	_ = planpb.VisitExprTree(condition, func(expr *Expr) error {
		controls.addMarker(expr)
		return nil
	})
	if len(controls.args) == 0 {
		return nil
	}
	returned := &Expr{Typ: source.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "coalesce"},
		Args: append(values.args, makePlan2NullConstExprWithType()),
	}}}
	return &Expr{Typ: source.Typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "case"},
		Args: []*Expr{controls.args[0], makePlan2NullConstExprWithType(), returned},
	}}}
}
