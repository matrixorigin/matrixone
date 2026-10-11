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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// Read the original declaration, not a common type assigned to an untyped NULL.
func preparedCaseTypedNullDeclaration(expr *Expr) (types.Type, bool) {
	visited := make(map[*Expr]struct{})
	for expr != nil {
		if _, seen := visited[expr]; seen {
			return types.Type{}, false
		}
		visited[expr] = struct{}{}
		if literal := expr.GetLit(); literal != nil {
			if !literal.Isnull {
				return types.Type{}, false
			}
			expr = literal.Src
			continue
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 {
			return types.Type{}, false
		}
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if fn.SyntaxExplicitCast || overload == 1 {
			if fn.Args[0].GetLit().GetIsnull() {
				return makeTypeByPlan2Expr(expr), true
			}
			return types.Type{}, false
		}
		if overload != 0 {
			return types.Type{}, false
		}
		expr = fn.Args[0]
	}
	return types.Type{}, false
}

// PreparedCaseTypedNullPresentationType describes the declaration/marker CASE
// schema without imposing its wire capacity on the unbounded runtime value.
func PreparedCaseTypedNullPresentationType(expr *Expr) (planpb.Type, bool) {
	if expr == nil || expr.Typ.Id != int32(types.T_varchar) || expr.Typ.Width >= 0 {
		return planpb.Type{}, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "case" || !preparedCaseHasStringNullDeclaration(fn.Args) {
		return planpb.Type{}, false
	}
	typ := expr.Typ
	typ.Width = types.MaxStringSize / 4
	if types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainBinary {
		typ.Id, typ.Width = int32(types.T_varbinary), types.MaxStringSize
	}
	return typ, true
}

func preparedCaseHasStringNullDeclaration(args []*Expr) bool {
	for index, arg := range args {
		if numericFunctionArgKeepsContext("case", index, len(args)) {
			if typ, declared := preparedCaseTypedNullDeclaration(arg); declared && typ.Oid.IsMySQLString() {
				return true
			}
		}
	}
	return false
}

func preparedCaseUnboundedResultArgument(arg *Expr) *Expr {
	copy := DeepCopyExpr(arg)
	for current := copy; current != nil && types.T(current.Typ.Id).IsMySQLString(); {
		current.Typ.Width = -1
		fn := current.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 || isExplicitPreparedCast(current) {
			break
		}
		if fn.Args[1].GetT() != nil && types.T(fn.Args[1].Typ.Id).IsMySQLString() {
			fn.Args[1].Typ.Width = -1
		}
		current = fn.Args[0]
	}
	return copy
}

// preparedCaseTypedNullHasUnboundedMarker recognizes only the declaration /
// unresolved-marker tuple normalized below, not general CASE value expressions.
func preparedCaseTypedNullHasUnboundedMarker(args []*Expr) bool {
	declared, marker := false, false
	for index, arg := range args {
		if !numericFunctionArgKeepsContext("case", index, len(args)) {
			continue
		}
		if typ, ok := preparedCaseTypedNullDeclaration(arg); ok {
			if !typ.Oid.IsMySQLString() {
				return false
			}
			declared = true
			continue
		}
		source := preparedCommonValueSource(arg)
		hasMarker, _, other := preparedFieldMarkerNullValues(arg)
		if hasMarker && !other && arg.Typ.Id == int32(types.T_varchar) {
			marker = true
			continue
		}
		if source == nil || source.Typ.Id != int32(types.T_any) || !source.GetLit().GetIsnull() {
			return false
		}
	}
	return declared && marker
}

// A declaration peer may constrain only a marker, not a value-producing SQL
// function or an authored value CAST. Conditions never participate.
func bindPreparedCaseTypedNullArguments(ctx context.Context, args []*Expr) ([]*Expr, error) {
	if preparedBindingState(ctx) == nil {
		return args, nil
	}
	declarations := make(map[int]types.Type)
	markers := make(map[int]*Expr)
	numeric := true
	var numericTypes []types.Type
	for index, arg := range args {
		if !numericFunctionArgKeepsContext("case", index, len(args)) {
			continue
		}
		source := preparedCommonValueSource(arg)
		if source != nil && source.GetP() != nil {
			markers[index] = source
			typ := makeTypeByPlan2Expr(source)
			if typ.IsNumeric() {
				numericTypes = append(numericTypes, typ)
			}
			continue
		}
		if typ, declared := preparedCaseTypedNullDeclaration(arg); declared {
			declarations[index] = typ
			if typ.IsNumeric() {
				numericTypes = append(numericTypes, typ)
			} else {
				numeric = false
			}
			continue
		}
		if source != nil && source.Typ.Id == int32(types.T_any) && source.GetLit().GetIsnull() {
			continue
		}
		return args, nil
	}
	if len(markers) == 0 {
		return args, nil
	}
	bound := append([]*Expr(nil), args...)
	for index, marker := range markers {
		typ := makeTypeByPlan2Expr(marker)
		if typ.Oid != types.T_binary || typ.Width >= 0 {
			continue
		}
		// A runtime SQL BINARY value without a declared extent must not
		// introduce a BINARY(-1) destination in the CASE's common casts.
		binary := types.T_varchar.ToType()
		binary.Charset = types.CharsetBinary
		binary.Width = -1
		converted, err := makePlan2CastExpr(ctx, marker, makePlan2Type(&binary))
		if err != nil {
			return nil, err
		}
		bound[index] = converted
	}
	if len(declarations) == 0 {
		return bound, nil
	}
	for index, typ := range declarations {
		if !typ.Oid.IsMySQLString() {
			continue
		}
		// A declared NULL owns charset, not a value extent. Keep its authored
		// CAST in Src while normalizing the unknown marker schema to VARCHAR.
		literal := &planpb.Literal{Isnull: true}
		if existing := args[index].GetLit(); existing != nil {
			literal = DeepCopyExpr(args[index]).GetLit()
		} else {
			rule.PreserveFoldedLiteralStringDomain(args[index], literal)
		}
		binary := types.T_varchar.ToType()
		if types.StaticStringDomain(typ) == types.StringDomainBinary {
			binary.Charset = types.CharsetBinary
		}
		binary.Width = -1
		bound[index] = &Expr{Typ: makePlan2Type(&binary), Expr: &planpb.Expr_Lit{Lit: literal}}
	}
	if !numeric {
		var domain types.StringDomain
		for _, typ := range declarations {
			current := types.StaticStringDomain(typ)
			if current == types.StringDomainNone || domain != types.StringDomainNone && current != domain {
				return bound, nil
			}
			domain = current
		}
		// An unresolved marker is VARCHAR/VARBINARY in the client schema,
		// not a new TEXT/LONGBLOB declaration. VARCHAR's unbounded envelope
		// preserves byte charset without the invalid BINARY(-1) destination.
		target := types.T_varchar.ToType()
		if domain == types.StringDomainBinary {
			target.Charset = types.CharsetBinary
		}
		target.Width = -1
		for index, marker := range markers {
			converted, err := makePlan2CastExpr(ctx, marker, makePlan2Type(&target))
			if err != nil {
				return nil, err
			}
			bound[index] = converted
		}
		return bound, nil
	}
	target := numericTypes[0]
	var err error
	if len(numericTypes) > 1 {
		common, commonErr := function.GetFunctionByName(ctx, "coalesce", numericTypes)
		if commonErr != nil {
			return nil, commonErr
		}
		target = common.GetReturnType()
	}
	state := preparedBindingState(ctx)
	allUntypedNull := true
	actualNumeric := false
	for _, marker := range markers {
		param := marker.GetP()
		binding, found := state.bindingForPosition(param.Pos)
		allUntypedNull = allUntypedNull && found && binding.Type.Oid == types.T_any
		actualNumeric = actualNumeric || found && binding.Type.IsNumeric()
	}
	if target.IsDecimal() {
		// Unlike the integer default, a DECIMAL declaration leaves later
		// marker strings native after NULL, in both SQL EXECUTE and COM.
		allUntypedNull = false
	}
	defaultType := types.T_any.ToType()
	if allUntypedNull {
		defaultType = target
	}
	if owner := state.fieldConsumers; owner != nil && owner.activeCase != 0 {
		var found bool
		defaultType, found = owner.pending[owner.activeCase]
		if !found {
			defaultType = types.T_any.ToType()
			if allUntypedNull {
				defaultType = target
			}
			owner.record(owner.activeCase, defaultType)
		} else if actualNumeric && defaultType.Oid != types.T_any {
			// A real numeric binding resolves the marker's source family; later
			// text is a native CASE value, not a forever-numeric declaration.
			defaultType = types.T_any.ToType()
			owner.record(owner.activeCase, defaultType)
		}
	}
	if defaultType.Oid == types.T_any {
		return bound, nil
	}
	target = defaultType
	for index, marker := range markers {
		sourceType := makeTypeByPlan2Expr(marker)
		if sourceType.IsNumeric() {
			continue
		}
		source := marker
		if sourceType.Oid.IsMySQLString() {
			text := types.T_text.ToType()
			text.Width = -1
			source, err = makePlan2CastExpr(ctx, source, makePlan2Type(&text))
			if err != nil {
				return nil, err
			}
		}
		bound[index], err = appendCastBeforeExprWithOverload(ctx, source, makePlan2Type(&target), 2)
		if err != nil {
			return nil, err
		}
	}
	return bound, nil
}
