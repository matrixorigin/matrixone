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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// A marker-only result has no independent SQL domain. Its TEXT transport must
// not prevent an enclosing common-value call from supplying an exact domain.
// Concrete strings, FLOAT and explicit casts still own their source domains.
func preparedCommonValueSource(expr *Expr) *Expr {
	for expr != nil {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) == 0 || fn.SyntaxExplicitCast {
			return expr
		}
		if _, overload := function.DecodeOverloadID(fn.Func.Obj); overload != 0 {
			return expr
		}
		source := fn.Args[0]
		if source.GetP() != nil || preparedCommonValueResultArgs(source) != nil ||
			(source.GetLit() != nil && source.GetLit().Isnull && source.Typ.Id == int32(types.T_any)) ||
			(types.T(source.Typ.Id).ToType().IsNumeric() && (types.T(expr.Typ.Id).IsMySQLString() || types.T(expr.Typ.Id).IsFloat())) {
			expr = source
			continue
		}
		return expr
	}
	return nil
}

// IFNULL is lowered as CASE WHEN ISNULL(first) THEN second ELSE first END.
// Only its results participate, in the original SQL operand order.
func preparedCommonValueResultArgs(expr *Expr) []*Expr {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil
	}
	if isPreparedCommonValueFunction(strings.ToLower(fn.Func.ObjName)) {
		return fn.Args
	}
	if expr.GetPreparedNumeric().GetIfnullCommonValue() && len(fn.Args) == 3 {
		return []*Expr{fn.Args[2], fn.Args[1]}
	}
	return nil
}

// Empty peers means unresolved; allowed=false means a concrete type boundary.
// Keep every concrete peer: their combined integral/scale constraints must
// survive propagation, independently of operand order.
// A plain NULL is a boundary only when it is the first non-marker operand.
func preparedCommonValueDomain(ctx context.Context, args []*Expr) (peers []*Expr, allowed bool) {
	allowed = true
	state := preparedBindingState(ctx)
	for _, arg := range args {
		source := preparedCommonValueSource(arg)
		if source == nil {
			return nil, false
		}
		if param := source.GetP(); param != nil {
			binding, ok := state.bindingForPosition(param.Pos)
			if !ok {
				return nil, false
			}
			if binding.Type.Oid == types.T_any {
				continue
			}
			if binding.Type.Oid.IsMySQLString() && param.Pos >= 0 && int(param.Pos) < len(state.values) {
				value, ok := state.values[param.Pos].(ParamValue)
				if ok && value.EnableNumericPrefix {
					continue
				}
			}
		}
		if results := preparedCommonValueResultArgs(source); results != nil {
			childPeers, childAllowed := preparedCommonValueDomain(ctx, results)
			if !childAllowed {
				return nil, false
			}
			if len(childPeers) == 0 {
				continue
			}
			// A resolved child owns its result domain, rather than inheriting
			// the enclosing call's domain a second time.
			if !types.T(source.Typ.Id).ToType().IsNumeric() {
				return nil, false
			}
		}
		if source.GetLit() != nil && source.GetLit().Isnull && source.Typ.Id == int32(types.T_any) {
			if len(peers) == 0 {
				return nil, false
			}
			continue
		}
		oid := types.T(source.Typ.Id)
		if oid.IsFloat() || !oid.ToType().IsNumeric() {
			return nil, false
		}
		peers = append(peers, source)
	}
	return peers, allowed
}

// Collect only unresolved result roles. A resolved child or an explicit
// boundary owns its domain; IFNULL's control condition is never a witness.
func preparedCommonValueMarkers(ctx context.Context, args []*Expr) ([]*Expr, bool) {
	state := preparedBindingState(ctx)
	var markers []*Expr
	nested := false
	for _, arg := range args {
		source := preparedCommonValueSource(arg)
		if source == nil {
			continue
		}
		if ref := source.GetP(); ref != nil {
			pos := int(ref.Pos)
			binding, found := state.bindingForPosition(ref.Pos)
			if !found || pos < 0 || pos >= len(state.values) ||
				(binding.Type.Oid != types.T_any && !binding.Type.Oid.IsMySQLString()) {
				continue
			}
			if param, ok := state.values[pos].(ParamValue); ok && param.EnableNumericPrefix {
				markers = append(markers, source)
			}
			continue
		}
		if results := preparedCommonValueResultArgs(source); results != nil {
			peers, allowed := preparedCommonValueDomain(ctx, results)
			if allowed && len(peers) == 0 {
				childMarkers, _ := preparedCommonValueMarkers(ctx, results)
				markers = append(markers, childMarkers...)
				nested = nested || len(childMarkers) != 0
			}
		}
	}
	return markers, nested
}

// Bind one complete inference region before rebuilding its overloads. Runtime
// marker scales may be rounded by the existing policy, unlike fixed peer scales.
// Only executable casts are inherited; witnesses never become extra operands.
func bindPreparedCommonValueResultArguments(ctx context.Context, args []*Expr, inherited map[int32]*Expr) ([]*Expr, error) {
	state := preparedBindingState(ctx)
	if state == nil {
		return args, nil
	}
	applyMarkers := inherited != nil
	if inherited == nil {
		// A root operand's concrete string spelling owns the entire region.
		// Check before inferring any child: leaving only the root unconverted
		// would still erase child spellings or raise a speculative overflow.
		for _, arg := range args {
			source := preparedCommonValueSource(arg)
			if source != nil && source.GetP() != nil &&
				preparedConcreteStringCommonValueBoundary(int(source.GetP().Pos), state.values) {
				preparedConfigurationValue(ctx, source)
				return args, nil
			}
		}
		peers, allowed := preparedCommonValueDomain(ctx, args)
		if !allowed {
			return args, nil
		}
		hasDecimal := false
		for _, peer := range peers {
			hasDecimal = hasDecimal || types.T(peer.Typ.Id).IsDecimal()
		}
		if !hasDecimal {
			return args, nil
		}
		markers, nested := preparedCommonValueMarkers(ctx, args)
		if !nested {
			// Direct operands keep the existing spelling-sensitive inference
			// gate (for example, a spelling with no numeric prefix stays text).
			return args, nil
		}
		var err error
		inherited, err = bindPreparedCommonValueContextMarkers(ctx, markers, peers)
		if err != nil {
			return nil, err
		}
		if len(inherited) == 0 {
			return args, nil
		}
	}
	bound := append([]*Expr(nil), args...)
	for i, arg := range args {
		source := preparedCommonValueSource(arg)
		if ref := source.GetP(); ref != nil {
			if converted, ok := inherited[ref.Pos]; ok && applyMarkers {
				bound[i] = converted
			}
			continue
		}
		results := preparedCommonValueResultArgs(source)
		if results == nil {
			continue
		}
		childPeers, childAllowed := preparedCommonValueDomain(ctx, results)
		if !childAllowed || len(childPeers) != 0 {
			continue
		}
		childArgs, err := bindPreparedCommonValueResultArguments(ctx, results, inherited)
		if err != nil {
			return nil, err
		}
		fn := source.GetF()
		name := fn.Func.ObjName
		if source.GetPreparedNumeric().GetIfnullCommonValue() {
			childArgs = []*Expr{fn.Args[0], childArgs[1], childArgs[0]}
		}
		child, err := BindFuncExprImplByPlanExpr(ctx, name, childArgs)
		if err != nil {
			return nil, err
		}
		if source.GetPreparedNumeric().GetIfnullCommonValue() {
			ensurePreparedNumericMetadata(child).IfnullCommonValue = true
		}
		bound[i] = child
	}
	return bound, nil
}

func bindPreparedCommonValueContextMarkers(ctx context.Context, markers, peers []*Expr) (map[int32]*Expr, error) {
	if len(markers) == 0 {
		return nil, nil
	}
	args := append(append([]*Expr(nil), markers...), peers...)
	witnesses := append([]*Expr(nil), args...)
	prefixArgs := make([]bool, len(args))
	prefixKinds := make([]types.StringConversionKind, len(args))
	state := preparedBindingState(ctx)
	for i, source := range markers {
		param := state.values[source.GetP().Pos].(ParamValue)
		value, present := preparedConfigurationValue(ctx, source)
		if !present {
			return nil, nil
		}
		witnesses[i] = makePlan2StringConstExprWithType(fmt.Sprint(value))
		if value == nil {
			witnesses[i] = makePlan2NullConstExprWithType()
		}
		prefixArgs[i], prefixKinds[i] = true, param.PrepareParamKind
	}
	converted, _, err := preparedNumericPrefixArgs(ctx, "coalesce", args, witnesses,
		prefixArgs, prefixKinds, nil, nil, true)
	if err != nil {
		return nil, err
	}
	bound := make(map[int32]*Expr, len(markers))
	for i, marker := range markers {
		bound[marker.GetP().Pos] = converted[i]
	}
	return bound, nil
}
