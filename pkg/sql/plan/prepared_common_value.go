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

// nil peer means unresolved; allowed=false means a concrete type boundary.
// A plain NULL is a boundary only when it is the first non-marker operand.
func preparedCommonValueDomain(ctx context.Context, args []*Expr) (peer *Expr, allowed bool) {
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
			childPeer, childAllowed := preparedCommonValueDomain(ctx, results)
			if !childAllowed {
				return nil, false
			}
			if childPeer == nil {
				continue
			}
			// A resolved child owns its result domain, rather than inheriting
			// the enclosing call's domain a second time.
			if !types.T(source.Typ.Id).ToType().IsNumeric() {
				return nil, false
			}
		}
		if source.GetLit() != nil && source.GetLit().Isnull && source.Typ.Id == int32(types.T_any) {
			if peer == nil {
				return nil, false
			}
			continue
		}
		oid := types.T(source.Typ.Id)
		if oid.IsFloat() || !oid.ToType().IsNumeric() {
			return nil, false
		}
		if peer == nil || oid.IsDecimal() {
			peer = source
		}
	}
	return peer, allowed
}

// Run before the owning common-value overload. Only unresolved result
// children inherit a peer; parameter conversion and precision/overflow policy
// remain in the existing fixed-DECIMAL numeric-prefix binder.
func bindPreparedCommonValueResultArguments(ctx context.Context, args []*Expr, inherited *Expr) ([]*Expr, error) {
	if preparedBindingState(ctx) == nil {
		return args, nil
	}
	peer, allowed := preparedCommonValueDomain(ctx, args)
	if !allowed {
		return args, nil
	}
	if peer == nil {
		peer = inherited
	}
	if peer == nil || !types.T(peer.Typ.Id).IsDecimal() {
		return args, nil
	}
	bound := append([]*Expr(nil), args...)
	for i, arg := range args {
		source := preparedCommonValueSource(arg)
		results := preparedCommonValueResultArgs(source)
		if results == nil {
			if inherited != nil && source.GetP() != nil {
				// The peer is a type witness only. Never retain it as an extra
				// executable operand of the nested call.
				converted, err := bindPreparedCommonValueContextMarker(ctx, source, peer)
				if err != nil {
					return nil, err
				}
				bound[i] = converted
			}
			continue
		}
		childPeer, childAllowed := preparedCommonValueDomain(ctx, results)
		if !childAllowed || childPeer != nil {
			continue
		}
		childArgs, err := bindPreparedCommonValueResultArguments(ctx, results, peer)
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

func bindPreparedCommonValueContextMarker(ctx context.Context, source, peer *Expr) (*Expr, error) {
	state := preparedBindingState(ctx)
	pos := int(source.GetP().Pos)
	if pos < 0 || pos >= len(state.values) {
		return source, nil
	}
	param, ok := state.values[pos].(ParamValue)
	if !ok || !param.EnableNumericPrefix {
		return source, nil
	}
	value, present := preparedConfigurationValue(ctx, source)
	if !present {
		return source, nil
	}
	witness := makePlan2StringConstExprWithType(fmt.Sprint(value))
	if value == nil {
		witness = makePlan2NullConstExprWithType()
	}
	// The inherited peer already proves the conversion contract. In
	// particular, an invalid spelling must not turn this unresolved child
	// into a new string boundary. Keep the existing prefix/overflow policy.
	converted, _, err := preparedNumericPrefixArgs(ctx, "coalesce", []*Expr{source, peer}, []*Expr{witness, peer},
		[]bool{true, false}, []types.StringConversionKind{param.PrepareParamKind, 0}, nil, nil, true)
	if err != nil {
		return nil, err
	}
	return converted[0], nil
}
