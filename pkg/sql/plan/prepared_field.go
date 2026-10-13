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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type preparedFieldStringKind uint8

const (
	fieldStringOther preparedFieldStringKind = iota
	fieldStringMarker
	fieldStringNull
	fieldStringText
	fieldStringBinary
)

type preparedFieldStringDomain struct {
	kind      preparedFieldStringKind
	parameter bool
	// Only a bare marker (through implicit casts) gets the materialized
	// parameter declaration. A parameter-dependent function is not a marker.
	directParameter bool
	history         bool
	resolved        types.Type
}

// This resolver runs while logical binding tags still identify their owners.
// Its graph references live only for this binding. The plan retains explicit
// conversion boundaries; the prepared handle retains only immutable type facts.
type preparedFieldStringResolver struct {
	binder               *baseBinder
	memo                 map[*Expr]preparedFieldStringDomain
	outputs              map[[3]int32]bool
	unresolvedParameters bool
	parameterTypes       map[int32]types.Type
	defaultMarker        preparedFieldStringKind
}

func newPreparedFieldStringResolver(b *baseBinder) *preparedFieldStringResolver {
	return &preparedFieldStringResolver{binder: b, memo: make(map[*Expr]preparedFieldStringDomain), outputs: make(map[[3]int32]bool)}
}

func fieldStringStaticDomain(expr *Expr) preparedFieldStringDomain {
	if expr == nil {
		return preparedFieldStringDomain{}
	}
	if !types.T(expr.Typ.Id).IsMySQLString() {
		return preparedFieldStringDomain{resolved: makeTypeByPlan2Expr(expr)}
	}
	kind := fieldStringText
	if types.StaticStringDomain(makeTypeByPlan2Expr(expr)) == types.StringDomainBinary {
		kind = fieldStringBinary
	}
	return preparedFieldStringDomain{kind: kind}
}

func (r *preparedFieldStringResolver) expr(expr *Expr) (result preparedFieldStringDomain) {
	if expr == nil {
		return preparedFieldStringDomain{}
	}
	if result, ok := r.memo[expr]; ok {
		return result
	}
	result = fieldStringStaticDomain(expr)
	r.memo[expr] = result
	defer func() { r.memo[expr] = result }()
	if state := preparedBindingState(r.binder.GetContext()); state != nil {
		if state.fieldTypes != nil && !r.unresolvedParameters && r.parameterTypes == nil {
			if producer, ok := state.fieldTypes.producers[expr]; ok {
				result = state.fieldTypes.resolve(r, expr, producer)
				return result
			}
		}
		if source, ok := state.fieldReturnSources[expr]; ok {
			// NULLIF owns its first operand's domain, but that domain must be
			// resolved in this pass's parameter environment, not the earlier bind.
			result = r.expr(source.value)
			if result.kind == fieldStringMarker || result.kind == fieldStringNull && result.directParameter {
				// A NULL value does not erase NULLIF's bare operand declaration.
				// A composite operand still owns its own (possibly NULL) domain.
				result.kind = fieldStringText
				if r.expr(source.peer).kind == fieldStringBinary {
					result.kind = fieldStringBinary
				}
			}
			result.directParameter = false
			return result
		}
	}
	if param := expr.GetP(); param != nil {
		result.directParameter = true
		result.parameter = true
		if typ, ok := r.parameterTypes[param.Pos]; ok {
			result = fieldStringStaticDomain(&Expr{Typ: makePlan2Type(&typ)})
			result.resolved = typ
			if typ.Oid == types.T_any {
				result.kind = fieldStringNull
			}
			result.parameter = true
			result.directParameter = true
			return result
		}
		if r.unresolvedParameters || types.T(expr.Typ.Id).IsMySQLString() || expr.Typ.Id == int32(types.T_any) {
			result = preparedFieldStringDomain{kind: fieldStringMarker, parameter: true, directParameter: true}
		}
		return result
	}
	if isExplicitPreparedCast(expr) {
		return result
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Src != nil && isExplicitPreparedCast(lit.Src) {
			return result
		}
		if lit.Isnull {
			result.kind = fieldStringNull
		}
		return result
	}
	if col := expr.GetCol(); col != nil {
		if source := r.binder.pendingColumnSource(col); source != nil {
			result = r.expr(source)
		} else if r.binder.builder != nil {
			if node, ok := r.binder.builder.tag2NodeID[col.RelPos]; ok {
				result = r.output(node, col.RelPos, col.ColPos, result)
			}
		}
		result = materializedFieldStringDomain(result)
		return result
	}
	if sub := expr.GetSub(); sub != nil && sub.Typ == planpb.SubqueryRef_SCALAR {
		result = r.output(sub.NodeId, -1, 0, result)
		result = materializedFieldStringDomain(result)
		return result
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return result
	}
	name := strings.ToLower(fn.Func.ObjName)
	if name == "cast" && len(fn.Args) > 0 {
		result = r.expr(fn.Args[0])
		return result
	}
	// Dependency is independent of return-domain inference. Walk every bound
	// argument, including selectors/conditions, even for functions with a fixed
	// result type. Do not assign their parameter contracts the return type.
	defer func() {
		result.directParameter = false
		for _, arg := range fn.Args {
			result.parameter = r.expr(arg).parameter || result.parameter
		}
	}()
	var values []*Expr
	switch name {
	case "case":
		for i := 1; i < len(fn.Args); i += 2 {
			values = append(values, fn.Args[i])
		}
		if len(fn.Args)%2 == 1 {
			values = append(values, fn.Args[len(fn.Args)-1])
		}
	case "if", "iff":
		values = fn.Args[1:]
	case "coalesce", "greatest", "least":
		values = fn.Args
	case "concat", "concat_ws":
		// These functions always return strings. Every string contributor can
		// supply a binary domain; numeric and NULL arguments retain TEXT as the
		// default declaration. Keep parameter dependencies for enclosing CASEs.
		previous := r.defaultMarker
		r.defaultMarker = fieldStringText
		result = r.common(fn.Args)
		r.defaultMarker = previous
		result.resolved = types.Type{}
		if result.kind != fieldStringBinary {
			result.kind = fieldStringText
		}
		return result
	case "lag", "lead":
		if len(fn.Args) > 0 {
			values = []*Expr{fn.Args[0]}
			if len(fn.Args) > 2 {
				values = append(values, fn.Args[2])
			}
		}
	case "min", "max", "any_value", "first_value", "last_value", "nth_value":
		if len(fn.Args) > 0 {
			previous := r.defaultMarker
			r.defaultMarker = fieldStringBinary
			result = r.expr(fn.Args[0])
			r.defaultMarker = previous
			return result
		}
	default:
		if pos := preparedStringDomainSourceIndex(name, len(fn.Args)); pos >= 0 {
			result = r.expr(fn.Args[pos])
			result.resolved = types.Type{}
			if result.kind != fieldStringBinary {
				result.kind = fieldStringText
			}
			return result
		}
		// Use the registered return-type contract for other functions rather
		// than a FIELD-specific function whitelist or this execution's payload
		// type. Unresolved markers have their declaration, not their wire type.
		if r.unresolvedParameters || r.parameterTypes != nil {
			inputs := make([]types.Type, len(fn.Args))
			dependent := false
			for i, arg := range fn.Args {
				domain := r.expr(arg)
				dependent = dependent || domain.parameter
				inputs[i] = fieldDomainType(domain, makeTypeByPlan2Expr(arg))
			}
			if dependent {
				if resolved, err := function.GetFunctionByName(r.binder.GetContext(), name, inputs); err == nil {
					typ := resolved.GetReturnType()
					result = fieldStringStaticDomain(&Expr{Typ: makePlan2Type(&typ)})
				}
			}
		}
	}
	if len(values) != 0 {
		result = r.common(values)
	}
	return result
}

func materializedFieldStringDomain(domain preparedFieldStringDomain) preparedFieldStringDomain {
	if domain.directParameter && (domain.kind == fieldStringMarker || domain.kind == fieldStringNull) {
		domain.kind = fieldStringText
	}
	domain.directParameter = false
	return domain
}

func (r *preparedFieldStringResolver) common(args []*Expr) preparedFieldStringDomain {
	domains := make([]preparedFieldStringDomain, len(args))
	peer := preparedFieldStringDomain{kind: fieldStringMarker}
	result := preparedFieldStringDomain{kind: fieldStringNull}
	for i, arg := range args {
		domains[i] = r.expr(arg)
		result.parameter = result.parameter || domains[i].parameter
		result.history = result.history || domains[i].history
		if peer.kind == fieldStringMarker && domains[i].kind != fieldStringMarker {
			peer = domains[i]
		}
	}
	if peer.kind == fieldStringMarker {
		peer.kind = r.defaultMarker
		if peer.kind == fieldStringOther {
			peer.kind = fieldStringText
		}
	}
	if peer.kind == fieldStringNull {
		peer.kind = fieldStringBinary
		peer.resolved = types.Type{}
	}
	var numeric []types.Type
	for _, domain := range domains {
		if domain.kind == fieldStringMarker {
			domain = peer
		}
		switch domain.kind {
		case fieldStringNull:
			continue
		case fieldStringOther:
			if domain.resolved.Oid != types.T_any {
				numeric = append(numeric, domain.resolved)
			}
		default:
			if result.kind == fieldStringNull || domain.kind == fieldStringBinary {
				result.kind = domain.kind
			}
		}
	}
	if result.kind != fieldStringNull || len(numeric) == 0 {
		return result
	}
	result.kind = fieldStringOther
	result.resolved = numeric[0]
	if len(numeric) > 1 {
		if common, err := function.GetFunctionByName(r.binder.GetContext(), "coalesce", numeric); err == nil {
			result.resolved = common.GetReturnType()
		}
	}
	return result
}

func (r *preparedFieldStringResolver) output(nodeID, tag, pos int32, fallback preparedFieldStringDomain) preparedFieldStringDomain {
	if r.binder.builder == nil || nodeID < 0 || int(nodeID) >= len(r.binder.builder.qry.Nodes) || pos < 0 {
		return fallback
	}
	key := [3]int32{nodeID, tag, pos}
	if r.outputs[key] {
		return fallback
	}
	r.outputs[key] = true
	defer delete(r.outputs, key)
	node := r.binder.builder.qry.Nodes[nodeID]
	if node == nil {
		return fallback
	}
	if isPreparedSetOperationNode(node.NodeType) {
		result := preparedFieldStringDomain{kind: fieldStringNull}
		for _, child := range node.Children {
			domain := r.output(child, -1, pos, fallback)
			if domain.kind == fieldStringMarker {
				domain.kind = fieldStringText
			}
			result.parameter = result.parameter || domain.parameter
			result.history = result.history || domain.history
			if domain.kind == fieldStringOther {
				return fallback
			}
			if domain.kind != fieldStringNull && (result.kind == fieldStringNull || domain.kind == fieldStringBinary) {
				result.kind = domain.kind
			}
		}
		return result
	}
	values := node.ProjectList
	switch node.NodeType {
	case planpb.Node_AGG:
		if len(node.BindingTags) > 0 && node.BindingTags[0] == tag {
			values = node.GroupBy
		}
		if len(node.BindingTags) > 1 && node.BindingTags[1] == tag {
			values = node.AggList
		}
	case planpb.Node_WINDOW:
		if node.WindowIdx == pos && len(node.WinSpecList) > 0 {
			return r.expr(node.WinSpecList[0].GetW().GetWindowFunc())
		}
		if len(node.Children) == 1 {
			return r.output(node.Children[0], tag, pos, fallback)
		}
	case planpb.Node_PARTITION:
		if len(node.Children) == 1 {
			return r.output(node.Children[0], tag, pos, fallback)
		}
	}
	if int(pos) < len(values) {
		return r.expr(values[pos])
	}
	return fallback
}

func (b *baseBinder) bindFieldStringArguments(args []*Expr) ([]*Expr, error) {
	if len(args) < 2 {
		return args, nil
	}
	r := newPreparedFieldStringResolver(b)
	domains := make([]preparedFieldStringDomain, len(args))
	inputs := make([]types.Type, len(args))
	retained := false
	allStrings := true
	for i, arg := range args {
		domains[i] = r.expr(arg)
		inputs[i] = makeTypeByPlan2Expr(arg)
		if domains[i].history {
			retained = true
			inputs[i] = fieldDomainType(domains[i], inputs[i])
		}
		if inputs[i].Oid != types.T_any && !inputs[i].Oid.IsMySQLString() {
			allStrings = false
		}
	}
	domain := domains[0]
	if retained && !allStrings {
		resolved, err := function.GetFunctionByName(b.GetContext(), "field", inputs)
		if err != nil {
			return nil, err
		}
		casts, needed := resolved.ShouldDoImplicitTypeCast()
		fallback := inputs[0]
		for _, typ := range inputs {
			if typ.Oid != types.T_any {
				fallback = typ
				break
			}
		}
		result := append([]*Expr(nil), args...)
		for i, d := range domains {
			if !d.history {
				continue
			}
			target := inputs[i]
			if needed {
				target = casts[i]
			} else if target.Oid == types.T_any {
				target = fallback
			}
			result[i], err = b.fieldNumericValue(args[i], target)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	for _, arg := range args {
		if arg.Typ.Id != int32(types.T_any) && !types.T(arg.Typ.Id).IsMySQLString() {
			return args, nil
		}
	}
	if domain.kind == fieldStringMarker {
		domain.kind = fieldStringText
		for _, arg := range args[1:] {
			peer := r.expr(arg)
			if peer.kind == fieldStringMarker {
				continue
			}
			domain.kind = peer.kind
			if domain.kind == fieldStringNull {
				domain.kind = fieldStringBinary
			}
			break
		}
	}
	if domain.kind == fieldStringNull {
		domain.kind = fieldStringBinary
	}
	if domain.kind != fieldStringText && domain.kind != fieldStringBinary {
		return args, nil
	}
	if fieldStringStaticDomain(args[0]).kind == domain.kind {
		return args, nil
	}
	target := types.T_text.ToType()
	target.Charset = types.CharsetUTF8
	if domain.kind == fieldStringBinary {
		target = types.T_blob.ToType()
	}
	converted, err := makePlan2CastExpr(b.GetContext(), args[0], makePlan2Type(&target))
	if err != nil {
		return nil, err
	}
	result := append([]*Expr(nil), args...)
	result[0] = converted
	return result, nil
}

type preparedFieldReturnSource struct {
	value *Expr
	peer  *Expr
}

// NULLIF is lowered to CASE, but only its first operand owns the returned
// domain. Keep its sources for this binding only so initial resolution and
// statement-wide reprepare use the same rule with their own parameter types.
func (b *baseBinder) rememberPreparedFieldNullIfDomain(expr *Expr, args []*Expr) {
	state := preparedBindingState(b.GetContext())
	if state == nil || len(args) != 3 || !types.T(expr.Typ.Id).IsMySQLString() {
		return
	}
	source := preparedFieldReturnSource{value: args[2]}
	if cmp := args[0].GetF(); cmp != nil && len(cmp.Args) == 2 {
		source.peer = cmp.Args[1]
	}
	if state.fieldReturnSources == nil {
		state.fieldReturnSources = make(map[*Expr]preparedFieldReturnSource)
	}
	state.fieldReturnSources[expr] = source
}
