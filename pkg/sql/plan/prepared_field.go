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
	// A bare marker (through implicit casts) is distinct from general
	// parameter dependence and a transparent wrapper's declaration.
	directParameter bool
	// A transparent value wrapper is not a bare marker, but relational and
	// function consumers still use its operand's parameter declaration.
	parameterDeclaration bool
	// NULL/unspecified-marker peers can infer BLOB without a declared binary
	// contributor. Value wrappers preserve it; CONCAT_WS does not consume it
	// as an explicit binary declaration (unlike CAST(NULL AS BINARY)).
	inferredBinary bool
	history        bool
	resolved       types.Type
}

// This resolver runs while logical binding tags still identify their owners.
// Its graph references live only for this binding. The plan retains explicit
// conversion boundaries; the prepared handle retains only immutable type facts.
type preparedFieldStringResolver struct {
	binder               *baseBinder
	memo                 map[*Expr]preparedFieldStringDomain
	outputs              map[[3]int32]bool
	unresolvedParameters bool
	// String consumers preview declarations without acquiring FIELD history.
	readOnly       bool
	parameterTypes map[int32]types.Type
	defaultMarker  preparedFieldStringKind
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

// Internal conversions can change representation without owning a declaration.
// Folded constants retain that conversion in Src; SQL-authored casts still win.
func preparedFieldPrivateCastSource(expr *Expr, overload int32) *Expr {
	if expr == nil {
		return nil
	}
	if lit := expr.GetLit(); lit != nil {
		expr = lit.Src
	}
	if !isCastOverload(expr, overload) || expr.GetF().GetSyntaxExplicitCast() || len(expr.GetF().Args) == 0 {
		return nil
	}
	return expr.GetF().Args[0]
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
			if result.kind == fieldStringMarker || result.kind == fieldStringNull && (result.directParameter || result.parameterDeclaration) {
				// A NULL value does not erase NULLIF's bare operand declaration.
				// A composite operand still owns its own (possibly NULL) domain.
				result.kind = fieldStringText
				peer := r.expr(source.peer)
				if peer.kind == fieldStringBinary {
					result.kind = fieldStringBinary
					result.inferredBinary = peer.inferredBinary
				} else if peer.kind == fieldStringOther && peer.resolved.Oid != types.T_any {
					// NULLIF's comparison also supplies a bare marker's numeric
					// declaration. A composite first operand keeps its own domain.
					result.kind, result.resolved = peer.kind, peer.resolved
					if typ, ok := preparedNumericReprepareType(peer.resolved); ok {
						result.resolved = typ
					}
				}
			}
			result.directParameter = false
			result.parameterDeclaration = false
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
	// CAST3 implements set-operation representation, not an explicit SQL
	// declaration. Merge branch sources before consuming the set output.
	if source := preparedFieldPrivateCastSource(expr, 3); source != nil {
		return r.expr(source)
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
			// A pending tag in this binding is not a relational projection.
			// Transparent value producers retain their value domain here;
			// an outer column/subquery consumer owns materialization instead.
			if result.parameterDeclaration {
				return result
			}
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
		// Retained/provisional declarations were resolved above. A concrete
		// binary CASE without such history owns its logical declaration, not
		// necessarily its common transport. Inferring bare markers again would
		// manufacture an inferred origin.
		// Lowered IFNULL instead merges its operands' declarations, including
		// restored contextual BLOBs, just like the other common-value consumers.
		if !r.unresolvedParameters && r.parameterTypes == nil && result.kind == fieldStringBinary &&
			!expr.GetPreparedNumeric().GetIfnullCommonValue() {
			return r.conditionalStringDeclaration(expr, result)
		}
		for i := 1; i < len(fn.Args); i += 2 {
			values = append(values, fn.Args[i])
		}
		if len(fn.Args)%2 == 1 {
			values = append(values, fn.Args[len(fn.Args)-1])
		}
	case "if", "iff":
		if result.kind == fieldStringBinary {
			declaration := r.conditionalStringDeclaration(expr, result)
			if declaration.kind == fieldStringText {
				return declaration
			}
		}
		values = fn.Args[1:]
	case "coalesce":
		values = fn.Args
	case "greatest", "least":
		// Comparison consumers do not turn a NULL-inferred BLOB into a real
		// binary contributor. Ignore their private string comparison casts,
		// but retain numeric conversions and SQL-authored declarations.
		domains := make([]preparedFieldStringDomain, len(fn.Args))
		for i, arg := range fn.Args {
			for arg != nil && types.T(arg.Typ.Id).IsMySQLString() {
				source := preparedFieldPrivateCastSource(arg, 2)
				if source == nil {
					break
				}
				arg = source
			}
			domains[i] = r.expr(arg)
			if domains[i].inferredBinary {
				domains[i].kind = fieldStringText
				domains[i].inferredBinary = false
				domains[i].resolved = types.Type{}
			}
		}
		return r.commonDomains(domains)
	case "concat_ws":
		// NULL-derived BLOB domains are not binary contributors, even through
		// value wrappers or in the separator. Explicit binary declarations remain.
		previous := r.defaultMarker
		r.defaultMarker = fieldStringText
		domains := make([]preparedFieldStringDomain, len(fn.Args))
		for i, arg := range fn.Args {
			domains[i] = r.expr(arg)
			if domains[i].inferredBinary {
				domains[i].kind = fieldStringNull
			}
		}
		result = r.commonDomains(domains)
		result.inferredBinary = false
		r.defaultMarker = previous
		result.resolved = types.Type{}
		if result.kind != fieldStringBinary {
			result.kind = fieldStringText
		}
		return result
	case "concat":
		// String consumers combine declarations, not first-execution payloads.
		// Literal NULL stays neutral; a NULL composite can declare BLOB.
		previous := r.defaultMarker
		r.defaultMarker = fieldStringText
		domains := make([]preparedFieldStringDomain, len(fn.Args))
		for i, arg := range fn.Args {
			domains[i] = fieldStringArgumentDeclaration(r.expr(arg))
		}
		result = r.commonDomains(domains)
		r.defaultMarker = previous
		result.resolved = types.Type{}
		if result.kind != fieldStringBinary {
			result.kind = fieldStringText
		}
		return result
	case "lag", "lead":
		if len(fn.Args) > 0 {
			if len(fn.Args) > 2 {
				value, fallback := r.expr(fn.Args[0]), r.expr(fn.Args[2])
				// Two parameter declarations share their default text contract.
				// A compound operand or literal NULL instead supplies its own peer
				// domain; materializing either marker first would erase that peer.
				if (value.directParameter || value.parameterDeclaration) && (fallback.directParameter || fallback.parameterDeclaration) {
					value = materializedFieldStringDomain(value)
					fallback = materializedFieldStringDomain(fallback)
				}
				return r.commonDomains([]preparedFieldStringDomain{value, fallback})
			}
			return materializedFieldStringDomain(r.expr(fn.Args[0]))
		}
	case "any_value":
		if len(fn.Args) > 0 {
			previous := r.defaultMarker
			r.defaultMarker = fieldStringBinary
			result = r.expr(fn.Args[0])
			r.defaultMarker = previous
			result.parameterDeclaration = result.directParameter || result.parameterDeclaration
			return result
		}
	case "min", "max", "first_value", "last_value", "nth_value":
		if len(fn.Args) > 0 {
			previous := r.defaultMarker
			r.defaultMarker = fieldStringBinary
			result = r.expr(fn.Args[0])
			r.defaultMarker = previous
			// Aggregate/window outputs retain a bare operand's declaration,
			// not its first NULL value. Composite operands own their own domain.
			result = materializedFieldStringDomain(result)
			return result
		}
	default:
		if pos, _ := preparedStringDomainSourceIndex(name, len(fn.Args)); pos >= 0 {
			domain := fieldStringArgumentDeclaration(r.expr(fn.Args[pos]))
			result = domain
			result.parameterDeclaration = false
			result.resolved = types.Type{}
			if result.kind != fieldStringBinary {
				result.kind = fieldStringText
			}
			return result
		}
		// Use the registered return-type contract for other functions rather
		// than a FIELD-specific function whitelist or this execution's payload
		// type. Unresolved markers have their declaration, not their wire type.
		resolveReturn := r.unresolvedParameters || r.parameterTypes != nil
		if !resolveReturn {
			// Restored/relational declarations may differ from their physical
			// comparison/set representation, even without runtime parameters.
			for _, arg := range fn.Args {
				domain := fieldStringArgumentDeclaration(r.expr(arg))
				differentStringDeclaration := (domain.kind == fieldStringText || domain.kind == fieldStringBinary) && domain.kind != fieldStringStaticDomain(arg).kind
				if domain.inferredBinary || differentStringDeclaration {
					resolveReturn = true
					break
				}
			}
		}
		if resolveReturn {
			inputs := make([]types.Type, len(fn.Args))
			var declaredInputs []types.Type
			dependent := false
			for i, arg := range fn.Args {
				domain := fieldStringArgumentDeclaration(r.expr(arg))
				dependent = dependent || domain.parameter
				inputs[i] = fieldDomainType(domain, makeTypeByPlan2Expr(arg))
				if declaredInputs != nil {
					declaredInputs[i] = inputs[i]
				}
				if domain.inferredBinary {
					if declaredInputs == nil {
						declaredInputs = append([]types.Type(nil), inputs...)
					}
					declaredInputs[i] = types.T_text.ToType()
				}
			}
			if dependent || !r.unresolvedParameters && r.parameterTypes == nil {
				if resolved, err := function.GetFunctionByName(r.binder.GetContext(), name, inputs); err == nil {
					typ := resolved.GetReturnType()
					result = fieldStringStaticDomain(&Expr{Typ: makePlan2Type(&typ)})
					if result.kind == fieldStringBinary && declaredInputs != nil {
						// Ask the same registered contract whether BINARY came only
						// from inferred inputs. Intrinsically binary functions (e.g.
						// UNHEX) and real binary peers remain explicit contributors.
						if declared, err := function.GetFunctionByName(r.binder.GetContext(), name, declaredInputs); err == nil {
							typ := declared.GetReturnType()
							result.inferredBinary = typ.Oid.IsMySQLString() && types.StaticStringDomain(typ) != types.StringDomainBinary
						}
					}
				}
			}
		}
	}
	if len(values) != 0 {
		result = r.common(values)
	}
	return result
}

// Both ordinary binding and history-free prepared producers consume the same
// logical declaration. Retained history and unresolved parameter inference keep
// their own owners; a common binary transport is not a competing declaration.
func (r *preparedFieldStringResolver) conditionalStringDeclaration(expr *Expr, domain preparedFieldStringDomain) preparedFieldStringDomain {
	if !r.unresolvedParameters && r.parameterTypes == nil && domain.kind == fieldStringBinary {
		declaration := r.collectStringDeclaration(expr)
		if declaration.hasStrongerTextDeclaration() {
			domain.kind = fieldStringText
		}
	}
	return domain
}

func materializedFieldStringDomain(domain preparedFieldStringDomain) preparedFieldStringDomain {
	if (domain.directParameter || domain.parameterDeclaration) && (domain.kind == fieldStringMarker || domain.kind == fieldStringNull) {
		domain.kind = fieldStringText
	}
	domain.directParameter = false
	domain.parameterDeclaration = false
	return domain
}

// Function arguments consume a transparent wrapper's parameter declaration,
// and a NULL composite's BLOB declaration. A bare NULL marker remains available
// for contextual inference (including numeric peers); literal NULL is neutral.
func fieldStringArgumentDeclaration(domain preparedFieldStringDomain) preparedFieldStringDomain {
	if domain.parameterDeclaration {
		domain = materializedFieldStringDomain(domain)
	}
	if domain.kind == fieldStringNull && domain.parameter && !domain.directParameter {
		domain.inferredBinary = true
		domain.kind = fieldStringBinary
	}
	return domain
}

func (r *preparedFieldStringResolver) common(args []*Expr) preparedFieldStringDomain {
	domains := make([]preparedFieldStringDomain, len(args))
	for i, arg := range args {
		domains[i] = r.expr(arg)
	}
	return r.commonDomains(domains)
}

func (r *preparedFieldStringResolver) commonDomains(domains []preparedFieldStringDomain) preparedFieldStringDomain {
	peer := preparedFieldStringDomain{kind: fieldStringMarker}
	result := preparedFieldStringDomain{kind: fieldStringNull}
	for _, domain := range domains {
		result.parameter = result.parameter || domain.parameter
		result.history = result.history || domain.history
		if peer.kind == fieldStringMarker && domain.kind != fieldStringMarker {
			peer = domain
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
		peer.inferredBinary = true
		peer.resolved = types.Type{}
	}
	var numeric []types.Type
	inferredBinary, declaredBinary, textPeer := false, false, false
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
			textPeer = textPeer || domain.kind == fieldStringText
			if domain.kind == fieldStringBinary {
				inferredBinary = inferredBinary || domain.inferredBinary
				declaredBinary = declaredBinary || !domain.inferredBinary
			}
			if result.kind == fieldStringNull || domain.kind == fieldStringBinary {
				result.kind = domain.kind
			}
		}
	}
	result.inferredBinary = inferredBinary && !declaredBinary
	if textPeer && result.inferredBinary {
		// Contextual BLOB inference is not a binary declaration competing
		// with a text peer. Apply this at every common-value consumer, not
		// only set outputs, without changing any retained producer fact.
		result.kind, result.inferredBinary = fieldStringText, false
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
		domains := make([]preparedFieldStringDomain, 0, len(node.Children))
		for _, child := range node.Children {
			// UNION consumes materialized branch declarations before merging
			// them, just as scalar and column references consume projections.
			domain := materializedFieldStringDomain(r.output(child, -1, pos, fallback))
			if domain.kind == fieldStringOther {
				return fallback
			}
			domains = append(domains, domain)
		}
		return r.commonDomains(domains)
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
	case planpb.Node_PARTITION, planpb.Node_FILTER, planpb.Node_SORT:
		// These nodes preserve their child's output declarations. Their
		// binding-time ProjectList need not expose that output yet.
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
// The bound payload type (including NULL's aggregate placeholder type) cannot
// decide whether provenance exists; numeric operands retain their numeric domain.
func (b *baseBinder) rememberPreparedFieldNullIfDomain(expr *Expr, args []*Expr) {
	if len(args) != 3 {
		return
	}
	source := preparedFieldReturnSource{value: args[2]}
	if domains := possibleStringDomainsForExpr(source.value); domains != 0 &&
		!b.stringSourceHasDeclarationBoundary(source.value, nil, nil) && !preparedStringResultOwnsDeclaration(source.value) {
		// Ordinary consumers need the SQL value owner too, even without a
		// prepared binding state. Publish only a flat, immutable source proof;
		// retain frozen P/V identities independently of type dependence.
		resolver := newPreparedFieldStringResolver(b)
		resolver.readOnly = true
		collector := resolver.collectStringDeclaration(source.value)
		ensurePreparedNumericMetadata(expr).StringDomainSource = collector.witness(source.value, domains)
	}
	if cmp := args[0].GetF(); cmp != nil && len(cmp.Args) == 2 {
		source.peer = cmp.Args[1]
	}
	b.rememberPreparedFieldReturnSource(expr, source)
}

// Declaration normalization owns runtime representation but retains its logical
// source for dependency/history resolution. Reuse the binding-local return map.
func (b *baseBinder) rememberPreparedFieldReturnSource(expr *Expr, source preparedFieldReturnSource) {
	state := preparedBindingState(b.GetContext())
	if state == nil {
		return
	}
	if state.fieldReturnSources == nil {
		state.fieldReturnSources = make(map[*Expr]preparedFieldReturnSource)
	}
	state.fieldReturnSources[expr] = source
}
