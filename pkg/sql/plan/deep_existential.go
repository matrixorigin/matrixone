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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type existentialConsumer uint8

const (
	existentialIneligible existentialConsumer = iota
	existentialFilterTrue
	existentialNegatedFilter
)
const maxExistentialArms = 8

var constTrue = MakePlan2BoolConstExprWithType(true)

type pendingExistential struct{ sub *plan.SubqueryRef }

func (builder *QueryBuilder) existentialNYI() error {
	return moerr.NewNYI(builder.GetContext(), "deep correlated predicate containing inner columns cannot be pulled above mark join: unsupported existential region")
}

// Ownership lookup precedes the depth gate: the depth-one subquery is the
// consumer that completes a pending depth-two region. No old plan is mutated
// during admission, and no failed rewrite retries the legacy pullup machinery.
func (builder *QueryBuilder) tryDeepExistential(id int32, sub *plan.SubqueryRef, ctx *BindContext, consumer existentialConsumer) (int32, *plan.Expr, bool, error) {
	sc := builder.ctxByNode[sub.NodeId]
	if sc == nil {
		return 0, nil, false, nil
	}
	if pending := builder.pendingExistentials[sc.existentialBlock]; pending != nil {
		if builder.isForUpdate || consumer == existentialIneligible || (sub.Typ != plan.SubqueryRef_EXISTS && sub.Typ != plan.SubqueryRef_NOT_EXISTS && sub.Typ != plan.SubqueryRef_IN) {
			return 0, nil, true, builder.existentialNYI()
		}
		root, expr, err := builder.lowerDeepExistential(id, sub, pending, ctx)
		if err == nil {
			delete(builder.pendingExistentials, sc.existentialBlock)
		}
		return root, expr, true, err
	}
	if consumer != existentialFilterTrue || ctx == nil || sc.subqueryNestingDepth != 2 ||
		(sub.Typ != plan.SubqueryRef_EXISTS && sub.Typ != plan.SubqueryRef_IN) {
		return 0, nil, false, nil
	}
	if !builder.hasRejectedDeepExistential(sub.NodeId) {
		return 0, nil, false, nil
	}
	if builder.pendingExistentials == nil {
		builder.pendingExistentials = make(map[uint64]*pendingExistential)
	}
	if builder.pendingExistentials[ctx.existentialBlock] != nil {
		return 0, nil, true, builder.existentialNYI()
	}
	builder.pendingExistentials[ctx.existentialBlock] = &pendingExistential{sub: sub}
	builder.hadPendingExistentials = true
	typ := plan.Type{Id: int32(types.T_bool), NotNullable: sub.Typ == plan.SubqueryRef_EXISTS}
	return id, &plan.Expr{Typ: typ, Expr: &plan.Expr_Sub{Sub: sub}}, true, nil
}

// Descriptors reference immutable bound inputs. Only TABLE_SCAN plus transparent
// projection/filter nodes are admitted; dropping an aggregate/order/limit would
// change which witnesses exist. Views and derived boundaries are not transparent.
type existentialRelation struct {
	scan     *plan.Node
	tag      int32
	filters  []*plan.Expr
	projects map[[2]int32]*plan.Expr
}

func (builder *QueryBuilder) readExistentialRelation(id int32) (*existentialRelation, bool) {
	owner := builder.ctxByNode[id].queryBlockOwner
	r := &existentialRelation{projects: make(map[[2]int32]*plan.Expr)}
	for {
		n := builder.qry.Nodes[id]
		if builder.ctxByNode[id].queryBlockOwner != owner {
			return nil, false
		}
		if n.Limit != nil || n.Offset != nil || len(n.OrderBy) != 0 || len(n.LockTargets) != 0 || len(n.OriginViews) != 0 || n.DirectView != "" {
			return nil, false
		}
		r.filters = append(r.filters, n.FilterList...)
		switch n.NodeType {
		case plan.Node_PROJECT:
			if len(n.BindingTags) != 1 || len(n.Children) != 1 {
				return nil, false
			}
			for i, e := range n.ProjectList {
				if !isTruncationSafeRowExpr(e) {
					return nil, false
				}
				r.projects[[2]int32{n.BindingTags[0], int32(i)}] = e
			}
		case plan.Node_FILTER:
			if len(n.Children) != 1 || n.FilterIsBarrier {
				return nil, false
			}
		case plan.Node_TABLE_SCAN:
			if len(n.BindingTags) != 1 || len(n.Children) != 0 || n.TableDef == nil || n.TableDef.ViewSql != nil {
				return nil, false
			}
			r.scan, r.tag = n, n.BindingTags[0]
			return r, true
		default:
			return nil, false
		}
		id = n.Children[0]
	}
}

// Only expression forms admitted by the totality proof may be transformed.
// Corr depths are checked against their original SQL block before conversion.
func resolveExistentialExpr(e *plan.Expr, local *existentialRelation, parents []map[int32]bool) (*plan.Expr, bool) {
	if e == nil {
		return nil, false
	}
	e = DeepCopyExpr(e)
	var resolve func(*plan.Expr, int) bool
	resolve = func(e *plan.Expr, budget int) bool {
		if budget == 0 {
			return false
		}
		switch x := e.Expr.(type) {
		case *plan.Expr_Col:
			if p := local.projects[[2]int32{x.Col.RelPos, x.Col.ColPos}]; p != nil {
				*e = *DeepCopyExpr(p)
				return resolve(e, budget-1)
			}
			return x.Col.RelPos == local.tag
		case *plan.Expr_Corr:
			c := x.Corr
			if c.Depth < 1 || int(c.Depth) > len(parents) || !parents[c.Depth-1][c.RelPos] {
				return false
			}
			e.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: c.RelPos, ColPos: c.ColPos}}
			return true
		case *plan.Expr_F:
			for _, a := range x.F.Args {
				if !resolve(a, budget) {
					return false
				}
			}
			return true
		case *plan.Expr_Lit:
			return x.Lit.Src == nil
		case *plan.Expr_T:
			return true
		default:
			return false
		}
	}
	return e, resolve(e, len(local.projects)+1)
}

func existentialWalk(e *plan.Expr, visit func(*plan.Expr)) {
	if e == nil {
		return
	}
	visit(e)
	switch x := e.Expr.(type) {
	case *plan.Expr_F:
		for _, a := range x.F.Args {
			existentialWalk(a, visit)
		}
	case *plan.Expr_List:
		for _, a := range x.List.List {
			existentialWalk(a, visit)
		}
	case *plan.Expr_Lit:
		existentialWalk(x.Lit.Src, visit)
	case *plan.Expr_Sub:
		existentialWalk(x.Sub.Child, visit)
	case *plan.Expr_W:
		existentialWalk(x.W.WindowFunc, visit)
		for _, a := range x.W.PartitionBy {
			existentialWalk(a, visit)
		}
		for _, a := range x.W.OrderBy {
			existentialWalk(a.Expr, visit)
		}
		if x.W.Frame != nil {
			if x.W.Frame.Start != nil {
				existentialWalk(x.W.Frame.Start.Val, visit)
			}
			if x.W.Frame.End != nil {
				existentialWalk(x.W.Frame.End.Val, visit)
			}
		}
	}
}

// Split only explicit OR arms. AND-over-OR distribution is deliberately absent.
func existentialSplit(e *plan.Expr, name string, out *[]*plan.Expr, limit int) bool {
	if f := e.GetF(); f != nil && f.Func.ObjName == name && len(f.Args) == 2 {
		return existentialSplit(f.Args[0], name, out, limit) && existentialSplit(f.Args[1], name, out, limit)
	}
	*out = append(*out, e)
	return len(*out) <= limit
}

func existentialEquality(e *plan.Expr) bool {
	f := e.GetF()
	if f == nil || !IsEqualFunc(f.Func.Obj) || len(f.Args) != 2 {
		return false
	}
	a, c := f.Args[0], f.Args[1]
	if a.GetCol() == nil || c.GetCol() == nil || a.Typ.Id != c.Typ.Id || a.Typ.Width != c.Typ.Width || a.Typ.Scale != c.Typ.Scale {
		return false
	}
	switch types.T(a.Typ.Id) {
	case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	}
	return false
}

// Each arm has one retained anchor, a SEMI witness edge, and pure outer
// equijoin keys. A disconnected input is summarized rather than enumerated.
type existentialArm struct {
	anchor                int32
	local                 map[int32][]*plan.Expr
	witness, outer, gates []*plan.Expr
}

func existentialSides(e *plan.Expr, i, j int32) int {
	sides := 0
	existentialWalk(e, func(e *plan.Expr) {
		if c := e.GetCol(); c != nil {
			switch c.RelPos {
			case i:
				sides |= 1
			case j:
				sides |= 2
			default:
				sides |= 4
			}
		}
	})
	return sides
}

// Equality classes are scoped to one conjunction and include the resolved type.
// Original constraints survive substitution, including nullable x=x predicates.
type existentialColumn [5]int32

func existentialColumnKey(e *plan.Expr) existentialColumn {
	c := e.GetCol()
	return existentialColumn{c.RelPos, c.ColPos, e.Typ.Id, e.Typ.Width, e.Typ.Scale}
}
func (builder *QueryBuilder) planExistentialArm(preds []*plan.Expr, i, j, anchor int32) (*existentialArm, bool) {
	arm := &existentialArm{anchor: anchor, local: make(map[int32][]*plan.Expr)}
	parents := make(map[existentialColumn]existentialColumn)
	var find func(existentialColumn) existentialColumn
	find = func(k existentialColumn) existentialColumn {
		p, ok := parents[k]
		if !ok {
			parents[k] = k
			return k
		}
		if p != k {
			parents[k] = find(p)
		}
		return parents[k]
	}
	for _, e := range preds {
		if !existentialPredicateSafe(e) {
			return nil, false
		}
		side := existentialSides(e, i, j)
		if side != 0 && side != 1 && side != 2 && side != 4 && !existentialEquality(e) {
			return nil, false
		}
		if !existentialEquality(e) {
			continue
		}
		a, c := e.GetF().Args[0], e.GetF().Args[1]
		ka, kc := existentialColumnKey(a), existentialColumnKey(c)
		parents[find(ka)] = find(kc)
	}
	// Use predicate order rather than map iteration to keep plan choice stable.
	representatives := make(map[existentialColumn]*plan.Expr)
	for _, e := range preds {
		if !existentialEquality(e) {
			continue
		}
		for _, a := range e.GetF().Args {
			if a.GetCol().RelPos == anchor {
				key := find(existentialColumnKey(a))
				if representatives[key] == nil {
					representatives[key] = a
				}
			}
		}
	}
	replacements := make(map[existentialColumn]*plan.Expr)
	for _, original := range preds {
		if lit := original.GetLit(); lit != nil && !lit.Isnull && original.Typ.Id == int32(types.T_bool) && lit.GetBval() {
			continue
		}
		side := existentialSides(original, i, j)
		if side == 0 || side == 4 {
			arm.gates = append(arm.gates, original)
			continue
		}
		e := DeepCopyExpr(original)
		valid := true
		existentialWalk(e, func(x *plan.Expr) {
			c := x.GetCol()
			if c == nil || c.RelPos == i || c.RelPos == j {
				return
			}
			key := existentialColumnKey(x)
			replacement := representatives[find(key)]
			if replacement == nil {
				valid = false
				return
			}
			if replacements[key] == nil {
				eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{DeepCopyExpr(x), DeepCopyExpr(replacement)})
				if err != nil {
					valid = false
					return
				}
				arm.outer = append(arm.outer, eq)
				replacements[key] = replacement
			}
			*x = *DeepCopyExpr(replacement)
		})
		if !valid {
			return nil, false
		}
		// A direct outer/anchor equality is already retained verbatim as a
		// final join key. Repeating its substituted anchor=anchor filter adds
		// no constraint (the retained equality already rejects NULL), but can
		// distort cardinality and reverse SEMI into a quadratic RIGHT SEMI
		// on duplicate keys. Other self equalities must still be preserved.
		if existentialEquality(original) && existentialEquality(e) &&
			existentialColumnKey(e.GetF().Args[0]) == existentialColumnKey(e.GetF().Args[1]) {
			a, c := original.GetF().Args[0].GetCol(), original.GetF().Args[1].GetCol()
			if (a.RelPos == anchor && c.RelPos != i && c.RelPos != j) ||
				(c.RelPos == anchor && a.RelPos != i && a.RelPos != j) {
				continue
			}
		}
		switch existentialSides(e, i, j) {
		case 1:
			arm.local[i] = append(arm.local[i], e)
		case 2:
			arm.local[j] = append(arm.local[j], e)
		case 3:
			arm.witness = append(arm.witness, e)
		default:
			return nil, false
		}
	}
	return arm, true
}

func (builder *QueryBuilder) lowerDeepExistential(id int32, outer *plan.SubqueryRef, pending *pendingExistential, ctx *BindContext) (int32, *plan.Expr, error) {
	if err := builder.checkPlanningCanceled(); err != nil {
		return 0, nil, err
	}
	middle, ok := builder.readExistentialRelation(outer.NodeId)
	if !ok {
		return 0, nil, builder.existentialNYI()
	}
	inner, ok := builder.readExistentialRelation(pending.sub.NodeId)
	if !ok {
		return 0, nil, builder.existentialNYI()
	}
	outerTags := builder.collectBindingTags(builder.qry.Nodes[id])
	middleTags := map[int32]bool{middle.tag: true}
	common := make([]*plan.Expr, 0, len(middle.filters))
	occurrences := 0
	for _, pred := range middle.filters {
		var conjuncts []*plan.Expr
		existentialSplit(pred, "and", &conjuncts, int(^uint(0)>>1))
		for _, p := range conjuncts {
			if s := p.GetSub(); s != nil && s == pending.sub {
				occurrences++
				continue
			}
			e, valid := resolveExistentialExpr(p, middle, []map[int32]bool{outerTags})
			if !valid {
				return 0, nil, builder.existentialNYI()
			}
			common = append(common, e)
		}
	}
	if occurrences != 1 {
		return 0, nil, builder.existentialNYI()
	}
	// Bind the same scalar equality as generateRowComparison, using copies of
	// the original typed operands. Never infer IN semantics from display types.
	for _, item := range []struct {
		sub         *plan.SubqueryRef
		rel, parent *existentialRelation
		parents     []map[int32]bool
	}{
		{outer, middle, nil, []map[int32]bool{outerTags}},
		{pending.sub, inner, middle, []map[int32]bool{middleTags, outerTags}},
	} {
		if item.sub.Typ != plan.SubqueryRef_IN {
			continue
		}
		sc := builder.ctxByNode[item.sub.NodeId]
		if item.sub.RowSize != 1 || item.sub.Child == nil || len(sc.results) != 1 {
			return 0, nil, builder.existentialNYI()
		}
		right, valid := resolveExistentialExpr(sc.results[0], item.rel, item.parents)
		if !valid {
			return 0, nil, builder.existentialNYI()
		}
		left := DeepCopyExpr(item.sub.Child)
		if item.parent != nil {
			left, valid = resolveExistentialExpr(left, item.parent, []map[int32]bool{outerTags})
		} else {
			valid = left.GetCol() != nil && outerTags[left.GetCol().RelPos]
		}
		if !valid {
			return 0, nil, builder.existentialNYI()
		}
		eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{left, right})
		if err != nil {
			return 0, nil, err
		}
		if !existentialEquality(eq) {
			return 0, nil, builder.existentialNYI()
		}
		common = append(common, eq)
	}
	// Multiple scalar filter entries are a conjunction; at most one may own
	// explicit OR arms. No distribution over multiple OR lists is attempted.
	var arms []*plan.Expr
	for _, pred := range inner.filters {
		p, valid := resolveExistentialExpr(pred, inner, []map[int32]bool{middleTags, outerTags})
		if !valid {
			return 0, nil, builder.existentialNYI()
		}
		var parts []*plan.Expr
		if !existentialSplit(p, "or", &parts, maxExistentialArms) {
			return 0, nil, builder.existentialNYI()
		}
		if len(parts) > 1 {
			if len(arms) != 0 {
				return 0, nil, builder.existentialNYI()
			}
			arms = parts
		} else {
			existentialSplit(p, "and", &common, int(^uint(0)>>1))
		}
	}
	if len(arms) == 0 {
		arms = []*plan.Expr{constTrue}
	}
	descriptors := make([]*existentialArm, 0, len(arms))
	for _, p := range arms {
		preds := append([]*plan.Expr(nil), common...)
		existentialSplit(p, "and", &preds, int(^uint(0)>>1))
		arm, valid := builder.planExistentialArm(preds, middle.tag, inner.tag, middle.tag)
		if !valid {
			arm, valid = builder.planExistentialArm(preds, middle.tag, inner.tag, inner.tag)
		}
		if !valid {
			return 0, nil, builder.existentialNYI()
		}
		if len(arms) > 1 && len(arm.outer) > 1 {
			for _, key := range arm.outer {
				for _, arg := range key.GetF().Args {
					if !arg.Typ.NotNullable || (outerTags[arg.GetCol().RelPos] && !builder.exprEffectivelyNotNullableBeforeRemap(arg, id)) {
						return 0, nil, builder.existentialNYI()
					}
				}
			}
		}
		descriptors = append(descriptors, arm)
	}
	// All proof obligations and the arm budget have passed. New nodes now own
	// their scans, tags and expressions. Publish only the completed root.
	markers := make([]*plan.Expr, 0, len(descriptors))
	for _, arm := range descriptors {
		if err := builder.checkPlanningCanceled(); err != nil {
			return 0, nil, err
		}
		root, marker, err := builder.emitExistentialArm(id, middle, inner, arm, ctx, len(arms) == 1, outer.Typ == plan.SubqueryRef_NOT_EXISTS)
		if err != nil {
			return 0, nil, err
		}
		id = root
		markers = append(markers, marker)
	}
	if len(arms) == 1 {
		return id, markers[0], nil
	}
	result, err := combinePlanExprsBalanced(builder.GetContext(), "or", markers)
	if err == nil && outer.Typ == plan.SubqueryRef_NOT_EXISTS {
		result, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{result})
	}
	return id, result, err
}

func (builder *QueryBuilder) emitExistentialArm(id int32, middle, inner *existentialRelation, arm *existentialArm, ctx *BindContext, single, negate bool) (int32, *plan.Expr, error) {
	tags := make(map[int32]int32, 2)
	scans := make(map[int32]int32, 2)
	for _, r := range []*existentialRelation{middle, inner} {
		scan := DeepCopyNode(r.scan)
		// Semantic predicates are restored from the arm descriptor. Block filters
		// are derived later from those predicates, never copied across OR arms.
		scan.FilterList, scan.BlockFilterList = nil, nil
		builder.rebindScanNode(scan)
		tags[r.tag] = scan.BindingTags[0]
		scans[r.tag] = builder.appendNode(scan, ctx)
	}
	clone := func(list []*plan.Expr) []*plan.Expr {
		out := make([]*plan.Expr, len(list))
		for i, e := range list {
			out[i] = DeepCopyExpr(e)
			for old, newTag := range tags {
				replaceColRefTag(out[i], old, newTag)
			}
		}
		return out
	}
	for tag, filters := range arm.local {
		if len(filters) > 0 {
			scans[tag] = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{scans[tag]}, FilterList: clone(filters)}, ctx)
		}
	}
	other := middle.tag
	if arm.anchor == other {
		other = inner.tag
	}
	anchorID, otherID := scans[arm.anchor], scans[other]
	gates := clone(arm.gates)
	if len(arm.witness) > 0 {
		anchorID = builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{anchorID, otherID}, OnList: clone(arm.witness), SpillMem: builder.joinSpillMem}, ctx)
	} else {
		var gate *plan.Expr
		var err error
		id, gate, err = builder.attachExistentialSummary(id, otherID, ctx)
		if err != nil {
			return 0, nil, err
		}
		gates = append(gates, gate)
	}
	keys := clone(arm.outer)
	var marker *plan.Expr
	var err error
	if len(keys) == 0 {
		id, marker, err = builder.attachExistentialSummary(id, anchorID, ctx)
	} else if single {
		jt := plan.Node_SEMI
		if negate {
			jt = plan.Node_ANTI
			if len(gates) > 0 {
				anchorID, keys, err = builder.addExistentialHashGate(anchorID, keys, gates, tags[arm.anchor], ctx)
				if err != nil {
					return 0, nil, err
				}
			}
		} else if len(gates) > 0 {
			id = builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{id}, FilterList: gates}, ctx)
		}
		id = builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: jt, Children: []int32{id, anchorID}, OnList: keys, SpillMem: builder.joinSpillMem}, ctx)
		return id, DeepCopyExpr(constTrue), nil
	} else {
		id, marker, err = builder.insertMarkJoin(id, anchorID, keys, nil, false, ctx)
	}
	if err != nil {
		return 0, nil, err
	}
	for _, g := range gates {
		truth, e := BindFuncExprImplByPlanExpr(builder.GetContext(), "istrue", []*plan.Expr{g})
		if e != nil {
			return 0, nil, e
		}
		marker, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{truth, marker})
		if err != nil {
			return 0, nil, err
		}
	}
	if single && negate {
		marker, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{marker})
	}
	return id, marker, err
}

// The no-group aggregate emits exactly one row even on an empty input. This
// bounded attachment preserves every O occurrence and is not an I x J product.
func (builder *QueryBuilder) attachExistentialSummary(id, input int32, ctx *BindContext) (int32, *plan.Expr, error) {
	count, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "starcount", []*plan.Expr{makePlan2Int64ConstExprWithType(1)})
	if err != nil {
		return 0, nil, err
	}
	groupTag, aggTag := builder.genNewBindTag(), builder.genNewBindTag()
	agg := builder.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{input}, BindingTags: []int32{groupTag, aggTag}, AggList: []*plan.Expr{count}, SpillMem: builder.aggSpillMem}, ctx)
	ref := &plan.Expr{Typ: count.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: aggTag, ColPos: 0}}}
	marker, err := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*plan.Expr{ref, makePlan2Int64ConstExprWithType(0)})
	if err != nil {
		return 0, nil, err
	}
	id = builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{id, agg}, OnList: []*plan.Expr{DeepCopyExpr(constTrue)}, SpillMem: builder.joinSpillMem}, ctx)
	return id, marker, nil
}

// This validation runs only for builders that ever deferred a region, before
// optimization or plan serialization. Unreachable old nodes are intentionally
// excluded; their original correlated expressions remain immutable.
type existentialValidation struct{ builder *QueryBuilder }

func (*existentialValidation) MatchNode(*Node) bool  { return false }
func (*existentialValidation) IsApplyExpr() bool     { return true }
func (*existentialValidation) ApplyNode(*Node) error { return nil }
func (v *existentialValidation) ApplyExpr(e *Expr) (*Expr, error) {
	invalid := false
	existentialWalk(e, func(x *plan.Expr) { invalid = invalid || x.GetSub() != nil || x.GetCorr() != nil })
	if invalid {
		return nil, v.builder.existentialNYI()
	}
	return e, nil
}
func (builder *QueryBuilder) checkPendingExistentials() error {
	if len(builder.pendingExistentials) != 0 {
		return builder.existentialNYI()
	}
	rules := []VisitPlanRule{&existentialValidation{builder: builder}}
	visitor := NewVisitPlan(&Plan{Plan: &plan.Plan_Query{Query: builder.qry}}, rules)
	for _, root := range builder.qry.Steps {
		if err := visitor.visitNode(builder.GetContext(), builder.qry, builder.qry.Nodes[root], root); err != nil {
			return err
		}
	}
	return visitMissingNodeExprs(builder.qry, builder.qry.Steps, rules)
}

// The normal binder narrows integer literals to the column's type. The generic
// totality proof intentionally rejects arbitrary narrowing casts; for a literal
// the existing range checker proves totality. This copy is used only for proof,
// never to change the bound expression that will execute.
func existentialPredicateSafe(e *plan.Expr) bool {
	if isTruncationSafePredicateExpr(e) {
		return true
	}
	proof := DeepCopyExpr(e)
	existentialWalk(proof, func(x *plan.Expr) {
		f := x.GetF()
		if f == nil || f.Func.ObjName != "cast" || len(f.Args) != 2 || f.Args[1].GetT() == nil {
			return
		}
		arg := f.Args[0]
		lit := arg.GetLit()
		if lit == nil || lit.Src != nil || !types.T(arg.Typ.Id).IsInteger() || !types.T(x.Typ.Id).IsInteger() {
			return
		}
		if checkNoNeedCast(makeTypeByPlan2Expr(arg), makeTypeByPlan2Expr(x), arg) {
			x.Expr = DeepCopyExpr(arg).Expr
		}
	})
	return isTruncationSafePredicateExpr(proof)
}

// Existing successful depth-two queries must not allocate a proof descriptor.
// Walk the simple chain twice: first locate the scan, then prove the precise
// legacy rejection predicate without collecting or rewriting expressions.
func (builder *QueryBuilder) hasRejectedDeepExistential(root int32) bool {
	id := root
	for {
		n := builder.qry.Nodes[id]
		if n.NodeType == plan.Node_TABLE_SCAN {
			break
		}
		if (n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER) || len(n.Children) != 1 {
			return false
		}
		id = n.Children[0]
	}
	scan := builder.qry.Nodes[id]
	if len(scan.BindingTags) != 1 {
		return false
	}
	tag := scan.BindingTags[0]
	for {
		n := builder.qry.Nodes[root]
		for _, pred := range n.FilterList {
			inner, deep := false, false
			existentialWalk(pred, func(e *plan.Expr) {
				if c := e.GetCol(); c != nil && c.RelPos == tag {
					inner = true
				}
				if c := e.GetCorr(); c != nil && c.Depth == 2 {
					deep = true
				}
			})
			if inner && deep {
				return true
			}
		}
		if root == id {
			return false
		}
		root = n.Children[0]
	}
}

func (builder *QueryBuilder) addExistentialHashGate(input int32, keys, gates []*plan.Expr, anchorTag int32, ctx *BindContext) (int32, []*plan.Expr, error) {
	tag := builder.genNewBindTag()
	projects := make([]*plan.Expr, 0, len(keys)+1)
	for _, key := range keys {
		for _, arg := range key.GetF().Args {
			col := arg.GetCol()
			if col == nil || col.RelPos != anchorTag {
				continue
			}
			pos := int32(len(projects))
			projects = append(projects, DeepCopyExpr(arg))
			arg.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: pos}}
		}
	}
	gatePos := int32(len(projects))
	projects = append(projects, DeepCopyExpr(constTrue))
	input = builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{input}, BindingTags: []int32{tag}, ProjectList: projects}, ctx)
	if builder.existentialGateProjects == nil {
		builder.existentialGateProjects = make(map[int32]struct{})
	}
	builder.existentialGateProjects[input] = struct{}{}
	gate, err := combinePlanExprsBalanced(builder.GetContext(), "and", gates)
	if err != nil {
		return 0, nil, err
	}
	gate, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "istrue", []*plan.Expr{gate})
	if err != nil {
		return 0, nil, err
	}
	buildGate := &plan.Expr{Typ: constTrue.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: gatePos}}}
	condition, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{gate, buildGate})
	if err != nil {
		return 0, nil, err
	}
	return input, append(keys, condition), nil
}
