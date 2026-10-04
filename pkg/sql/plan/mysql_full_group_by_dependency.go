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
	"maps"
	"slices"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type fullGroupByColumn = [2]int32

// Every exported rule is NULL-friendly: all-NULL determinants imply all-NULL
// dependents. This lets it survive an enclosing outer join's NULL extension.
// Constants and non-null facts do NOT have that property and are not rules.
type fullGroupByRule struct {
	from, to []fullGroupByColumn
}

type fullGroupByKey struct {
	fullGroupByRule
	requireNonNull []fullGroupByColumn
}

type fullGroupByRelation struct {
	columns map[fullGroupByColumn]pbplan.Type
	nonNull map[fullGroupByColumn]bool
	rules   []fullGroupByRule
	keys    []fullGroupByKey // only candidates still awaiting explicit non-null proof
}

type fullGroupByDependencyProof struct {
	relation *fullGroupByRelation
	groups   []*Expr
	flags    []bool
	known    map[fullGroupByColumn]bool
}

func (builder *QueryBuilder) fullGroupByDependencyAllows(ctx *BindContext, tag, pos int32) bool {
	if !ctx.fullGroupByInputReady {
		return false
	}
	proof := ctx.fullGroupByProof
	if proof == nil {
		proof = &fullGroupByDependencyProof{relation: builder.fullGroupByRelation(ctx.fullGroupByInputNode, make(map[int32]bool))}
		ctx.fullGroupByProof = proof
	}
	if proof.known == nil || !slices.Equal(proof.groups, ctx.groups) || !slices.Equal(proof.flags, ctx.groupingFlag) {
		var seeds []fullGroupByColumn
		for i, expr := range ctx.groups {
			if len(ctx.groupingFlag) > 0 && (i >= len(ctx.groupingFlag) || !ctx.groupingFlag[i]) {
				continue
			}
			if col, ok := fullGroupByDirectColumn(expr); ok {
				seeds = append(seeds, col)
			}
		}
		for col := range proof.relation.columns {
			if filterListHasSingleValueEqualityOnCol(ctx.whereFilters, col[0], col[1]) {
				seeds = append(seeds, col)
			}
		}
		proof.known = fullGroupByClosure(proof.relation.rules, seeds)
		proof.groups = slices.Clone(ctx.groups)
		proof.flags = slices.Clone(ctx.groupingFlag)
	}
	return proof.known[fullGroupByColumn{tag, pos}]
}

func fullGroupByClosure(rules []fullGroupByRule, seeds []fullGroupByColumn) map[fullGroupByColumn]bool {
	known := make(map[fullGroupByColumn]bool)
	waiters := make(map[fullGroupByColumn][]int)
	remaining := make([]int, len(rules))
	for i, rule := range rules {
		remaining[i] = len(rule.from)
		for _, col := range rule.from {
			waiters[col] = append(waiters[col], i)
		}
	}
	queue := slices.Clone(seeds)
	for i := 0; i < len(queue); i++ {
		col := queue[i]
		if known[col] {
			continue
		}
		known[col] = true
		for _, idx := range waiters[col] {
			remaining[idx]--
			if remaining[idx] == 0 {
				queue = append(queue, rules[idx].to...)
			}
		}
	}
	return known
}

func newFullGroupByRelation() *fullGroupByRelation {
	return &fullGroupByRelation{columns: make(map[fullGroupByColumn]pbplan.Type), nonNull: make(map[fullGroupByColumn]bool)}
}

func fullGroupByDirectColumn(expr *Expr) (fullGroupByColumn, bool) {
	if expr != nil && expr.GetCol() != nil {
		col := expr.GetCol()
		return fullGroupByColumn{col.RelPos, col.ColPos}, true
	}
	return fullGroupByColumn{}, false
}

func (r *fullGroupByRelation) activateKeys() {
	remaining := r.keys[:0]
	for _, key := range r.keys {
		ready := true
		for _, col := range key.requireNonNull {
			if !r.nonNull[col] {
				ready = false
				break
			}
		}
		if ready {
			r.rules = append(r.rules, key.fullGroupByRule)
		} else {
			remaining = append(remaining, key)
		}
	}
	r.keys = remaining
}

func (builder *QueryBuilder) fullGroupByRelation(id int32, visiting map[int32]bool) *fullGroupByRelation {
	r := newFullGroupByRelation()
	if id < 0 || int(id) >= len(builder.qry.Nodes) || visiting[id] {
		return r
	}
	n := builder.qry.Nodes[id]
	if n == nil || n.Limit != nil || n.Offset != nil {
		return r
	}
	visiting[id] = true
	defer delete(visiting, id)
	switch n.NodeType {
	case pbplan.Node_TABLE_SCAN:
		if n.TableDef == nil || len(n.BindingTags) != 1 {
			return r
		}
		tag := n.BindingTags[0]
		var all []fullGroupByColumn
		for i, col := range n.TableDef.Cols {
			if col == nil || col.Hidden {
				continue
			}
			ref := fullGroupByColumn{tag, int32(i)}
			r.columns[ref] = col.Typ
			r.nonNull[ref] = col.Default != nil && !col.Default.NullAbility
			all = append(all, ref)
		}
		if positions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(n.TableDef); ok {
			var key []fullGroupByColumn
			for _, pos := range positions {
				ref := fullGroupByColumn{tag, pos}
				key = append(key, ref)
				r.nonNull[ref] = true
			}
			r.rules = append(r.rules, fullGroupByRule{key, all})
		}
		for _, index := range n.TableDef.Indexes {
			positions, ok := fullGroupByUniqueKeyPositions(n.TableDef, index)
			if !ok {
				continue
			}
			key := fullGroupByKey{fullGroupByRule: fullGroupByRule{to: all}}
			for _, pos := range positions {
				ref := fullGroupByColumn{tag, pos}
				key.from = append(key.from, ref)
				if !r.nonNull[ref] {
					key.requireNonNull = append(key.requireNonNull, ref)
				}
			}
			r.keys = append(r.keys, key)
		}
	case pbplan.Node_FILTER, pbplan.Node_SORT:
		if len(n.Children) != 1 {
			return r
		}
		r = builder.fullGroupByRelation(n.Children[0], visiting)
	case pbplan.Node_PROJECT:
		if len(n.Children) != 1 || len(n.BindingTags) != 1 {
			return r
		}
		r = projectFullGroupByRelation(builder.fullGroupByRelation(n.Children[0], visiting), n)
	case pbplan.Node_JOIN:
		if len(n.Children) != 2 || (n.JoinType != pbplan.Node_INNER && n.JoinType != pbplan.Node_LEFT && n.JoinType != pbplan.Node_RIGHT) {
			return r
		}
		left := builder.fullGroupByRelation(n.Children[0], visiting)
		right := builder.fullGroupByRelation(n.Children[1], visiting)
		if n.JoinType == pbplan.Node_RIGHT {
			left, right = right, left
		}
		maps.Copy(r.columns, left.columns)
		maps.Copy(r.columns, right.columns)
		maps.Copy(r.nonNull, left.nonNull)
		r.rules = append(r.rules, left.rules...)
		r.rules = append(r.rules, right.rules...)
		r.keys = append(r.keys, left.keys...)
		r.keys = append(r.keys, right.keys...)
		if n.JoinType == pbplan.Node_INNER {
			maps.Copy(r.nonNull, right.nonNull)
			r.addPredicates(n.OnList, false)
		} else {
			r.addOuterEqualities(n.OnList, left.columns, right.columns)
		}
	default:
		return r
	}
	r.addPredicates(n.FilterList, true)
	r.activateKeys()
	return r
}

func (r *fullGroupByRelation) equality(expr *Expr) (fullGroupByColumn, fullGroupByColumn, bool) {
	if expr == nil || expr.GetF() == nil {
		return fullGroupByColumn{}, fullGroupByColumn{}, false
	}
	f := expr.GetF()
	if f.Func == nil || f.Func.ObjName != "=" || len(f.Args) != 2 {
		return fullGroupByColumn{}, fullGroupByColumn{}, false
	}
	l, lok := fullGroupByDirectColumn(f.Args[0])
	rr, rok := fullGroupByDirectColumn(f.Args[1])
	lt, ltok := r.columns[l]
	rt, rtok := r.columns[rr]
	return l, rr, lok && rok && ltok && rtok &&
		fullGroupBySameValueType(f.Args[0].Typ, lt) && fullGroupBySameValueType(f.Args[1].Typ, rt) &&
		primaryKeyColumnTypeSupportsSQLEqualityProof(lt) &&
		primaryKeyColumnTypeSupportsSQLEqualityProof(rt) && sqlEqualityJoinUsesOneIdentityDomain(lt, rt)
}

func fullGroupBySameValueType(a, b pbplan.Type) bool {
	return a.Id == b.Id && a.Scale == b.Scale && a.Charset == b.Charset && a.Enumvalues == b.Enumvalues
}

func (r *fullGroupByRelation) addPredicates(predicates []*Expr, nonNull bool) {
	for _, expr := range predicates {
		if expr == nil || expr.GetF() == nil {
			continue
		}
		f := expr.GetF()
		if f.Func == nil {
			continue
		}
		if f.Func.ObjName == "and" {
			r.addPredicates(f.Args, nonNull)
			continue
		}
		if l, rr, ok := r.equality(expr); ok {
			r.rules = append(r.rules, fullGroupByRule{[]fullGroupByColumn{l}, []fullGroupByColumn{rr}}, fullGroupByRule{[]fullGroupByColumn{rr}, []fullGroupByColumn{l}})
		}
		if !nonNull {
			continue
		}
		var arg *Expr
		if f.Func.ObjName == "isnotnull" && len(f.Args) == 1 {
			arg = f.Args[0]
		}
		if f.Func.ObjName == "not" && len(f.Args) == 1 && f.Args[0].GetF() != nil {
			inner := f.Args[0].GetF()
			if inner.Func != nil && inner.Func.ObjName == "isnull" && len(inner.Args) == 1 {
				arg = inner.Args[0]
			}
		}
		if col, ok := fullGroupByDirectColumn(arg); ok {
			if _, exists := r.columns[col]; exists {
				r.nonNull[col] = true
			}
		}
	}
}

func fullGroupByStableReferences(expr *Expr, columns map[fullGroupByColumn]pbplan.Type, refs map[fullGroupByColumn]bool) bool {
	if col, ok := fullGroupByDirectColumn(expr); ok {
		if _, exists := columns[col]; !exists {
			return false
		}
		refs[col] = true
		return true
	}
	if expr == nil {
		return false
	}
	if f := expr.GetF(); f != nil {
		if f.Func == nil {
			return false
		}
		overload, ok := function.GetFunctionByIdWithoutError(f.Func.Obj)
		if !ok || overload.CannotFold() {
			return false
		}
		for _, arg := range f.Args {
			if !fullGroupByStableReferences(arg, columns, refs) {
				return false
			}
		}
		return true
	}
	return isMySQLFullGroupBySingleValueExpr(expr)
}

func (r *fullGroupByRelation) addOuterEqualities(predicates []*Expr, strong, weak map[fullGroupByColumn]pbplan.Type) {
	refs := make(map[fullGroupByColumn]bool)
	for _, expr := range predicates {
		if !fullGroupByStableReferences(expr, r.columns, refs) {
			return
		}
	}
	var determinants []fullGroupByColumn
	for ref := range refs {
		if _, ok := strong[ref]; ok {
			determinants = append(determinants, ref)
		}
	}
	if len(determinants) == 0 {
		return
	}
	for _, expr := range predicates {
		l, rr, ok := r.equality(expr)
		if !ok {
			continue
		}
		if _, ok := strong[l]; !ok {
			l, rr = rr, l
		}
		_, ls := strong[l]
		_, rw := weak[rr]
		if ls && rw {
			r.rules = append(r.rules, fullGroupByRule{determinants, []fullGroupByColumn{rr}})
		}
	}
}

// Project summaries contain only outward column identities. Hidden intermediate
// proof columns cannot connect two references to the same CTE or catalog table.
func projectFullGroupByRelation(child *fullGroupByRelation, n *pbplan.Node) *fullGroupByRelation {
	r := newFullGroupByRelation()
	mapping := make(map[fullGroupByColumn][]fullGroupByColumn)
	for i, expr := range n.ProjectList {
		source, ok := fullGroupByDirectColumn(expr)
		typ, exists := child.columns[source]
		// A direct projection copies values; it does not compare two domains.
		// Collated payload columns therefore survive, although the separate
		// key/equality guards still refuse to use them as uniqueness evidence.
		if !ok || !exists || !fullGroupBySameValueType(expr.Typ, typ) {
			continue
		}
		out := fullGroupByColumn{n.BindingTags[0], int32(i)}
		mapping[source] = append(mapping[source], out)
		r.columns[out] = expr.Typ
		r.nonNull[out] = child.nonNull[source]
	}
	translate := func(cols []fullGroupByColumn) ([]fullGroupByColumn, bool) {
		out := make([]fullGroupByColumn, 0, len(cols))
		for _, col := range cols {
			if len(mapping[col]) == 0 {
				return nil, false
			}
			out = append(out, mapping[col][0])
		}
		return out, true
	}
	for _, outputs := range mapping {
		for _, out := range outputs[1:] {
			r.rules = append(r.rules, fullGroupByRule{[]fullGroupByColumn{outputs[0]}, []fullGroupByColumn{out}}, fullGroupByRule{[]fullGroupByColumn{out}, []fullGroupByColumn{outputs[0]}})
		}
	}
	for _, rule := range child.rules {
		from, ok := translate(rule.from)
		if !ok {
			continue
		}
		known := fullGroupByClosure(child.rules, rule.from)
		var to []fullGroupByColumn
		for source, outputs := range mapping {
			if known[source] {
				to = append(to, outputs...)
			}
		}
		r.rules = append(r.rules, fullGroupByRule{from, to})
	}
	for _, key := range child.keys {
		from, ok := translate(key.from)
		if !ok {
			continue
		}
		nn, ok := translate(key.requireNonNull)
		if !ok {
			continue
		}
		var to []fullGroupByColumn
		for _, col := range key.to {
			to = append(to, mapping[col]...)
		}
		r.keys = append(r.keys, fullGroupByKey{fullGroupByRule{from, to}, nn})
	}
	r.activateKeys()
	return r
}
