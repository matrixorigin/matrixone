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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// localCTEDomain describes a statement-local relation of (row identity, outer
// parameters). The identity, rather than parameter equality, isolates NULL and
// repeated parameter values across recursive iterations.
type localCTEDomain struct {
	builder  *QueryBuilder
	outerID  int32
	ctx      *BindContext
	values   []*plan.Expr
	params   map[[2]int32]int
	equality *plan.Expr
	nodes    map[int32]bool
	scans    map[int32][]*plan.Expr
}

func (builder *QueryBuilder) parameterizeLocalCTEs(outerID, subID int32, ctx *BindContext) (int32, error) {
	if len(builder.localCTERoots) == 0 {
		return subID, nil
	}
	domains := make(map[int32]*localCTEDomain)
	var admit func(int32) error
	admit = func(id int32) error {
		if builder.localCTERoots[id] {
			d := &localCTEDomain{builder: builder, outerID: outerID, ctx: ctx,
				params: make(map[[2]int32]int), nodes: make(map[int32]bool), scans: make(map[int32][]*plan.Expr)}
			d.collect(id)
			needsDomain := false
			for nodeID := range d.nodes {
				n := builder.qry.Nodes[nodeID]
				for _, e := range localCTENodeExprs(n) {
					if hasCorrCol(e) && (n.NodeType == plan.Node_PROJECT || n.NodeType == plan.Node_VALUE_SCAN || len(n.SourceStep) > 0 || len(builder.qry.Nodes[id].SourceStep) > 0) {
						needsDomain = true
					}
				}
			}
			if needsDomain {
				if err := d.admit(); err != nil {
					return err
				}
				domains[id] = d
				return nil
			}
		}
		for _, child := range builder.qry.Nodes[id].Children {
			if err := admit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := admit(subID); err != nil {
		return 0, err
	}
	var lower func(int32) int32
	lower = func(id int32) int32 {
		if d := domains[id]; d != nil {
			return d.lower(id)
		}
		n := builder.qry.Nodes[id]
		for i, child := range n.Children {
			n.Children[i] = lower(child)
		}
		return id
	}
	return lower(subID), nil
}

func (d *localCTEDomain) collect(id int32) {
	if d.nodes[id] {
		return
	}
	d.nodes[id] = true
	n := d.builder.qry.Nodes[id]
	for _, child := range n.Children {
		d.collect(child)
	}
	for _, step := range n.SourceStep {
		d.collect(d.builder.qry.Steps[step])
	}
}

func localCTENodeExprs(n *plan.Node) []*plan.Expr {
	exprs := append([]*plan.Expr(nil), n.ProjectList...)
	exprs = append(exprs, n.FilterList...)
	exprs = append(exprs, n.OnList...)
	if n.RowsetData != nil {
		for _, column := range n.RowsetData.Cols {
			for _, row := range column.Data {
				exprs = append(exprs, row.Expr)
			}
		}
	}
	return exprs
}

func walkLocalCTEExpr(e *plan.Expr, visit func(*plan.Expr)) {
	if e == nil {
		return
	}
	visit(e)
	switch x := e.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range x.F.Args {
			walkLocalCTEExpr(arg, visit)
		}
	case *plan.Expr_List:
		for _, arg := range x.List.List {
			walkLocalCTEExpr(arg, visit)
		}
	}
}

func (d *localCTEDomain) unsupported(reason string) error {
	return moerr.NewNYIf(d.builder.GetContext(), "correlated local CTE: %s", reason)
}

// Admission is read-only. In particular, do not append columns to a recursive
// scan before all members and the outer domain have passed the same contract.
func (d *localCTEDomain) admit() error {
	b := d.builder
	rowID := b.correlatedScalarOuterRowID(d.outerID, d.ctx)
	if rowID == nil || b.isForUpdate {
		return d.unsupported("outer input must preserve a single base-table row identity")
	}
	var err error
	d.equality, err = BindFuncExprImplByPlanExpr(b.GetContext(), "=", []*plan.Expr{DeepCopyExpr(rowID), DeepCopyExpr(rowID)})
	if err != nil {
		return err
	}
	outerTag := rowID.GetCol().RelPos
	for id := d.outerID; ; {
		n := b.qry.Nodes[id]
		if n.Limit != nil || n.Offset != nil || len(n.LockTargets) != 0 || n.DirectView != "" || len(n.OriginViews) != 0 {
			return d.unsupported("outer input cannot be safely replayed")
		}
		for _, e := range n.ProjectList {
			if !isTruncationSafeRowExpr(e) {
				return d.unsupported("outer input contains a non-total expression")
			}
		}
		for _, e := range n.FilterList {
			if !localCTEDomainPredicateReplaySafe(e) {
				return d.unsupported("outer input predicate cannot be safely replayed")
			}
		}
		if n.NodeType == plan.Node_TABLE_SCAN && len(n.BindingTags) == 1 && n.BindingTags[0] == outerTag {
			break
		}
		if (n.NodeType != plan.Node_FILTER && n.NodeType != plan.Node_SORT) || len(n.Children) != 1 {
			return d.unsupported("outer input is not a transparent base-table scan")
		}
		id = n.Children[0]
	}
	keys := make(map[[2]int32]*plan.Expr)
	for id := range d.nodes {
		n := b.qry.Nodes[id]
		if n.Limit != nil || n.Offset != nil || len(n.OrderBy) != 0 || len(n.LockTargets) != 0 || len(n.GroupBy) != 0 || len(n.AggList) != 0 || len(n.WinSpecList) != 0 {
			return d.unsupported("producer contains pagination, ordering, aggregation, windowing or locking")
		}
		switch n.NodeType {
		case plan.Node_PROJECT, plan.Node_SINK:
			if len(n.Children) != 1 || len(n.BindingTags) != 1 {
				return d.unsupported("producer projection has no unique binding")
			}
		case plan.Node_FILTER:
			if len(n.Children) != 1 || n.FilterIsBarrier {
				return d.unsupported("producer filter is a barrier")
			}
		case plan.Node_TABLE_SCAN, plan.Node_VALUE_SCAN:
			// Only the implicit one-row VALUE_SCAN can be evaluated before
			// joining the domain. Explicit VALUES owns expression executors
			// below that join and cannot consume its parameter columns.
			if n.NodeType == plan.Node_VALUE_SCAN && n.RowsetData != nil {
				return d.unsupported("explicit VALUES producer")
			}
			if len(n.Children) != 0 || len(n.SourceStep) != 0 {
				return d.unsupported("producer scan has dependent inputs")
			}
		case plan.Node_JOIN:
			if n.JoinType != plan.Node_INNER || len(n.Children) != 2 {
				return d.unsupported("producer join must be an inner join")
			}
		case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
			if len(n.BindingTags) != 1 || len(n.SourceStep) == 0 {
				return d.unsupported("recursive scan has no producer binding")
			}
		default:
			return d.unsupported("producer operator " + n.NodeType.String())
		}
		for _, e := range localCTENodeExprs(n) {
			valid := true
			walkLocalCTEExpr(e, func(x *plan.Expr) {
				switch v := x.Expr.(type) {
				case *plan.Expr_Corr:
					if v.Corr.Depth != 1 || v.Corr.RelPos != outerTag {
						valid = false
						return
					}
					keys[[2]int32{v.Corr.RelPos, v.Corr.ColPos}] = GetColExpr(x.Typ, v.Corr.RelPos, v.Corr.ColPos)
				case *plan.Expr_Col, *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_P, *plan.Expr_F, *plan.Expr_List:
				default:
					valid = false
				}
			})
			if !valid || containsVolatileFunction(e) {
				return d.unsupported("producer expression is volatile or references a different query block")
			}
		}
	}
	d.values = append(d.values, rowID)
	ordered := make([][2]int32, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i][1] < ordered[j][1] })
	for _, key := range ordered {
		d.params[key] = len(d.values)
		d.values = append(d.values, keys[key])
	}
	return nil
}

// A bound literal/parameter cast has a statement-constant value (or error).
// Replaying it for the same base rows is safe, unlike a potentially failing
// row-dependent cast. Use typed placeholders only for the admission proof; the
// executable domain retains the original casts and parameters unchanged.
func localCTEDomainPredicateReplaySafe(expr *plan.Expr) bool {
	proof := DeepCopyExpr(expr)
	walkLocalCTEExpr(proof, func(e *plan.Expr) {
		fn := e.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 || fn.Args[1].GetT() == nil {
			return
		}
		if fn.Args[0].GetLit() != nil || fn.Args[0].GetP() != nil {
			e.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
		}
	})
	return isTruncationSafePredicateExpr(proof)
}

// cloneDomain retains the bound table object, transaction snapshot and tenant
// predicates, but gives the replay a fresh binding tag. No catalog resolution is
// repeated and no user expression is evaluated for the purpose of identity.
func (d *localCTEDomain) cloneDomain() (int32, []*plan.Expr) {
	b := d.builder
	oldTag := d.values[0].GetCol().RelPos
	newTag := b.genNewBindTag()
	var clone func(int32) int32
	clone = func(id int32) int32 {
		n := DeepCopyNode(b.qry.Nodes[id])
		for i, child := range n.Children {
			n.Children[i] = clone(child)
		}
		for i, tag := range n.BindingTags {
			if tag == oldTag {
				n.BindingTags[i] = newTag
			}
		}
		exprs := append(localCTENodeExprs(n), n.BlockFilterList...)
		for _, e := range exprs {
			walkLocalCTEExpr(e, func(x *plan.Expr) {
				if col := x.GetCol(); col != nil && col.RelPos == oldTag {
					col.RelPos = newTag
				}
			})
		}
		// Ordering does not change a domain without LIMIT/OFFSET.
		if n.NodeType == plan.Node_SORT {
			return n.Children[0]
		}
		return b.appendNode(n, d.ctx)
	}
	id := clone(d.outerID)
	values := make([]*plan.Expr, len(d.values))
	for i, value := range d.values {
		values[i] = GetColExpr(value.Typ, newTag, value.GetCol().ColPos)
	}
	return id, values
}

func (d *localCTEDomain) inject(id int32) (int32, []*plan.Expr) {
	b := d.builder
	domain, values := d.cloneDomain()
	id = b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER,
		Children: []int32{id, domain}, OnList: []*plan.Expr{DeepCopyExpr(constTrue)}, SpillMem: b.joinSpillMem}, b.ctxByNode[id])
	return id, values
}

func (d *localCTEDomain) replace(n *plan.Node, values []*plan.Expr) {
	for _, e := range localCTENodeExprs(n) {
		walkLocalCTEExpr(e, func(x *plan.Expr) {
			if corr := x.GetCorr(); corr != nil {
				*x = *DeepCopyExpr(values[d.params[[2]int32{corr.RelPos, corr.ColPos}]])
			}
		})
	}
}

func (d *localCTEDomain) appendPayload(n *plan.Node, values []*plan.Expr) []*plan.Expr {
	payload := make([]*plan.Expr, len(values))
	for i, value := range values {
		pos := int32(len(n.ProjectList))
		n.ProjectList = append(n.ProjectList, DeepCopyExpr(value))
		payload[i] = GetColExpr(value.Typ, n.BindingTags[0], pos)
	}
	return payload
}

func (d *localCTEDomain) hasRecursiveScan(id int32) bool {
	n := d.builder.qry.Nodes[id]
	if n.NodeType == plan.Node_RECURSIVE_SCAN {
		return true
	}
	for _, child := range n.Children {
		if d.hasRecursiveScan(child) {
			return true
		}
	}
	return false
}

func (d *localCTEDomain) lowerTree(id int32, force bool) (int32, []*plan.Expr) {
	b := d.builder
	n := b.qry.Nodes[id]
	if values, ok := d.scans[id]; ok {
		return id, values
	}
	var values []*plan.Expr
	if n.NodeType == plan.Node_JOIN {
		leftForce := force && !d.hasRecursiveScan(n.Children[1])
		left, lv := d.lowerTree(n.Children[0], leftForce)
		right, rv := d.lowerTree(n.Children[1], force && len(lv) == 0)
		n.Children = []int32{left, right}
		values = lv
		if len(values) == 0 {
			values = rv
		} else if len(rv) > 0 {
			n.OnList = append(n.OnList, d.equalIdentity(lv[0], rv[0]))
		}
	} else if len(n.Children) == 1 {
		child, v := d.lowerTree(n.Children[0], force)
		n.Children[0], values = child, v
	}
	needs := force
	for _, e := range localCTENodeExprs(n) {
		needs = needs || hasCorrCol(e)
	}
	if len(values) == 0 && needs {
		if len(n.Children) == 1 {
			n.Children[0], values = d.inject(n.Children[0])
		} else {
			// A scan predicate cannot read the domain before the join. Move it
			// to a FILTER over the product, retaining all original predicates.
			filters := n.FilterList
			n.FilterList = nil
			id, values = d.inject(id)
			if len(filters) > 0 {
				filter := &plan.Node{NodeType: plan.Node_FILTER, Children: []int32{id}, FilterList: filters}
				d.replace(filter, values)
				id = b.appendNode(filter, b.ctxByNode[n.NodeId])
			}
		}
	}
	if len(values) > 0 {
		d.replace(n, values)
		if n.NodeType == plan.Node_PROJECT || n.NodeType == plan.Node_SINK {
			values = d.appendPayload(n, values)
		}
	}
	return id, values
}

func (d *localCTEDomain) equalIdentity(left, right *plan.Expr) *plan.Expr {
	eq := DeepCopyExpr(d.equality)
	eq.GetF().Args = []*plan.Expr{DeepCopyExpr(left), DeepCopyExpr(right)}
	return eq
}

func (d *localCTEDomain) lower(root int32) int32 {
	b := d.builder
	// All scan schemas are extended before traversing members: recursive back
	// edges refer to the final sink, not to an acyclic child subtree.
	ids := make([]int32, 0, len(d.nodes))
	for id := range d.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		n := b.qry.Nodes[id]
		switch n.NodeType {
		case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
			values := make([]*plan.Expr, len(d.values))
			for i, v := range d.values {
				values[i] = GetColExpr(v.Typ, n.BindingTags[0], int32(len(n.ProjectList)+i))
			}
			d.scans[id] = d.appendPayload(n, values)
			if n.TableDef != nil {
				n.TableDef = CloneTableDefForPlan(n.TableDef, true)
				for _, v := range d.values {
					n.TableDef.Cols = append(n.TableDef.Cols, &plan.ColDef{Typ: v.Typ, Hidden: true})
				}
			}
			if b.preserveScanProjection == nil {
				b.preserveScanProjection = make(map[int32]struct{})
			}
			b.preserveScanProjection[id] = struct{}{}
		}
	}
	for _, id := range ids {
		n := b.qry.Nodes[id]
		if n.NodeType != plan.Node_SINK {
			continue
		}
		d.lowerTree(id, true)
		if b.preserveSinkProjection == nil {
			b.preserveSinkProjection = make(map[int32]struct{})
		}
		b.preserveSinkProjection[id] = struct{}{}
	}
	id, values := d.lowerTree(root, true)
	outer := DeepCopyExpr(d.values[0])
	col := outer.GetCol()
	outer.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: col.RelPos, ColPos: col.ColPos, Depth: 1}}
	eq := d.equalIdentity(values[0], outer)
	for _, cte := range b.cteRefs {
		for i := range cte.occurrences {
			if d.nodes[cte.occurrences[i].rootID] {
				cte.occurrences[i].isCorrelated = true
			}
		}
	}
	return b.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{id}, FilterList: []*plan.Expr{eq}}, b.ctxByNode[root])
}
