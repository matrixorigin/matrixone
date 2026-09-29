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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// localCTEDomain describes a statement-local relation of (row identity, outer
// parameters). The identity, rather than parameter equality, isolates NULL and
// repeated parameter values across recursive iterations.
type localCTEDomain struct {
	builder *QueryBuilder
	outerID int32
	ctx     *BindContext
	subType plan.SubqueryRef_Type
	guarded bool
	// Outer-only consumer WHERE predicates constrain replay before producer
	// expressions run; the original consumer predicate is retained as well.
	demandFilters   []*plan.Expr
	consumerRoot    int32
	consumerFilters map[int32]bool
	values          []*plan.Expr
	params          map[[2]int32]int
	equality        *plan.Expr
	nodes           map[int32]bool
	scans           map[int32][]*plan.Expr
}

// Limit demand-domain preparation to local CTE consumers; ordinary subqueries
// must retain their existing predicate pushdown and index selection paths.
func (builder *QueryBuilder) hasLocalCTEConsumer(expr *plan.Expr) bool {
	if len(builder.localCTERoots) == 0 {
		return false
	}
	var contains func(int32) bool
	contains = func(id int32) bool {
		if builder.localCTERoots[id] {
			return true
		}
		for _, child := range builder.qry.Nodes[id].Children {
			if contains(child) {
				return true
			}
		}
		return false
	}
	found := false
	walkLocalCTEExpr(expr, func(e *plan.Expr) {
		if sub := e.GetSub(); sub != nil && contains(sub.NodeId) {
			found = true
		}
	})
	return found
}

func (builder *QueryBuilder) parameterizeLocalCTEs(
	outerID, subID int32, ctx *BindContext, subType plan.SubqueryRef_Type, guarded bool,
) (int32, error) {
	if len(builder.localCTERoots) == 0 {
		return subID, nil
	}
	domains := make(map[int32]*localCTEDomain)
	var admit func(int32) error
	admit = func(id int32) error {
		if builder.localCTERoots[id] {
			d := &localCTEDomain{builder: builder, outerID: outerID, ctx: ctx, subType: subType, guarded: guarded,
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
				// These consumers require their own per-identity rewrite. Letting
				// the generic predicate pull-up cross them merges outer rows or
				// leaves hidden columns out of the set-operation schema.
				if err := d.admitConsumer(subID); err != nil {
					return err
				}
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

// Inspect every path from the subquery root to this CTE. Moving HAVING
// through pagination or a branching/outer join boundary is not equivalent to
// evaluating the original subquery separately for each outer identity.
func (d *localCTEDomain) admitConsumer(root int32) error {
	d.consumerRoot = root
	d.consumerFilters = make(map[int32]bool)
	var visit func(id int32, filtered, paginated, branched, having, windowed, aggregated bool) error
	visit = func(id int32, filtered, paginated, branched, having, windowed, aggregated bool) error {
		if id < 0 || int(id) >= len(d.builder.qry.Nodes) {
			return nil
		}
		n := d.builder.qry.Nodes[id]
		if d.guarded && !localCTEReplaySafeNode(n, true) {
			return d.unsupported("consumer expression may fail outside the consumer evaluation domain")
		}
		if n.Limit != nil || n.Offset != nil {
			// An implicit scalar aggregate has at most one result row.
			// LIMIT 1 (OFFSET 0) above it preserves that row and is
			// rewritten into the per-identity window by decorrelation.
			var limit *plan.Literal
			if n.Limit != nil {
				limit = n.Limit.GetLit()
			}
			offsetZero := n.Offset == nil ||
				(n.Offset.GetLit() != nil && n.Offset.GetLit().GetU64Val() == 0)
			if aggregated || d.subType != plan.SubqueryRef_SCALAR || limit == nil ||
				limit.GetU64Val() != 1 || !offsetZero {
				paginated = true
			}
		}
		switch n.NodeType {
		case plan.Node_UNION, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
			plan.Node_MINUS, plan.Node_MINUS_ALL:
			return d.unsupported("consumer set operation needs per-outer-row output schema")
		case plan.Node_WINDOW:
			windowed = true
		case plan.Node_UNION_ALL:
			branched = true
		case plan.Node_JOIN:
			for _, on := range n.OnList {
				if hasCorrCol(on) {
					return d.unsupported("consumer join ON has an unhandled outer reference")
				}
			}
			if n.JoinType != plan.Node_INNER {
				return d.unsupported("consumer outer join cannot pull up an identity predicate")
			}
			if having {
				return d.unsupported("consumer HAVING across join branches")
			}
			branched = true
		case plan.Node_AGG:
			if windowed && len(n.AggList) != 0 {
				return d.unsupported("consumer aggregate window cannot use COUNT empty-input fallback")
			}
			if len(n.GroupBy) == 0 && (d.subType != plan.SubqueryRef_SCALAR || branched || windowed ||
				aggregated || paginated) {
				return d.unsupported("consumer scalar aggregate cannot preserve empty-input or result-row semantics")
			}
			aggregated = true
			having = filtered || len(n.FilterList) > 0
			if having && (paginated || branched) {
				return d.unsupported("consumer HAVING across pagination or branches")
			}
			if having && (len(n.AggList) != 1 || len(n.GroupBy) != 0 ||
				n.AggList[0].GetF() == nil || n.AggList[0].GetF().Func == nil ||
				(n.AggList[0].GetF().Func.ObjName != "count" && n.AggList[0].GetF().Func.ObjName != "starcount")) {
				return d.unsupported("consumer HAVING needs per-outer-row empty-group semantics")
			}
		case plan.Node_FILTER:
			if !having && !windowed && !branched && len(n.Children) == 1 && d.consumerFilterReadsCTE(n.Children[0]) {
				for _, cond := range n.FilterList {
					if hasCorrCol(cond) {
						d.consumerFilters[id] = true
					}
					if demand := d.outerOnlyDemand(cond); demand != nil {
						d.demandFilters = append(d.demandFilters, demand)
					}
				}
			}
			if having || windowed {
				for _, cond := range n.FilterList {
					if hasCorrCol(cond) {
						return d.unsupported("consumer correlated filter must run before aggregation or window")
					}
				}
			}
			filtered = true
		}
		if d.builder.localCTERoots[id] && id != root && id != d.outerID {
			return nil
		}
		for _, child := range n.Children {
			if d.nodes[child] {
				continue
			}
			if err := visit(child, filtered, paginated, branched, having, windowed, aggregated); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root, false, false, false, false, false, false)
}

// An outer-only predicate selects whole identity partitions. It commutes
// with per-identity pagination and pure DISTINCT, but not with an implicit
// aggregate's empty result row. Keep true aggregate/HAVING boundaries intact.
func (d *localCTEDomain) consumerFilterReadsCTE(id int32) bool {
	if d.nodes[id] {
		return true
	}
	n := d.builder.qry.Nodes[id]
	if len(n.Children) != 1 {
		return false
	}
	switch n.NodeType {
	case plan.Node_PROJECT, plan.Node_FILTER, plan.Node_SORT, plan.Node_DISTINCT, plan.Node_WINDOW:
		return d.consumerFilterReadsCTE(n.Children[0])
	default:
		return false
	}
}

// A consumer predicate can constrain the producer domain only when it reads
// exclusively the immediate outer row and cannot observe CTE data. Conditional
// consumers require totality; unconditional demand also admits parameter casts.
func (d *localCTEDomain) outerOnlyDemand(expr *plan.Expr) *plan.Expr {
	rowID := d.builder.correlatedScalarOuterRowID(d.outerID, d.ctx)
	if rowID == nil || !hasCorrCol(expr) {
		return nil
	}
	copy := DeepCopyExpr(expr)
	valid := true
	walkLocalCTEExpr(copy, func(e *plan.Expr) {
		switch x := e.Expr.(type) {
		case *plan.Expr_Corr:
			if x.Corr.Depth != 1 || x.Corr.RelPos != rowID.GetCol().RelPos {
				valid = false
				return
			}
			e.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: x.Corr.RelPos, ColPos: x.Corr.ColPos}}
		case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_P, *plan.Expr_F, *plan.Expr_List:
		default:
			valid = false
		}
	})
	if !valid {
		return nil
	}
	// Without a conditional consumer, this outer-only WHERE is the demand
	// predicate itself. Statement-constant parameter conversions must retain
	// their runtime value/error, not be required to fit every possible type
	// value at PREPARE time. Conditional consumers still require totality.
	proof := copy
	if !d.guarded {
		proof = DeepCopyExpr(copy)
		walkLocalCTEExpr(proof, func(e *plan.Expr) {
			fn := e.GetF()
			if fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
				fn.Args[0].GetP() != nil && fn.Args[1].GetT() != nil {
				e.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
			}
		})
	}
	if !localCTEGuardedPredicateSafe(proof) {
		// This predicate can suppress an entire partition, but its demand
		// cannot be replayed safely. Do not silently start recursive work
		// for all outer rows just because no demand filter was produced.
		d.guarded = true
		return nil
	}
	return copy
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
			// Expression totality cannot prove recursive termination: an
			// unconsumed outer partition can still hit the recursion cap.
			if d.guarded && (n.NodeType == plan.Node_RECURSIVE_SCAN || n.NodeType == plan.Node_RECURSIVE_CTE) {
				return d.unsupported("recursive producer may fail outside the consumer evaluation domain")
			}
		default:
			return d.unsupported("producer operator " + n.NodeType.String())
		}
		// The producer is evaluated for the replayed outer domain before
		// CASE (or another consumer) decides which rows use this CTE.
		// Volatility alone cannot prove an expression is total: ABS on
		// MinInt64, arithmetic overflow and casts can all fail.
		if d.guarded && !localCTEReplaySafeNode(n, false) {
			return d.unsupported("producer expression may fail outside the consumer evaluation domain")
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
	// Consumer WHERE must retain its position below potentially throwing
	// projections, even when a predicate mixes outer columns and CTE values.
	// Carry those outer columns in the payload rather than pulling WHERE up.
	for id := range d.consumerFilters {
		for _, expr := range b.qry.Nodes[id].FilterList {
			valid := true
			walkLocalCTEExpr(expr, func(e *plan.Expr) {
				if corr := e.GetCorr(); corr != nil {
					if corr.Depth != 1 || corr.RelPos != outerTag {
						valid = false
						return
					}
					keys[[2]int32{corr.RelPos, corr.ColPos}] = GetColExpr(e.Typ, corr.RelPos, corr.ColPos)
				}
			})
			if !valid {
				return d.unsupported("consumer WHERE references a different query block")
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

// Reuse the totality proof from aggregate truncation safety on both sides of
// the producer boundary. Only the proof copy changes correlated references;
// the executable expression and its depth remain untouched.
func localCTEReplaySafeNode(n *plan.Node, consumer bool) bool {
	proof := func(expr *plan.Expr) *plan.Expr {
		copy := DeepCopyExpr(expr)
		walkLocalCTEExpr(copy, func(e *plan.Expr) {
			if corr := e.GetCorr(); corr != nil {
				e.Expr = &plan.Expr_Col{Col: &plan.ColRef{RelPos: corr.RelPos, ColPos: corr.ColPos}}
			}
		})
		return copy
	}
	for _, expr := range n.ProjectList {
		if expr.GetP() == nil && !isTruncationSafeRowExpr(proof(expr)) {
			return false
		}
	}
	for _, expr := range n.FilterList {
		if !localCTEGuardedPredicateSafe(proof(expr)) {
			return false
		}
	}
	for _, expr := range n.OnList {
		if !localCTEGuardedPredicateSafe(proof(expr)) {
			return false
		}
	}
	if consumer {
		for _, expr := range n.GroupBy {
			if !isTruncationSafeRowExpr(proof(expr)) {
				return false
			}
		}
		for _, expr := range n.AggList {
			fn := expr.GetF()
			if fn == nil || fn.Func == nil ||
				(fn.Func.ObjName != "count" && fn.Func.ObjName != "starcount") {
				return false
			}
			for _, arg := range fn.Args {
				if !isTruncationSafeRowExpr(proof(arg)) {
					return false
				}
			}
		}
		if len(n.WinSpecList) != 0 {
			return false
		}
	}
	return true
}

// Constant casts in a replayed predicate are harmless only when their entire
// input type is safe. Do not use the outer-domain proof's statement-constant
// shortcut for a skipped consumer: even a constant error could run too early.
func localCTEGuardedPredicateSafe(expr *plan.Expr) bool {
	total := true
	walkLocalCTEExpr(expr, func(e *plan.Expr) {
		fn := e.GetF()
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
			(fn.Args[0].GetLit() != nil || fn.Args[0].GetP() != nil) &&
			!singleRowCastIsTotal(fn.Args[0].Typ, e.Typ) {
			lit := fn.Args[0].GetLit()
			// A bound ASCII string literal fits a wider string target even
			// when the generic type-domain cast proof cannot prove all bytes.
			if lit == nil || !(lit.Isnull || localCTEStringLiteralFits(lit, e.Typ) ||
				localCTEIntegerLiteralFits(lit, types.T(e.Typ.Id))) {
				total = false
			}
		}
	})
	return total && localCTEDomainPredicateReplaySafe(expr)
}

func localCTEStringLiteralFits(lit *plan.Literal, target plan.Type) bool {
	_, ok := lit.Value.(*plan.Literal_Sval)
	return ok && types.T(target.Id).IsMySQLString() &&
		target.Width >= int32(len(lit.GetSval())) && isASCII(lit.GetSval())
}

// Only bound integer literals, not arbitrary integer columns or parameters,
// can discharge a narrowing cast with a value-level proof.
func localCTEIntegerLiteralFits(lit *plan.Literal, target types.T) bool {
	var value int64
	switch x := lit.Value.(type) {
	case *plan.Literal_I8Val:
		value = int64(x.I8Val)
	case *plan.Literal_I16Val:
		value = int64(x.I16Val)
	case *plan.Literal_I32Val:
		value = int64(x.I32Val)
	case *plan.Literal_I64Val:
		value = x.I64Val
	default:
		return false
	}
	switch target {
	case types.T_int8:
		return value >= -128 && value <= 127
	case types.T_int16:
		return value >= -32768 && value <= 32767
	case types.T_int32:
		return value >= -2147483648 && value <= 2147483647
	case types.T_int64:
		return true
	case types.T_uint8:
		return value >= 0 && value <= 255
	case types.T_uint16:
		return value >= 0 && value <= 65535
	case types.T_uint32:
		return value >= 0 && value <= 4294967295
	case types.T_uint64:
		return value >= 0
	default:
		return false
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			return false
		}
	}
	return true
}

// A bound literal/parameter cast has a statement-constant value (or error).
// Replaying it for the same base rows is safe, unlike a potentially failing
// row-dependent cast. Use typed placeholders only for the admission proof; the
// executable domain retains the original casts and parameters unchanged.
func localCTEDomainPredicateReplaySafe(expr *plan.Expr) bool {
	proof := DeepCopyExpr(expr)
	walkLocalCTEExpr(proof, func(e *plan.Expr) {
		if localCTEWidenedAbsIsTotal(e) {
			e.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
			return
		}
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

// ABS accepts signed integers as INT64. An INT8/16/32 operand widened to
// INT64 cannot produce MinInt64, so its absolute value is representable for
// the entire input domain. The operand itself must also be total; a cast
// from INT64 or an overflowing expression does not establish this proof.
func localCTEWidenedAbsIsTotal(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "abs" || len(fn.Args) != 1 ||
		types.T(expr.Typ.Id) != types.T_int64 {
		return false
	}
	arg := fn.Args[0]
	cast := arg.GetF()
	if cast == nil || cast.Func == nil || cast.Func.ObjName != "cast" || len(cast.Args) != 2 ||
		cast.Args[1].GetT() == nil || types.T(arg.Typ.Id) != types.T_int64 {
		return false
	}
	switch types.T(cast.Args[0].Typ.Id) {
	case types.T_int8, types.T_int16, types.T_int32:
		return isTruncationSafeRowExpr(arg)
	default:
		return false
	}
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
	if len(d.demandFilters) > 0 {
		filters := DeepCopyExprList(d.demandFilters)
		for _, filter := range filters {
			walkLocalCTEExpr(filter, func(e *plan.Expr) {
				if col := e.GetCol(); col != nil && col.RelPos == oldTag {
					col.RelPos = newTag
				}
			})
		}
		id = b.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{id},
			FilterList: filters, FilterIsBarrier: true}, d.ctx)
	}
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
		Children: []int32{id, domain}, OnList: []*plan.Expr{makePlan2BoolConstExprWithType(true)}, SpillMem: b.joinSpillMem}, b.ctxByNode[id])
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

// Forward the domain payload through consumer projections, DISTINCT, and
// existing per-identity pagination. WHERE reads local columns and cannot be
// pulled above a throwing SELECT expression by correlated-predicate decorrelation.
func (d *localCTEDomain) lowerConsumerFilters(id, root int32, values []*plan.Expr) []*plan.Expr {
	if id == root {
		return values
	}
	n := d.builder.qry.Nodes[id]
	if len(n.Children) != 1 {
		return nil
	}
	payload := d.lowerConsumerFilters(n.Children[0], root, values)
	if len(payload) == 0 {
		return nil
	}
	if d.consumerFilters[id] {
		for _, expr := range n.FilterList {
			walkLocalCTEExpr(expr, func(e *plan.Expr) {
				if corr := e.GetCorr(); corr != nil {
					*e = *DeepCopyExpr(payload[d.params[[2]int32{corr.RelPos, corr.ColPos}]])
				}
			})
		}
	}
	switch n.NodeType {
	case plan.Node_PROJECT:
		return d.appendPayload(n, payload)
	case plan.Node_DISTINCT:
		// rewriteDistinctToAGG groups the child projection, including the
		// identity payload. PAD SPACE uses an explicit physical-key list;
		// extend that list too, or equal text would merge different rows.
		if len(n.PhysicalEqualityKeyList) > 0 {
			project := d.builder.qry.Nodes[n.Children[0]]
			for _, value := range payload {
				n.PhysicalEqualityKeyList = append(n.PhysicalEqualityKeyList,
					DeepCopyExpr(project.ProjectList[value.GetCol().ColPos]))
			}
		}
		return payload
	case plan.Node_FILTER, plan.Node_SORT, plan.Node_WINDOW:
		// WINDOW preserves its input columns. Identity predicates below it
		// are still pulled up to extend the existing per-identity partition;
		// the consumer WHERE remains above the completed window calculation.
		return payload
	default:
		return nil
	}
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
	if len(d.consumerFilters) > 0 {
		d.lowerConsumerFilters(d.consumerRoot, root, values)
	}
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
