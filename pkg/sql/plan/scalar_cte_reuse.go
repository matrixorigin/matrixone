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
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// ScalarCTESinkOption identifies the bounded, local fan-out contract. Unlike a
// general materialized CTE this producer emits at most one fixed-width row; it
// needs neither an independent replay store nor a spill/cleanup protocol.
const ScalarCTESinkOption = "scalar_cte"

func (builder *QueryBuilder) recordScalarCTE(ref *CTERef, nodeID int32) {
	if builder.scalarCTEOccurrences == nil {
		builder.scalarCTEOccurrences = make(map[*CTERef][]int32)
	}
	if _, ok := builder.scalarCTEOccurrences[ref]; !ok {
		builder.scalarCTEOrder = append(builder.scalarCTEOrder, ref)
	}
	builder.scalarCTEOccurrences[ref] = append(builder.scalarCTEOccurrences[ref], nodeID)
}

func (builder *QueryBuilder) reuseScalarCTEs() {
	// Do not renumber recursive/DML steps or broaden their publication contract.
	// A plain INSERT SELECT (including CTAS) may have one fully consuming step.
	if len(builder.scalarCTEOrder) == 0 || len(builder.qry.Steps) != 1 || builder.isForUpdate ||
		(builder.qry.StmtType != planpb.Query_SELECT && builder.qry.StmtType != planpb.Query_INSERT) ||
		(builder.optimizerHints != nil && builder.optimizerHints.scalarCTEReuse == 2) {
		return
	}
	if builder.qry.StmtType == planpb.Query_SELECT {
		value, err := builder.compCtx.ResolveVariable("sql_select_limit", true, false)
		limit, ok := value.(uint64)
		if err != nil || !ok || limit != math.MaxUint64 {
			return
		}
	}
	parents := make(map[int32]int32)
	var walk func(int32) bool
	walk = func(id int32) bool {
		node := builder.qry.Nodes[id]
		if len(node.SourceStep) != 0 {
			return false
		}
		for _, child := range node.Children {
			if _, ok := parents[child]; ok {
				return false
			}
			parents[child] = id
			if !walk(child) {
				return false
			}
		}
		return true
	}
	root := builder.qry.Steps[0]
	if !walk(root) {
		return
	}
	bounds := make(map[int32]uint64)
	for _, ref := range builder.scalarCTEOrder {
		var refs []int32
		for _, id := range builder.scalarCTEOccurrences[ref] {
			if _, reachable := parents[id]; reachable {
				refs = append(refs, id)
			}
		}
		if ref.isRecursive || len(refs) < 2 || len(refs) > 64 {
			continue
		}
		first := builder.qry.Nodes[refs[0]]
		if first.NodeType != planpb.Node_PROJECT || len(first.BindingTags) != 1 || len(first.ProjectList) == 0 || len(first.ProjectList) > 256 ||
			builder.cteScalarUnionRowBound(refs[0], bounds) != 1 {
			continue
		}
		width := 0
		for _, expr := range first.ProjectList {
			if expr.Typ.Id <= 0 || expr.Typ.Id > math.MaxUint8 {
				width = 0
				break
			}
			oid := types.T(expr.Typ.Id)
			if !oid.IsFixedLen() || oid.TypeLen() <= 0 {
				width = 0
				break
			}
			width += oid.TypeLen() + 1
		}
		if width == 0 || width > 64*1024 {
			continue
		}
		cost := 0.0
		if !builder.scalarCTESourceSafe(refs[0], &cost) || math.IsNaN(cost) || math.IsInf(cost, 0) ||
			cost*float64(len(refs)-1) <= float64(len(refs)) {
			continue
		}
		safe := true
		for _, id := range refs {
			otherCost := 0.0
			if !builder.scalarCTESourceSafe(id, &otherCost) || !builder.scalarCTEConsumerSafe(id, parents, bounds) ||
				!builder.scalarCTESourcesEqual(refs[0], id, make(map[int32]int32)) {
				safe = false
				break
			}
		}
		if !safe {
			continue
		}

		// Keep the original occurrence IDs/tags for the consumers. Only the
		// producer root is cloned; its private child tree is no longer reachable
		// from any inline consumer. The source root owns a fresh output tag.
		source := DeepCopyNode(first)
		tag := builder.genNewBindTag()
		source.BindingTags = []int32{tag}
		for i := range source.ProjectList {
			builder.nameByColRef[[2]int32{tag, int32(i)}] = builder.nameByColRef[[2]int32{first.BindingTags[0], int32(i)}]
		}
		ctx := builder.ctxByNode[refs[0]]
		sourceID := builder.appendNode(source, ctx)
		sinkID := appendSinkNodeWithTag(builder, ctx, sourceID, tag)
		builder.qry.Nodes[sinkID].ExtraOptions = ScalarCTESinkOption
		// Only readers seed output demand. A SINK output tag would make the
		// final remapper retain every source column before publication.
		builder.qry.Nodes[sinkID].BindingTags = nil
		builder.qry.Steps = []int32{sinkID, root}
		for _, id := range refs {
			original := builder.qry.Nodes[id]
			reader := &Node{NodeId: id, NodeType: planpb.Node_SINK_SCAN,
				BindingTags: original.BindingTags, ProjectList: DeepCopyExprList(original.ProjectList),
				SourceStep: []int32{0}, ExtraOptions: ScalarCTESinkOption}
			builder.qry.Nodes[id] = reader
			ReCalcNodeStats(id, builder, false, true, true)
		}
		// One bounded producer per query avoids shared-source dependency chains.
		return
	}
}

// This release deliberately accepts only read-only, total expression trees.
// Consequently widening one reader's projected demand to the union of all
// readers cannot expose a data error hidden in another occurrence. Unknown
// functions, fallible casts, checked SUM overflow and volatile calls stay inline.
func scalarCTEExprIsTotal(expr *Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *planpb.Expr_Col, *planpb.Expr_Lit, *planpb.Expr_P, *planpb.Expr_T, *planpb.Expr_Vec:
		return true
	case *planpb.Expr_List:
		for _, item := range e.List.List {
			if !scalarCTEExprIsTotal(item) {
				return false
			}
		}
		return true
	case *planpb.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		ov, ok := function.GetFunctionByIdWithoutError(e.F.Func.Obj)
		if !ok || ov.CannotFold() || ov.IsRealTimeRelated() || ov.IsWin() {
			return false
		}
		for _, arg := range e.F.Args {
			if !scalarCTEExprIsTotal(arg) {
				return false
			}
		}
		id, _ := function.DecodeOverloadID(e.F.Func.Obj)
		args := e.F.Args
		oid := types.T(expr.Typ.Id)
		switch id {
		case function.CASE, function.AND, function.OR, function.NOT, function.ISNULL, function.ISNOTNULL,
			function.COUNT, function.STARCOUNT:
			return true
		case function.IN, function.NOT_IN:
			// The binder stores a typed constant set as a tuple/vector, whose
			// outer Expr type is T_tuple rather than the element's scalar type.
			if len(args) != 2 || !scalarCTEComparableType(types.T(args[0].Typ.Id)) {
				return false
			}
			if list := args[1].GetList(); list != nil {
				for _, item := range list.List {
					if item.Typ.Id != args[0].Typ.Id {
						return false
					}
				}
				return true
			}
			return args[1].GetVec() != nil
		case function.EQUAL, function.NOT_EQUAL, function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
			return len(args) == 2 && args[0].Typ.Id == args[1].Typ.Id && scalarCTEComparableType(types.T(args[0].Typ.Id))
		case function.MIN, function.MAX:
			return len(args) == 1 && scalarCTEComparableType(types.T(args[0].Typ.Id))
		case function.SUM:
			if len(args) != 1 {
				return false
			}
			arg := args[0].Typ
			// Match this branch's actual accumulator contract, including merge
			// and DISTINCT Flush. Wider Decimal128 inputs use checked addition.
			return types.T(arg.Id).IsFloat() || (scalarCTEDecimalTypeValid(arg) &&
				(types.T(arg.Id) == types.T_decimal64 || arg.Width <= 28))
		case function.UNARY_MINUS, function.UNARY_PLUS:
			// Unary negation uses the fixed-width Minus operation, not ABS or
			// checked subtraction. It has no data-error path even at MinInt.
			return len(args) == 1 && (oid.IsInteger() || oid.IsFloat() || scalarCTEDecimalTypeValid(expr.Typ))
		case function.PLUS, function.MINUS, function.MULTI:
			if !oid.IsFloat() {
				return false
			}
			for _, arg := range args {
				if !types.T(arg.Typ.Id).IsFloat() {
					return false
				}
			}
			return true
		case function.CAST:
			if len(args) != 2 || args[1].GetT() == nil {
				return false
			}
			if lit := args[0].GetLit(); lit != nil && lit.Isnull {
				return true
			}
			from := types.T(args[0].Typ.Id)
			if scalarCTEDecimalTypeValid(expr.Typ) {
				if scalarCTEDecimalTypeValid(args[0].Typ) && args[0].Typ.Scale == expr.Typ.Scale &&
					args[0].Typ.Width <= expr.Typ.Width && (from == oid || from == types.T_decimal64) {
					return true
				}
				if lit := args[0].GetLit(); lit != nil {
					var digits string
					switch value := lit.Value.(type) {
					case *planpb.Literal_I64Val:
						digits = strings.TrimPrefix(strconv.FormatInt(value.I64Val, 10), "-")
					case *planpb.Literal_U64Val:
						digits = strconv.FormatUint(value.U64Val, 10)
					default:
						return false
					}
					return digits == "0" || len(digits) <= int(expr.Typ.Width-expr.Typ.Scale)
				}
			}
			return (from == oid && (from.IsInteger() || from.IsFloat())) ||
				(oid == types.T_float64 && (from.IsInteger() || from.IsFloat())) ||
				(from == types.T_varchar && oid == from && args[0].Typ.Width > 0 && expr.Typ.Width >= args[0].Typ.Width)
		case function.LIKE:
			if len(args) != 2 || !types.T(args[0].Typ.Id).IsMySQLString() {
				return false
			}
			pattern := args[1]
			for pattern.GetF() != nil && pattern.GetF().Func.ObjName == "cast" && len(pattern.GetF().Args) == 2 {
				// The recursive proof above already admitted only total casts.
				pattern = pattern.GetF().Args[0]
			}
			return pattern.GetLit() != nil && !pattern.GetLit().Isnull && !strings.Contains(pattern.GetLit().GetSval(), "\\")
		default:
			return false
		}
	default:
		return false
	}
}

func scalarCTEComparableType(oid types.T) bool {
	return oid.IsInteger() || oid.IsFloat() || oid.IsMySQLString() || oid == types.T_bool ||
		oid == types.T_decimal64 || oid == types.T_decimal128
}

func scalarCTEDecimalTypeValid(typ Type) bool {
	maxWidth := int32(0)
	switch types.T(typ.Id) {
	case types.T_decimal64:
		maxWidth = 18
	case types.T_decimal128:
		maxWidth = 38
	default:
		return false
	}
	return typ.Width > 0 && typ.Width <= maxWidth && typ.Scale >= 0 && typ.Scale <= typ.Width
}

func (builder *QueryBuilder) scalarCTESourceSafe(id int32, cost *float64) bool {
	node := builder.qry.Nodes[id]
	if node.Limit != nil || node.Offset != nil || len(node.SourceStep) != 0 ||
		len(node.OrderBy) != 0 || len(node.WinSpecList) != 0 || len(node.OnList) != 0 {
		return false
	}
	if len(node.GroupingFlag) != 0 && len(node.GroupingFlag) != len(node.GroupBy) {
		return false
	}
	for _, present := range node.GroupingFlag {
		// Ordinary GROUP BY marks every key present; a rolled-up key is false.
		if !present {
			return false
		}
	}
	switch node.NodeType {
	case planpb.Node_TABLE_SCAN:
		if len(node.Children) != 0 || node.TableDef == nil || node.TableDef.Partition != nil || node.ObjRef == nil {
			return false
		}
		if node.Stats != nil {
			*cost += node.Stats.Cost
		}
	case planpb.Node_PROJECT, planpb.Node_AGG, planpb.Node_FILTER:
		if len(node.Children) != 1 || !builder.scalarCTESourceSafe(node.Children[0], cost) {
			return false
		}
	default:
		return false
	}
	for _, exprs := range [][]*Expr{node.ProjectList, node.FilterList, node.GroupBy, node.AggList} {
		for _, expr := range exprs {
			if !scalarCTEExprIsTotal(expr) {
				return false
			}
		}
	}
	return true
}

// Before the first UNION ALL accept only projections, without row-domain
// reduction or early stopping. Reader isolation is established separately by
// the compiler's private copy boundary, including for projecting expressions.
// Source row counts and all LIMIT proofs are structural, never statistics-based.
func (builder *QueryBuilder) scalarCTEConsumerSafe(id int32, parents map[int32]int32, bounds map[int32]uint64) bool {
	crossedUnion := false
	for {
		parent, ok := parents[id]
		if !ok {
			return crossedUnion
		}
		id = parent
		node := builder.qry.Nodes[id]
		switch node.NodeType {
		case planpb.Node_PROJECT:
		case planpb.Node_UNION_ALL:
			if len(node.Children) != 2 {
				return false
			}
			crossedUnion = true
		case planpb.Node_WINDOW, planpb.Node_SORT, planpb.Node_AGG:
			if !crossedUnion || len(node.Children) != 1 {
				return false
			}
		case planpb.Node_MULTI_UPDATE:
			if !crossedUnion || builder.qry.StmtType != planpb.Query_INSERT || len(node.Children) != 1 ||
				len(node.UpdateCtxList) != 1 || len(node.UpdateCtxList[0].DeleteCols) != 0 {
				return false
			}
		default:
			return false
		}
		if len(node.FilterList) != 0 {
			return false
		}
		if node.Limit != nil {
			lit := node.Limit.GetLit()
			if !crossedUnion || lit == nil || lit.Isnull {
				return false
			}
			value, ok := lit.Value.(*planpb.Literal_U64Val)
			bound := builder.cteScalarUnionRowBound(id, bounds)
			if !ok || value.U64Val == 0 || bound == 0 || value.U64Val < bound {
				return false
			}
		}
		if node.Offset != nil && !crossedUnion {
			return false
		}
	}
}

// Zero denotes unknown, not an empty relation. LIMIT, OFFSET and estimates must
// never tighten this upper bound; a cycle or overflowing union also stays unknown.
func (builder *QueryBuilder) cteScalarUnionRowBound(id int32, memo map[int32]uint64) uint64 {
	if bound, ok := memo[id]; ok {
		return bound
	}
	memo[id] = 0
	if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil {
		return 0
	}
	node := builder.qry.Nodes[id]
	var bound uint64
	switch node.NodeType {
	case planpb.Node_AGG:
		if len(node.Children) == 1 && len(node.GroupBy) == 0 && len(node.GroupingFlag) == 0 {
			bound = 1
		}
	case planpb.Node_PROJECT, planpb.Node_FILTER, planpb.Node_WINDOW, planpb.Node_SORT:
		if len(node.Children) == 1 {
			bound = builder.cteScalarUnionRowBound(node.Children[0], memo)
		}
	case planpb.Node_UNION_ALL:
		if len(node.Children) == 2 {
			left := builder.cteScalarUnionRowBound(node.Children[0], memo)
			right := builder.cteScalarUnionRowBound(node.Children[1], memo)
			if left > 0 && right > 0 && left <= math.MaxUint64-right {
				bound = left + right
			}
		}
	}
	memo[id] = bound
	return bound
}

// Declaration identity alone is insufficient: an occurrence may bind against a
// shadowed name or a different snapshot. Compare the actual bound source modulo
// fresh binding tags, not SQL text, aliases, hashes or estimated cardinality.
func (builder *QueryBuilder) scalarCTESourcesEqual(left, right int32, tags map[int32]int32) bool {
	a, b := builder.qry.Nodes[left], builder.qry.Nodes[right]
	if a.NodeType != b.NodeType || len(a.Children) != len(b.Children) || len(a.BindingTags) != len(b.BindingTags) ||
		!reflect.DeepEqual(a.ObjRef, b.ObjRef) || !reflect.DeepEqual(a.ScanSnapshot, b.ScanSnapshot) ||
		!reflect.DeepEqual(a.TableDef, b.TableDef) || !reflect.DeepEqual(a.GroupingFlag, b.GroupingFlag) ||
		b.Limit != nil || b.Offset != nil || len(b.SourceStep) != 0 {
		return false
	}
	for i, tag := range a.BindingTags {
		if old, ok := tags[tag]; ok && old != b.BindingTags[i] {
			return false
		}
		tags[tag] = b.BindingTags[i]
	}
	for i := range a.Children {
		if !builder.scalarCTESourcesEqual(a.Children[i], b.Children[i], tags) {
			return false
		}
	}
	leftLists := [][]*Expr{a.ProjectList, a.FilterList, a.GroupBy, a.AggList}
	rightLists := [][]*Expr{b.ProjectList, b.FilterList, b.GroupBy, b.AggList}
	for i, list := range leftLists {
		if len(list) != len(rightLists[i]) {
			return false
		}
		for j, expr := range list {
			if !scalarCTEExprEqual(expr, rightLists[i][j], tags) {
				return false
			}
		}
	}
	return true
}

func scalarCTEExprEqual(a, b *Expr, tags map[int32]int32) bool {
	if a == nil || b == nil || !reflect.DeepEqual(a.Typ, b.Typ) {
		return false
	}
	if col := a.GetCol(); col != nil {
		other := b.GetCol()
		tag, ok := tags[col.RelPos]
		return ok && other != nil && tag == other.RelPos && col.ColPos == other.ColPos
	}
	if fn := a.GetF(); fn != nil {
		other := b.GetF()
		if other == nil || !reflect.DeepEqual(fn.Func, other.Func) || len(fn.Args) != len(other.Args) {
			return false
		}
		for i := range fn.Args {
			if !scalarCTEExprEqual(fn.Args[i], other.Args[i], tags) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a.Expr, b.Expr)
}

func (builder *QueryBuilder) remapScalarCTEReaders(sinkColRef map[[2]int32]int) error {
	for _, node := range builder.qry.Nodes {
		if node.NodeType != planpb.Node_SINK_SCAN || node.ExtraOptions != ScalarCTESinkOption {
			continue
		}
		for _, expr := range node.ProjectList {
			col := expr.GetCol()
			pos, ok := sinkColRef[[2]int32{node.SourceStep[0], col.ColPos}]
			if !ok {
				return moerr.NewInternalError(builder.GetContext(), "missing scalar CTE publication column")
			}
			col.ColPos = int32(pos)
		}
	}
	return nil
}
