// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"context"
	"errors"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// These limits bound analysis and the added candidate data per query. String
// values are charged using their actual bytes, not the column's declared width.
const (
	maxCompositePointNodes = 2048
	maxCompositePointDepth = 256
	maxCompositePoints     = 128
	compositePointCopies   = 4
	// The budget is shared by all candidates in one query, not per scan. The
	// charge below allows for the row and block filters and their working copies.
	// Execution fan-out can still multiply memory across readers and nodes.
	maxCompositePointBytes = 8 << 20
)

type compositePointAnalysis struct {
	ctx     context.Context
	proc    *process.Process
	tag     int32
	parts   map[int32]int
	types   []plan.Type
	values  []*plan.Expr
	visited int
}

type compositePointSet struct {
	values     [][]*plan.Expr
	checked    []*plan.Expr
	shapeBytes uint64
}

// compositePointCandidate identifies a value-independent shape. Its references
// must be frozen before ordinary lowering can mutate the original predicates.
// A nil result keeps the existing access path.
func compositePointCandidate(ctx context.Context, node *plan.Node, remaining uint64) *compositePointSet {
	if node == nil || node.TableDef == nil || node.TableDef.Pkey == nil ||
		len(node.BindingTags) == 0 || node.TableDef.ClusterBy != nil ||
		remaining < 2*compositePointCopies*types.DefaultPackerCapacity() {
		return nil
	}
	table := node.TableDef
	key := table.Pkey
	if len(key.Names) < 2 || len(key.Names) > maxCompositePointNodes ||
		key.PkeyColName != catalog.CPrimaryKeyColName || key.CompPkeyCol == nil ||
		tableDefHasNativeUnicodeKeyPart(table, key.Names) {
		return nil
	}
	// Most composite-key scans contain no disjunction. Avoid allocating the
	// fact map or examining scalar expressions on that common path.
	nodesRemaining := maxCompositePointNodes
	hasDisjunction := false
	for _, filter := range node.FilterList {
		if compositePointHasOr(filter, 0, &nodesRemaining) {
			hasDisjunction = true
			break
		}
	}
	if !hasDisjunction {
		return nil
	}
	keyPos, ok := table.Name2ColIndex[key.PkeyColName]
	if !ok || keyPos < 0 || int(keyPos) >= len(table.Cols) || table.Cols[keyPos] == nil ||
		!table.Cols[keyPos].Hidden || table.Cols[keyPos].Name != key.PkeyColName ||
		types.T(table.Cols[keyPos].Typ.Id) != types.T_varchar {
		return nil
	}
	a := compositePointAnalysis{
		ctx:   ctx,
		tag:   node.BindingTags[0],
		parts: make(map[int32]int, len(key.Names)),
		types: make([]plan.Type, len(key.Names)),
	}
	for i, name := range key.Names {
		pos, found := table.Name2ColIndex[name]
		if !found || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].Name != name {
			return nil
		}
		if _, duplicate := a.parts[pos]; duplicate || !compositePointType(table.Cols[pos].Typ) {
			return nil
		}
		a.parts[pos], a.types[i] = i, table.Cols[pos].Typ
	}
	points, ok := a.conjunction(node.FilterList, make([]*plan.Expr, len(key.Names)), false, 0)
	if !ok || len(points) < 2 {
		return nil
	}
	// The point count is known without inspecting parameter values. If even
	// the minimum packers do not fit, the fallback is not value-dependent.
	if uint64(len(points)) > remaining/(compositePointCopies*types.DefaultPackerCapacity()) {
		return nil
	}
	// A point candidate can suppress evaluation on rows it prunes. Prove every
	// predicate's shape is safe before any execution-bound value is inspected.
	for _, filter := range node.FilterList {
		if !a.safePredicate(filter, 0) {
			return nil
		}
	}
	// Charge independently owned value expressions and tuple references before
	// copying them. Values used only by an unselected equality are needed too:
	// early pruning must not suppress a fallible evaluation of that equality.
	var bytes uint64
	checked := make([]*plan.Expr, 0, len(a.values))
	seen := make(map[*plan.Expr]struct{}, len(a.values))
	for _, value := range a.values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		cost := uint64(value.ProtoSize()) + 1024
		if cost < 1024 || cost > remaining-bytes {
			return nil
		}
		bytes += cost
		checked = append(checked, value)
	}
	for _, point := range points {
		cost := uint64(len(point))*8 + 24
		if cost > remaining-bytes {
			return nil
		}
		bytes += cost
	}
	return &compositePointSet{values: points, checked: checked, shapeBytes: bytes}
}

// candidateBytes validates the current execution's values only after the scan
// and its witnesses survive access selection. The returned charge is separate
// from the already reserved frozen shape and witness memory.
func (p *compositePointSet) candidateBytes(ctx context.Context, proc *process.Process, remaining uint64) (uint64, bool) {
	a := compositePointAnalysis{ctx: ctx, proc: proc, values: p.checked}
	valueSizes, ok := a.safeValueSizes()
	if !ok {
		return 0, false
	}
	var bytes uint64
	for _, point := range p.values {
		var tupleBytes, largestPart uint64
		for _, value := range point {
			bound, supported := valueSizes[value]
			// Account for expression and encoded data copies in the row filter,
			// block filter, and compilation. The allowance also bounds list nodes.
			unit := bound + uint64(value.ProtoSize()) + 128
			if !supported || unit < bound || unit > (remaining-bytes)/compositePointCopies || bound > maxCompositePointBytes-tupleBytes {
				return 0, false
			}
			bytes += compositePointCopies * unit
			tupleBytes += bound
			largestPart = max(largestPart, bound)
		}
		// Each SERIAL executor owns a packer, even for tiny or NULL values.
		packerBytes, ok := types.PackerCapacityUpperBound(tupleBytes, largestPart)
		if !ok || packerBytes > (remaining-bytes)/compositePointCopies {
			return 0, false
		}
		bytes += compositePointCopies * packerBytes
	}
	return bytes, true
}

func compositePointValueSize(ctx context.Context, proc *process.Process, expr *plan.Expr) (uint64, bool) {
	paramExpr := expr
	var castWidth int32
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 {
		paramExpr = fn.Args[0]
		castWidth = expr.Typ.Width
	}
	if param := paramExpr.GetP(); param != nil {
		state := preparedBindingState(ctx)
		if state == nil {
			return 0, false
		}
		// Both accepting and rejecting a candidate can depend on this value.
		// A type-only cached fallback must not hide a later smaller binding.
		state.valueDependent = true
		binding, ok := state.bindingForPosition(param.Pos)
		if !ok || param.Pos < 0 || int(param.Pos) >= len(state.values) ||
			!binding.Type.Oid.IsMySQLString() || proc == nil || proc.Base == nil ||
			(paramExpr == expr && binding.Type.Oid != types.T(expr.Typ.Id)) {
			return 0, false
		}
		value, err := proc.GetPrepareParamsAt(int(param.Pos))
		if err != nil {
			return 0, false
		}
		if value == nil {
			return 0, true
		}
		bound, ok := compositePointStringSize(len(value))
		if !ok || castWidth > 0 && (uint64(len(value)) > uint64(castWidth)*utf8.UTFMax || utf8.RuneCount(value) > int(castWidth)) {
			return 0, false
		}
		return bound, true
	}
	if lit := expr.GetLit(); lit != nil && types.T(expr.Typ.Id).IsMySQLString() {
		if lit.Isnull {
			return 0, true
		}
		value, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return 0, false
		}
		// Folded literals need not be bounded by their expression's declared
		// width. Count their actual bytes, including worst-case zero escaping.
		return compositePointStringSize(len(value.Sval))
	}
	return function.SerialEncodedTypeSizeBound(makeTypeByPlan2Expr(expr))
}

func compositePointStringSize(length int) (uint64, bool) {
	if length < 0 || uint64(length) > (maxCompositePointBytes-3)/2 {
		return 0, false
	}
	return 2*uint64(length) + 3, true
}

func compositePointHasOr(expr *plan.Expr, depth int, remaining *int) bool {
	if expr == nil || depth > maxCompositePointDepth || *remaining <= 0 {
		return false
	}
	*remaining = *remaining - 1
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		return true
	}
	if fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if compositePointHasOr(arg, depth+1, remaining) {
				return true
			}
		}
	}
	return false
}

func compositePointType(typ plan.Type) bool {
	switch types.T(typ.Id) {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_year, types.T_uuid,
		types.T_varchar, types.T_binary, types.T_varbinary:
		return true
	default:
		// CHAR padding, float signed zero, JSON and arrays do not have the
		// byte-equality contract needed by serialized point lookups.
		return false
	}
}

func (a *compositePointAnalysis) step(depth int) bool {
	a.visited++
	return depth <= maxCompositePointDepth && a.visited <= maxCompositePointNodes
}

func (a *compositePointAnalysis) safePredicate(expr *plan.Expr, depth int) bool {
	if expr == nil || !a.step(depth) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "and", "or":
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if len(fn.Args) < 2 || fn.Func.ObjName == "and" && id != function.AND || fn.Func.ObjName == "or" && id != function.OR {
			return false
		}
		for _, arg := range fn.Args {
			if !a.safePredicate(arg, depth+1) {
				return false
			}
		}
		return true
	case "=":
		_, value, ok := a.equality(expr)
		if ok {
			// Even an equality not selected for the candidate can be skipped
			// on pruned rows. Check its value after the whole shape is admitted.
			a.values = append(a.values, value)
			return true
		}
	}
	// Other predicates may remain as residuals only when the planner's
	// existing totality proof says skipping their evaluation is unobservable.
	return a.localResidual(expr, depth+1) && isTruncationSafePredicateExpr(expr)
}

func (a *compositePointAnalysis) safeValueSizes() (map[*plan.Expr]uint64, bool) {
	sizes := make(map[*plan.Expr]uint64, len(a.values))
	for _, value := range a.values {
		if _, exists := sizes[value]; exists {
			continue
		}
		size, safe := compositePointValueSize(a.ctx, a.proc, value)
		if !safe {
			return nil, false
		}
		sizes[value] = size
	}
	return sizes, true
}

// Bound and restrict the tree before invoking the shared totality proof.
func (a *compositePointAnalysis) localResidual(expr *plan.Expr, depth int) bool {
	if expr == nil || !a.step(depth) {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col != nil && e.Col.RelPos == a.tag
	case *plan.Expr_Lit:
		return e.Lit != nil && e.Lit.Src == nil
	case *plan.Expr_P:
		return e.P != nil
	case *plan.Expr_T:
		return e.T != nil
	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		for _, arg := range e.F.Args {
			if !a.localResidual(arg, depth+1) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		if e.List == nil {
			return false
		}
		for _, arg := range e.List.List {
			if !a.localResidual(arg, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// equality accepts only a bare local column and a total, stable scalar in the
// same physical comparison domain. It examines commuted equality without
// reordering the original expression.
func (a *compositePointAnalysis) equality(expr *plan.Expr) (int32, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return 0, nil, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.EQUAL {
		return 0, nil, false
	}
	colExpr, value := fn.Args[0], fn.Args[1]
	if colExpr.GetCol() == nil {
		colExpr, value = value, colExpr
	}
	col := colExpr.GetCol()
	if col == nil || col.RelPos != a.tag || !compositePointType(colExpr.Typ) ||
		colExpr.Typ.Id != value.Typ.Id || colExpr.Typ.Scale != value.Typ.Scale || !compositePointScalar(value) {
		return 0, nil, false
	}
	if part, ok := a.parts[col.ColPos]; ok {
		actual := a.types[part]
		if actual.Id != colExpr.Typ.Id || actual.Scale != colExpr.Typ.Scale ||
			!actual.SameCollation(colExpr.Typ) || !actual.SameCollation(value.Typ) ||
			hasPadSpaceStringProvenance(colExpr) || hasPadSpaceStringProvenance(value) ||
			!serializedIndexComparisonUsable(a.ctx, fn, actual) {
			return 0, nil, false
		}
	} else {
		return 0, nil, false
	}
	return col.ColPos, value, true
}

func compositePointScalar(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil
	}
	// String parameters are sized from this execution's transport vector.
	if expr.GetP() != nil {
		return types.T(expr.Typ.Id) == types.T_varchar || types.T(expr.Typ.Id) == types.T_varbinary
	}
	// Binary protocol binding can leave its ordinary TEXT -> VARCHAR transport
	// cast in the predicate. Only the non-explicit, normal overload is eligible;
	// the current value must also fit its target width without transformation.
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || fn.SyntaxExplicitCast ||
		len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[0].GetP() == nil ||
		fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
		types.T(fn.Args[0].Typ.Id) != types.T_text || types.T(expr.Typ.Id) != types.T_varchar ||
		fn.Args[1].Typ.Id != expr.Typ.Id || fn.Args[1].Typ.Width != expr.Typ.Width ||
		!fn.Args[1].Typ.SameCollation(expr.Typ) || expr.Typ.Width <= 0 {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST && overload == 0
}

func (a *compositePointAnalysis) factors(expr *plan.Expr, out *[]*plan.Expr, depth int) bool {
	if expr == nil || !a.step(depth) {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if !a.factors(arg, out, depth+1) {
				return false
			}
		}
		return true
	}
	*out = append(*out, expr)
	return true
}

// conjunction selects one independently sufficient OR, never a Cartesian
// product of sibling ORs. All arms of the chosen OR must yield a full key.
func (a *compositePointAnalysis) conjunction(exprs []*plan.Expr, inherited []*plan.Expr, arm bool, depth int) ([][]*plan.Expr, bool) {
	var factors []*plan.Expr
	for _, expr := range exprs {
		if !a.factors(expr, &factors, depth+1) {
			return nil, false
		}
	}
	facts := append([]*plan.Expr(nil), inherited...)
	for _, factor := range factors {
		if pos, value, ok := a.equality(factor); ok {
			if part, found := a.parts[pos]; found && facts[part] == nil {
				facts[part] = value
			}
		}
	}
	complete := true
	for _, fact := range facts {
		complete = complete && fact != nil
	}
	if complete {
		if arm {
			return [][]*plan.Expr{facts}, true
		}
		// The existing singleton lowering is already a point lookup.
		return nil, false
	}
	for _, factor := range factors {
		fn := factor.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "or" {
			continue
		}
		if points, ok := a.disjunction(fn, facts, depth+1); ok {
			return points, true
		}
	}
	return nil, false
}

func (a *compositePointAnalysis) disjunction(fn *plan.Function, inherited []*plan.Expr, depth int) ([][]*plan.Expr, bool) {
	var points [][]*plan.Expr
	if !a.appendDisjunction(fn, inherited, depth, &points) {
		return nil, false
	}
	return points, len(points) > 0
}

// appendDisjunction flattens directly nested ORs into one result. Conjunctions
// keep their own alternatives and point limit, as in the ordinary analysis.
func (a *compositePointAnalysis) appendDisjunction(fn *plan.Function, inherited []*plan.Expr, depth int, points *[][]*plan.Expr) bool {
	if !a.step(depth) {
		return false
	}
	start := len(*points)
	for _, arg := range fn.Args {
		if f := arg.GetF(); f != nil && f.Func != nil && f.Func.ObjName == "or" {
			if !a.appendDisjunction(f, inherited, depth+1, points) {
				return false
			}
			continue
		}
		branch, ok := a.conjunction([]*plan.Expr{arg}, inherited, true, depth+1)
		if !ok || len(*points)+len(branch) > maxCompositePoints {
			return false
		}
		*points = append(*points, branch...)
	}
	return len(*points) > start
}

// A proposal is kept outside the plan while the ordinary optimizer chooses
// access paths. Its witnesses are the scan filters after normal lowering.
type compositePointProposal struct {
	node      *plan.Node
	tableID   uint64
	object    *plan.ObjectRef
	snapshot  *plan.Snapshot
	key       *plan.PrimaryKeyDef
	columns   []compositePointColumn
	tag       int32
	shape     *compositePointSet
	witnesses [][]byte
	// Revalidation uses temporary wire buffers. Reserve their high-water mark
	// once per proposal, even when the same proposal is checked again.
	comparisonBytes uint64
}

type compositePointColumn struct {
	position int32
	id       uint64
	name     string
	typ      *plan.Type
}

type compositePointCapture struct {
	remaining *uint64
	proposals map[int32]*compositePointProposal
	order     []int32
}

// collect is called during the existing component-filter walk, before the
// ordinary composite-key rewrite. It does not alter the scan or its costs.
func (c *compositePointCapture) collect(builder *QueryBuilder, id int32, node *plan.Node) {
	if c == nil || c.remaining == nil || len(node.BindingTags) != 1 {
		return
	}
	points := compositePointCandidate(builder.GetContext(), node, *c.remaining)
	if points == nil || points.shapeBytes > *c.remaining {
		return
	}
	table := node.TableDef
	metadataBytes := uint64(table.Pkey.ProtoSize()) + 1024
	if node.ObjRef != nil {
		metadataBytes += uint64(node.ObjRef.ProtoSize())
	}
	if node.ScanSnapshot != nil {
		metadataBytes += uint64(node.ScanSnapshot.ProtoSize())
	}
	for _, name := range append(append([]string(nil), table.Pkey.Names...), table.Pkey.PkeyColName) {
		pos := table.Name2ColIndex[name]
		metadataBytes += uint64(table.Cols[pos].Typ.ProtoSize()) + uint64(len(name)) + 64
	}
	if metadataBytes > *c.remaining-points.shapeBytes {
		return
	}
	// Freeze each distinct source once. Tuple references share only this owned
	// copy; binding will take an independent copy for each tuple.
	frozen := make(map[*plan.Expr]*plan.Expr, len(points.checked))
	for i, value := range points.checked {
		copy := DeepCopyExpr(value)
		frozen[value] = copy
		points.checked[i] = copy
	}
	for _, point := range points.values {
		for i, value := range point {
			copy, ok := frozen[value]
			if !ok {
				return
			}
			point[i] = copy
		}
	}
	proposal := &compositePointProposal{
		node: node, tableID: table.TblId, tag: node.BindingTags[0], shape: points,
		key: proto.Clone(table.Pkey).(*plan.PrimaryKeyDef),
	}
	if node.ObjRef != nil {
		proposal.object = proto.Clone(node.ObjRef).(*plan.ObjectRef)
	}
	if node.ScanSnapshot != nil {
		proposal.snapshot = proto.Clone(node.ScanSnapshot).(*plan.Snapshot)
	}
	for _, name := range append(append([]string(nil), table.Pkey.Names...), table.Pkey.PkeyColName) {
		pos := table.Name2ColIndex[name]
		col := table.Cols[pos]
		proposal.columns = append(proposal.columns, compositePointColumn{
			position: pos, id: col.ColId, name: col.Name,
			typ: proto.Clone(&col.Typ).(*plan.Type),
		})
	}
	*c.remaining -= points.shapeBytes + metadataBytes
	if c.proposals == nil {
		c.proposals = make(map[int32]*compositePointProposal)
	}
	c.proposals[id] = proposal
	c.order = append(c.order, id)
}

func (builder *QueryBuilder) bindCompositePointCandidate(node *plan.Node, points [][]*plan.Expr) (*plan.Expr, error) {
	pos := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	key := &plan.Expr{Typ: node.TableDef.Cols[pos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
		RelPos: node.BindingTags[0], ColPos: pos, Name: node.TableDef.Pkey.PkeyColName,
	}}}
	values := make([]*plan.Expr, len(points))
	ctx := builder.GetContext()
	for i, point := range points {
		value, err := BindFuncExprImplByPlanExpr(ctx, "serial", DeepCopyExprList(point))
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	keyType := makeTypeByPlan2Expr(key)
	overload, err := function.GetFunctionByName(ctx, "in", []types.Type{keyType, keyType})
	if err != nil {
		return nil, err
	}
	overloadID := overload.GetEncodedOverloadID()
	resultType := overload.GetReturnType()
	args := []*plan.Expr{key, {Typ: key.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: values}}}}
	typ := makePlan2Type(&resultType)
	typ.NotNullable = function.DeduceNotNullable(overloadID, args)
	return &plan.Expr{Typ: typ, Selectivity: 1, Expr: &plan.Expr_F{F: &plan.Function{
		Func: getFunctionObjRef(overloadID, "in"), Args: args,
	}}}, nil
}

// snapshot records exactly the filters produced by the existing early pass.
// If a later rewrite moves or changes a witness, publication fails open.
func (c *compositePointCapture) snapshot() {
	for _, id := range c.order {
		p := c.proposals[id]
		if p == nil || len(p.node.FilterList) == 0 || len(p.node.FilterList) > maxCompositePointNodes {
			delete(c.proposals, id)
			continue
		}
		var size uint64
		for _, expr := range p.node.FilterList {
			if expr == nil {
				size = *c.remaining + 1
				break
			}
			cost := uint64(expr.ProtoSize()) + 128
			if cost > *c.remaining-size {
				size = *c.remaining + 1
				break
			}
			size += cost
		}
		if size > *c.remaining {
			delete(c.proposals, id)
			continue
		}
		witnesses := make([][]byte, len(p.node.FilterList))
		for i, expr := range p.node.FilterList {
			wire, err := expr.Marshal()
			if err != nil {
				witnesses = nil
				break
			}
			witnesses[i] = wire
		}
		if witnesses == nil {
			delete(c.proposals, id)
			continue
		}
		*c.remaining -= size
		p.witnesses = witnesses
	}
}

// revalidate closes the initial proof before the existing late compound-key
// lowering replaces logical predicates with equivalent physical predicates.
// Only those proven scans may take a new witness after that owned rewrite.
func (c *compositePointCapture) revalidate(builder *QueryBuilder) {
	for _, id := range c.order {
		if p := c.proposals[id]; p != nil && !p.valid(builder.qry.Nodes[id], c.remaining) {
			delete(c.proposals, id)
		}
	}
}

func (p *compositePointProposal) valid(node *plan.Node, remaining *uint64) bool {
	if remaining == nil || node != p.node || node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil ||
		node.TableDef.TblId != p.tableID || len(node.BindingTags) != 1 || node.BindingTags[0] != p.tag ||
		!proto.Equal(p.object, node.ObjRef) || !proto.Equal(p.snapshot, node.ScanSnapshot) ||
		!proto.Equal(p.key, node.TableDef.Pkey) || len(node.FilterList) > maxCompositePointNodes {
		return false
	}
	for _, saved := range p.columns {
		pos, ok := node.TableDef.Name2ColIndex[saved.name]
		if !ok || pos != saved.position || pos < 0 || int(pos) >= len(node.TableDef.Cols) {
			return false
		}
		col := node.TableDef.Cols[pos]
		if col == nil || col.ColId != saved.id || col.Name != saved.name || !proto.Equal(saved.typ, &col.Typ) {
			return false
		}
	}
	// Full wire identity includes parameter ordinals and Literal.Src, and is
	// deliberately stricter than structural expression equality. A changed
	// statistic or other metadata merely keeps the baseline access path.
	var currentBytes uint64
	for _, filter := range node.FilterList {
		if filter == nil {
			return false
		}
		size := uint64(filter.ProtoSize())
		if size > maxCompositePointBytes-currentBytes {
			return false
		}
		currentBytes += size
	}
	// Marshal the current filters only after reserving their temporary buffers
	// and matching bookkeeping. Repeated checks use the same reservation.
	comparisonBytes := currentBytes + uint64(len(node.FilterList))*32 + 128
	if comparisonBytes > p.comparisonBytes {
		extra := comparisonBytes - p.comparisonBytes
		if extra > *remaining {
			return false
		}
		*remaining -= extra
		p.comparisonBytes = comparisonBytes
	}
	current := make([][]byte, len(node.FilterList))
	for i, filter := range node.FilterList {
		var err error
		current[i], err = filter.Marshal()
		if err != nil {
			return false
		}
	}
	matched := make([]bool, len(node.FilterList))
	for _, witness := range p.witnesses {
		found := false
		for i, actual := range current {
			if !matched[i] && bytes.Equal(witness, actual) {
				matched[i], found = true, true
				break
			}
		}
		if !found {
			return false
		}
	}
	// New conjuncts may be skipped on pruned rows, so they must be total too.
	a := compositePointAnalysis{tag: p.tag}
	for i, filter := range node.FilterList {
		if !matched[i] && !a.safePredicate(filter, 0) {
			return false
		}
	}
	return true
}

// publish visits only the surviving plan. Scan copies are deliberately not
// inferred from table identity; a copy without its own proof keeps baseline.
func (c *compositePointCapture) publish(builder *QueryBuilder, rootID int32) error {
	if len(c.order) == 0 && len(c.proposals) == 0 {
		return nil
	}
	if err := builder.checkPlanningCanceled(); err != nil {
		return err
	}
	if len(c.proposals) == 0 {
		return nil
	}
	visited := make(map[int32]struct{})
	var publishErr error
	var visit func(int32)
	visit = func(id int32) {
		if publishErr != nil {
			return
		}
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		for _, child := range node.Children {
			visit(child)
		}
		if publishErr != nil {
			return
		}
		p := c.proposals[id]
		if p == nil || !p.valid(node, c.remaining) {
			return
		}
		// Do not duplicate an existing complete-key IN filter.
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			if fn != nil && fn.Func != nil && fn.Func.ObjName == "in" && len(fn.Args) == 2 {
				if col := fn.Args[0].GetCol(); col != nil && col.RelPos == p.tag &&
					col.ColPos == node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName] {
					return
				}
			}
		}
		// A known minimum that cannot fit must not inspect parameters or make
		// an otherwise reusable prepared fallback value-dependent.
		if uint64(len(p.shape.values)) > *c.remaining/(compositePointCopies*types.DefaultPackerCapacity()) {
			return
		}
		if publishErr = builder.checkPlanningCanceled(); publishErr != nil {
			return
		}
		charge, ok := p.shape.candidateBytes(builder.GetContext(), builder.compCtx.GetProcess(), *c.remaining)
		if !ok {
			return
		}
		candidate, err := builder.bindCompositePointCandidate(node, p.shape.values)
		if err != nil {
			// Binding an optional candidate may fail normally. Cancellation is
			// owned by the planner and must not be converted into fallback.
			publishErr = builder.checkPlanningCanceled()
			if publishErr == nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				publishErr = err
			}
			return
		}
		if publishErr = builder.checkPlanningCanceled(); publishErr != nil {
			return
		}
		*c.remaining -= charge
		node.FilterList = append(node.FilterList, candidate)
		if (builder.optimizerHints == nil || builder.optimizerHints.blockFilter != 2) &&
			ExprIsZonemappable(builder.GetContext(), candidate) {
			appendUniqueBlockFilter(node, candidate, true)
		}
	}
	visit(rootID)
	if publishErr != nil {
		return publishErr
	}
	return builder.checkPlanningCanceled()
}
