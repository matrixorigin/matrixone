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
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const temporalCacheMaxOutput = 16

// TemporalCacheProof is an opaque, bounded statement-local certificate. It
// retains no relation, transaction, AST, process or executor.
type TemporalCacheProof struct {
	valid                            bool
	baseID                           uint64
	baseVersion                      uint32
	database, table, column, primary string
	columnPos                        int32
	kind                             types.T
	columnScale, scale               int32
	op                               types.TemporalOp
	columnFirst                      bool
	columnNotNullable                bool
	outputs                          [temporalCacheMaxOutput]temporalCacheOutput
	outputCount                      int
	hiddenID                         uint64
	hiddenVersion                    uint32
	hiddenName, indexName            string
	serialized                       bool
}
type temporalCacheOutput struct {
	name                  string
	position, kind, scale int32
}

// EnableOrdinaryCache is called only by the single external COM_QUERY owner,
// before the scope is claimed. Every other construction keeps the default off.
func (scope *TemporalPlanningScope) EnableOrdinaryCache() bool {
	if !scope.Usable() || !scope.nativeMu.TryLock() {
		return false
	}
	defer scope.nativeMu.Unlock()
	if !scope.Usable() || scope.ordinaryCache.Load() || scope.root.Load() == nil || !scope.intent.ParameterFree {
		return false
	}
	scope.ordinaryCache.Store(true)
	return true
}

func (scope *TemporalPlanningScope) TemporalCacheProof(query *Query) (TemporalCacheProof, bool) {
	if !scope.MatchesQuery(query) || !scope.nativeMu.TryLock() {
		return TemporalCacheProof{}, false
	}
	defer scope.nativeMu.Unlock()
	return scope.cacheProof, scope.Usable() && scope.cacheProof.valid && scope.cacheProof.hiddenID != 0
}

// ValidateTemporalCacheRead rechecks borrowed native source state without holding
// the scope gate across transaction callbacks. Nothing escapes the lexical call.
func (scope *TemporalPlanningScope) ValidateTemporalCacheRead(ctx context.Context, query *Query) bool {
	if ctx == nil || context.Cause(ctx) != nil || !scope.MatchesQuery(query) || !scope.nativeMu.TryLock() {
		return false
	}
	proof := scope.cacheProof
	var bindings [2]temporalNativeBinding
	found := 0
	if scope.MatchesQuery(query) && proof.valid {
		for i := range scope.sourceCount {
			binding := scope.sources[i]
			if binding.full.TblId == proof.baseID && binding.full.Version == proof.baseVersion {
				bindings[0] = binding
				found |= 1
			}
			if proof.hiddenID != 0 && binding.full.TblId == proof.hiddenID && binding.full.Version == proof.hiddenVersion {
				bindings[1] = binding
				found |= 2
			}
		}
	}
	scope.nativeMu.Unlock()
	want := 1
	if proof.hiddenID != 0 {
		want = 3
	}
	if found != want {
		return false
	}
	for i := range bindings {
		if found&(1<<i) == 0 {
			continue
		}
		provider, ok := bindings[i].relation.(engine.TemporalIndexRelation)
		if !ok || !provider.ValidateTemporalRead(ctx, bindings[i].read) {
			return false
		}
	}
	return scope.MatchesQuery(query) && context.Cause(ctx) == nil
}

// This optional audit runs after ordinary original-owner admission. Its budget
// cannot consume or change candidate admission, and failure changes no flags.
func (builder *QueryBuilder) auditTemporalOriginalCacheOwner(root int32) {
	scope, q := builder.temporalPlanningScope, builder.qry
	if !scope.Usable() || scope.root.Load() != nil || !scope.ordinaryCache.Load() || !builder.hasTemporalPlanningIntent || !builder.temporalPlanningIntent.ParameterFree || !q.TemporalIndexNeedsReplan || len(q.Nodes) != 3 || len(builder.indexHintsByScan) != 0 || len(q.Params) != 0 || root < 0 || int(root) >= len(q.Nodes) {
		return
	}
	project := q.Nodes[root]
	if project == nil || project != builder.temporalCacheProject || project.NodeType != plan.Node_PROJECT || !project.NotCacheable || len(project.Children) != 1 || len(project.ProjectList) == 0 || len(project.ProjectList) > temporalCacheMaxOutput {
		return
	}
	filterID := project.Children[0]
	if filterID < 0 || int(filterID) >= len(q.Nodes) {
		return
	}
	filter := q.Nodes[filterID]
	if filter == nil || filter == project || filter != builder.temporalCacheFilter || filter.NodeType != plan.Node_FILTER || !filter.NotCacheable || len(filter.Children) != 1 || len(filter.FilterList) != 1 {
		return
	}
	scanID := filter.Children[0]
	if scanID < 0 || int(scanID) >= len(q.Nodes) || !builder.temporalOriginalSafeScans[scanID] {
		return
	}
	scan := q.Nodes[scanID]
	if scan == nil || scan == filter || scan == project || scan.NodeType != plan.Node_TABLE_SCAN || scan.NotCacheable || scan.TableDef == nil || scan.TableDef.Pkey == nil || scan.ObjRef == nil || len(scan.Children) != 0 || len(scan.FilterList) != 0 || len(scan.ProjectList) != 0 || len(scan.BindingTags) != 1 {
		return
	}
	atom, ok := temporalCandidateAtomForExpr(filter.FilterList[0])
	if !ok || atom.bound.GetF() == nil || atom.column.GetCol().RelPos != scan.BindingTags[0] {
		return
	}
	pos := atom.column.GetCol().ColPos
	if pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
		return
	}
	column := scan.TableDef.Cols[pos]
	relation, full, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	provider, native := relation.(engine.TemporalIndexRelation)
	if !ok || !native || !provider.ValidateTemporalRead(builder.compCtx.GetContext(), read) || full.IsDynamic || full.IsTemporary || len(column.Name) == 0 || len(column.Name) > engine.MaxTemporalNameBytes {
		return
	}
	for _, node := range q.Nodes {
		if node == nil || node.Limit != nil || node.Offset != nil || node.ScanSnapshot != nil && IsSnapshotValid(node.ScanSnapshot) {
			return
		}
	}
	proof := TemporalCacheProof{valid: true, baseID: scan.TableDef.TblId, baseVersion: scan.TableDef.Version, database: scan.ObjRef.SchemaName, table: scan.ObjRef.ObjName, column: column.Name, primary: scan.TableDef.Pkey.PkeyColName, columnPos: pos, kind: types.T(atom.column.Typ.Id), columnScale: atom.column.Typ.Scale, scale: atom.scale, op: atom.op, columnFirst: filter.FilterList[0].GetF().Args[0] == atom.column, columnNotNullable: atom.column.Typ.NotNullable, outputCount: len(project.ProjectList)}
	for i, expr := range project.ProjectList {
		if expr == nil || expr.GetCol() == nil || expr.GetCol().RelPos != scan.BindingTags[0] || temporalCandidateHasUnprovedSource(expr) {
			return
		}
		c := expr.GetCol().ColPos
		if c < 0 || int(c) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[c] == nil || len(scan.TableDef.Cols[c].Name) > engine.MaxTemporalNameBytes {
			return
		}
		proof.outputs[i] = temporalCacheOutput{name: scan.TableDef.Cols[c].Name, position: c, kind: expr.Typ.Id, scale: expr.Typ.Scale}
	}
	if !scope.nativeMu.TryLock() {
		return
	}
	defer scope.nativeMu.Unlock()
	if !scope.Usable() || !scope.ordinaryCache.Load() {
		return
	}
	scope.cacheProof = proof
	filter.NotCacheable, project.NotCacheable = false, false
}

// Complete protobuf preflight precedes generated size/equality/clone methods.
// Every pointer visit is charged independently, so sharing and cycles cannot
// evade the fixed depth/work limit; no visited graph is retained.
func temporalCachePlanFits(ctx context.Context, p *plan.Plan) bool {
	if ctx == nil || context.Cause(ctx) != nil || p == nil {
		return false
	}
	query, ok := p.Plan.(*plan.Plan_Query)
	if !ok || query == nil || query.Query == nil || len(query.Query.Nodes) > 64 {
		return false
	}
	work, bytes := engine.MaxTemporalBindingWork, 128*1024
	var visit func(reflect.Value, int) bool
	visit = func(v reflect.Value, depth int) bool {
		work--
		if work <= 0 || depth > 64 || work&63 == 0 && context.Cause(ctx) != nil {
			return false
		}
		if !v.IsValid() {
			return true
		}
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				return true
			}
			value := v.Elem()
			if value.Kind() != reflect.Pointer || value.IsNil() {
				return false
			}
			return visit(value, depth)
		case reflect.Pointer:
			return v.IsNil() || visit(v.Elem(), depth)
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if !visit(v.Field(i), depth+1) {
					return false
				}
			}
		case reflect.String:
			n := v.Len()
			if n > bytes {
				return false
			}
			bytes -= n
		case reflect.Slice, reflect.Array:
			n := v.Len()
			if v.Type().Elem().Kind() == reflect.Uint8 {
				// This narrow raw owner needs no opaque byte payloads. Rejecting
				// every nonempty one also excludes unknown protobuf fields,
				// without inspecting struct descriptors for every scalar field.
				return n == 0
			}
			if n > engine.MaxTemporalTableColumns || n > work {
				return false
			}
			for i := 0; i < n; i++ {
				if !visit(v.Index(i), depth+1) {
					return false
				}
			}
		case reflect.Map:
			n := v.Len()
			if n > engine.MaxTemporalTableColumns || n > work/2 {
				return false
			}
			it := v.MapRange()
			for it.Next() {
				if !visit(it.Key(), depth+1) || !visit(it.Value(), depth+1) {
					return false
				}
			}
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		default:
			return false
		}
		return true
	}
	return visit(reflect.ValueOf(p), 0) && p.ProtoSize() <= 128*1024 && context.Cause(ctx) == nil
}

func (scope *TemporalPlanningScope) recordTemporalCacheIndex(base *TableDef, hidden *TableDef, index *plan.IndexDef) {
	if !scope.Usable() || !scope.nativeMu.TryLock() {
		return
	}
	defer scope.nativeMu.Unlock()
	proof := &scope.cacheProof
	if !proof.valid || base == nil || hidden == nil || index == nil || proof.baseID != base.TblId || proof.baseVersion != base.Version || len(index.IndexName) > engine.MaxTemporalNameBytes || len(index.IndexTableName) > engine.MaxTemporalNameBytes {
		return
	}
	proof.hiddenID, proof.hiddenVersion = hidden.TblId, hidden.Version
	proof.hiddenName, proof.indexName, proof.serialized = index.IndexTableName, index.IndexName, indexTableStoresSerializedKey(index)
}

// CloneTemporalCachePlan copies only an already raw, bounded simple plan. The
// frontend still needs the original certificate (miss) or trusted existing LRU
// publication marker (hit); this helper alone grants no cache authority.
func CloneTemporalCachePlan(ctx context.Context, p *plan.Plan) (*plan.Plan, bool) {
	if !temporalCacheRawInvariant(ctx, p) {
		return nil, false
	}
	copy := proto.Clone(p).(*plan.Plan)
	return copy, context.Cause(ctx) == nil
}

func temporalCacheRawInvariant(ctx context.Context, p *plan.Plan) bool {
	if !temporalCachePlanFits(ctx, p) {
		return false
	}
	q := p.GetQuery()
	if !q.TemporalIndexNeedsReplan || q.StmtType != plan.Query_SELECT || len(q.Steps) != 1 || len(q.Params) != 0 || q.HasForeignKeyAction || q.HasReturning || len(q.UnresolvedIndexHints) != 0 || len(q.BackgroundQueries) != 0 || len(q.DetectSqls) != 0 || len(q.ViewReferences) != 0 || q.Correlation != nil {
		return false
	}
	owner, ok := temporalCacheOwner(q)
	if !ok {
		return false
	}
	scan := owner[len(owner)-1]
	if !scan.IndexScanInfo.IsIndexScan || scan.ParentObjRef == nil || len(scan.BlockFilterList) != 1 || !scan.BlockFilterList[0].TemporalCandidateFilter {
		return false
	}
	for _, n := range q.Nodes {
		if n == nil || n.NotCacheable || n.Limit != nil || n.Offset != nil || len(n.OnList) != 0 || len(n.GroupBy) != 0 || len(n.AggList) != 0 || len(n.OrderBy) != 0 || len(n.WinSpecList) != 0 {
			return false
		}
		switch n.NodeType {
		case plan.Node_TABLE_SCAN, plan.Node_PROJECT, plan.Node_FILTER:
		default:
			return false
		}
		for _, list := range [][]*plan.Expr{n.ProjectList, n.FilterList, n.BlockFilterList} {
			for _, e := range list {
				if !temporalCacheRawExpr(e) {
					return false
				}
			}
		}
	}
	return context.Cause(ctx) == nil
}
func temporalCacheRawExpr(e *plan.Expr) bool {
	if e == nil || temporalCandidateHasUnprovedSource(e) {
		return false
	}
	if e.GetCol() != nil || e.GetT() != nil {
		return true
	}
	if lit := e.GetLit(); lit != nil {
		return lit.Src == nil
	}
	f := e.GetF()
	if f == nil || f.Func == nil || f.Func.PubInfo != nil || f.Func.Snapshot != nil || len(f.AggConfig) != 0 {
		return false
	}
	id, _ := function.DecodeOverloadID(f.Func.Obj)
	if _, ok := function.GetFunctionByIdWithoutError(f.Func.Obj); !ok {
		return false
	}
	switch id {
	case function.CURRENT_TIMESTAMP:
		if !temporalCandidateBoundSafe(e) {
			return false
		}
	case function.TEMPORAL_DATETIME_BOUND, function.TEMPORAL_TIMESTAMP_BOUND:
		if len(f.Args) != 5 || f.Args[4] == nil || f.Args[4].GetLit() == nil || types.T(f.Args[4].Typ.Id) != types.T_bool || f.Args[4].GetLit().Isnull || f.Args[4].GetLit().GetBval() {
			return false
		}
	case function.TEMPORAL_BOUND_NOT_EMPTY, function.AND, function.OR, function.BETWEEN, function.EQUAL, function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL, function.PREFIX_BETWEEN, function.PREFIX_EQ, function.SERIAL, function.SERIAL_EXTRACT:
	default:
		return false
	}
	for _, arg := range f.Args {
		if !temporalCacheRawExpr(arg) {
			return false
		}
	}
	return true
}

// AuditTemporalCacheRaw binds an optimized raw plan to the original owner and
// selected native-pair certificate. Unproved final shapes simply stay uncached.
func AuditTemporalCacheRaw(ctx context.Context, p *plan.Plan, proof TemporalCacheProof, proc *process.Process) bool {
	if proc == nil || proc.Base == nil || proc.Mp() == nil || !proof.valid || proof.hiddenID == 0 || !temporalCacheRawInvariant(ctx, p) {
		return false
	}
	q := p.GetQuery()
	owner, ok := temporalCacheOwner(q)
	if !ok {
		return false
	}
	scan := owner[len(owner)-1]
	if scan.ObjRef == nil || scan.TableDef == nil {
		return false
	}
	if scan.ScanSnapshot != nil && (IsSnapshotValid(scan.ScanSnapshot) || scan.ScanSnapshot.Tenant != nil || scan.ScanSnapshot.ExtraInfo != nil) {
		return false
	}
	budget := 4096
	var row *plan.Expr
	for level, n := range owner {
		if len(n.FilterList) == 0 {
			continue
		}
		if row != nil || len(n.FilterList) != 1 || n.FilterList[0].TemporalCandidateFilter {
			return false
		}
		row = temporalCacheResolveExpr(n.FilterList[0], owner, level, 0, &budget)
		if row == nil {
			return false
		}
	}
	if row == nil {
		return false
	}
	f := row.GetF()
	if f == nil || len(f.Args) != 2 {
		return false
	}
	side := 0
	if !proof.columnFirst {
		side = 1
	}
	mapped := f.Args[side]
	if !temporalCacheColumnMatches(scan, mapped, proof.column, proof) {
		return false
	}
	logicalType := mapped.Typ
	logicalType.NotNullable = proof.columnNotNullable
	logical := GetColExpr(logicalType, 0, proof.columnPos)
	f.Args[side] = logical
	atom, ok := temporalCandidateAtomForExpr(row)
	if !ok || atom.bound.GetF() == nil || atom.op != proof.op || atom.scale != proof.scale || types.T(atom.column.Typ.Id) != proof.kind || atom.column.Typ.Scale != proof.columnScale {
		return false
	}
	projected := false
	for level, n := range owner {
		if len(n.ProjectList) == 0 {
			continue
		}
		if len(n.ProjectList) != proof.outputCount {
			return false
		}
		for i, e := range n.ProjectList {
			output := proof.outputs[i]
			resolved := temporalCacheResolveExpr(e, owner, level, 0, &budget)
			if resolved == nil || resolved.Typ.Id != output.kind || resolved.Typ.Scale != output.scale || !temporalCacheColumnMatches(scan, resolved, output.name, proof) {
				return false
			}
		}
		projected = true
		break
	}
	if !projected {
		return false
	}

	if !scan.IndexScanInfo.IsIndexScan || scan.TableDef.TblId != proof.hiddenID || scan.TableDef.Version != proof.hiddenVersion || scan.ObjRef.SchemaName != proof.database || scan.ObjRef.ObjName != proof.hiddenName || uint64(scan.ObjRef.Obj) != proof.hiddenID || scan.IndexScanInfo.IndexName != proof.indexName || scan.ParentObjRef == nil || uint64(scan.ParentObjRef.Obj) != proof.baseID || scan.ParentObjRef.SchemaName != proof.database || scan.ParentObjRef.ObjName != proof.table || scan.Stats == nil || !scan.Stats.ForceOneCN || len(scan.BlockFilterList) != 1 || !scan.BlockFilterList[0].TemporalCandidateFilter {
		return false
	}
	dependency := false
	for _, ref := range q.CatalogDependencies {
		if ref != nil && uint64(ref.Obj) == proof.baseID && ref.SchemaName == proof.database && ref.ObjName == proof.table && ref.Server == int64(proof.baseVersion) {
			dependency = true
		}
	}
	if !dependency {
		return false
	}
	physical := mapped
	if proof.serialized {
		physical = mapped.GetF().Args[0]
	}
	var expected, guard *plan.Expr
	var err error
	if proof.serialized {
		expected, guard, err = makeEncodedTemporalCandidate(ctx, physical, logical, atom.bound, nil, true, atom.op, atom.scale)
	} else {
		expected, guard, err = makeDirectTemporalCandidate(ctx, physical, atom.bound, atom.op, atom.scale)
	}
	if err != nil {
		return false
	}
	expected.TemporalCandidateFilter = true
	guard.TemporalCandidateGuard = true
	budget = 4096
	expected, ok = temporalCacheNormalizeStaticSerial(ctx, expected, scan.BlockFilterList[0], proc, &budget)
	if !ok {
		return false
	}
	budget = engine.MaxTemporalBindingWork
	return temporalCostFiltersEqual(ctx, expected, scan.BlockFilterList[0], &budget) && context.Cause(ctx) == nil
}

// Ordinary optimization can materialize a constructor-owned static SERIAL
// leaf. Normalize only that corresponding leaf, through the same folding rule;
// raw NOW and all dynamic helper/control subtrees are never submitted for fold.
func temporalCacheNormalizeStaticSerial(ctx context.Context, expected, actual *plan.Expr, proc *process.Process, budget *int) (*plan.Expr, bool) {
	*budget--
	if *budget <= 0 || context.Cause(ctx) != nil || expected == nil || actual == nil {
		return expected, false
	}
	fn := expected.GetF()
	if fn == nil || fn.Func == nil {
		return expected, true
	}
	if actual.GetLit() != nil && fn.Func.Obj == function.SerialFunctionEncodeID && len(fn.Args) == 1 && fn.Args[0] != nil && types.T(fn.Args[0].Typ.Id) == types.T_datetime {
		literal := fn.Args[0].GetLit()
		if literal == nil || literal.Isnull || literal.Src != nil || proc == nil {
			return expected, false
		}
		if _, typed := literal.Value.(*plan.Literal_Datetimeval); !typed {
			return expected, false
		}
		if !spendTemporalCostWork(ctx, budget, expected.ProtoSize()+64) {
			return expected, false
		}
		probe := proc.NewNoContextChildProc(0)
		probe.Ctx = ctx
		node := &plan.Node{BlockFilterList: []*plan.Expr{DeepCopyExpr(expected)}}
		rule.NewConstantFold(false).Apply(node, nil, probe)
		folded := node.BlockFilterList[0]
		result := folded.GetLit()
		return folded, result != nil && !result.Isnull && result.Src == nil && result.IsSerialized && context.Cause(ctx) == nil
	}
	other := actual.GetF()
	if other == nil || other.Func == nil || fn.Func.Obj != other.Func.Obj || len(fn.Args) != len(other.Args) {
		return expected, true // Exact comparison below rejects different structure.
	}
	for i := range fn.Args {
		var ok bool
		fn.Args[i], ok = temporalCacheNormalizeStaticSerial(ctx, fn.Args[i], other.Args[i], proc, budget)
		if !ok {
			return expected, false
		}
	}
	return expected, true
}

func temporalCacheColumnMatches(scan *plan.Node, e *plan.Expr, name string, proof TemporalCacheProof) bool {
	if e == nil {
		return false
	}

	if name == proof.column && proof.serialized {
		f := e.GetF()
		if f == nil || f.Func == nil || len(f.Args) != 3 || f.Args[0] == nil || f.Args[0].GetCol() == nil || f.Args[1] == nil || f.Args[1].GetLit() == nil || f.Args[2] == nil || f.Args[2].GetT() == nil {
			return false
		}
		id, _ := function.DecodeOverloadID(f.Func.Obj)
		return id == function.SERIAL_EXTRACT && f.Args[1].GetLit().GetI64Val() == 0 && !f.Args[1].GetLit().Isnull && f.Args[2].Typ.Id == int32(proof.kind) && f.Args[2].Typ.Scale == proof.columnScale && temporalCacheHiddenColumn(scan, f.Args[0], catalog.IndexTableIndexColName)
	}
	if name == proof.column {
		return temporalCacheHiddenColumn(scan, e, catalog.IndexTableIndexColName)
	}
	return scan.ParentObjRef != nil && name == proof.primary && temporalCacheHiddenColumn(scan, e, catalog.IndexTablePrimaryColName)
}
func temporalCacheHiddenColumn(scan *plan.Node, e *plan.Expr, name string) bool {
	c := e.GetCol()
	if c == nil || c.RelPos != 0 {
		return false
	}
	if c.ColPos >= 0 && int(c.ColPos) < len(scan.TableDef.Cols) && scan.TableDef.Cols[c.ColPos] != nil && scan.TableDef.Cols[c.ColPos].Name == name {
		return true
	}
	// Block-only metadata positions survive pruning outside the row projection.
	return c.Name == name && c.ColPos >= int32(len(scan.TableDef.Cols))
}

// Only one bounded unary owner is reusable. A cycle, fan-out or extra scan
// declines the optional cache, even if a generic plan could execute it.
func temporalCacheOwner(q *plan.Query) ([]*plan.Node, bool) {
	if q == nil || len(q.Steps) != 1 {
		return nil, false
	}
	owner := make([]*plan.Node, 0, 4)
	id := q.Steps[0]
	for range len(q.Nodes) {
		if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
			return nil, false
		}
		n := q.Nodes[id]
		owner = append(owner, n)
		if n.NodeType == plan.Node_TABLE_SCAN {
			return owner, len(n.Children) == 0
		}
		if n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER || len(n.Children) != 1 {
			return nil, false
		}
		id = n.Children[0]
	}
	return nil, false
}

// Resolve each local input through its child's output without composing an
// unbounded expression tree. All copies consume a separate optional allowance.
func temporalCacheResolveExpr(e *plan.Expr, owner []*plan.Node, level, depth int, budget *int) *plan.Expr {
	*budget--
	if e == nil || *budget <= 0 || depth > 64 {
		return nil
	}
	if c := e.GetCol(); c != nil && level < len(owner)-1 {
		if c.RelPos != 0 || c.ColPos < 0 {
			return nil
		}
		child := owner[level+1]
		if len(child.ProjectList) == 0 {
			return temporalCacheResolveExpr(e, owner, level+1, depth+1, budget)
		}
		if int(c.ColPos) >= len(child.ProjectList) {
			return nil
		}
		return temporalCacheResolveExpr(child.ProjectList[c.ColPos], owner, level+1, depth+1, budget)
	}
	copy := *e
	if f := e.GetF(); f != nil {
		fn := *f
		fn.Args = make([]*plan.Expr, len(f.Args))
		for i, a := range f.Args {
			fn.Args[i] = temporalCacheResolveExpr(a, owner, level, depth+1, budget)
			if fn.Args[i] == nil {
				return nil
			}
		}
		copy.Expr = &plan.Expr_F{F: &fn}
	}
	return &copy
}
