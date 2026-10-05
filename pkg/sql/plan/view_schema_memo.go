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
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Nested reuse is deliberately limited to transparent column-projection
// chains. Other views still use the authoritative binder in full. In particular
// a successful parent does NOT validate unused MATCH/function outputs of a
// nested view, nor does it prove ENUM/SET conversion structures interchangeable.
func transparentViewProjection(stmt *tree.Select) bool {
	if stmt == nil || stmt.With != nil || stmt.Limit != nil || len(stmt.OrderBy) > 0 || stmt.TimeWindow != nil || stmt.RankOption != nil || stmt.Ep != nil || len(stmt.IntoVars) > 0 || stmt.SelectLockInfo != nil || stmt.IsPerform || stmt.RewriteOption != nil {
		return false
	}
	selectStmt, ok := stmt.Select.(*tree.SelectClause)
	if !ok || selectStmt.Distinct || selectStmt.Option != 0 || selectStmt.Where != nil || selectStmt.GroupBy != nil || selectStmt.Having != nil || len(selectStmt.Windows) > 0 || len(selectStmt.IntoVars) > 0 || selectStmt.IntoExport != nil || selectStmt.From == nil || len(selectStmt.From.Tables) != 1 {
		return false
	}
	for _, projection := range selectStmt.Exprs {
		switch projection.Expr.(type) {
		case *tree.UnresolvedName, tree.UnqualifiedStar, *tree.UnqualifiedStar:
		default:
			return false
		}
	}
	table := selectStmt.From.Tables[0]
	if join, ok := table.(*tree.JoinTableExpr); ok {
		if join.Right != nil || join.Cond != nil || join.Option != "" {
			return false
		}
		table = join.Left
	}
	if alias, ok := table.(*tree.AliasedTableExpr); ok {
		if len(alias.IndexHints) > 0 {
			return false
		}
		table = alias.Expr
	}
	_, ok = table.(*tree.TableName)
	return ok
}

type viewSchemaMemoColumn struct {
	State       ProvenanceState
	Policy      CTASDefaultPolicy
	SourceTable uint64
	Heading     []viewSchemaHeadingPart
}
type viewSchemaHeadingPart struct {
	Text    string
	Literal bool
}
type viewSchemaBoundary struct {
	Columns      []byte
	Sources      []byte
	Metadata     []viewSchemaMemoColumn
	SemanticKeys []string
}
type viewSchemaMemoFrame struct {
	requiredProtocol           int64
	state                      *viewSchemaDerivation
	key                        [32]byte
	work, slots, depth, cursor int
	parentEligible             bool
}

func (s *viewSchemaDerivation) beginMemo(obj *ObjectRef, def *TableDef, snapshot *Snapshot, lower int64) *viewSchemaMemoFrame {
	key := viewSchemaKey(obj, def, snapshot)
	// Legacy nested definitions inherit the enclosing View name-folding mode.
	var identity [40]byte
	copy(identity[:32], key[:])
	binary.LittleEndian.PutUint64(identity[32:], uint64(lower))
	return &viewSchemaMemoFrame{state: s, key: sha256.Sum256(identity[:]), work: s.work - 1, slots: s.slots, depth: s.depth, cursor: len(s.dependencyLog), parentEligible: s.memoEligible}
}
func (f *viewSchemaMemoFrame) finish(ctx *BindContext, err error) {
	s := f.state
	s.memoFrames = s.memoFrames[:len(s.memoFrames)-1]
	eligible := s.memoEligible
	s.memoEligible = f.parentEligible && eligible
	if err != nil || !eligible || s.request.memoDisabled {
		return
	}
	cols := make([]*ColDef, len(ctx.headings))
	sources := make([]*ColDef, len(cols))
	metadata := make([]viewSchemaMemoColumn, len(cols))
	if len(ctx.results) < len(cols) {
		return
	}
	for i, name := range ctx.headings {
		origin := ctx.outputColumnProvenanceForProject(int32(i))
		typ := ctx.results[i].Typ
		if origin.State != ProvenanceSingleSource || origin.Source == nil || types.T(typ.Id) == types.T_enum || isSetPlanType(&typ) {
			return
		}
		cols[i] = &ColDef{Name: name, Typ: typ}
		source := origin.Source
		sources[i] = &ColDef{Typ: source.Metadata.Typ, Default: DeepCopyDefault(source.Metadata.Default)}
		metadata[i] = viewSchemaMemoColumn{State: origin.State, Policy: origin.CTASDefaultPolicy, SourceTable: source.TableID}
		for _, part := range ctx.headingProvenance[int32(i)].parts {
			metadata[i].Heading = append(metadata[i].Heading, viewSchemaHeadingPart{part.text, part.literal})
		}
	}
	columnBytes, marshalErr := (&TableDef{Cols: cols}).Marshal()
	if marshalErr != nil {
		return
	}
	sourceBytes, marshalErr := (&TableDef{Cols: sources}).Marshal()
	if marshalErr != nil {
		return
	}
	boundary, marshalErr := json.Marshal(viewSchemaBoundary{Columns: columnBytes, Sources: sourceBytes, Metadata: metadata, SemanticKeys: ctx.projectSemanticKeys})
	if marshalErr != nil || len(boundary) > viewSchemaMemoLimit {
		return
	}
	deps, marshalErr := json.Marshal(s.dependencyLog[f.cursor:])
	if marshalErr != nil {
		return
	}
	value := &viewSchemaMemoEntry{requiredProtocol: f.requiredProtocol, boundary: boundary, dependencies: deps, work: s.work - f.work, slots: s.slots - f.slots, depth: s.maxDepth - f.depth + 1}
	s.request.rememberIn(s.request.nestedMemo, f.key, value)
}
func (f *viewSchemaMemoFrame) load(builder *QueryBuilder, ctx *BindContext, obj *ObjectRef, def *TableDef, snapshot *Snapshot, tableName string) (int32, bool, error) {
	s := f.state
	value := s.request.nestedMemo[f.key]
	if !s.memoEligible || s.request.memoDisabled || value == nil {
		return 0, false, nil
	}
	if s.depth-1+value.depth > viewSchemaDepthLimit {
		return 0, false, ErrViewSchemaLimit
	}
	if err := s.charge(value.work-1, value.slots); err != nil {
		return 0, false, err
	}
	s.maxDepth = max(s.maxDepth, s.depth-1+value.depth)
	s.observeProtocol(value.requiredProtocol)
	var boundary viewSchemaBoundary
	if err := json.Unmarshal(value.boundary, &boundary); err != nil {
		return 0, false, err
	}
	var table, sourceTable TableDef
	if err := table.Unmarshal(boundary.Columns); err != nil {
		return 0, false, err
	}
	if err := sourceTable.Unmarshal(boundary.Sources); err != nil {
		return 0, false, err
	}
	var deps []ViewDependency
	if err := json.Unmarshal(value.dependencies, &deps); err != nil {
		return 0, false, err
	}
	for _, dep := range deps {
		s.capture.deps[viewSchemaDependencyKey(dep)] = dep
		s.dependencyLog = append(s.dependencyLog, dep)
	}
	table.Name, table.DbName, table.TableType = def.Name, def.DbName, catalog.SystemViewRel
	scanTag := builder.genNewBindTag()
	scanID := int32(len(builder.qry.Nodes))
	if s.memoScans == nil {
		s.memoScans = make(map[*QueryBuilder]map[int32]bool)
	}
	if s.memoScans[builder] == nil {
		s.memoScans[builder] = make(map[int32]bool)
	}
	s.memoScans[builder][scanID] = true
	builder.appendNode(&pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: &table, ObjRef: DeepCopyObjectRef(obj), ScanSnapshot: DeepCopySnapshot(snapshot), BindingTags: []int32{scanTag}, Stats: DefaultStats()}, ctx)
	ctx.projectTag = builder.genNewBindTag()
	ctx.cteName = tableName
	ctx.restoreViewMySQLSpecialTypes = true
	ctx.projectSemanticKeys = append([]string(nil), boundary.SemanticKeys...)
	ctx.headingProvenance = make(headingProvenanceMap)
	for i, col := range table.Cols {
		expr := GetColExpr(col.Typ, scanTag, int32(i))
		ctx.headings = append(ctx.headings, col.Name)
		ctx.projects = append(ctx.projects, expr)
		ctx.results = append(ctx.results, expr)
		metadata := boundary.Metadata[i]
		source := sourceTable.Cols[i]
		ctx.setOutputColumnProvenance(int32(i), OutputColumnProvenance{State: metadata.State, CTASDefaultPolicy: metadata.Policy, Source: &SourceColumn{RelPos: scanTag, ColPos: int32(i), TableID: metadata.SourceTable, Metadata: snapshotSourceColumnMetadata(source)}})
		var heading headingProvenance
		for _, part := range metadata.Heading {
			heading.parts = append(heading.parts, headingPart{text: part.Text, literal: part.Literal})
		}
		ctx.headingProvenance[int32(i)] = heading
	}
	nodeID := builder.appendNode(&pb.Node{NodeType: pb.Node_PROJECT, ProjectList: ctx.projects, BindingTags: []int32{ctx.projectTag}, Children: []int32{scanID}}, ctx)
	s.request.hits++
	return nodeID, true, nil
}
func (builder *QueryBuilder) isViewSchemaMemoScan(nodeID int32) bool {
	state, _ := builder.GetContext().Value(viewSchemaContextKey{}).(*viewSchemaDerivation)
	return state != nil && state.memoScans[builder][nodeID]
}
