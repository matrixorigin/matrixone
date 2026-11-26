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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type repeatedUniqueLookup struct {
	index    *planpb.IndexDef
	primary  bool
	original int
	current  int
	proposed int
	lockKey  int
	hit      int
	ref      *planpb.ObjectRef
	table    *planpb.TableDef
}

// repeatedPrimaryKey returns the stored primary-key value and its identity
// representation. FLOAT/DOUBLE identity follows the existing duplicate check.
func repeatedPrimaryKey(
	builder *QueryBuilder, table *planpb.TableDef, input []*planpb.Expr, positions []int, original bool,
) (*planpb.Expr, *planpb.Expr, error) {
	if table.Pkey == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE primary key")
	}
	pkPos, _ := getPkPos(table, false)
	if pkPos < 0 || pkPos >= len(table.Cols) {
		return nil, nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE primary key")
	}
	var raw *planpb.Expr
	if table.Pkey.PkeyColName != catalog.CPrimaryKeyColName || original {
		pos := positions[pkPos]
		if pos < 0 || pos >= len(input) {
			return nil, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE primary key")
		}
		raw = DeepCopyExpr(input[pos])
	} else {
		parts := make([]*planpb.Expr, len(table.Pkey.Names))
		for i, name := range table.Pkey.Names {
			col, ok := table.Name2ColIndex[name]
			if !ok || col < 0 || int(col) >= len(positions) || positions[col] < 0 || positions[col] >= len(input) {
				return nil, nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE primary-key column")
			}
			parts[i] = DeepCopyExpr(input[positions[col]])
		}
		var err error
		raw, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", parts)
		if err != nil {
			return nil, nil, err
		}
	}
	identity, err := bindPrimaryKeyIdentityExpr(builder, DeepCopyExpr(raw), raw.Typ)
	return raw, identity, err
}

func appendRepeatedPrimaryLookup(
	builder *QueryBuilder, bindCtx *BindContext, last int32, target *dmlPlanCtx, keyPos int,
) (int32, error) {
	// The writer rewrites its column list; this scan keeps the original RowID layout.
	table := CloneTableDefForPlan(target.tableDef, true)
	pkPos, _ := getPkPos(table, false)
	if pkPos < 0 || pkPos >= len(table.Cols) || target.rowIdPos < 0 || target.rowIdPos >= len(table.Cols) {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE primary lookup")
	}
	scanProject := make([]*planpb.Expr, len(table.Cols))
	for i, col := range table.Cols {
		scanProject[i] = &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(i), Name: col.Name}}}
	}
	scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{}, ObjRef: target.objRef, TableDef: table, ProjectList: scanProject}
	scanID := builder.appendNode(scan, bindCtx)
	left := &planpb.Expr{Typ: table.Cols[pkPos].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(pkPos), Name: table.Cols[pkPos].Name}}}
	left, err := bindPrimaryKeyIdentityExpr(builder, left, left.Typ)
	if err != nil {
		return -1, err
	}
	input := builder.qry.Nodes[last].ProjectList
	if keyPos < 0 || keyPos >= len(input) {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE primary proposal")
	}
	right := &planpb.Expr{Typ: input[keyPos].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: int32(keyPos)}}}
	cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{left, right})
	if err != nil {
		return -1, err
	}
	project := getProjectionByLastNodeForRightJoin(builder, last)
	project = append(project, &planpb.Expr{Typ: table.Cols[target.rowIdPos].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: int32(target.rowIdPos), Name: catalog.Row_ID}}})
	join := &planpb.Node{
		NodeType: planpb.Node_JOIN, Children: []int32{scanID, last}, JoinType: planpb.Node_RIGHT,
		IsRightJoin: true, OnList: []*planpb.Expr{cond}, ProjectList: project,
	}
	if left.GetCol() != nil {
		tag := builder.genNewMsgTag()
		buildKey := &planpb.Expr{Typ: cond.GetF().Args[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
		probe, build, ok := builder.makeExactRuntimeFilterPair(tag, false,
			GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), scan.Stats.TableCnt), left, buildKey, false)
		if ok {
			scan.RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{probe}
			scan.Stats.ForceOneCN = true
			join.RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{build}
		}
	}
	return builder.appendNode(join, bindCtx), nil
}

// repeatedIndexKey uses the same part and composite-key expressions as the
// hidden-index writer, including prefix lengths and NULL handling.
func repeatedIndexKey(
	builder *QueryBuilder, table *planpb.TableDef, index *planpb.IndexDef,
	input []*planpb.Expr, positions []int,
) (*planpb.Expr, error) {
	prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(index.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	parts := make([]*planpb.Expr, len(index.Parts))
	for i, part := range index.Parts {
		name := catalog.ResolveAlias(part)
		col, ok := table.Name2ColIndex[name]
		if !ok || col < 0 || int(col) >= len(positions) {
			return nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE index column")
		}
		pos := positions[col]
		if pos < 0 || pos >= len(input) {
			return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE index position")
		}
		parts[i], err = builder.makeIndexPartExpr(0, int32(pos), name, input[pos].Typ, prefixes)
		if err != nil {
			return nil, err
		}
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", parts)
}

// appendRepeatedIndexAdmission adds candidate-driven snapshot lookups and
// locks for the UNIQUE indexes changed by one role. A single owner subsequently
// admits or rejects all of those keys together.
func appendRepeatedIndexAdmission(
	builder *QueryBuilder, bindCtx *BindContext, last int32, target *dmlPlanCtx,
	current []int, names []string, values []int,
) (int32, []planpb.UpdateUniqueKey, error) {
	table := target.tableDef
	if len(names) != len(values) || len(current) != len(table.Cols) {
		return -1, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE key input")
	}
	changed := make(map[string]struct{}, len(names))
	proposed := append([]int(nil), current...)
	for i, name := range names {
		col, ok := table.Name2ColIndex[name]
		if !ok || col < 0 || int(col) >= len(proposed) {
			return -1, nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE key column")
		}
		changed[name] = struct{}{}
		proposed[col] = values[i]
	}
	input := getProjectionByLastNode(builder, last)
	projection := append([]*planpb.Expr(nil), input...)
	original := make([]int, len(current))
	for i := range original {
		original[i] = i
	}
	// Most updates do not change a key; allocate only when a lookup is needed.
	var lookups []repeatedUniqueLookup //nolint:prealloc
	primaryChanged := false
	if table.Pkey != nil && table.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		for _, name := range table.Pkey.Names {
			if _, ok := changed[name]; ok {
				primaryChanged = true
				col, exists := table.Name2ColIndex[name]
				if !exists || col < 0 || int(col) >= len(table.Cols) {
					return -1, nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE primary-key column")
				}
			}
		}
	}
	if primaryChanged {
		lookup := repeatedUniqueLookup{primary: true, ref: target.objRef, table: table, original: len(projection)}
		_, key, err := repeatedPrimaryKey(builder, table, input, original, true)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.current = len(projection)
		_, key, err = repeatedPrimaryKey(builder, table, input, current, false)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.proposed = len(projection)
		raw, key, err := repeatedPrimaryKey(builder, table, input, proposed, false)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.lockKey = len(projection)
		projection = append(projection, raw)
		lookups = append(lookups, lookup)
	}
	for _, index := range table.Indexes {
		if !index.Unique || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
			continue
		}
		affected := false
		for _, part := range index.Parts {
			if _, ok := changed[catalog.ResolveAlias(part)]; ok {
				affected = true
				break
			}
		}
		if !affected {
			continue
		}
		if len(index.Parts) == 0 {
			return -1, nil, moerr.NewInternalErrorNoCtx("empty repeated UPDATE unique key")
		}
		lookup := repeatedUniqueLookup{index: index, original: len(projection)}
		key, err := repeatedIndexKey(builder, table, index, input, original)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.current = len(projection)
		key, err = repeatedIndexKey(builder, table, index, input, current)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.proposed = len(projection)
		key, err = repeatedIndexKey(builder, table, index, input, proposed)
		if err != nil {
			return -1, nil, err
		}
		projection = append(projection, key)
		lookup.ref, lookup.table, err = builder.compCtx.ResolveIndexTableByRef(target.objRef, index.IndexTableName, nil)
		if err != nil {
			return -1, nil, err
		}
		lookups = append(lookups, lookup)
	}
	if len(lookups) == 0 {
		return last, nil, nil
	}
	last = builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: projection}, bindCtx)
	posMap := make(map[string]int, len(table.Cols))
	typMap := make(map[string]planpb.Type, len(table.Cols))
	for i, col := range table.Cols {
		pos := proposed[i]
		posMap[col.Name] = pos
		typMap[col.Name] = input[pos].Typ
	}
	for i := range lookups {
		lookup := &lookups[i]
		lookup.hit = len(builder.qry.Nodes[last].ProjectList)
		var err error
		if lookup.primary {
			last, err = appendRepeatedPrimaryLookup(builder, bindCtx, last, target, lookup.proposed)
		} else {
			last, err = appendDeleteIndexTablePlan(builder, bindCtx, lookup.ref, lookup.table, lookup.index, typMap, posMap, last, table, true)
		}
		if err != nil {
			return -1, nil, err
		}
	}

	pkPos, pkType := getPkPos(table, false)
	locks := []*planpb.LockTarget{{TableId: table.TblId, PrimaryColIdxInBat: int32(pkPos), PrimaryColTyp: pkType, RefreshTsIdxInBat: -1}}
	for _, lookup := range lookups {
		if lookup.primary {
			locks = append(locks, &planpb.LockTarget{
				TableId: table.TblId, PrimaryColIdxInBat: int32(lookup.lockKey),
				PrimaryColTyp: projection[lookup.lockKey].Typ, RefreshTsIdxInBat: -1,
			})
			continue
		}
		for _, pos := range []int{lookup.original, lookup.proposed} {
			locks = append(locks, &planpb.LockTarget{
				TableId: lookup.table.TblId, PrimaryColIdxInBat: int32(pos),
				PrimaryColTyp: projection[pos].Typ, RefreshTsIdxInBat: -1,
			})
		}
	}
	last = builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_LOCK_OP, Children: []int32{last}, LockTargets: locks,
		ProjectList: getProjectionByLastNode(builder, last),
	}, bindCtx)
	projection = getProjectionByLastNode(builder, last)
	keys := make([]planpb.UpdateUniqueKey, len(lookups))
	for i, lookup := range lookups {
		hit, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnotnull", []*planpb.Expr{DeepCopyExpr(projection[lookup.hit])})
		if err != nil {
			return -1, nil, err
		}
		keys[i] = planpb.UpdateUniqueKey{
			Original: int32(lookup.original), Current: int32(lookup.current), Proposed: int32(lookup.proposed), SnapshotHit: int32(len(projection)),
		}
		projection = append(projection, hit)
	}
	last = builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{last}, ProjectList: projection}, bindCtx)
	return last, keys, nil
}
