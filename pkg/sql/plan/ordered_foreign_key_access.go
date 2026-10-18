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
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
)

func foreignKeyParentColumnNames(ctx context.Context, parent *plan.TableDef, ids []uint64) ([]string, error) {
	idToName := make(map[uint64]string, len(parent.Cols))
	for _, col := range parent.Cols {
		idToName[col.ColId] = col.Name
	}
	names := make([]string, len(ids))
	for i, id := range ids {
		var ok bool
		if names[i], ok = idToName[id]; !ok {
			return nil, moerr.NewInternalErrorf(ctx, "foreign-key parent column %d not found", id)
		}
	}
	return names, nil
}

func foreignKeyIndexPartsEqual(parts, names []string) bool {
	if len(parts) != len(names) {
		return false
	}
	for i, part := range parts {
		if catalog.ResolveAlias(part) != names[i] {
			return false
		}
	}
	return true
}

// OrderedForeignKeyEqualityKeys derives both inputs from the same typed SQL
// equality. It does not mutate prepared metadata or add a raw serialization
// predicate to the SQL truth condition.
func OrderedForeignKeyEqualityKeys(ctx context.Context, check *plan.OrderedForeignKey) ([]*plan.Expr, []*plan.Expr, error) {
	if check == nil || check.Predicate == nil || len(check.ChildColumns) == 0 || len(check.ChildColumns) != len(check.ParentColumns) {
		return nil, nil, moerr.NewInvalidInput(ctx, "invalid ordered foreign-key equality")
	}
	conditions := colexec.SplitAndExprs([]*plan.Expr{DeepCopyExpr(check.Predicate)})
	if len(conditions) != len(check.ChildColumns) {
		return nil, nil, moerr.NewInvalidInput(ctx, "ordered foreign-key equality arity mismatch")
	}
	parents, children := make([]*plan.Expr, len(conditions)), make([]*plan.Expr, len(conditions))
	for _, condition := range conditions {
		if !IsEquiJoinCondition(condition) {
			return nil, nil, moerr.NewInvalidInput(ctx, "ordered foreign key requires complete component equalities")
		}
		parent, child := condition.GetF().Args[0], condition.GetF().Args[1]
		childForRefs := DeepCopyExpr(child)
		replaceColRefTag(childForRefs, 1, 0)
		parentRefs, childRefs := collectRefColPos(parent), collectRefColPos(childForRefs)
		if len(parentRefs) != 1 || len(childRefs) != 1 {
			return nil, nil, moerr.NewInvalidInput(ctx, "ordered foreign-key component must name one column per side")
		}
		slot := childRefs[0]
		if slot < 0 || int(slot) >= len(children) || children[slot] != nil || parentRefs[0] != check.ParentColumns[slot] {
			return nil, nil, moerr.NewInvalidInput(ctx, "ordered foreign-key component mapping mismatch")
		}
		parents[slot], children[slot] = parent, child
	}
	return parents, children, nil
}

// orderedForeignKeyCanPointReadPK preserves the existing native access guards.
// A UNIQUE lookup also needs this proof for its base-row recheck.
func orderedForeignKeyCanPointReadPK(table *plan.TableDef) bool {
	if table == nil || table.Pkey == nil || table.Name2ColIndex == nil || PrimaryKeyUsesUnicodeCollation(table) {
		return false
	}
	names := table.Pkey.Names
	if len(names) == 0 {
		names = []string{table.Pkey.PkeyColName}
	}
	for _, name := range names {
		pos, ok := table.Name2ColIndex[name]
		if !ok || pos < 0 || int(pos) >= len(table.Cols) || isFloatIndexFilterExpr(GetColExpr(table.Cols[pos].Typ, 0, pos)) {
			return false
		}
	}
	pos, _ := getPkPos(table, false)
	return pos >= 0
}

func (builder *QueryBuilder) orderedForeignKeyAccess(bindCtx *BindContext, check *plan.OrderedForeignKey) (*plan.OrderedForeignKeyAccess, error) {
	ctx, parent := builder.GetContext(), check.TableDef
	if parent == nil || parent.Pkey == nil {
		return nil, moerr.NewInvalidInput(ctx, "ordered foreign-key parent has no physical primary key")
	}
	pkPos, pkType := getPkPos(parent, false)
	if pkPos < 0 {
		return nil, moerr.NewInvalidInput(ctx, "ordered foreign-key parent primary key is missing")
	}
	access := &plan.OrderedForeignKeyAccess{
		Kind:      plan.OrderedForeignKeyAccess_RESIDENT,
		LookupRef: DeepCopyObjectRef(check.Ref), LookupTableDef: DeepCopyTableDef(parent, true),
		LockTarget:     &plan.LockTarget{TableId: parent.TblId, ObjRef: DeepCopyObjectRef(check.Ref), PrimaryColTyp: pkType, Mode: lockpb.LockMode_Shared},
		WitnessLockKey: GetColExpr(pkType, 0, int32(pkPos)),
	}
	parents, children, err := OrderedForeignKeyEqualityKeys(ctx, check)
	if err != nil {
		return nil, err
	}
	if !orderedForeignKeyCanPointReadPK(parent) {
		return access, nil
	}
	names := make([]string, len(check.ParentColumns))
	for i, pos := range check.ParentColumns {
		if pos < 0 || int(pos) >= len(parent.Cols) {
			return nil, moerr.NewInvalidInput(ctx, "invalid ordered foreign-key parent ordinal")
		}
		names[i] = parent.Cols[pos].Name
		col := parents[i].GetCol()
		if col == nil || col.ColPos != pos || !regularIndexCursorTypeMatches(parents[i].Typ, parent.Cols[pos].Typ) || !regularIndexCursorTypeMatches(children[i].Typ, parent.Cols[pos].Typ) {
			return access, nil
		}
	}
	pkNames := parent.Pkey.Names
	if len(pkNames) == 0 {
		pkNames = []string{parent.Pkey.PkeyColName}
	}
	if foreignKeyIndexPartsEqual(pkNames, names) {
		key := children[0]
		if parent.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			key, err = BindFuncExprImplByPlanExpr(ctx, "serial", children)
			if err != nil {
				return nil, err
			}
		}
		access.LookupFilter, err = BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{GetColExpr(pkType, 0, int32(pkPos)), key})
		if err != nil {
			return nil, err
		}
		access.Kind = plan.OrderedForeignKeyAccess_PK_POINT
		return access, nil
	}
	if err := validateTableIndexDefinitions(parent); err != nil {
		return nil, err
	}
	for _, index := range parent.Indexes {
		if !index.Unique || !index.TableExist || !foreignKeyIndexPartsEqual(index.Parts, names) {
			continue
		}
		if indexUsesUnicodeCollation(index, parent) && index.KeyFormat != collation.KeyFormatV1 {
			continue
		}
		ref, table, err := builder.compCtx.ResolveIndexTableByRef(check.Ref, index.IndexTableName, bindCtx.snapshot)
		if err != nil {
			return nil, err
		}
		if ref == nil || table == nil || table.Pkey == nil {
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		keyPos, keyType := getPkPos(table, false)
		basePKPos, ok := table.Name2ColIndex[catalog.IndexTablePrimaryColName]
		if keyPos < 0 || !ok || basePKPos < 0 || int(basePKPos) >= len(table.Cols) || !orderedForeignKeyCanPointReadPK(table) {
			continue
		}
		probeParts, witnessParts := make([]*plan.Expr, len(children)), make([]*plan.Expr, len(children))
		for i := range children {
			probeParts[i], err = builder.makeIndexLookupPartExpr(index, i, DeepCopyExpr(children[i]))
			if err != nil {
				return nil, err
			}
			witnessParts[i], err = builder.makeIndexLookupPartExpr(index, i, DeepCopyExpr(parents[i]))
			if err != nil {
				return nil, err
			}
		}
		key, err := bindIndexKeyExpr(ctx, index, probeParts)
		if err != nil {
			return nil, err
		}
		key, err = makePlan2CastExpr(ctx, key, keyType)
		if err != nil {
			return nil, err
		}
		filter, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{GetColExpr(keyType, 0, int32(keyPos)), key})
		if err != nil {
			return nil, err
		}
		if filter.GetF().Args[0].GetCol() == nil {
			continue
		}
		witnessKey, err := bindIndexKeyExpr(ctx, index, witnessParts)
		if err != nil {
			return nil, err
		}
		witnessKey, err = makePlan2CastExpr(ctx, witnessKey, keyType)
		if err != nil {
			return nil, err
		}
		access.Kind = plan.OrderedForeignKeyAccess_UNIQUE_POINT
		access.LookupRef, access.LookupTableDef, access.LookupFilter = DeepCopyObjectRef(ref), DeepCopyTableDef(table, true), filter
		access.LookupBasePk = GetColExpr(table.Cols[basePKPos].Typ, 0, basePKPos)
		access.LockTarget = &plan.LockTarget{TableId: table.TblId, ObjRef: DeepCopyObjectRef(ref), PrimaryColTyp: keyType, Mode: lockpb.LockMode_Shared}
		access.WitnessLockKey = witnessKey
		builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, prepareSchemaRefWithSnapshot(ref, table, bindCtx.snapshot))
		return access, nil
	}
	return access, nil
}

// ValidateOrderedInsertIgnore owns the immutable ordered payload contract at
// local preparation and both remote boundaries. Vector widths are checked by
// the executor when it receives the actual input batch.
func ValidateOrderedInsertIgnore(ctx context.Context, metadata *plan.PreInsertUkCtx) error {
	if metadata == nil {
		return moerr.NewInvalidInput(ctx, "missing pre-insert unique context")
	}
	spec := metadata.OrderedAutoIncrement
	checks := metadata.OrderedForeignKeys
	if spec != nil {
		if !metadata.InsertIgnoreMultiDedup || !metadata.AutoIncrementReorder || spec.TableDef == nil || spec.ColumnIndex < 0 || int(spec.ColumnIndex) >= len(spec.TableDef.Cols) {
			return moerr.NewInvalidInput(ctx, "invalid ordered AUTO_INCREMENT context")
		}
		col := spec.TableDef.Cols[spec.ColumnIndex]
		if col == nil || !col.Typ.AutoIncr || !types.T(col.Typ.Id).ToType().IsIntOrUint() {
			return moerr.NewInvalidInput(ctx, "invalid ordered AUTO_INCREMENT column")
		}
		if len(checks) > 0 {
			return moerr.NewInvalidInput(ctx, "invalid independent ordered foreign-key context")
		}
		checks = spec.ForeignKeys
	} else if metadata.AutoIncrementReorder && metadata.RowEligibility != nil {
		return moerr.NewInvalidInput(ctx, "eager AUTO_INCREMENT cannot consume ordered eligibility")
	}
	if len(checks) > 0 && !metadata.InsertIgnoreMultiDedup {
		return moerr.NewInvalidInput(ctx, "invalid independent ordered foreign-key context")
	}
	for _, check := range checks {
		if check == nil || check.TableDef == nil || check.TableDef.Pkey == nil || check.Predicate == nil || len(check.ChildColumns) == 0 || len(check.ParentColumns) != len(check.ChildColumns) {
			return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key context")
		}
		if (spec == nil || len(check.SelfParentColumns) > 0) && len(check.SelfParentColumns) != len(check.ChildColumns) {
			return moerr.NewInvalidInput(ctx, "invalid self-referencing foreign-key context")
		}
		for _, columns := range [][]*plan.ColRef{check.ChildColumns, check.SelfParentColumns} {
			for _, col := range columns {
				if col == nil || col.ColPos < 0 {
					return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key child column")
				}
			}
		}
		for _, pos := range check.ParentColumns {
			if pos < 0 || int(pos) >= len(check.TableDef.Cols) || check.TableDef.Cols[pos] == nil {
				return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key parent column")
			}
		}
		if _, _, err := OrderedForeignKeyEqualityKeys(ctx, check); err != nil {
			return err
		}
		access := check.ParentAccess
		if access == nil || access.LookupTableDef == nil || access.LookupTableDef.Pkey == nil || access.LockTarget == nil || access.WitnessLockKey == nil || access.LockTarget.LockTable || access.LockTarget.Mode != lockpb.LockMode_Shared {
			return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key access descriptor")
		}
		if access.LockTarget.PrimaryColTyp.Id != access.WitnessLockKey.Typ.Id || access.LockTarget.PrimaryColTyp.Scale != access.WitnessLockKey.Typ.Scale || HasColExpr(access.WitnessLockKey, -1) != 0 {
			return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key witness lock key")
		}
		for _, pos := range collectRefColPos(access.WitnessLockKey) {
			if pos < 0 || int(pos) >= len(check.TableDef.Cols) || check.TableDef.Cols[pos] == nil {
				return moerr.NewInvalidInput(ctx, "invalid ordered foreign-key witness ordinal")
			}
		}
		switch access.Kind {
		case plan.OrderedForeignKeyAccess_RESIDENT:
			if access.LookupFilter != nil || access.LookupBasePk != nil || access.LookupTableDef.TblId != check.TableDef.TblId || access.LockTarget.TableId != check.TableDef.TblId {
				return moerr.NewInvalidInput(ctx, "invalid resident foreign-key access")
			}
		case plan.OrderedForeignKeyAccess_PK_POINT:
			if access.LookupFilter == nil || access.LookupBasePk != nil || access.LookupTableDef.TblId != check.TableDef.TblId || access.LockTarget.TableId != check.TableDef.TblId {
				return moerr.NewInvalidInput(ctx, "invalid primary foreign-key access")
			}
		case plan.OrderedForeignKeyAccess_UNIQUE_POINT:
			if access.LookupFilter == nil || access.LookupBasePk == nil || access.LockTarget.TableId != access.LookupTableDef.TblId {
				return moerr.NewInvalidInput(ctx, "invalid unique foreign-key access")
			}
		default:
			return moerr.NewInvalidInput(ctx, "unknown ordered foreign-key access kind")
		}
		if access.Kind != plan.OrderedForeignKeyAccess_UNIQUE_POINT {
			pkPos, _ := getPkPos(check.TableDef, false)
			witness := access.WitnessLockKey.GetCol()
			if witness == nil || witness.RelPos != 0 || witness.ColPos != int32(pkPos) {
				return moerr.NewInvalidInput(ctx, "foreign-key witness must lock the physical parent primary key")
			}
		} else {
			basePK := access.LookupBasePk.GetCol()
			if basePK == nil || basePK.RelPos != 0 || basePK.ColPos < 0 || int(basePK.ColPos) >= len(access.LookupTableDef.Cols) || access.LookupTableDef.Cols[basePK.ColPos] == nil || access.LookupTableDef.Cols[basePK.ColPos].Name != catalog.IndexTablePrimaryColName {
				return moerr.NewInvalidInput(ctx, "invalid foreign-key base-row locator")
			}
		}
		if access.LookupFilter != nil {
			filter := DeepCopyExpr(access.LookupFilter)
			if !IsEquiJoinCondition(filter) {
				return moerr.NewInvalidInput(ctx, "invalid foreign-key lookup predicate")
			}
			col := filter.GetF().Args[0].GetCol()
			pkPos, _ := getPkPos(access.LookupTableDef, false)
			if col == nil || col.RelPos != 0 || col.ColPos != int32(pkPos) {
				return moerr.NewInvalidInput(ctx, "foreign-key point lookup must name its physical primary key")
			}
		}
		for _, expr := range []*plan.Expr{access.LookupFilter, access.LookupBasePk} {
			for _, pos := range collectRefColPos(expr) {
				if pos < 0 || int(pos) >= len(access.LookupTableDef.Cols) || access.LookupTableDef.Cols[pos] == nil {
					return moerr.NewInvalidInput(ctx, "invalid foreign-key access ordinal")
				}
			}
		}
	}
	return nil
}
