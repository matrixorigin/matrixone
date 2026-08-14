// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func skipPkDedup(old, new *TableDef, sourceColumns map[string]selectExpr) bool {
	oldPk := old.Pkey
	newPk := new.Pkey

	noOldPk := oldPk == nil || oldPk.PkeyColName == catalog.FakePrimaryKeyColName
	noNewPk := newPk == nil || newPk.PkeyColName == catalog.FakePrimaryKeyColName
	if noNewPk {
		return true
	}

	if noOldPk {
		return false
	}

	// The copy INSERT can skip PK dedup only when every target key value is
	// guaranteed to be identical to its source value. Matching column names are
	// not enough: a type conversion can collapse distinct values during copy.
	if !slices.Equal(oldPk.Names, newPk.Names) {
		return false
	}
	parts := newPk.Names
	if len(parts) == 0 {
		parts = []string{newPk.PkeyColName}
	}
	return alterCopyKeyPartsValueUnchanged(old, new, parts, sourceColumns)
}

func skipUniqueIdxDedup(old, new *TableDef, sourceColumns map[string]selectExpr) map[string]bool {
	var skip map[string]bool
	// In spite of the O(n^2) complexity,
	// it's rare for a table to have enough indexes to cause
	// meaningful performance degradation.
	for _, idx := range new.Indexes {
		if !idx.Unique {
			continue
		}
		for _, oldidx := range old.Indexes {
			if !oldidx.Unique {
				continue
			}
			if oldidx.IndexName == idx.IndexName &&
				slices.Equal(idx.Parts, oldidx.Parts) &&
				oldidx.IndexAlgo == idx.IndexAlgo &&
				oldidx.IndexAlgoParams == idx.IndexAlgoParams &&
				alterCopyKeyPartsValueUnchanged(old, new, idx.Parts, sourceColumns) {
				if skip == nil {
					skip = make(map[string]bool)
				}
				skip[idx.IndexName] = true
				break
			}
		}
	}
	return skip
}

func alterCopyKeyPartsValueUnchanged(
	old, new *TableDef,
	parts []string,
	sourceColumns map[string]selectExpr,
) bool {
	for _, part := range parts {
		name := catalog.ResolveAlias(part)
		source, ok := sourceColumns[name]
		// A same-name DROP/ADD creates a new target column, even when its type is
		// identical to the removed column. The copy INSERT then supplies a default
		// (or generated) value instead of reading the old column. Dedup can only be
		// skipped when the planner's source mapping proves this exact target key is
		// copied from the corresponding old key column.
		if !ok || source.sexprType != exprColumnName || !strings.EqualFold(source.sexprStr, name) {
			return false
		}
		oldCol := FindColumn(old.Cols, name)
		newCol := FindColumn(new.Cols, name)
		if !alterCopyKeyColumnValueUnchanged(oldCol, newCol) {
			return false
		}
	}
	return true
}

func alterCopyKeyColumnValueUnchanged(oldCol, newCol *ColDef) bool {
	if oldCol == nil || newCol == nil {
		return false
	}
	// Generated columns are recomputed for the copy INSERT. Even an unchanged
	// generated expression can produce different key values when one of its
	// input columns is altered, so keep target-side dedup enabled.
	if oldCol.GeneratedCol != nil || newCol.GeneratedCol != nil {
		return false
	}
	oldTyp, newTyp := oldCol.Typ, newCol.Typ
	return oldTyp.Id == newTyp.Id &&
		oldTyp.NotNullable == newTyp.NotNullable &&
		oldTyp.AutoIncr == newTyp.AutoIncr &&
		oldTyp.Width == newTyp.Width &&
		oldTyp.Scale == newTyp.Scale &&
		oldTyp.Table == newTyp.Table &&
		oldTyp.Enumvalues == newTyp.Enumvalues
}

func tableHasAutoIncrementColumn(tableDef *TableDef) bool {
	for _, col := range tableDef.Cols {
		if col.Typ.AutoIncr && !col.Hidden {
			return true
		}
	}
	return false
}

func reconcileAlterCopyIndexVisibility(ctx CompilerContext, tableID uint64, tableDef *TableDef) error {
	if len(tableDef.Indexes) == 0 {
		return nil
	}
	result, err := runSql(ctx, fmt.Sprintf(
		"SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = %d",
		tableID,
	))
	if err != nil {
		return err
	}
	defer result.Close()

	visibility := make(map[string]bool)
	var readErr error
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) != 2 {
			readErr = moerr.NewInternalErrorf(ctx.GetContext(),
				"invalid mo_indexes visibility result: expected 2 columns, got %d", len(cols))
			return false
		}
		names := executor.GetStringRows(cols[0])
		visible := executor.GetFixedRows[int8](cols[1])
		if len(names) != rows || len(visible) != rows {
			readErr = moerr.NewInternalErrorf(ctx.GetContext(),
				"invalid mo_indexes visibility result: expected %d rows", rows)
			return false
		}
		for i, name := range names {
			value := visible[i] != 0
			key := strings.ToLower(name)
			if previous, ok := visibility[key]; ok && previous != value {
				readErr = moerr.NewInternalErrorf(ctx.GetContext(),
					"inconsistent visibility metadata for index '%s'", name)
				return false
			}
			visibility[key] = value
		}
		return true
	})
	if readErr != nil {
		return readErr
	}

	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil {
			return moerr.NewInternalError(ctx.GetContext(), "nil index metadata")
		}
		if visible, ok := visibility[strings.ToLower(indexDef.IndexName)]; ok {
			indexDef.Visible = visible
		}
	}
	return nil
}

func autoIncrementValueToOffset(value uint64) uint64 {
	if value > 0 {
		return value - 1
	}
	return 0
}

func buildAlterTableCopy(stmt *tree.AlterTable, cctx CompilerContext) (*Plan, error) {
	ctx := cctx.GetContext()
	// 1. get origin table name and Schema name
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = cctx.DefaultDatabase()
	}

	var snapshot *Snapshot
	_, tableDef, err := cctx.Resolve(schemaName, tableName, snapshot)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx, schemaName, tableName)
	}

	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	accountId, err := cctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if isClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx, "only the sys account can alter the cluster table")
	}

	// 2. split alter_option list
	copyTableDef, err := buildCopyTableDef(ctx, tableDef)
	if err != nil {
		return nil, err
	}
	// IndexDef.visible historically used the proto3 false zero value for both
	// default-visible and explicitly invisible indexes. mo_indexes is the
	// authoritative source, so normalize the copied definition before applying
	// the ALTER actions and serializing the temporary CREATE TABLE statement.
	if err := reconcileAlterCopyIndexVisibility(cctx, tableDef.TblId, copyTableDef); err != nil {
		return nil, err
	}
	// The copied definition contains the source allocator's cached offset. It
	// is not a user request and can be far ahead of the actual rows. The copy
	// executor reconciles the explicit request, copied maximum, and source
	// allocator state after the rows are visible in this transaction.
	copyTableDef.AutoIncrOffset = 0
	alterTableCtx := initAlterTableContext(tableDef, copyTableDef, schemaName)

	// 3. check alter_option list
	// set name for anonymous foreign key.
	tmpForeignKeyId := 0
	validAlterSpecs := stmt.Options
	for _, spec := range validAlterSpecs {
		if alterOpt, ok := spec.(*tree.AlterOptionAdd); ok {
			if foreignKey, ok2 := alterOpt.Def.(*tree.ForeignKey); ok2 && foreignKey.Name == "" {
				foreignKey.Name = fmt.Sprintf("fk_%d", tmpForeignKeyId)
			}
		}
	}

	// 4. traverse and handle alter options
	alterTablePlan := &plan.AlterTable{
		Database:       schemaName,
		TableDef:       tableDef,
		CopyTableDef:   copyTableDef,
		IsClusterTable: isClusterTable,
		AlgorithmType:  plan.AlterTable_COPY,
	}

	var (
		pkAffected             bool
		hasAutoIncrementOption bool

		affectedCols        = make([]string, 0, len(tableDef.Cols))
		affectedIndexes     = make([]string, 0, len(tableDef.Indexes))
		unsupportedErrorFmt = "unsupported alter option in copy mode: %s"
		copyFakePKCol       = catalog.IsFakePkName(tableDef.Pkey.PkeyColName)
	)

	affectedAllIdxCols := func() {
		copyFakePKCol = false
		affectedCols = affectedCols[:0]
		for _, colDef := range tableDef.Cols {
			affectedCols = append(affectedCols, colDef.Name)
		}

		affectedIndexes = affectedIndexes[:0]
		for _, idxDef := range tableDef.Indexes {
			affectedIndexes = append(affectedIndexes, idxDef.IndexName)
		}
	}

	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAdd:
			switch optionAdd := option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				err = AddPrimaryKey(cctx, alterTablePlan, optionAdd, alterTableCtx)
				affectedAllIdxCols()
			default:
				// column adding is handled in *tree.AlterAddCol
				// various indexes\fks adding are handled in inplace mode.
				return nil, moerr.NewInvalidInputf(ctx,
					unsupportedErrorFmt, formatTreeNode(option))
			}
		case *tree.AlterOptionDrop:
			switch option.Typ {
			case tree.AlterTableDropColumn:
				pkAffected, err = DropColumn(cctx, alterTablePlan, string(option.Name), alterTableCtx)
				affectedCols = append(affectedCols, string(option.Name))
			case tree.AlterTableDropPrimaryKey:
				err = DropPrimaryKey(cctx, alterTablePlan, alterTableCtx)
				affectedAllIdxCols()
			default:
				// various indexes\fks dropping are handled in inplace mode.
				return nil, moerr.NewInvalidInputf(ctx,
					unsupportedErrorFmt, formatTreeNode(option))
			}
		case *tree.AlterAddCol:
			pkAffected, err = AddColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.Column.Name.ColName())
		case *tree.AlterTableModifyColumnClause:
			pkAffected, err = ModifyColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.NewColumn.Name.ColName())
		case *tree.AlterTableChangeColumnClause:
			pkAffected, err = ChangeColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = appendAffectedAlterColumnNames(
				affectedCols,
				option.OldColumnName.ColName(),
				option.NewColumn.Name.ColName(),
			)
		case *tree.AlterTableRenameColumnClause:
			err = RenameColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.OldColumnName.ColName())
		case *tree.AlterTableAlterColumnClause:
			pkAffected, err = AlterColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.ColumnName.String())
		case *tree.TableOptionAutoIncrement:
			hasAutoIncrementOption = true
			copyTableDef.AutoIncrOffset = autoIncrementValueToOffset(option.Value)
			alterTablePlan.Actions = append(alterTablePlan.Actions, &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterAutoIncrement{
					AlterAutoIncrement: &plan.AlterTableAutoIncrement{
						NewOffset: copyTableDef.AutoIncrOffset,
					},
				},
			})
		case *tree.AlterTableOrderByColumnClause:
			err = OrderByColumn(cctx, alterTablePlan, option, alterTableCtx)
			for _, order := range option.AlterOrderByList {
				affectedCols = append(affectedCols, order.Column.ColName())
			}
		case *tree.AlterOptionAlgorithm:
			// algorithm hint parsed for compatibility; the actual algorithm
			// is resolved by ResolveAlterTableAlgorithm via the full options list
		case *tree.AlterOptionLock:
			// lock already validated by resolveAndValidateLock; no-op here
		default:
			return nil, moerr.NewInvalidInputf(ctx,
				unsupportedErrorFmt, formatTreeNode(option))
		}
		if err != nil {
			return nil, err
		}
	}
	if hasAutoIncrementOption && !tableHasAutoIncrementColumn(copyTableDef) {
		return nil, moerr.NewInvalidInputf(ctx,
			"Table '%s' does not have an AUTO_INCREMENT column", tableDef.Name)
	}

	if pkAffected {
		affectedAllIdxCols()
	} else {
		affectedCols, err = collectAffectedIndexNamesForAlter(tableDef.Indexes, affectedCols)
		if err != nil {
			return nil, err
		}
	}

	createTmpDdl, _, err := ConstructCreateTableSQL(cctx, copyTableDef, snapshot, true, nil)
	if err != nil {
		return nil, err
	}

	alterTablePlan.CreateTmpTableSql = createTmpDdl
	alterTablePlan.AffectedCols = affectedCols

	opt := &plan.AlterCopyOpt{
		SkipPkDedup:        skipPkDedup(tableDef, copyTableDef, alterTableCtx.alterColMap),
		TargetTableName:    copyTableDef.Name,
		SkipUniqueIdxDedup: skipUniqueIdxDedup(tableDef, copyTableDef, alterTableCtx.alterColMap),
	}

	opt.SkipIndexesCopy = make(map[string]bool)
	for _, idxCol := range tableDef.Indexes {
		if len(affectedIndexes) > 0 {
			// the only way to has non-empty affectedIndexes is by calling affectedAllIdxCols()
			// AffectedCols has all Columns and AffectedIndexes has all indexes
			if slices.Index(affectedIndexes, idxCol.IndexName) == -1 {
				opt.SkipIndexesCopy[idxCol.IndexName] = true
			}
		} else {
			// affectedIndexes is empty
			if slices.Index(affectedCols, idxCol.IndexName) == -1 {
				opt.SkipIndexesCopy[idxCol.IndexName] = true
			}
		}
	}

	alterTablePlan.Options = opt
	logutil.Info("alter copy option",
		zap.Any("originPk", tableDef.Pkey),
		zap.Any("copyPk", copyTableDef.Pkey),
		zap.Strings("affectedCols", affectedCols),
		zap.Any("option", opt))

	insertTmpDml, err := buildAlterInsertDataSQL(cctx, alterTableCtx, copyTableDef, copyFakePKCol)
	if err != nil {
		return nil, err
	}
	alterTablePlan.InsertTmpDataSql = insertTmpDml

	alterTablePlan.ChangeTblColIdMap = alterTableCtx.changColDefMap
	alterTablePlan.UpdateFkSqls = append(alterTablePlan.UpdateFkSqls, alterTableCtx.UpdateSqls...)
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_TABLE,
				Definition: &plan.DataDefinition_AlterTable{
					AlterTable: alterTablePlan,
				},
			},
		},
	}, nil
}

func appendAffectedAlterColumnNames(affectedCols []string, oldColName, newColName string) []string {
	affectedCols = append(affectedCols, oldColName)
	if newColName != oldColName {
		affectedCols = append(affectedCols, newColName)
	}
	return affectedCols
}

var ID atomic.Int64

func buildAlterInsertDataSQL(
	ctx CompilerContext,
	alterCtx *AlterTableContext,
	copyTableDef *TableDef,
	copyFakePKCol bool,
) (string, error) {

	schemaName := alterCtx.schemaName
	originTableName := alterCtx.originTableName
	copyTableName := alterCtx.copyTableName

	insertBuffer := bytes.NewBufferString("")
	selectBuffer := bytes.NewBufferString("")

	isFirst := true
	for key, value := range alterCtx.alterColMap {
		copyCol := FindColumn(copyTableDef.Cols, key)
		if copyCol != nil && copyCol.GeneratedCol != nil {
			continue
		}
		if isFirst {
			insertBuffer.WriteString("`" + key + "`")
			if value.sexprType == exprColumnName {
				selectBuffer.WriteString("`" + value.sexprStr + "`")
			} else {
				selectBuffer.WriteString(value.sexprStr)
			}
			isFirst = false
		} else {
			insertBuffer.WriteString(", " + "`" + key + "`")

			if value.sexprType == exprColumnName {
				selectBuffer.WriteString(", " + "`" + value.sexprStr + "`")
			} else {
				selectBuffer.WriteString(", " + value.sexprStr)
			}
		}
	}

	if copyFakePKCol {
		// why select fake pk col here?
		// we want to clone unaffected indexes to avoid deep copy table.
		// but if the primary table has tombstones, the re-generated fake pk column
		// will be mismatched with these index tables, the shallow copy won't work.
		// so we need to select these fake pks into the new table.
		//
		// example:
		// create table t1(a int, b int, index(b));
		// insert into t1 select *, * from generate_series(1,1000*100)g;
		// delete from t1 where a = 1;
		// alter table t1 add column c int;
		// delete from t1 where a = 2;
		// fails, cannot find this row by join index table and the primary table.
		//
		str := fmt.Sprintf(", `%s`", catalog.FakePrimaryKeyColName)
		insertBuffer.WriteString(str)
		selectBuffer.WriteString(str)
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) SELECT %s FROM `%s`.`%s`",
		formatStr(schemaName), formatStr(copyTableName), insertBuffer.String(),
		selectBuffer.String(), formatStr(schemaName), formatStr(originTableName))

	return insertSQL, nil
}

const UnKnownColId uint64 = math.MaxUint64

type AlterTableContext struct {
	// key   --> Copy table column name, letter case: lower
	// value --> Original table column name
	alterColMap     map[string]selectExpr
	schemaName      string
	originTableName string
	copyTableName   string
	// key oldColId -> new ColDef
	changColDefMap map[uint64]*ColDef
	UpdateSqls     []string
}

type exprType int

const (
	exprConstValue exprType = iota
	exprColumnName
)

type selectExpr struct {
	sexprType exprType
	sexprStr  string
}

func initAlterTableContext(originTableDef *TableDef, copyTableDef *TableDef, schemaName string) *AlterTableContext {
	alterTblColMap := make(map[string]selectExpr)
	changTblColIdMap := make(map[uint64]*ColDef)
	for _, coldef := range originTableDef.Cols {
		if coldef.Hidden {
			continue
		}

		alterTblColMap[coldef.Name] = selectExpr{
			sexprType: exprColumnName,
			sexprStr:  coldef.Name,
		}

		changTblColIdMap[coldef.ColId] = &plan.ColDef{
			ColId:      UnKnownColId,
			Name:       coldef.Name,
			OriginName: coldef.OriginName,
		}
	}
	return &AlterTableContext{
		alterColMap:     alterTblColMap,
		schemaName:      schemaName,
		originTableName: originTableDef.Name,
		copyTableName:   copyTableDef.Name,
		changColDefMap:  changTblColIdMap,
	}
}

func buildCopyTableDef(ctx context.Context, tableDef *TableDef) (*TableDef, error) {
	replicaTableDef := DeepCopyTableDef(tableDef, true)

	id, err := uuid.NewV7()
	if err != nil {
		return nil, moerr.NewInternalError(ctx, "new uuid failed")
	}
	replicaTableDef.Name = replicaTableDef.Name + "_copy_" + id.String()
	return replicaTableDef, nil
}

func buildAlterTable(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	// ALTER TABLE tbl_name
	//		[alter_option [, alter_option] ...]
	//		[partition_options]
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = ctx.DefaultDatabase()
	}
	objRef, tableDef, err := ctx.Resolve(schemaName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
	}
	if err := validateTableIndexDefinitions(tableDef); err != nil {
		return nil, err
	}
	isMongoDB, err := IsMongoDBTableDef(ctx.GetContext(), tableDef)
	if err != nil {
		return nil, err
	}
	if isMongoDB {
		return nil, moerr.NewNotSupported(ctx.GetContext(),
			"ALTER TABLE on a MongoDB external table; drop and recreate the external table to change its schema")
	}

	if tableDef.IsTemporary {
		// Only allow a safe subset of alter operations on temporary tables.
		// For now: add index / drop index.
		if !allowTempTableAlterForIndex(stmt) {
			return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
		}
	}

	if tableDef.ViewSql != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
	}
	if objRef.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
	}
	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if isClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
	}

	if stmt.PartitionOption != nil {
		if stmt.Options != nil {
			return nil, moerr.NewParseError(ctx.GetContext(), "Unsupported multi schema change")
		}
		return buildAlterTableInplace(stmt, ctx)
	}

	algorithm, err := ResolveAlterTableAlgorithm(ctx.GetContext(), stmt.Options, tableDef)
	if err != nil {
		return nil, err
	}

	if err := resolveAndValidateLock(ctx.GetContext(), stmt.Options, algorithm); err != nil {
		return nil, err
	}

	if algorithm == plan.AlterTable_COPY {
		return buildAlterTableCopy(stmt, ctx)
	} else {
		return buildAlterTableInplace(stmt, ctx)
	}
}

// allowTempTableAlterForIndex returns true if the alter table statement
// is limited to add/drop index operations, which we support for temp tables.
func allowTempTableAlterForIndex(stmt *tree.AlterTable) bool {
	// partition alter is not allowed for temp table
	if stmt.PartitionOption != nil {
		return false
	}
	for _, opt := range stmt.Options {
		switch o := opt.(type) {
		case *tree.AlterOptionAlgorithm, *tree.AlterOptionLock:
			// hints only; validated later by ResolveAlterTableAlgorithm / resolveAndValidateLock
		case *tree.AlterOptionAdd:
			switch o.Def.(type) {
			case *tree.Index, *tree.UniqueIndex, *tree.FullTextIndex:
				// supported add index variants
			default:
				return false
			}
		case *tree.AlterOptionDrop:
			switch o.Typ {
			case tree.AlterTableDropIndex, tree.AlterTableDropKey:
				// supported drop index/key
			default:
				return false
			}
		default:
			return false
		}
	}
	return true
}

func ResolveAlterTableAlgorithm(
	ctx context.Context,
	validAlterSpecs []tree.AlterTableOption,
	tableDef *TableDef,
) (algorithm plan.AlterTable_AlgorithmType, err error) {
	algorithm = plan.AlterTable_COPY

	// First pass: resolve algorithm based on operations, skipping ALGORITHM/LOCK hints.
Loop:
	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAlgorithm, *tree.AlterOptionLock:
			continue
		case *tree.AlterOptionAdd:
			switch option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				algorithm = plan.AlterTable_COPY
			case *tree.ForeignKey:
				algorithm = plan.AlterTable_INPLACE
			case *tree.UniqueIndex:
				algorithm = plan.AlterTable_INPLACE
			case *tree.Index:
				algorithm = plan.AlterTable_INPLACE
			default:
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterOptionDrop:
			switch option.Typ {
			case tree.AlterTableDropColumn:
				algorithm = plan.AlterTable_COPY
			case tree.AlterTableDropIndex:
				algorithm = plan.AlterTable_INPLACE
			case tree.AlterTableDropKey:
				algorithm = plan.AlterTable_INPLACE
			case tree.AlterTableDropPrimaryKey:
				algorithm = plan.AlterTable_COPY
			case tree.AlterTableDropForeignKey:
				algorithm = plan.AlterTable_INPLACE
			default:
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterOptionAlterIndex:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionAlterReIndex:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionAlterAutoUpdate:
			algorithm = plan.AlterTable_INPLACE
		case *tree.TableOptionComment:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionTableName:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterAddCol:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableModifyColumnClause:
			algorithm = plan.AlterTable_COPY
			var ok bool
			ok, err = isInplaceModifyColumn(ctx, option, tableDef)
			if err != nil {
				return
			}
			if ok {
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterTableChangeColumnClause:
			algorithm = plan.AlterTable_COPY
			var ok bool
			ok, err = isInplaceChangeColumn(ctx, option, tableDef)
			if err != nil {
				return
			}
			if ok {
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterTableRenameColumnClause:
			requiresRebuild, err := renameColumnRequiresPluginIndexRebuild(tableDef, option.OldColumnName.ColName())
			if err != nil {
				return plan.AlterTable_DEFAULT, err
			}
			if requiresRebuild {
				algorithm = plan.AlterTable_COPY
			} else {
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterTableAlterColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableOrderByColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.TableOptionAutoIncrement:
			algorithm = plan.AlterTable_INPLACE
		default:
			algorithm = plan.AlterTable_INPLACE
		}
		if algorithm == plan.AlterTable_COPY {
			break Loop
		}
	}

	requiredAlgorithm := algorithm // stable baseline for hint validation; algorithm is mutated below

	// Second pass: apply ALGORITHM hint (takes precedence over operation-based resolution).
	for _, spec := range validAlterSpecs {
		alg, ok := spec.(*tree.AlterOptionAlgorithm)
		if !ok {
			continue
		}
		userAlg := resolveAlgorithmHint(alg.Type)
		if userAlg == plan.AlterTable_DEFAULT {
			continue
		}
		if requiredAlgorithm == plan.AlterTable_COPY && userAlg != plan.AlterTable_COPY {
			return algorithm, moerr.NewInvalidInputf(ctx,
				"ALGORITHM=%s is not supported. Reason: this operation requires ALGORITHM=COPY. Try ALGORITHM=COPY.",
				strings.ToUpper(alg.Type))
		}
		algorithm = userAlg
	}

	return
}

func resolveAlgorithmHint(algType string) plan.AlterTable_AlgorithmType {
	switch strings.ToUpper(algType) {
	case "INSTANT":
		return plan.AlterTable_INSTANT
	case "INPLACE":
		return plan.AlterTable_INPLACE
	case "COPY":
		return plan.AlterTable_COPY
	default:
		return plan.AlterTable_DEFAULT
	}
}

func resolveAndValidateLock(
	ctx context.Context,
	options []tree.AlterTableOption,
	algorithm plan.AlterTable_AlgorithmType,
) error {
	// MySQL uses the last LOCK clause if multiple are specified.
	lockType := ""
	for _, opt := range options {
		if lock, ok := opt.(*tree.AlterOptionLock); ok {
			lockType = strings.ToUpper(lock.Type)
		}
	}
	if lockType == "NONE" && algorithm == plan.AlterTable_COPY {
		return moerr.NewInvalidInputf(ctx,
			"LOCK=NONE is not supported. Reason: COPY algorithm requires an exclusive lock. Try LOCK=SHARED.")
	}
	return nil
}

func isInplaceModifyColumn(
	ctx context.Context,
	clause *tree.AlterTableModifyColumnClause,
	tableDef *TableDef,
) (ok bool, err error) {
	return isInplaceColumnDefinition(ctx, clause.NewColumn, clause.Position, tableDef)
}

func isInplaceChangeColumn(
	ctx context.Context,
	clause *tree.AlterTableChangeColumnClause,
	tableDef *TableDef,
) (ok bool, err error) {
	// CHANGE keeps the original spelling for catalog metadata. A case-only
	// rename is therefore not equivalent to MODIFY when identifiers compare
	// case-insensitively: it must use COPY to update foreign-key catalog rows.
	if clause.OldColumnName.ColNameOrigin() != clause.NewColumn.Name.ColNameOrigin() {
		return false, nil
	}
	return isInplaceColumnDefinition(ctx, clause.NewColumn, clause.Position, tableDef)
}

func isInplaceColumnDefinition(
	ctx context.Context,
	column *tree.ColumnTableDef,
	position *tree.ColumnPosition,
	tableDef *TableDef,
) (ok bool, err error) {
	oCol := FindColumn(tableDef.Cols, column.Name.ColName())
	if oCol == nil {
		err = moerr.NewBadFieldError(
			ctx, column.Name.ColNameOrigin(), tableDef.Name)
		return
	}

	ok, err = positionMatched(ctx, position, tableDef, oCol)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	ok, err = storageAgnosticType(ctx, column, oCol, tableDef.DefaultCharset)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	ok, err = storageAgnosticAttrs(ctx, column, oCol)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	return
}

func positionMatched(
	ctx context.Context,
	nPos *tree.ColumnPosition,
	tableDef *TableDef,
	oCol *ColDef,
) (ok bool, err error) {
	ok = true
	if nPos != nil && nPos.Typ != tree.ColumnPositionNone {
		var newPos int
		newPos, err = findPositionRelativeColumn(ctx, tableDef.Cols, nPos)
		if err != nil {
			ok = false
			return
		}
		if newPos != int(oCol.ColId-1) {
			ok = false
		}
	}
	return
}

// return true for char and varchar with increased width
func storageAgnosticType(
	ctx context.Context,
	nCol *tree.ColumnTableDef,
	oCol *ColDef,
	defaultCharset uint32,
) (ok bool, err error) {

	nTy, err := getTypeFromAst(ctx, nCol.Type)
	if err != nil {
		return
	}
	nTy.Charset = uint32(types.CharsetType(types.T(nTy.Id)))
	if err = applyDefaultAndColumnAttributesToType(ctx, &nTy, defaultCharset, nCol.Attributes); err != nil {
		return
	}

	oTy := oCol.Typ

	if oTy.Id != nTy.Id ||
		oTy.Scale != nTy.Scale ||
		oTy.Enumvalues != nTy.Enumvalues ||
		oTy.AutoIncr != nTy.AutoIncr ||
		oTy.Charset != nTy.Charset {
		return
	}

	if nTy.Id == int32(types.T_varchar) || nTy.Id == int32(types.T_char) {
		ok = oTy.Width <= nTy.Width
		return
	}

	// For every other type, only a byte-for-byte identical storage layout can
	// avoid a COPY. Attribute changes are checked separately below.
	ok = oTy.Width == nTy.Width
	return
}

func storageAgnosticAttrs(
	_ context.Context,
	nCol *tree.ColumnTableDef,
	oCol *ColDef,
) (ok bool, err error) {
	ok = true
	for _, attr := range nCol.Attributes {
		switch a := attr.(type) {
		case *tree.AttributeNull:
			oCanBeNull := oCol.Default != nil && oCol.Default.NullAbility
			nCanBeNull := a.Is
			// ❌ Null -> Not Null: rewrite to check
			// ✅ Not Null -> Null: drop not null is allowed
			// ✅ Not Null -> Not Null
			// ✅ Null -> Null
			if oCanBeNull != nCanBeNull && oCanBeNull {
				ok = false
			}
		case *tree.AttributeOnUpdate:
			oExpr := ""
			if oCol.OnUpdate != nil {
				oExpr = oCol.OnUpdate.OriginString
			}
			nExpr := tree.String(a.Expr, dialect.MYSQL)
			if oExpr != nExpr {
				ok = false
			}
		case *tree.AttributeComment, *tree.AttributeDefault:
			// keep ok true, we don't care about what comment or default is
			ok = true
		default:
			// key, primary key, unique key, auto increment, reference etc.
			// all of these involve third party constraint tables
			// so we don't support them in inplace alter table
			ok = false
		}
		if !ok {
			return
		}
	}
	return
}

func buildNotNullColumnVal(col *ColDef) string {
	var defaultValue string
	// SET uses T_uint64 as its underlying OID, so this check must come before
	// the integer branch below to avoid treating SET columns as plain uint64.
	if isSetPlanType(&col.Typ) {
		defaultValue = "''"
	} else if isGeometryPlanType(&col.Typ) {
		defaultValue = buildGeometryNotNullColumnVal(col)
	} else if col.Typ.Id == int32(types.T_int8) ||
		col.Typ.Id == int32(types.T_int16) ||
		col.Typ.Id == int32(types.T_int32) ||
		col.Typ.Id == int32(types.T_int64) ||
		col.Typ.Id == int32(types.T_uint8) ||
		col.Typ.Id == int32(types.T_uint16) ||
		col.Typ.Id == int32(types.T_uint32) ||
		col.Typ.Id == int32(types.T_uint64) ||
		col.Typ.Id == int32(types.T_float32) ||
		col.Typ.Id == int32(types.T_float64) ||
		col.Typ.Id == int32(types.T_decimal64) ||
		col.Typ.Id == int32(types.T_decimal128) ||
		col.Typ.Id == int32(types.T_decimal256) ||
		col.Typ.Id == int32(types.T_bool) ||
		col.Typ.Id == int32(types.T_bit) {
		defaultValue = "0"
	} else if col.Typ.Id == int32(types.T_varchar) ||
		col.Typ.Id == int32(types.T_char) ||
		col.Typ.Id == int32(types.T_text) ||
		col.Typ.Id == int32(types.T_datalink) ||
		col.Typ.Id == int32(types.T_binary) ||
		col.Typ.Id == int32(types.T_blob) {
		defaultValue = "''"
	} else if col.Typ.Id == int32(types.T_date) {
		defaultValue = "'0001-01-01'"
	} else if col.Typ.Id == int32(types.T_datetime) {
		defaultValue = "'0001-01-01 00:00:00'"
	} else if col.Typ.Id == int32(types.T_time) {
		defaultValue = "'00:00:00'"
	} else if col.Typ.Id == int32(types.T_timestamp) {
		defaultValue = "'0001-01-01 00:00:00'"
	} else if col.Typ.Id == int32(types.T_json) {
		//defaultValue = "null"
		defaultValue = "'{}'"
	} else if isEnumPlanType(&col.Typ) {
		enumvalues := strings.Split(col.Typ.Enumvalues, ",")
		defaultValue = enumvalues[0]
	} else if types.T(col.Typ.Id).IsArrayRelate() {
		// IsArrayRelate covers all six vector types. Enumerating only f32/f64
		// here made ALTER TABLE ... ADD v VECF16(n) NOT NULL fall through to
		// "null" below — an invalid backfill for a NOT NULL column, where the
		// same statement on vecf32 synthesized a zero vector.
		if col.Typ.Width > 0 {
			zerosWithCommas := strings.Repeat("0,", int(col.Typ.Width)-1)
			arrayAsString := zerosWithCommas + "0" // final zero
			defaultValue = fmt.Sprintf("'[%s]'", arrayAsString)
		} else {
			defaultValue = "'[]'"
		}
	} else {
		defaultValue = "null"
	}
	return defaultValue
}

func buildGeometryNotNullColumnVal(col *ColDef) string {
	emptyWKT := geometryEmptyWKTForSubtype(geometrySubtypeName(&col.Typ))
	srid, ok := geometrySRIDValue(&col.Typ)
	if ok {
		return fmt.Sprintf("st_geomfromtext('%s', %d)", emptyWKT, srid)
	}
	return fmt.Sprintf("st_geomfromtext('%s')", emptyWKT)
}

func geometryEmptyWKTForSubtype(subtype string) string {
	subtype = normalizeGeometrySubtype(subtype)
	switch subtype {
	case "", "GEOMETRY", "GEOMETRYCOLLECTION":
		return "GEOMETRYCOLLECTION EMPTY"
	case "POINT":
		return "POINT EMPTY"
	case "LINESTRING":
		return "LINESTRING EMPTY"
	case "POLYGON":
		return "POLYGON EMPTY"
	case "MULTIPOINT":
		return "MULTIPOINT EMPTY"
	case "MULTILINESTRING":
		return "MULTILINESTRING EMPTY"
	case "MULTIPOLYGON":
		return "MULTIPOLYGON EMPTY"
	default:
		return subtype + " EMPTY"
	}
}
