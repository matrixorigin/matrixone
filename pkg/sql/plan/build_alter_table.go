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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"go.uber.org/zap"
)

func skipPkDedup(old, new *TableDef) bool {
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

	// oldPk and newPk are not nil, check if the primary key is the same
	return slices.Equal(oldPk.Names, newPk.Names)
}

func skipUniqueIdxDedup(old, new *TableDef) map[string]bool {
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
				slices.Equal(idx.Parts, oldidx.Parts) {
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
		pkAffected bool

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
			affectedCols = append(affectedCols, option.NewColumn.Name.ColName())
		case *tree.AlterTableRenameColumnClause:
			err = RenameColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.OldColumnName.ColName())
		case *tree.AlterTableAlterColumnClause:
			pkAffected, err = AlterColumn(cctx, alterTablePlan, option, alterTableCtx)
			affectedCols = append(affectedCols, option.ColumnName.String())
		case *tree.AlterTableOrderByColumnClause:
			err = OrderByColumn(cctx, alterTablePlan, option, alterTableCtx)
			for _, order := range option.AlterOrderByList {
				affectedCols = append(affectedCols, order.Column.ColName())
			}
		default:
			return nil, moerr.NewInvalidInputf(ctx,
				unsupportedErrorFmt, formatTreeNode(option))
		}
		if err != nil {
			return nil, err
		}
	}

	createTmpDdl, _, err := ConstructCreateTableSQL(cctx, copyTableDef, snapshot, true, nil)
	if err != nil {
		return nil, err
	}

	alterTablePlan.CreateTmpTableSql = createTmpDdl

	if pkAffected {
		affectedAllIdxCols()
	}

	alterTablePlan.AffectedCols = affectedCols

	opt := &plan.AlterCopyOpt{
		SkipPkDedup:        skipPkDedup(tableDef, copyTableDef),
		TargetTableName:    copyTableDef.Name,
		SkipUniqueIdxDedup: skipUniqueIdxDedup(tableDef, copyTableDef),
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

	insertTmpDml, err := buildAlterInsertDataSQL(cctx, alterTableCtx, copyFakePKCol)
	if err != nil {
		return nil, err
	}
	alterTablePlan.InsertTmpDataSql = insertTmpDml

	alterTablePlan.ChangeTblColIdMap = alterTableCtx.changColDefMap
	alterTablePlan.UpdateFkSqls = append(alterTablePlan.UpdateFkSqls, alterTableCtx.UpdateSqls...)
	//delete copy table records from mo_catalog.mo_foreign_keys
	alterTablePlan.UpdateFkSqls = append(alterTablePlan.UpdateFkSqls, getSqlForDeleteTable(schemaName, alterTableCtx.copyTableName))
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

var ID atomic.Int64

func buildAlterInsertDataSQL(
	ctx CompilerContext,
	alterCtx *AlterTableContext,
	copyFakePKCol bool,
) (string, error) {

	schemaName := alterCtx.schemaName
	originTableName := alterCtx.originTableName
	copyTableName := alterCtx.copyTableName

	insertBuffer := bytes.NewBufferString("")
	selectBuffer := bytes.NewBufferString("")

	isFirst := true
	for key, value := range alterCtx.alterColMap {
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

	if tableDef.IsTemporary {
		return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
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
	if algorithm == plan.AlterTable_COPY {
		return buildAlterTableCopy(stmt, ctx)
	} else {
		return buildAlterTableInplace(stmt, ctx)
	}
}

func ResolveAlterTableAlgorithm(
	ctx context.Context,
	validAlterSpecs []tree.AlterTableOption,
	tableDef *TableDef,
) (algorithm plan.AlterTable_AlgorithmType, err error) {
	algorithm = plan.AlterTable_COPY
	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
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
		case *tree.AlterTableRenameColumnClause:
			algorithm = plan.AlterTable_INPLACE
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
			return
		}
	}
	return
}

func isInplaceModifyColumn(
	ctx context.Context,
	clause *tree.AlterTableModifyColumnClause,
	tableDef *TableDef,
) (ok bool, err error) {
	oCol := FindColumn(tableDef.Cols, clause.NewColumn.Name.ColName())
	if oCol == nil {
		err = moerr.NewBadFieldError(
			ctx, clause.NewColumn.Name.ColNameOrigin(), tableDef.Name)
		return
	}

	ok, err = positionMatched(ctx, clause.Position, tableDef, oCol)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	ok, err = storageAgnosticType(ctx, clause.NewColumn, oCol)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	ok, err = storageAgnosticAttrs(ctx, clause.NewColumn, oCol)
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
) (ok bool, err error) {

	nTy, err := getTypeFromAst(ctx, nCol.Type)
	if err != nil {
		return
	}

	oTy := oCol.Typ

	if oTy.Id != nTy.Id {
		return
	}

	if nTy.Id != int32(types.T_varchar) && nTy.Id != int32(types.T_char) {
		return
	}

	// leave autoInrement check to storage agnostic attrs because the autoInrement
	// in nTy should be determined by nCol.Attributes

	scaleMatch := oTy.Scale == nTy.Scale
	enumMatch := oTy.Enumvalues == nTy.Enumvalues

	if !scaleMatch || !enumMatch {
		return
	}

	widthIncrease := oTy.Width <= nTy.Width
	if !widthIncrease {
		return
	}

	ok = true
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
	if col.Typ.Id == int32(types.T_int8) ||
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
	} else if col.Typ.Id == int32(types.T_enum) {
		enumvalues := strings.Split(col.Typ.Enumvalues, ",")
		defaultValue = enumvalues[0]
	} else if col.Typ.Id == int32(types.T_array_float32) || col.Typ.Id == int32(types.T_array_float64) {
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
