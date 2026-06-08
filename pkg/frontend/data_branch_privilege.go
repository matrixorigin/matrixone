// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

type branchPrivilegeRequirement struct {
	objType                       objectType
	databaseName                  string
	tableName                     string
	privilegeTypes                []PrivilegeType
	writeDatabaseAndTableDirectly bool
	isClusterTable                bool
	clusterTableOperation         clusterTableOperationType
}

func isDataBranchStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.DataBranchCreateTable,
		*tree.DataBranchCreateDatabase,
		*tree.DataBranchDeleteTable,
		*tree.DataBranchDeleteDatabase,
		*tree.DataBranchDiff,
		*tree.DataBranchMerge,
		*tree.DataBranchPick:
		return true
	default:
		return false
	}
}

func authenticateDataBranchStatement(
	ctx context.Context,
	ses *Session,
	stmt tree.Statement,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	if skipDataBranchPrivilegeCheck(ses) {
		return stats, nil
	}

	add := func(delta statistic.StatsArray) {
		stats.Add(&delta)
	}

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		delta, err := authenticateDataBranchCreateTable(ctx, ses, st)
		add(delta)
		return stats, err
	case *tree.DataBranchCreateDatabase:
		// CREATE DATABASE source resolution must be shared with execution to
		// avoid authorizing a different object set. It runs inside
		// dataBranchCreateDatabase after collectCloneDatabaseSource.
		return stats, nil
	case *tree.DataBranchDeleteTable:
		delta, err := authenticateDataBranchDeleteTable(ctx, ses, st)
		add(delta)
		return stats, err
	case *tree.DataBranchDeleteDatabase:
		delta, err := authenticateDataBranchDeleteDatabase(ctx, ses, st)
		add(delta)
		return stats, err
	case *tree.DataBranchDiff:
		delta, err := authenticateDataBranchDiff(ctx, ses, st)
		add(delta)
		return stats, err
	case *tree.DataBranchMerge:
		delta, err := authenticateDataBranchMerge(ctx, ses, st)
		add(delta)
		return stats, err
	case *tree.DataBranchPick:
		delta, err := authenticateDataBranchPick(ctx, ses, st)
		add(delta)
		return stats, err
	default:
		return stats, moerr.NewNotSupportedNoCtxf("data branch not supported: %v", stmt)
	}
}

func skipDataBranchPrivilegeCheck(ses *Session) bool {
	return getPu(ses.GetService()).SV.SkipCheckPrivilege ||
		ses.skipAuthForSpecialUser() ||
		ses.GetTenantInfo() == nil
}

func authenticateDataBranchCreateTable(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchCreateTable,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	dstDBName, err := branchDatabaseName(ctx, ses, stmt.CreateTable.Table.SchemaName.String())
	if err != nil {
		return stats, err
	}
	delta, err := requireAllBranchPrivileges(ctx, ses, []branchPrivilegeRequirement{
		branchCreateTableRequirement(dstDBName),
	})
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}

	delta, err = withBranchPrivilegeBackgroundExec(ctx, ses, func(bh BackgroundExec) error {
		return validateBranchCloneTableAccount(ctx, ses, bh, stmt)
	})
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}

	delta, err = requireBranchReadOnTableName(ctx, ses, stmt.SrcTable)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func authenticateDataBranchCreateDatabase(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchCreateDatabase,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	delta, err := requireAllBranchPrivileges(ctx, ses, []branchPrivilegeRequirement{
		branchCreateDatabaseRequirement(),
	})
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func authenticateDataBranchCreateDatabaseSourceTables(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchCreateDatabase,
	source cloneDatabaseSource,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	if skipDataBranchPrivilegeCheck(ses) {
		return stats, nil
	}

	var (
		delta statistic.StatsArray
		err   error
	)
	for _, tblInfo := range source.srcTblInfos {
		srcName := makeBranchTableName(
			source.srcPrivilegeDBName,
			tblInfo.tblName,
			stmt.AtTsExpr,
		)
		delta, err = requireBranchReadOnTableName(ctx, ses, srcName)
		stats.Add(&delta)
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func authenticateDataBranchDeleteTable(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDeleteTable,
) (statistic.StatsArray, error) {
	dbName, tableName, err := branchTableName(ctx, ses, stmt.TableName)
	if err != nil {
		var stats statistic.StatsArray
		stats.Reset()
		return stats, err
	}
	return requireBranchDropTablePrivilege(ctx, ses, dbName, tableName)
}

func authenticateDataBranchDeleteDatabase(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDeleteDatabase,
) (statistic.StatsArray, error) {
	return requireBranchDropDatabasePrivilege(ctx, ses, stmt.DatabaseName.String())
}

func authenticateDataBranchDiff(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	delta, err := requireBranchReadOnTableName(ctx, ses, stmt.TargetTable)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	delta, err = requireBranchReadOnTableName(ctx, ses, stmt.BaseTable)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func authenticateDataBranchMerge(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchMerge,
) (statistic.StatsArray, error) {
	return authenticateDataBranchReadWriteTables(ctx, ses, stmt.SrcTable, stmt.DstTable)
}

func authenticateDataBranchPick(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchPick,
) (statistic.StatsArray, error) {
	return authenticateDataBranchReadWriteTables(ctx, ses, stmt.SrcTable, stmt.DstTable)
}

func authenticateDataBranchReadWriteTables(
	ctx context.Context,
	ses *Session,
	srcTable tree.TableName,
	dstTable tree.TableName,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	delta, err := requireBranchReadOnTableName(ctx, ses, srcTable)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	delta, err = requireBranchReadOnTableName(ctx, ses, dstTable)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}

	dstDBName, dstTableName, err := branchTableName(ctx, ses, dstTable)
	if err != nil {
		return stats, err
	}
	requirements := []branchPrivilegeRequirement{
		branchWriteTableRequirement(dstDBName, dstTableName, PrivilegeTypeInsert, clusterTableModify),
		branchWriteTableRequirement(dstDBName, dstTableName, PrivilegeTypeUpdate, clusterTableModify),
		branchWriteTableRequirement(dstDBName, dstTableName, PrivilegeTypeDelete, clusterTableModify),
	}
	delta, err = requireAllBranchPrivileges(ctx, ses, requirements)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func validateBranchCloneTableAccount(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchCreateTable,
) error {
	opAccountId, toAccountId, snapshot, err := getOpAndToAccountId(
		ctx, ses, bh, stmt.ToAccountOpt, stmt.SrcTable.AtTsExpr,
	)
	if err != nil {
		return err
	}
	if snapshot == nil && opAccountId != toAccountId {
		return moerr.NewInternalErrorNoCtxf("clone table between different accounts need a snapshot")
	}
	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return moerr.NewInternalErrorNoCtxf("only sys can clone table to another account")
	}
	return nil
}

func branchCreateTableRequirement(dbName string) branchPrivilegeRequirement {
	return branchPrivilegeRequirement{
		objType:                       objectTypeDatabase,
		databaseName:                  dbName,
		privilegeTypes:                []PrivilegeType{PrivilegeTypeCreateTable, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership},
		writeDatabaseAndTableDirectly: true,
	}
}

func branchCreateDatabaseRequirement() branchPrivilegeRequirement {
	return branchPrivilegeRequirement{
		objType:                       objectTypeAccount,
		privilegeTypes:                []PrivilegeType{PrivilegeTypeCreateDatabase, PrivilegeTypeAccountAll},
		writeDatabaseAndTableDirectly: true,
	}
}

func branchWriteTableRequirement(
	dbName string,
	tableName string,
	typ PrivilegeType,
	clusterOp clusterTableOperationType,
) branchPrivilegeRequirement {
	return branchPrivilegeRequirement{
		objType:                       objectTypeTable,
		databaseName:                  dbName,
		tableName:                     tableName,
		privilegeTypes:                []PrivilegeType{typ, PrivilegeTypeTableAll, PrivilegeTypeTableOwnership},
		writeDatabaseAndTableDirectly: true,
		isClusterTable:                isClusterTable(dbName, tableName),
		clusterTableOperation:         clusterOp,
	}
}

func branchDropTableRequirement(dbName string, tableName string) branchPrivilegeRequirement {
	return branchPrivilegeRequirement{
		objType:                       objectTypeDatabase,
		databaseName:                  dbName,
		tableName:                     tableName,
		privilegeTypes:                []PrivilegeType{PrivilegeTypeDropTable, PrivilegeTypeDropObject, PrivilegeTypeDatabaseAll, PrivilegeTypeDatabaseOwnership},
		writeDatabaseAndTableDirectly: true,
		isClusterTable:                isClusterTable(dbName, tableName),
		clusterTableOperation:         clusterTableDrop,
	}
}

func branchDropDatabaseRequirement(dbName string) branchPrivilegeRequirement {
	return branchPrivilegeRequirement{
		objType:                       objectTypeAccount,
		databaseName:                  dbName,
		privilegeTypes:                []PrivilegeType{PrivilegeTypeDropDatabase, PrivilegeTypeAccountAll},
		writeDatabaseAndTableDirectly: true,
	}
}

func requireAllBranchPrivileges(
	ctx context.Context,
	ses *Session,
	requirements []branchPrivilegeRequirement,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()
	for _, req := range requirements {
		ok, delta, err := determineBranchRequirement(ctx, ses, req)
		stats.Add(&delta)
		if err != nil {
			return stats, err
		}
		if !ok {
			return stats, moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
		}
	}
	return stats, nil
}

func determineBranchRequirement(
	ctx context.Context,
	ses *Session,
	req branchPrivilegeRequirement,
) (bool, statistic.StatsArray, error) {
	priv := branchRequirementToPrivilege(req)
	return determineUserHasPrivilegeSet(ctx, ses, priv)
}

func branchRequirementToPrivilege(req branchPrivilegeRequirement) *privilege {
	entries := make([]privilegeEntry, 0, len(req.privilegeTypes))
	for _, typ := range req.privilegeTypes {
		entry := privilegeEntriesMap[typ]
		entry.objType = req.objType
		entry.databaseName = req.databaseName
		entry.tableName = req.tableName
		entry.privilegeEntryTyp = privilegeEntryTypeGeneral
		entry.compound = nil
		entries = append(entries, entry)
	}
	return &privilege{
		kind:                          privilegeKindGeneral,
		objType:                       req.objType,
		entries:                       entries,
		writeDatabaseAndTableDirectly: req.writeDatabaseAndTableDirectly,
		isClusterTable:                req.isClusterTable,
		clusterTableOperation:         req.clusterTableOperation,
	}
}

func requireBranchDropTablePrivilege(
	ctx context.Context,
	ses *Session,
	dbName string,
	tableName string,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	ok, delta, err := determineBranchRequirement(ctx, ses, branchDropTableRequirement(dbName, tableName))
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	if ok {
		return stats, nil
	}
	if ses.GetFromRealUser() && ses.GetTenantInfo() != nil {
		if _, inSet := sysDatabases[dbName]; !inSet {
			ok, delta, err = checkRoleWhetherTableOwner(ctx, ses, dbName, tableName, false)
			stats.Add(&delta)
			if err != nil {
				return stats, err
			}
			if ok {
				return stats, nil
			}
		}
	}
	return stats, moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
}

func requireBranchDropDatabasePrivilege(
	ctx context.Context,
	ses *Session,
	dbName string,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	ok, delta, err := determineBranchRequirement(ctx, ses, branchDropDatabaseRequirement(dbName))
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	if ok {
		return stats, nil
	}
	if ses.GetFromRealUser() && ses.GetTenantInfo() != nil {
		if _, inSet := sysDatabases[dbName]; !inSet {
			ok, delta, err = checkRoleWhetherDatabaseOwner(ctx, ses, dbName, false)
			stats.Add(&delta)
			if err != nil {
				return stats, err
			}
			if ok {
				return stats, nil
			}
		}
	}
	return stats, moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
}

func requireBranchReadOnTableName(
	ctx context.Context,
	ses *Session,
	name tree.TableName,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	normalized, err := normalizeBranchTableName(ctx, ses, name)
	if err != nil {
		return stats, err
	}
	stmt := branchSelectForTableName(normalized)
	p, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), stmt)
	if err != nil {
		return stats, err
	}
	ok, delta, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ctx, ses, stmt, p)
	stats.Add(&delta)
	if err != nil {
		return stats, err
	}
	if !ok {
		return stats, moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
	}
	return stats, nil
}

func branchSelectForTableName(name tree.TableName) *tree.Select {
	tableName := name
	return tree.NewSelect(&tree.SelectClause{
		Exprs: tree.SelectExprs{{
			Expr: tree.UnqualifiedStar{},
		}},
		From: tree.NewFrom(tree.TableExprs{
			tree.NewAliasedTableExpr(&tableName, "", nil),
		}),
	}, nil, nil)
}

func withBranchPrivilegeBackgroundExec(
	ctx context.Context,
	ses *Session,
	fn func(BackgroundExec) error,
) (stats statistic.StatsArray, err error) {
	stats.Reset()
	var (
		bh       BackgroundExec
		deferred func(error) error
	)
	if bh, deferred, err = getBackExecutor(ctx, ses); err != nil {
		return stats, err
	}
	defer func() {
		stats = bh.GetExecStatsArray()
		if deferred != nil {
			err = deferred(err)
		}
	}()
	err = fn(bh)
	return stats, err
}

func normalizeBranchTableName(
	ctx context.Context,
	ses *Session,
	name tree.TableName,
) (tree.TableName, error) {
	dbName := name.SchemaName.String()
	if dbName == "" {
		dbName = ses.GetDatabaseName()
	}
	if dbName == "" {
		dbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}
	if dbName == "" {
		return name, moerr.NewNoDB(ctx)
	}
	if name.ObjectName.String() == "" {
		return name, moerr.NewInternalError(ctx, "table name cannot be empty")
	}
	name.SchemaName = tree.Identifier(dbName)
	name.ExplicitSchema = true
	return name, nil
}

func branchTableName(
	ctx context.Context,
	ses *Session,
	name tree.TableName,
) (string, string, error) {
	normalized, err := normalizeBranchTableName(ctx, ses, name)
	if err != nil {
		return "", "", err
	}
	return normalized.SchemaName.String(), normalized.ObjectName.String(), nil
}

func branchDatabaseName(ctx context.Context, ses *Session, dbName string) (string, error) {
	if dbName == "" {
		dbName = ses.GetDatabaseName()
	}
	if dbName == "" {
		dbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}
	if dbName == "" {
		return "", moerr.NewNoDB(ctx)
	}
	return dbName, nil
}

func makeBranchTableName(dbName string, tableName string, atTsExpr *tree.AtTimeStamp) tree.TableName {
	return *tree.NewTableName(
		tree.Identifier(tableName),
		tree.ObjectNamePrefix{
			SchemaName:     tree.Identifier(dbName),
			ExplicitSchema: true,
		},
		atTsExpr,
	)
}

func validateDataBranchDeleteTableTarget(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dbName string,
	tableName string,
) (uint64, error) {
	accId, err := defines.GetAccountId(ctx)
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf(
		"select rel_id from %s.%s where account_id = %d and reldatabase = %s and relname = %s",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		accId,
		quoteSQLStringLiteral(dbName),
		quoteSQLStringLiteral(tableName),
	)
	sqlRet, err := runSql(ctx, ses, bh, sql, nil, nil)
	if err != nil {
		return 0, err
	}
	defer sqlRet.Close()

	var tableID uint64
	found := false
	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		tableID = vector.GetFixedAtWithTypeCheck[uint64](cols[0], 0)
		found = true
		return false
	})
	if !found {
		return 0, moerr.NewNoSuchTable(ctx, dbName, tableName)
	}
	if err = validateActiveBranchChildTableIDs(ctx, ses, bh, map[uint64]string{
		tableID: dbName + "." + tableName,
	}); err != nil {
		return 0, err
	}
	return tableID, nil
}

func validateDataBranchDeleteDatabaseTarget(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dbName string,
) ([]uint64, error) {
	accId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	if err = validateBranchDatabaseExists(ctx, ses, bh, accId, dbName); err != nil {
		return nil, err
	}

	sql := branchDeleteDatabaseTableIDsSQL(accId, dbName)
	sqlRet, err := runSql(ctx, ses, bh, sql, nil, nil)
	if err != nil {
		return nil, err
	}
	defer sqlRet.Close()

	tableNames := make(map[uint64]string)
	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		ids := executor.GetFixedRows[uint64](cols[0])
		names := executor.GetStringRows(cols[1])
		for i, id := range ids {
			name := fmt.Sprintf("%s.<unknown>", dbName)
			if i < len(names) {
				name = dbName + "." + names[i]
			}
			tableNames[id] = name
		}
		return true
	})
	if len(tableNames) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "DATA BRANCH DELETE target %s is not an active branch database", dbName)
	}
	if err = validateActiveBranchChildTableIDs(ctx, ses, bh, tableNames); err != nil {
		return nil, err
	}
	tableIDs := make([]uint64, 0, len(tableNames))
	for id := range tableNames {
		tableIDs = append(tableIDs, id)
	}
	sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })
	return tableIDs, nil
}

func branchDeleteDatabaseTableIDsSQL(accId uint32, dbName string) string {
	whereClause := buildTableInfoListWhereClause(dbName, "", accId)
	whereClause += fmt.Sprintf(" and relkind != %s", quoteSQLStringLiteral(catalog.SystemViewRel))
	return fmt.Sprintf(
		"select rel_id, relname from %s.%s where %s",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		whereClause,
	)
}

func validateBranchDatabaseExists(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accId uint32,
	dbName string,
) error {
	sql := fmt.Sprintf(
		"select dat_id from %s.%s where account_id = %d and datname = %s",
		catalog.MO_CATALOG,
		catalog.MO_DATABASE,
		accId,
		quoteSQLStringLiteral(dbName),
	)
	sqlRet, err := runSql(ctx, ses, bh, sql, nil, nil)
	if err != nil {
		return err
	}
	defer sqlRet.Close()

	found := false
	sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
		found = rows > 0
		return false
	})
	if !found {
		return moerr.NewBadDB(ctx, dbName)
	}
	return nil
}

func validateActiveBranchChildTableIDs(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tableNames map[uint64]string,
) error {
	if len(tableNames) == 0 {
		return nil
	}
	idList := make([]uint64, 0, len(tableNames))
	for id := range tableNames {
		idList = append(idList, id)
	}
	sort.Slice(idList, func(i, j int) bool { return idList[i] < idList[j] })
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)

	active := make(map[uint64]struct{}, len(tableNames))

	for start := 0; start < len(idList); start += dataBranchMetadataIDBatchSize {
		end := start + dataBranchMetadataIDBatchSize
		if end > len(idList) {
			end = len(idList)
		}

		sql := fmt.Sprintf(
			"select table_id from %s.%s where table_deleted = false and table_id in (%s)",
			catalog.MO_CATALOG,
			catalog.MO_BRANCH_METADATA,
			formatUintList(idList[start:end]),
		)
		sqlRet, err := runSql(sysCtx, ses, bh, sql, nil, nil)
		if err != nil {
			return err
		}
		sqlRet.ReadRows(func(rows int, cols []*vector.Vector) bool {
			if rows == 0 {
				return true
			}
			for _, id := range executor.GetFixedRows[uint64](cols[0]) {
				active[id] = struct{}{}
			}
			return true
		})
		sqlRet.Close()
	}

	for _, id := range idList {
		name := tableNames[id]
		if _, ok := active[id]; !ok {
			return moerr.NewInternalErrorf(ctx, "DATA BRANCH DELETE target %s is not an active branch table", name)
		}
	}
	return nil
}

func formatUintList(ids []uint64) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}

func quoteIdentifierForSQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
