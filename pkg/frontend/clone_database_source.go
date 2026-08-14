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
	"slices"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type cloneDatabaseSource struct {
	srcResolveDBName   string
	srcPrivilegeDBName string
	srcTblInfos        []*tableInfo
	userDefinedFuncs   []userDefinedFunctionDefinition
	storedProcedures   []storedProcedureDefinition
	viewMap            map[string]*tableInfo
	sortedFkTbls       []string
	fkTableMap         map[string]*tableInfo
	hasFkCycle         bool
	snapshot           *plan.Snapshot
	opAccountId        uint32
	toAccountId        uint32
}

type cloneDatabaseAccountResolution struct {
	opAccountId uint32
	toAccountId uint32
	snapshot    *plan.Snapshot
}

func (source *cloneDatabaseSource) branchTableCount() int64 {
	var count int64
	for _, table := range source.srcTblInfos {
		if table.typ != view {
			count++
		}
	}
	return count
}

func collectCloneDatabaseSource(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
	resolvedAccounts *cloneDatabaseAccountResolution,
) (cloneDatabaseSource, error) {
	source := cloneDatabaseSource{
		srcPrivilegeDBName: stmt.SrcDatabase.String(),
		viewMap:            make(map[string]*tableInfo),
	}

	var accounts cloneDatabaseAccountResolution
	if resolvedAccounts != nil {
		accounts = *resolvedAccounts
	} else {
		var err error
		if accounts, err = resolveCloneDatabaseAccounts(ctx, ses, bh, stmt); err != nil {
			return source, err
		}
	}
	if err := validateCloneDatabaseAccounts(ctx, accounts); err != nil {
		return source, err
	}
	snapshot := accounts.snapshot

	srcDBName := stmt.SrcDatabase.String()
	subMeta, err := ses.GetTxnCompileCtx().GetSubscriptionMeta(srcDBName, snapshot)
	if err != nil {
		return source, err
	}
	if subMeta != nil {
		srcDBName = subMeta.DbName
		if snapshot != nil {
			snapshot.Tenant = &plan.SnapshotTenant{TenantID: uint32(subMeta.AccountId)}
		} else {
			snapshot = &plan.Snapshot{
				Tenant: &plan.SnapshotTenant{TenantID: uint32(subMeta.AccountId)},
			}
		}
	}
	if err := validateCloneDatabaseSourceAccess(accounts.opAccountId, srcDBName); err != nil {
		return source, err
	}

	sourceExists, err := checkDatabaseExistsAtSnapshot(ctx, bh, snapshot, srcDBName)
	if err != nil {
		return source, err
	}
	if !sourceExists {
		return source, moerr.NewBadDB(ctx, srcDBName)
	}

	srcTblInfos, err := getTableInfos(ctx, ses.GetService(), bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
	userDefinedFuncs, storedProcedures, err := getCloneDatabaseRoutineInfos(ctx, bh, snapshot, srcDBName, subMeta)
	if err != nil {
		return source, err
	}
	fkDeps, err := getFkDeps(ctx, bh, snapshot, srcDBName, "")
	if err != nil {
		return source, err
	}
	schemaFkDeps, err := getFkDepsFromTableInfos(ctx, srcTblInfos)
	if err != nil {
		return source, err
	}
	mergeFkDeps(fkDeps, schemaFkDeps)
	sortedFkTbls, hasFkCycle := cloneFkTableOrder(fkDeps)
	fkTableMap, err := getTableInfoMap(ctx, ses.GetService(), bh, snapshot, srcDBName, "", sortedFkTbls)
	if err != nil {
		return source, err
	}

	for _, srcTbl := range srcTblInfos {
		if srcTbl.typ == view {
			source.viewMap[genKey(srcTbl.dbName, srcTbl.tblName)] = srcTbl
		}
	}

	source.srcResolveDBName = srcDBName
	source.srcTblInfos = srcTblInfos
	source.userDefinedFuncs = userDefinedFuncs
	source.storedProcedures = storedProcedures
	source.sortedFkTbls = sortedFkTbls
	source.fkTableMap = fkTableMap
	source.hasFkCycle = hasFkCycle
	source.snapshot = snapshot
	source.opAccountId = accounts.opAccountId
	source.toAccountId = accounts.toAccountId
	return source, nil
}

// getCloneDatabaseRoutineInfos reads routine metadata that is not represented by
// mo_tables. Publications scope tables only, so a subscription clone must not
// query or copy publisher routine metadata.
func getCloneDatabaseRoutineInfos(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	dbName string,
	subMeta *plan.SubscriptionMeta,
) ([]userDefinedFunctionDefinition, []storedProcedureDefinition, error) {
	if subMeta != nil {
		return nil, nil, nil
	}
	userDefinedFuncs, err := getUserDefinedFunctionInfos(ctx, bh, snapshot, dbName)
	if err != nil {
		return nil, nil, err
	}
	storedProcedures, err := getStoredProcedureInfos(ctx, bh, snapshot, dbName)
	if err != nil {
		return nil, nil, err
	}
	return userDefinedFuncs, storedProcedures, nil
}

func getUserDefinedFunctionInfos(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	dbName string,
) ([]userDefinedFunctionDefinition, error) {
	rows, err := getDatabaseRoutineMetadataRows(
		ctx, bh, snapshot, dbName,
		"mo_catalog.mo_user_defined_function",
		"name, args, retType, body, language, sql_mode",
		0, 1, 2, 3, 4, 5,
	)
	if err != nil {
		return nil, err
	}

	functions := make([]userDefinedFunctionDefinition, len(rows))
	for i, row := range rows {
		functions[i] = userDefinedFunctionDefinition{
			name:    row[0],
			args:    row[1],
			retType: row[2],
			body:    row[3],
			lang:    row[4],
			sqlMode: row[5],
			dbName:  dbName,
		}
	}
	return functions, nil
}

func getStoredProcedureInfos(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	dbName string,
) ([]storedProcedureDefinition, error) {
	rows, err := getDatabaseRoutineMetadataRows(
		ctx, bh, snapshot, dbName,
		"mo_catalog.mo_stored_procedure",
		"name, args, lang, body, sql_mode",
		0, 1, 2, 3, 4,
	)
	if err != nil {
		return nil, err
	}

	procedures := make([]storedProcedureDefinition, len(rows))
	for i, row := range rows {
		procedures[i] = storedProcedureDefinition{
			name:    row[0],
			args:    row[1],
			lang:    row[2],
			body:    row[3],
			sqlMode: row[4],
			dbName:  dbName,
		}
	}
	return procedures, nil
}

func getDatabaseRoutineMetadataRows(
	ctx context.Context,
	bh BackgroundExec,
	snapshot *plan.Snapshot,
	dbName string,
	catalogTable string,
	columns string,
	colIndices ...uint64,
) ([][]string, error) {
	queryCtx := ctx
	sql := fmt.Sprintf("select %s from %s", columns, catalogTable)
	if snapshot != nil {
		if snapshot.TS != nil {
			sql += fmt.Sprintf(" {MO_TS = %d}", snapshot.TS.PhysicalTime)
		}
		if snapshot.Tenant != nil {
			queryCtx = defines.AttachAccountId(queryCtx, snapshot.Tenant.TenantID)
		}
	}
	sql += fmt.Sprintf(" where db = %s order by name", quoteSQLStringLiteral(dbName))
	return getStringColsList(queryCtx, bh, sql, colIndices...)
}

func restoreCloneDatabaseUserDefinedFunctions(
	ctx context.Context,
	bh BackgroundExec,
	tenant *TenantInfo,
	functions []userDefinedFunctionDefinition,
	dbName string,
) error {
	for _, function := range functions {
		function.dbName = dbName
		if err := persistUserDefinedFunction(
			ctx, bh, tenant, tenant.GetDefaultRoleID(), function, nil,
		); err != nil {
			return err
		}
	}
	return nil
}

func restoreCloneDatabaseStoredProcedures(
	ctx context.Context,
	bh BackgroundExec,
	tenant *TenantInfo,
	procedures []storedProcedureDefinition,
	dbName string,
) error {
	for _, procedure := range procedures {
		procedure.dbName = dbName
		if err := upsertStoredProcedure(ctx, bh, tenant, procedure, false); err != nil {
			return err
		}
	}
	return nil
}

func rewriteCloneStoredProcedureBodies(
	ctx context.Context,
	procedures []storedProcedureDefinition,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) ([]storedProcedureDefinition, error) {
	rewritten := slices.Clone(procedures)
	for i := range rewritten {
		if !strings.EqualFold(rewritten[i].lang, string(tree.SQL)) {
			continue
		}
		body, err := rewriteCloneSQLRoutineBody(
			ctx,
			rewritten[i].body,
			rewritten[i].sqlMode,
			srcDBName,
			dstDBName,
			lowerCaseTableNames,
		)
		if err != nil {
			return nil, err
		}
		rewritten[i].body = body
	}
	return rewritten, nil
}

func rewriteCloneSQLRoutineBody(
	ctx context.Context,
	body string,
	sqlMode string,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) (string, error) {
	if srcDBName == "" || srcDBName == dstDBName {
		return body, nil
	}

	stmts, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, body, lowerCaseTableNames, sqlMode)
	if err != nil {
		return "", err
	}
	defer freeStatements(stmts)

	options := []tree.FmtCtxOption{tree.WithSingleQuoteString(), tree.WithQuoteIdentifier()}
	if mysql.ParseSQLModeFlags(sqlMode).Has(mysql.SQLModeNoBackslashEscapes) {
		options = append(options, tree.WithNoBackslashEscape())
	}
	original := formatCloneRoutineStatements(stmts, options...)
	if err := applyRemapDb(ctx, stmts, map[string]string{srcDBName: dstDBName}, lowerCaseTableNames); err != nil {
		return "", err
	}
	rewritten := formatCloneRoutineStatements(stmts, options...)
	if rewritten == original {
		return body, nil
	}
	return rewritten, nil
}

func formatCloneRoutineStatements(stmts []tree.Statement, options ...tree.FmtCtxOption) string {
	parts := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		if stmt != nil {
			parts = append(parts, tree.StringWithOpts(stmt, dialect.MYSQL, options...))
		}
	}
	return strings.Join(parts, "; ")
}

// validateCloneDatabaseSourceAccess preserves the clone privilege contract
// before source metadata lookup. System databases are only clone sources for
// sys; checking their existence first would expose them as missing to tenants.
func validateCloneDatabaseSourceAccess(opAccountID uint32, sourceDatabase string) error {
	if opAccountID != sysAccountID &&
		slices.Contains(catalog.SystemDatabases, strings.ToLower(sourceDatabase)) {
		return moerr.NewInternalErrorNoCtx("non-sys account cannot clone data from system database")
	}
	return nil
}

func resolveCloneDatabaseAccounts(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.CloneDatabase,
) (cloneDatabaseAccountResolution, error) {
	opAccountId, toAccountId, snapshot, err := getOpAndToAccountId(
		ctx, ses, bh, stmt.ToAccountOpt, stmt.AtTsExpr,
	)
	if err != nil {
		return cloneDatabaseAccountResolution{}, err
	}
	return cloneDatabaseAccountResolution{
		opAccountId: opAccountId,
		toAccountId: toAccountId,
		snapshot:    snapshot,
	}, nil
}

func validateCloneDatabaseAccounts(
	ctx context.Context,
	accounts cloneDatabaseAccountResolution,
) error {
	if accounts.snapshot == nil && accounts.opAccountId != accounts.toAccountId {
		return moerr.NewInternalErrorNoCtxf("clone database between different accounts need a snapshot")
	}
	if accounts.opAccountId != sysAccountID && accounts.opAccountId != accounts.toAccountId {
		return moerr.NewInternalError(ctx, "only sys can clone table to another account")
	}
	return nil
}

func cloneFkTableOrder(fkDeps map[string][]string) (sortedTbls []string, hasCycle bool) {
	g := toposort{next: make(map[string][]string)}
	for key, deps := range fkDeps {
		g.addVertex(key)
		for _, depTbl := range deps {
			if key != depTbl {
				g.addEdge(depTbl, key)
			}
		}
	}

	sortedTbls, err := g.sort()
	if err == nil {
		return sortedTbls, false
	}

	// CREATE TABLE resolves forward foreign-key references while
	// foreign_key_checks is disabled. A deterministic order is sufficient for
	// a cyclic component because creating its later tables backfills those
	// references.
	sortedTbls = sortedTbls[:0]
	for key := range g.next {
		sortedTbls = append(sortedTbls, key)
	}
	sort.Strings(sortedTbls)
	return sortedTbls, true
}
