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
	"encoding/json"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

// isCloneableCloneDatabaseTable is the single relation-kind policy for a
// database clone. External tables are catalog objects, but their data lives
// outside MatrixOne and the database-clone contract intentionally omits them.
func isCloneableCloneDatabaseTable(tblInfo *tableInfo) bool {
	return tblInfo != nil && !shouldSkipRestoreTableInBulk(tblInfo)
}

func (source cloneDatabaseSource) cloneableTableInfos() []*tableInfo {
	tables := make([]*tableInfo, 0, len(source.srcTblInfos))
	for _, table := range source.srcTblInfos {
		if isCloneableCloneDatabaseTable(table) {
			tables = append(tables, table)
		}
	}
	return tables
}

// sourceTableInfosForLifecycle contains every non-view source relation whose
// catalog row may be consulted after source collection. External tables stay
// in this set even though they are not cloneable: live DATA BRANCH clones
// re-plan views after advancing the timestamp, and that dependency planning
// reads the external relation metadata. The lifecycle fence must therefore
// cover the row without granting, accounting, or materializing the table.
func (source cloneDatabaseSource) sourceTableInfosForLifecycle() []*tableInfo {
	tables := make([]*tableInfo, 0, len(source.srcTblInfos))
	for _, table := range source.srcTblInfos {
		if table != nil && table.typ != view {
			tables = append(tables, table)
		}
	}
	return tables
}

func (source *cloneDatabaseSource) branchTableCount() int64 {
	var count int64
	for _, table := range source.cloneableTableInfos() {
		if table.typ != view && !isSequence(table) {
			count++
		}
	}
	return count
}

const (
	cloneRoutineFunctionKind    = "function"
	cloneRoutineProcedureKind   = "procedure"
	cloneDatabaseObjectNodeKind = "object"
)

type cloneRoutineReferences struct {
	tables     map[string]struct{}
	procedures map[string]struct{}
	functions  map[string]struct{}
}

type cloneDatabaseOmissionSet struct {
	objects    map[string]struct{}
	functions  map[string]struct{}
	procedures map[string]struct{}
}

type cloneDatabaseDependencyNode struct {
	kind string
	key  string
}

type cloneRoutineDependencyStatus uint8

const (
	cloneRoutineDependenciesInspected cloneRoutineDependencyStatus = iota
	cloneRoutineDependenciesOpaque
	cloneRoutineDependenciesUninspectable
)

func newCloneRoutineReferences() cloneRoutineReferences {
	return cloneRoutineReferences{
		tables:     make(map[string]struct{}),
		procedures: make(map[string]struct{}),
		functions:  make(map[string]struct{}),
	}
}

func cloneDatabaseObjectKey(databaseName, objectName string, lowerCaseTableNames int64) string {
	return genKey(
		tree.NewCStr(databaseName, lowerCaseTableNames).Compare(),
		tree.NewCStr(objectName, lowerCaseTableNames).Compare(),
	)
}

// cloneRoutineFamilyKey intentionally identifies a SQL routine family by kind,
// database, and name. The dependency walker does not resolve UDF overloads at
// every call site, so omitting one overload conservatively omits the whole
// same-name family instead of risking a copied overload that still references
// omitted metadata.
func cloneRoutineFamilyKey(kind, databaseName, routineName string, lowerCaseTableNames int64) string {
	return kind + KeySep + cloneDatabaseObjectKey(databaseName, routineName, lowerCaseTableNames)
}

func cloneDatabaseSourceObjectKey(source cloneDatabaseSource, tblInfo *tableInfo, lowerCaseTableNames int64) string {
	databaseName := tblInfo.dbName
	if databaseName == "" {
		databaseName = source.srcResolveDBName
	}
	return cloneDatabaseObjectKey(databaseName, tblInfo.tblName, lowerCaseTableNames)
}

func collectCloneFunctionReference(
	references *cloneRoutineReferences,
	defaultDBName string,
	lowerCaseTableNames int64,
) func(*tree.UnresolvedName) {
	return func(name *tree.UnresolvedName) {
		if name == nil || name.NumParts == 0 {
			return
		}
		databaseName := defaultDBName
		if name.NumParts >= 3 {
			databaseName = name.DbNameOrigin()
		} else if name.NumParts >= 2 {
			databaseName = name.TblNameOrigin()
		}
		references.functions[cloneRoutineFamilyKey(
			cloneRoutineFunctionKind, databaseName,
			name.ColNameOrigin(), lowerCaseTableNames,
		)] = struct{}{}
	}
}

func collectCloneViewDependencies(
	ctx context.Context,
	tblInfo *tableInfo,
	lowerCaseTableNames int64,
) (cloneRoutineReferences, error) {
	references := newCloneRoutineReferences()
	statements, err := parseViewCreateSQLForRestore(ctx, tblInfo, lowerCaseTableNames)
	if err != nil {
		return references, err
	}
	defer freeStatements(statements)
	if len(statements) != 1 {
		return references, moerr.NewInternalErrorNoCtxf(
			"clone view SQL for %s.%s produced %d statements",
			tblInfo.dbName, tblInfo.tblName, len(statements),
		)
	}
	createView, ok := statements[0].(*tree.CreateView)
	if !ok {
		return references, moerr.NewInternalErrorNoCtxf(
			"clone view SQL for %s.%s is %T, expected *tree.CreateView",
			tblInfo.dbName, tblInfo.tblName, statements[0],
		)
	}
	remapDbInSelect(createView.AsSource, remapDbContext{
		lowerCaseTableNames: lowerCaseTableNames,
		collectTableName: func(name *tree.TableName) {
			databaseName := tblInfo.dbName
			if name.ExplicitSchema {
				databaseName = string(name.SchemaName)
			}
			references.tables[cloneDatabaseObjectKey(
				databaseName, string(name.ObjectName), lowerCaseTableNames,
			)] = struct{}{}
		},
		collectFunctionName: collectCloneFunctionReference(
			&references, tblInfo.dbName, lowerCaseTableNames,
		),
	})
	return references, nil
}

// collectCloneDatabaseOmissionSet builds one closure over tables, views,
// functions, and procedures. A routine can depend on a view, and a view can
// depend on a routine; keeping these edges in one graph prevents either
// restoration order from publishing metadata whose dependency was omitted.
// Reverse edges let each omission propagate to its direct dependents once,
// rather than rescanning every object for every level of a dependency chain.
func collectCloneDatabaseOmissionSet(
	ctx context.Context,
	source cloneDatabaseSource,
	lowerCaseTableNames int64,
) (cloneDatabaseOmissionSet, error) {
	omissions := cloneDatabaseOmissionSet{
		objects:    make(map[string]struct{}),
		functions:  make(map[string]struct{}),
		procedures: make(map[string]struct{}),
	}
	dependents := make(map[cloneDatabaseDependencyNode]map[cloneDatabaseDependencyNode]struct{})
	queue := make([]cloneDatabaseDependencyNode, 0)

	enqueue := func(node cloneDatabaseDependencyNode) {
		var omissionsForKind map[string]struct{}
		switch node.kind {
		case cloneDatabaseObjectNodeKind:
			omissionsForKind = omissions.objects
		case cloneRoutineFunctionKind:
			omissionsForKind = omissions.functions
		case cloneRoutineProcedureKind:
			omissionsForKind = omissions.procedures
		default:
			return
		}
		if _, alreadyOmitted := omissionsForKind[node.key]; alreadyOmitted {
			return
		}
		omissionsForKind[node.key] = struct{}{}
		queue = append(queue, node)
	}
	addDependency := func(dependent, dependency cloneDatabaseDependencyNode) {
		if dependents[dependency] == nil {
			dependents[dependency] = make(map[cloneDatabaseDependencyNode]struct{})
		}
		dependents[dependency][dependent] = struct{}{}
	}

	for _, tblInfo := range source.srcTblInfos {
		if tblInfo != nil && !isCloneableCloneDatabaseTable(tblInfo) {
			enqueue(cloneDatabaseDependencyNode{
				kind: cloneDatabaseObjectNodeKind,
				key:  cloneDatabaseSourceObjectKey(source, tblInfo, lowerCaseTableNames),
			})
		}
	}
	for _, tblInfo := range source.viewMap {
		if tblInfo != nil && tblInfo.unservable {
			enqueue(cloneDatabaseDependencyNode{
				kind: cloneDatabaseObjectNodeKind,
				key:  cloneDatabaseSourceObjectKey(source, tblInfo, lowerCaseTableNames),
			})
		}
	}
	if len(queue) == 0 {
		return omissions, nil
	}

	for _, tblInfo := range source.viewMap {
		if tblInfo == nil {
			continue
		}
		viewKey := cloneDatabaseSourceObjectKey(source, tblInfo, lowerCaseTableNames)
		dependencies, err := collectCloneViewDependencies(ctx, tblInfo, lowerCaseTableNames)
		if err != nil {
			return omissions, err
		}
		viewNode := cloneDatabaseDependencyNode{
			kind: cloneDatabaseObjectNodeKind,
			key:  viewKey,
		}
		for tableKey := range dependencies.tables {
			addDependency(viewNode, cloneDatabaseDependencyNode{
				kind: cloneDatabaseObjectNodeKind,
				key:  tableKey,
			})
		}
		for functionKey := range dependencies.functions {
			addDependency(viewNode, cloneDatabaseDependencyNode{
				kind: cloneRoutineFunctionKind,
				key:  functionKey,
			})
		}
		for procedureKey := range dependencies.procedures {
			addDependency(viewNode, cloneDatabaseDependencyNode{
				kind: cloneRoutineProcedureKind,
				key:  procedureKey,
			})
		}
	}

	addRoutineDependencies := func(
		routineNode cloneDatabaseDependencyNode,
		references cloneRoutineReferences,
		status cloneRoutineDependencyStatus,
	) {
		if status == cloneRoutineDependenciesUninspectable {
			enqueue(routineNode)
		}
		for tableKey := range references.tables {
			addDependency(routineNode, cloneDatabaseDependencyNode{
				kind: cloneDatabaseObjectNodeKind,
				key:  tableKey,
			})
		}
		for functionKey := range references.functions {
			addDependency(routineNode, cloneDatabaseDependencyNode{
				kind: cloneRoutineFunctionKind,
				key:  functionKey,
			})
		}
		for procedureKey := range references.procedures {
			addDependency(routineNode, cloneDatabaseDependencyNode{
				kind: cloneRoutineProcedureKind,
				key:  procedureKey,
			})
		}
	}
	for _, definition := range source.userDefinedFuncs {
		references, status, err := collectCloneRoutineReferences(
			ctx, definition.body, definition.lang, definition.sqlMode,
			source.srcResolveDBName, lowerCaseTableNames, true,
		)
		if err != nil {
			return omissions, err
		}
		addRoutineDependencies(cloneDatabaseDependencyNode{
			kind: cloneRoutineFunctionKind,
			key: cloneRoutineFamilyKey(
				cloneRoutineFunctionKind, source.srcResolveDBName, definition.name, lowerCaseTableNames,
			),
		}, references, status)
	}
	for _, definition := range source.storedProcedures {
		references, status, err := collectCloneRoutineReferences(
			ctx, definition.body, definition.lang, definition.sqlMode,
			source.srcResolveDBName, lowerCaseTableNames, false,
		)
		if err != nil {
			return omissions, err
		}
		addRoutineDependencies(cloneDatabaseDependencyNode{
			kind: cloneRoutineProcedureKind,
			key: cloneRoutineFamilyKey(
				cloneRoutineProcedureKind, source.srcResolveDBName, definition.name, lowerCaseTableNames,
			),
		}, references, status)
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		for dependent := range dependents[node] {
			enqueue(dependent)
		}
	}
	return omissions, nil
}

func applyCloneDatabaseOmissionSet(
	source *cloneDatabaseSource,
	omissions cloneDatabaseOmissionSet,
	lowerCaseTableNames int64,
) {
	filteredFunctions := make([]userDefinedFunctionDefinition, 0, len(source.userDefinedFuncs))
	for _, definition := range source.userDefinedFuncs {
		key := cloneRoutineFamilyKey(
			cloneRoutineFunctionKind, source.srcResolveDBName, definition.name, lowerCaseTableNames,
		)
		if _, skipped := omissions.functions[key]; !skipped {
			filteredFunctions = append(filteredFunctions, definition)
		}
	}
	filteredProcedures := make([]storedProcedureDefinition, 0, len(source.storedProcedures))
	for _, definition := range source.storedProcedures {
		key := cloneRoutineFamilyKey(
			cloneRoutineProcedureKind, source.srcResolveDBName, definition.name, lowerCaseTableNames,
		)
		if _, skipped := omissions.procedures[key]; !skipped {
			filteredProcedures = append(filteredProcedures, definition)
		}
	}
	filteredViews := make(map[string]*tableInfo, len(source.viewMap))
	for key, view := range source.viewMap {
		if view == nil {
			continue
		}
		viewKey := cloneDatabaseSourceObjectKey(*source, view, lowerCaseTableNames)
		if _, skipped := omissions.objects[viewKey]; !skipped {
			filteredViews[key] = view
		}
	}
	source.userDefinedFuncs = filteredFunctions
	source.storedProcedures = filteredProcedures
	source.viewMap = filteredViews
}

func collectCloneRoutineReferences(
	ctx context.Context,
	body string,
	lang string,
	sqlMode string,
	srcDBName string,
	lowerCaseTableNames int64,
	isFunction bool,
) (cloneRoutineReferences, cloneRoutineDependencyStatus, error) {
	references := newCloneRoutineReferences()
	if !strings.EqualFold(lang, string(tree.SQL)) {
		if isFunction {
			// Non-SQL UDFs are persisted as opaque bodies. Imported packages are
			// rejected by validateCloneUserDefinedFunctions before this graph is
			// built; accepted inline UDFs do not contain SQL relation metadata for
			// this walker to inspect.
			return references, cloneRoutineDependenciesOpaque, nil
		}
		return references, cloneRoutineDependenciesUninspectable, nil
	}

	parseBody := body
	parseAsExpression := isFunction && !cloneSQLFunctionBodyIsQuery(body)
	if parseAsExpression {
		// SQL UDF metadata stores scalar bodies without a SELECT wrapper. Parse
		// them as a projection so calls to another SQL UDF remain visible to the
		// dependency graph without changing the stored definition.
		parseBody = "select " + body
	}
	statements, err := parsers.ParseWithSQLMode(
		ctx, dialect.MYSQL, parseBody, lowerCaseTableNames, sqlMode,
	)
	if err != nil {
		if parseAsExpression {
			// Existing scalar UDF clone support does not parse these bodies. Keep
			// that compatibility behavior, but let the caller omit this
			// uninspectable SQL routine whenever the clone already omits a
			// relation.
			return references, cloneRoutineDependenciesUninspectable, nil
		}
		return references, cloneRoutineDependenciesUninspectable, err
	}
	defer freeStatements(statements)

	unsupported := false
	remappable := remapDbInStatements(statements, remapDbContext{
		lowerCaseTableNames:   lowerCaseTableNames,
		unsupported:           &unsupported,
		rejectUseStateChanges: true,
		collectTableName: func(name *tree.TableName) {
			databaseName := srcDBName
			if name.ExplicitSchema {
				databaseName = string(name.SchemaName)
			}
			references.tables[cloneDatabaseObjectKey(
				databaseName, string(name.ObjectName), lowerCaseTableNames,
			)] = struct{}{}
		},
		collectProcedureName: func(name *tree.ProcedureName) {
			databaseName := srcDBName
			if name.Name.ExplicitSchema {
				databaseName = string(name.Name.SchemaName)
			}
			references.procedures[cloneRoutineFamilyKey(
				cloneRoutineProcedureKind, databaseName,
				string(name.Name.ObjectName), lowerCaseTableNames,
			)] = struct{}{}
		},
		collectFunctionName: collectCloneFunctionReference(
			&references, srcDBName, lowerCaseTableNames,
		),
	})
	if !remappable || unsupported {
		return references, cloneRoutineDependenciesUninspectable, nil
	}
	return references, cloneRoutineDependenciesInspected, nil
}

// filterCloneDatabaseRoutines keeps independent routine families and omits
// families whose direct or transitive dependencies cannot exist in the target.
// SQL routines whose bodies cannot be inspected are omitted when the source
// has an omitted relation. Supported non-SQL UDFs are opaque but cloneable and
// are preserved after validateCloneUserDefinedFunctions rejects imported
// package bodies. UDF overloads are one family here: call sites do not carry
// enough resolved type information for this metadata pass to distinguish
// overloads safely.
func filterCloneDatabaseRoutines(
	ctx context.Context,
	source cloneDatabaseSource,
	lowerCaseTableNames int64,
) ([]userDefinedFunctionDefinition, []storedProcedureDefinition, error) {
	omissions, err := collectCloneDatabaseOmissionSet(ctx, source, lowerCaseTableNames)
	if err != nil {
		return nil, nil, err
	}
	applyCloneDatabaseOmissionSet(&source, omissions, lowerCaseTableNames)
	return source.userDefinedFuncs, source.storedProcedures, nil
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
	if err := validateCloneUserDefinedFunctions(userDefinedFuncs); err != nil {
		return nil, nil, err
	}
	storedProcedures, err := getStoredProcedureInfos(ctx, bh, snapshot, dbName)
	if err != nil {
		return nil, nil, err
	}
	return userDefinedFuncs, storedProcedures, nil
}

// validateCloneUserDefinedFunctions rejects imported non-SQL UDFs before the
// target database is created. Their package object is not versioned with the
// catalog snapshot and transaction outcome, so copying it here would either
// make a historical clone depend on a deleted live object or leave ownership
// ambiguous after an unknown commit result.
func validateCloneUserDefinedFunctions(functions []userDefinedFunctionDefinition) error {
	for _, definition := range functions {
		if strings.EqualFold(definition.lang, string(tree.SQL)) {
			continue
		}

		var body function.NonSqlUdfBody
		if json.Unmarshal([]byte(definition.body), &body) == nil && body.Import {
			return moerr.NewNotSupportedNoCtxf(
				"CREATE DATABASE CLONE with imported %s function %s is not supported: imported UDF packages are not snapshot-versioned",
				definition.lang,
				definition.name,
			)
		}
	}
	return nil
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
		argTypes, err := userDefinedFunctionArgumentTypesFromJSON(row[1])
		if err != nil {
			return nil, err
		}
		functions[i] = userDefinedFunctionDefinition{
			name:     row[0],
			args:     row[1],
			argTypes: argTypes,
			retType:  row[2],
			body:     row[3],
			lang:     row[4],
			sqlMode:  row[5],
			dbName:   dbName,
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

// resolveCloneDatabaseRoutineTenant preserves the caller identity for a
// same-account clone and uses the target account's administrator identity for
// a cross-account clone. Routine metadata must not pair a target account with
// a source-account owner or definer.
func resolveCloneDatabaseRoutineTenant(
	ctx context.Context,
	bh BackgroundExec,
	caller *TenantInfo,
	targetAccountID uint32,
) (*TenantInfo, error) {
	if caller.GetTenantID() == targetAccountID {
		return caller, nil
	}
	if targetAccountID == sysAccountID {
		return getDefaultAccount(), nil
	}

	query := fmt.Sprintf(
		"select account_name, admin_name from mo_catalog.mo_account where account_id = %d",
		targetAccountID,
	)
	rows, err := getStringColsList(defines.AttachAccountId(ctx, sysAccountID), bh, query, 0, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("target account %d has no administrator metadata", targetAccountID)
	}
	return &TenantInfo{
		Tenant:        rows[0][0],
		User:          rows[0][1],
		DefaultRole:   accountAdminRoleName,
		TenantID:      targetAccountID,
		UserID:        GetAdminUserId(),
		DefaultRoleID: accountAdminRoleID,
	}, nil
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

func rewriteCloneUserDefinedFunctionBodies(
	ctx context.Context,
	functions []userDefinedFunctionDefinition,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) ([]userDefinedFunctionDefinition, error) {
	rewritten := slices.Clone(functions)
	for i := range rewritten {
		if !strings.EqualFold(rewritten[i].lang, string(tree.SQL)) {
			continue
		}
		body, err := rewriteCloneSQLFunctionBody(
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

func cloneSQLFunctionBodyIsQuery(body string) bool {
	trimmed := strings.TrimSpace(body)
	lower := strings.ToLower(trimmed)
	return strings.HasPrefix(lower, "select ") ||
		strings.HasPrefix(lower, "select\n") ||
		strings.HasPrefix(lower, "select\t") ||
		strings.HasPrefix(lower, "with ") ||
		strings.HasPrefix(lower, "with\n") ||
		strings.HasPrefix(lower, "with\t")
}

// SQL UDF query bodies need the same case-insensitive query detection as the
// binder. Scalar expressions cannot contain a qualified table reference, so
// they need no database remap and remain byte-for-byte unchanged.
func rewriteCloneSQLFunctionBody(
	ctx context.Context,
	body string,
	sqlMode string,
	srcDBName string,
	dstDBName string,
	lowerCaseTableNames int64,
) (string, error) {
	if !cloneSQLFunctionBodyIsQuery(body) {
		return body, nil
	}
	return rewriteCloneSQLRoutineBody(
		ctx, body, sqlMode, srcDBName, dstDBName, lowerCaseTableNames,
	)
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
	if err := remapCloneRoutineStatements(
		ctx, stmts, map[string]string{srcDBName: dstDBName}, lowerCaseTableNames,
	); err != nil {
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
