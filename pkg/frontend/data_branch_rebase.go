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

package frontend

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Endpoint order is Bf, F, Bu, U. Keeping all four records is intentional:
// physically different copies can represent the same logical base state.
type rebaseEndpoint struct {
	database string
	at       *tree.AtTimeStamp
}

type rebaseDatabaseRequest struct {
	destination string
	endpoints   [4]rebaseEndpoint
	tables      []string
	policy      rebaseConflictPolicy

	// Public R1 requests never set this. Publication must borrow its caller's
	// transaction so replay cannot commit separately from history/pointer CAS.
	sourceExpectation *databaseInventoryToken
}

type rebaseBoundDatabase struct {
	stmt   tree.CloneDatabase
	source cloneDatabaseSource
	defs   map[string]*plan.TableDef
}

type rebaseDatabaseSummary struct {
	database, baseDatabase string
	databaseID             uint64
	baseTimestamp          int64
	tables                 uint64
	// feature/upstream/conflict keys; applied inserts/updates/deletes.
	counts [6]uint64
}

type rebaseAppliedObserver func(context.Context, string, string)

var rebaseAppliedHook atomic.Pointer[rebaseAppliedObserver]
var rebaseSourceExpectationForTest atomic.Bool

// SetRebaseSourceExpectationForTest enables only the unqualified EXPECT
// candidate in an exclusive SQL fixture. Production has no activation path.
func SetRebaseSourceExpectationForTest(enabled bool) func() {
	previous := rebaseSourceExpectationForTest.Swap(enabled)
	return func() { rebaseSourceExpectationForTest.Store(previous) }
}

// SetRebaseTableAppliedHookForTest exposes the partial-apply boundary missing
// from clone admission hooks. Tests must restore it and use the exclusive SQL
// fixture. Production pays only one nil atomic load per processed table.
func SetRebaseTableAppliedHookForTest(hook func(context.Context, string, string)) func() {
	observer := rebaseAppliedObserver(hook)
	previous := rebaseAppliedHook.Swap(&observer)
	return func() { rebaseAppliedHook.Store(previous) }
}

func handleBranchRebase(execCtx *ExecCtx, ses *Session, stmt *tree.DataBranchRebase) error {
	var expectation *databaseInventoryToken
	if stmt.SourceExpectation != nil {
		if !rebaseSourceExpectationForTest.Load() {
			return moerr.NewNotSupported(execCtx.reqCtx, "Rebase EXPECT publication is disabled")
		}
		account, err := defines.GetAccountId(execCtx.reqCtx)
		if err != nil {
			return err
		}
		expectation = &databaseInventoryToken{accountID: account,
			databaseID: stmt.SourceExpectation.DatabaseID, epoch: stmt.SourceExpectation.Epoch, tracked: true}
	}
	request := rebaseDatabaseRequest{
		destination:       stmt.Destination.String(),
		sourceExpectation: expectation,
		endpoints: [4]rebaseEndpoint{
			{stmt.FeatureBase.Database.String(), stmt.FeatureBase.AtTsExpr},
			{stmt.Feature.Database.String(), stmt.Feature.AtTsExpr},
			{stmt.UpstreamBase.Database.String(), stmt.UpstreamBase.AtTsExpr},
			{stmt.Upstream.Database.String(), stmt.Upstream.AtTsExpr},
		},
	}
	for _, name := range stmt.Tables {
		request.tables = append(request.tables, string(name))
	}
	if stmt.ConflictOpt != nil {
		switch stmt.ConflictOpt.Opt {
		case tree.CONFLICT_FAIL:
			request.policy = rebaseFail
		case tree.CONFLICT_SKIP:
			request.policy = rebaseKeepUpstream
		case tree.CONFLICT_ACCEPT:
			request.policy = rebaseKeepBranch
		default:
			return moerr.NewInvalidInput(execCtx.reqCtx, "invalid Rebase conflict policy")
		}
	}
	summary, err := executeRebaseDatabase(execCtx, ses, request)
	if err != nil {
		return err
	}
	// A private owner has committed. In an explicit transaction this is a
	// provisional statement result; the caller still owns commit or rollback.
	mrs := &MysqlResultSet{}
	names := []string{"database", "database_id", "base_database", "base_ts", "tables",
		"feature_changed_keys", "upstream_changed_keys", "conflict_keys", "applied_insert_rows", "applied_update_rows", "applied_delete_rows"}
	for i, name := range names {
		column := &MysqlColumn{}
		column.SetName(name)
		if i == 0 || i == 2 {
			column.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		} else {
			column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
			column.SetSigned(i == 3)
		}
		mrs.AddColumn(column)
	}
	row := []interface{}{summary.database, summary.databaseID, summary.baseDatabase, summary.baseTimestamp, summary.tables}
	for _, count := range summary.counts {
		row = append(row, count)
	}
	mrs.AddRow(row)
	ses.SetMysqlResultSet(mrs)
	return nil
}

// executeRebaseDatabase uses one transaction for native R1. It does not
// publish adapter generations/history. A caller-owned summary is provisional.
func executeRebaseDatabase(execCtx *ExecCtx, ses *Session, request rebaseDatabaseRequest) (summary rebaseDatabaseSummary, err error) {
	ctx := execCtx.reqCtx
	if request.destination == "" || strings.ContainsRune(request.destination, 0) || request.policy > rebaseKeepBranch {
		return summary, moerr.NewInvalidInput(ctx, "invalid Rebase request")
	}
	borrowOwner := ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN)
	if request.sourceExpectation != nil && !borrowOwner {
		return summary, moerr.NewNotSupported(ctx, "Rebase publication requires an explicit transaction")
	}
	if dataBranchPickTxnNotAllowed(ses) && !borrowOwner {
		return summary, moerr.NewNotSupported(ctx, "Rebase requires an independent transaction or explicit BEGIN")
	}
	policy, err := captureRewritePolicy(ctx, ses)
	if err != nil {
		return summary, err
	}
	if len(policy.sessionRules) != 0 || len(policy.sessionRemapDb) != 0 {
		return summary, moerr.NewNotSupported(ctx, "Rebase does not support active session rewrites")
	}
	for _, endpoint := range request.endpoints {
		if endpoint.database == "" || strings.ContainsRune(endpoint.database, 0) || strings.EqualFold(endpoint.database, request.destination) ||
			endpoint.at == nil || (endpoint.at.Type != tree.ATTIMESTAMPSNAPSHOT && endpoint.at.Type != tree.ATMOTIMESTAMP) {
			return summary, moerr.NewInvalidInput(ctx, "Rebase requires four fixed endpoints and a new destination")
		}
	}
	if err = requireDataBranchDatabaseIdentity(ctx, currentProtocolVersion(ses.proc)); err != nil {
		return
	}
	bh, admission, finish, err := getDataBranchComponentExecutor(ctx, ses, borrowOwner)
	if err != nil {
		return summary, err
	}
	defer func() {
		if err != nil {
			summary = rebaseDatabaseSummary{}
		}
	}()
	defer finishDataBranchComponent(ctx, finish, &err)
	// Rule writers take this gate before changing rules or their target tables.
	// Keep it until commit so the four source inventories cannot gain a rule.
	if err = lockRoleRuleLifecycleInBackground(ctx, bh); err != nil {
		return summary, err
	}
	restore := installDataBranchCloneContext(execCtx, tree.NormalCloneLevelDatabase, catalog.SystemDBTypeDataBranch)
	defer restore()
	ctx = execCtx.reqCtx
	defer clearRebaseResults(bh)
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	var sources [4]rebaseBoundDatabase
	var requests []branchCloneSource
	for i, endpoint := range request.endpoints {
		bound := &sources[i]
		bound.stmt = tree.CloneDatabase{DstDatabase: tree.Identifier(request.destination), SrcDatabase: tree.Identifier(endpoint.database), AtTsExpr: endpoint.at}
		create := &tree.DataBranchCreateDatabase{CloneDatabase: bound.stmt}
		if !skipDataBranchPrivilegeCheck(ses) {
			if _, err = authenticateDataBranchCreateDatabase(ctx, ses, create); err != nil {
				return
			}
		}
		bound.source, err = collectCloneDatabaseSource(ctx, ses, bh, &bound.stmt, nil)
		if err != nil {
			return
		}
		source := bound.source
		if err = validateRebaseTimestamp(ctx, source.snapshot); err != nil {
			return
		}
		if source.opAccountId != account || source.toAccountId != account || source.srcResolveDBName != endpoint.database ||
			source.snapshot == nil || source.snapshot.TS == nil || source.snapshot.TS.PhysicalTime <= 0 ||
			(source.snapshot.Tenant != nil && source.snapshot.Tenant.TenantID != account) {
			err = moerr.NewNotSupported(ctx, "Rebase requires same-account, non-subscription fixed endpoints")
			return
		}
		if _, err = authenticateDataBranchCreateDatabaseSourceTables(ctx, ses, create, source); err != nil {
			return
		}
		requests = append(requests, cloneDatabaseAdmissionRequests(source, &bound.stmt)...)
		clearRebaseResults(bh)
	}
	dag, err := admission.admitBranchCloneRC(ctx, ses, bh, requests, branchCloneDatabase{account, request.destination}, "", true, nil)
	if err != nil {
		return summary, err
	}
	execCtx.reqCtx = context.WithValue(ctx, branchCloneComponentKey{}, &dag)
	ctx = execCtx.reqCtx
	for i := range sources {
		bound := &sources[i]
		old := bound.source
		current, readErr := collectCloneDatabaseSource(ctx, ses, bh, &bound.stmt,
			&cloneDatabaseAccountResolution{opAccountId: account, toAccountId: account, snapshot: old.requestSnapshot})
		if readErr != nil {
			return summary, readErr
		}
		if !sameBranchCloneDatabaseSource(old, current) {
			return summary, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		bound.source = current
		bound.defs, err = resolveRebaseTableDefinitions(ctx, ses, bh, current)
		if err != nil {
			return
		}
		clearRebaseResults(bh)
	}
	if err = rejectRebaseSourceRewriteRules(ctx, bh, sources, parserLowerCaseTableNames(ses)); err != nil {
		return summary, err
	}
	if exists, readErr := checkDatabaseExistsAtSnapshot(ctx, bh, nil, request.destination); readErr != nil {
		return summary, readErr
	} else if exists {
		return summary, moerr.NewDBAlreadyExists(ctx, request.destination)
	}
	if err = checkBranchQuotaForAccount(ctx, ses, bh, ses.GetTenantInfo().GetTenant(), account, int64(len(sources[3].defs))); err != nil {
		return
	}
	if err = protectRebaseEndpoints(ctx, ses, bh, admission, sources, dag); err != nil {
		return
	}
	tables, reads, err := preflightRebaseTables(ctx, bh, sources, dag, request)
	if err != nil {
		return summary, err
	}
	if request.sourceExpectation != nil {
		if err = reserveRebaseFeatureSource(ctx, admission.proc.GetSessionInfo().StorageEngine,
			admission.proc.GetTxnOperator(), sources[1], *request.sourceExpectation); err != nil {
			return summary, err
		}
	}
	receipts, err := handleCloneDatabaseWithSource(execCtx, ses, bh, &sources[3].stmt, &sources[3].source)
	if err != nil {
		return summary, err
	}
	if len(receipts) != len(tables) {
		return summary, moerr.NewInternalError(ctx, "Rebase clone inventory changed")
	}
	for i := range receipts {
		receipt := &receipts[i]
		if err = updateBranchMetaTable(ctx, ses, bh, receipt); err != nil {
			return
		}
		parent := sources[3].defs[receipt.srcTbl]
		if parent == nil || receipt.srcTableID != parent.TblId || receipt.srcDb != reads[3].database || receipt.snapshotTS != reads[3].timestamp {
			return summary, moerr.NewInternalError(ctx, "Rebase clone did not retain fixed upstream identity")
		}
		if err = admission.lockBranchSnapshotName(ctx, receipt.dstTableID); err != nil {
			return
		}
		if err = createBranchProtectSnapshot(ctx, ses, bh, receipt); err != nil {
			return
		}
	}
	for _, table := range tables {
		if table.sql.changes == "" {
			continue // Independently proven unchanged no-PK table.
		}
		counts, readErr := readRebaseCounts(ctx, bh, table.sql.changes, 6)
		if readErr != nil {
			return summary, readErr
		}
		if request.policy == rebaseFail && counts[2] != 0 {
			return summary, moerr.NewInvalidInputf(ctx, "Rebase conflict in table %s", table.name)
		}
		if counts[3] != 0 || counts[4] != 0 || counts[5] != 0 {
			for _, query := range []string{table.sql.deleteRows, table.sql.insertRows} {
				if err = bh.Exec(ctx, query); err != nil {
					return
				}
				clearRebaseResults(bh)
			}
		}
		for i, value := range counts {
			if math.MaxUint64-summary.counts[i] < value {
				return summary, moerr.NewInternalError(ctx, "Rebase summary overflow")
			}
			summary.counts[i] += value
		}
		if observer := rebaseAppliedHook.Load(); observer != nil && *observer != nil {
			(*observer)(ctx, request.destination, table.name)
		}
		if err = ctx.Err(); err != nil {
			return
		}
	}
	ids, err := readRebaseCounts(ctx, bh, fmt.Sprintf("select dat_id from mo_catalog.mo_database where account_id=%d and datname=%s", account, rebaseTextLiteral(request.destination)), 1)
	if err != nil {
		return summary, err
	}
	summary.database, summary.databaseID = request.destination, ids[0]
	summary.baseDatabase, summary.baseTimestamp = reads[3].database, reads[3].timestamp
	summary.tables = uint64(len(tables))
	return
}

// Reject rules for any role on an endpoint: role membership can change while
// the statement is running, independently of the role-rule lifecycle gate.
func rejectRebaseSourceRewriteRules(ctx context.Context, bh BackgroundExec, sources [4]rebaseBoundDatabase, lowerCaseTableNames int64) error {
	defer clearRebaseResults(bh)
	names := make(map[string]struct{})
	for _, source := range sources {
		for table := range source.defs {
			db, tbl, err := parsers.NormalizeRewriteKeyParts(ctx, source.source.srcResolveDBName, table, lowerCaseTableNames)
			if err != nil {
				return err
			}
			names[db+"."+tbl] = struct{}{}
		}
	}
	if len(names) == 0 {
		return nil
	}
	counts, err := readRebaseCounts(ctx, bh, "select count(*) from mo_catalog.mo_role_rule", 1)
	if err != nil {
		return err
	}
	total := counts[0]
	const pageSize = 128
	var lastRole int64
	var lastName string
	var seen uint64
	for seen < total {
		if err := ctx.Err(); err != nil {
			return err
		}
		query := "select role_id, rule_name from mo_catalog.mo_role_rule"
		if seen != 0 {
			// Encode the cursor rather than quoting it: a backslash in a rule
			// name must retain its value under every session SQL mode.
			query += fmt.Sprintf(" where role_id > %d or (role_id = %d and rule_name > %s)", lastRole, lastRole, rebaseTextLiteral(lastName))
		}
		query += fmt.Sprintf(" order by role_id, rule_name limit %d", pageSize)
		clearRebaseResults(bh)
		if err := bh.Exec(ctx, query); err != nil {
			return err
		}
		results, err := getResultSet(ctx, bh)
		if err != nil {
			clearRebaseResults(bh)
			return err
		}
		expected := min(uint64(pageSize), total-seen)
		if len(results) != 1 || results[0].GetColumnCount() != 2 || results[0].GetRowCount() != expected {
			clearRebaseResults(bh)
			return moerr.NewInternalError(ctx, "invalid Rebase rewrite rule result")
		}
		count := results[0].GetRowCount()
		for i := uint64(0); i < count; i++ {
			lastRole, err = results[0].GetInt64(ctx, i, 0)
			if err == nil {
				lastName, err = results[0].GetString(ctx, i, 1)
			}
			if err != nil {
				clearRebaseResults(bh)
				return err
			}
			name, _, _, err := parsers.NormalizeRewriteKey(ctx, lastName, lowerCaseTableNames)
			if err != nil {
				clearRebaseResults(bh)
				return err
			}
			if _, exists := names[name]; exists {
				clearRebaseResults(bh)
				return moerr.NewNotSupported(ctx, "Rebase does not support rewrite rules on source tables")
			}
		}
		clearRebaseResults(bh)
		seen += count
	}
	return nil
}

// rebaseTextLiteral is independent of the session's backslash-escape mode.
func rebaseTextLiteral(value string) string {
	return fmt.Sprintf("cast(unhex('%x') as char)", []byte(value))
}

func clearRebaseResults(bh BackgroundExec) {
	bh.ClearExecResultBatches()
	bh.ClearExecResultSet()
}

func readRebaseCounts(ctx context.Context, bh BackgroundExec, query string, columns uint64) ([]uint64, error) {
	clearRebaseResults(bh)
	defer clearRebaseResults(bh)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := bh.Exec(ctx, query); err != nil {
		return nil, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if len(results) != 1 || results[0].GetRowCount() != 1 || results[0].GetColumnCount() != columns {
		return nil, moerr.NewInternalError(ctx, "invalid Rebase aggregate result")
	}
	counts := make([]uint64, columns)
	for i := range counts {
		counts[i], err = results[0].GetUint64(ctx, 0, uint64(i))
		if err != nil {
			return nil, err
		}
	}
	return counts, nil
}

func resolveRebaseTableDefinitions(ctx context.Context, ses *Session, bh BackgroundExec, source cloneDatabaseSource) (map[string]*plan.TableDef, error) {
	if len(source.viewMap) != 0 || len(source.userDefinedFuncs) != 0 || len(source.storedProcedures) != 0 {
		return nil, moerr.NewNotSupported(ctx, "Rebase does not support views or routines")
	}
	defs := make(map[string]*plan.TableDef, len(source.srcTblInfos))
	for _, info := range source.srcTblInfos {
		if info == nil || isSequence(info) || !isCloneableCloneDatabaseTable(info) {
			return nil, moerr.NewNotSupported(ctx, "Rebase requires ordinary stored tables")
		}
		resolved, err := resolveBranchCloneSource(ctx, ses, bh, branchCloneSource{source.opAccountId, info.dbName, info.tblName, source.snapshot})
		if err != nil {
			return nil, err
		}
		defs[info.tblName] = resolved.def
	}
	return defs, nil
}

type rebasePlannedTable struct {
	name   string
	replay bool
	sql    rebaseTableSQL
}

func preflightRebaseTables(ctx context.Context, bh BackgroundExec, sources [4]rebaseBoundDatabase, dag databranchutils.BranchReclaimDag, request rebaseDatabaseRequest) ([]rebasePlannedTable, [4]rebaseSQLRead, error) {
	var reads [4]rebaseSQLRead
	for i, source := range sources {
		reads[i] = rebaseSQLRead{source.source.srcResolveDBName, source.source.snapshot.TS.PhysicalTime}
		if len(source.defs) != len(sources[0].defs) {
			return nil, reads, moerr.NewNotSupported(ctx, "Rebase endpoint table inventories differ")
		}
	}
	// R1 retains the new base with one branch pin per table. An empty
	// database has no such pin, so its returned base timestamp would not
	// remain protected after the caller drops the input snapshot.
	if len(sources[0].defs) == 0 {
		return nil, reads, moerr.NewNotSupported(ctx, "Rebase requires at least one table to retain its base")
	}
	selected := make(map[string]bool, len(request.tables))
	for _, name := range request.tables {
		if name == "" || strings.ContainsRune(name, 0) || selected[name] {
			return nil, reads, moerr.NewInvalidInput(ctx, "invalid or duplicate Rebase table selection")
		}
		if _, exists := sources[0].defs[name]; !exists {
			return nil, reads, moerr.NewInvalidInputf(ctx, "Rebase selected table %s does not exist", name)
		}
		selected[name] = true
	}
	names := make([]string, 0, len(sources[0].defs))
	for name := range sources[0].defs {
		names = append(names, name)
	}
	slices.Sort(names)
	tables := make([]rebasePlannedTable, 0, len(names))
	for _, name := range names {
		var defs [4]*plan.TableDef
		for i := range sources {
			defs[i] = sources[i].defs[name]
		}
		replay := len(request.tables) == 0 || selected[name]
		var columns []rebaseSQLColumn
		var err error
		if replay {
			columns, err = validateRebaseTableSchema(ctx, defs)
		} else {
			columns, err = validateRebaseOmittedTableSchema(ctx, defs)
		}
		if err != nil {
			return nil, reads, err
		}
		for _, pair := range [][2]int{{0, 1}, {2, 3}} {
			if !rebaseBaseIsAncestor(dag, defs[pair[0]].TblId, reads[pair[0]].timestamp, defs[pair[1]].TblId, reads[pair[1]].timestamp) {
				return nil, reads, moerr.NewInvalidInput(ctx, "Rebase base is not a protected ancestor of its head")
			}
		}
		table := rebasePlannedTable{name: name, replay: replay}
		hasPK := slices.ContainsFunc(columns, func(column rebaseSQLColumn) bool { return column.primary })
		var checks []string
		if hasPK && replay {
			table.sql, err = buildRebaseTableSQL(ctx, name, request.destination, reads, columns, request.policy)
			checks = append(checks, table.sql.baseMismatch)
		} else if !hasPK {
			for _, pair := range [][2]int{{0, 2}, {0, 1}, {2, 3}} {
				var query string
				query, err = buildRebaseBagMismatchSQL(ctx, name, reads[pair[0]], reads[pair[1]], columns)
				if err != nil {
					break
				}
				checks = append(checks, query)
			}
		}
		if err != nil {
			return nil, reads, err
		}
		for _, query := range checks {
			counts, err := readRebaseCounts(ctx, bh, query, 1)
			if err != nil {
				return nil, reads, err
			}
			if counts[0] != 0 {
				return nil, reads, moerr.NewNotSupported(ctx, "Rebase requires matching bases and rejects changed no-PK tables")
			}
		}
		tables = append(tables, table)
	}
	return tables, reads, nil
}
