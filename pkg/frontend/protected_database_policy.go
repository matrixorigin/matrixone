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

package frontend

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// The session privilege cache owns this proof. It contains no storage
// pointers and is usable only after establishing the statement snapshot.
type protectedPolicyCertificate struct {
	account      uint32
	dependency   [4]engine.TableContentDependency
	version      [4]engine.TableContentVersion
	reusableFrom timestamp.Timestamp
	value        string
}

func (proof *protectedPolicyCertificate) valid(ctx context.Context, ses *Session, snapshot timestamp.Timestamp) bool {
	if proof.reusableFrom.IsEmpty() || snapshot.Less(proof.reusableFrom) || proof.account != ses.GetTenantInfo().GetTenantID() {
		return false
	}
	reader, ok := getPu(ses.GetService()).StorageEngine.(engine.TableContentReader)
	if !ok {
		return false
	}
	var observed [4]engine.TableContentVersion
	return reader.ReadTableContentVersions(ctx, snapshot, proof.dependency[:], observed[:]) && observed == proof.version
}

func prepareProtectedPolicyCertificate(ctx context.Context, ses *Session, bh BackgroundExec, snapshot timestamp.Timestamp) (protectedPolicyCertificate, bool) {
	var proof protectedPolicyCertificate
	txnSource, ok := bh.(interface{ GetTxnOperator() TxnOperator })
	if !ok || snapshot.IsEmpty() {
		return proof, false
	}
	op := txnSource.GetTxnOperator()
	if op == nil || !op.SnapshotTS().Equal(snapshot) || op.Txn().Isolation != pbtxn.TxnIsolation_SI || !op.TxnOptions().ReadOnly() {
		return proof, false
	}
	storage := getPu(ses.GetService()).StorageEngine
	reader, ok := storage.(engine.TableContentReader)
	if !ok {
		return proof, false
	}
	proof.account = ses.GetTenantInfo().GetTenantID()
	tableCtx := defines.AttachAccountId(ctx, proof.account)
	db, err := storage.Database(tableCtx, catalog.MO_CATALOG, op)
	if err != nil {
		return protectedPolicyCertificate{}, false
	}
	dbID, err := strconv.ParseUint(db.GetDatabaseId(tableCtx), 10, 64)
	if err != nil {
		return protectedPolicyCertificate{}, false
	}
	const table = "mo_mysql_compatibility_mode"
	relation, err := db.Relation(tableCtx, table, nil)
	if err != nil || relation == nil {
		return protectedPolicyCertificate{}, false
	}
	tableID := relation.GetTableID(tableCtx)
	if err := storage.TryToSubscribeTable(tableCtx, uint64(proof.account), dbID, tableID, catalog.MO_CATALOG, table); err != nil {
		return protectedPolicyCertificate{}, false
	}
	proof.dependency[0] = engine.TableContentDependency{DatabaseID: dbID, TableID: tableID}
	coreCatalog := defines.AttachAccountId(ctx, sysAccountID)
	for i, dependency := range [...]struct {
		name string
		id   uint64
	}{
		{name: catalog.MO_DATABASE, id: catalog.MO_DATABASE_ID},
		{name: catalog.MO_TABLES, id: catalog.MO_TABLES_ID},
		{name: catalog.MO_COLUMNS, id: catalog.MO_COLUMNS_ID},
	} {
		if err := storage.TryToSubscribeTable(coreCatalog, uint64(sysAccountID), catalog.MO_CATALOG_ID, dependency.id, catalog.MO_CATALOG, dependency.name); err != nil {
			return protectedPolicyCertificate{}, false
		}
		proof.dependency[i+1] = engine.TableContentDependency{DatabaseID: catalog.MO_CATALOG_ID, TableID: dependency.id}
	}
	proof.reusableFrom = snapshot
	if !reader.ReadTableContentVersions(ctx, snapshot, proof.dependency[:], proof.version[:]) {
		return protectedPolicyCertificate{}, false
	}
	return proof, true
}

func protectedPolicyEngine(storage engine.Engine) engine.Engine {
	for {
		entire, ok := storage.(*engine.EntireEngine)
		if !ok {
			return storage
		}
		if entire == nil {
			return nil
		}
		storage = entire.Engine
	}
}

// protectedPolicyFrontier orders authorization after completed policy updates.
// The HLC upper-bound fallback is the same rolling-upgrade mechanism used for
// the role-grant graph when a TN does not support an ordered logtail barrier.
func protectedPolicyFrontier(ctx context.Context, ses *Session) (timestamp.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return timestamp.Timestamp{}, context.Cause(ctx)
	}
	pu := getPu(ses.GetService())
	if pu == nil || pu.StorageEngine == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "missing storage engine for protected database policy")
	}
	eng := protectedPolicyEngine(pu.StorageEngine)
	peerAvailable := false
	if eng != nil {
		if barrier, ok := eng.(engine.PeerLogtailReadBarrier); ok {
			peerAvailable = true
			frontier, supported, err := barrier.AcquirePeerLogtailReadBarrier(ctx)
			if err != nil {
				return timestamp.Timestamp{}, err
			}
			if supported && frontier.IsEmpty() {
				return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "empty protected database policy frontier")
			}
			if supported {
				return frontier, nil
			}
		}
	}
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil || rt.Clock() == nil || rt.Clock().MaxOffset() < 0 {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "invalid transaction clock for protected database policy")
	}
	if !peerAvailable {
		if value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			if version, valid := value.(int64); valid && version >= defines.MORPCVersion11 {
				return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "protected database policy peer barrier is unavailable")
			}
		}
	}
	_, upper := rt.Clock().Now()
	if upper.PhysicalTime < 0 || upper.PhysicalTime == math.MaxInt64 {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "protected database policy timestamp overflow")
	}
	return timestamp.Timestamp{PhysicalTime: upper.PhysicalTime + 1}, nil
}

func protectedPolicyReadSnapshot(ctx context.Context, ses *Session, min timestamp.Timestamp) (timestamp.Timestamp, error) {
	frontier, err := protectedPolicyFrontier(ctx, ses)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	if min.Greater(frontier) {
		frontier = min
	}
	return waitProtectedPolicySnapshot(ctx, ses, frontier)
}

func waitProtectedPolicySnapshot(ctx context.Context, ses *Session, min timestamp.Timestamp) (timestamp.Timestamp, error) {
	storage := protectedPolicyEngine(getPu(ses.GetService()).StorageEngine)
	waiter, ok := storage.(engine.LogtailAppliedSnapshot)
	if !ok {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "current logtail snapshot is unavailable for protected database policy")
	}
	snapshot, err := waiter.WaitLogtailAppliedSnapshot(ctx, min)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	if !snapshot.Greater(min) {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "protected database policy snapshot did not advance")
	}
	return snapshot, nil
}

// protectedPolicyCachedSnapshot uses the policy row's table-local target only
// when a cached certificate can prove both the row and its catalog bindings
// unchanged on the target replay generation. A cold or stale certificate
// uses the existing ordered logtail barrier below.
func protectedPolicyCachedSnapshot(ctx context.Context, ses *Session, cached protectedPolicyCertificate, min timestamp.Timestamp) (timestamp.Timestamp, bool, error) {
	if cached.account != ses.GetTenantInfo().GetTenantID() || cached.reusableFrom.IsEmpty() ||
		cached.dependency[0].DatabaseID != catalog.MO_CATALOG_ID || cached.dependency[0].TableID == 0 {
		return timestamp.Timestamp{}, false, nil
	}
	storage := protectedPolicyEngine(getPu(ses.GetService()).StorageEngine)
	reader, ok := storage.(engine.PeerTableReadSnapshot)
	if !ok {
		return timestamp.Timestamp{}, false, nil
	}
	table := api.TableID{
		DbId:   cached.dependency[0].DatabaseID,
		TbId:   cached.dependency[0].TableID,
		AccId:  cached.account,
		DbName: catalog.MO_CATALOG,
		TbName: "mo_mysql_compatibility_mode",
	}
	snapshot, generation, supported, err := reader.AcquirePeerTableReadSnapshot(ctx, table, min)
	if err != nil || !supported {
		return timestamp.Timestamp{}, supported, err
	}
	if !snapshot.Greater(min) {
		return timestamp.Timestamp{}, true, moerr.NewInternalError(ctx, "protected database policy snapshot did not advance")
	}
	if generation == 0 || !cached.valid(ctx, ses, snapshot) {
		return timestamp.Timestamp{}, true, nil
	}
	for _, version := range cached.version {
		if version.Replay != generation {
			return timestamp.Timestamp{}, true, nil
		}
	}
	return snapshot, true, nil
}

func protectedPolicyForStatement(ctx context.Context, ses *Session) (map[string]struct{}, error) {
	if ses == nil || !ses.GetFromRealUser() || canWriteProtectedDatabase(ses) {
		return nil, nil
	}
	raw, err := protectedPolicyValueForStatement(ctx, ses)
	if err != nil {
		return nil, err
	}
	return protectedDatabaseSetFromString(ses, raw), nil
}

func protectedPolicyValueForStatement(ctx context.Context, ses *Session) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	var execution *ExecCtx
	if compiler := ses.GetTxnCompileCtx(); compiler != nil {
		compiler.mu.Lock()
		execution = compiler.execCtx
		compiler.mu.Unlock()
	}
	if execution != nil && (execution.ses != ses || execution.reqCtx == nil) {
		execution = nil
	}
	if execution != nil {
		execution.authorizationMu.Lock()
		defer execution.authorizationMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	if execution != nil && execution.protectedPolicyLoaded {
		return execution.protectedPolicyRaw, nil
	}
	// The table-local target only orders policy changes. It cannot establish
	// visibility for grants or other authorization dependencies by itself.
	general, err := authorizationReadSnapshotLocked(ctx, ses, execution)
	if err != nil {
		return "", err
	}
	if general.IsEmpty() {
		return "", moerr.NewInternalError(ctx, "authorization snapshot is unavailable for protected database policy")
	}
	min := ses.getLastCommitTS()
	if previous := general.Prev(); previous.Greater(min) {
		min = previous
	}
	cache := ses.GetPrivilegeCache()
	var cached protectedPolicyCertificate
	if cache != nil {
		cache.protectedPolicyMu.Lock()
		cached = cache.protectedPolicy
		cache.protectedPolicyMu.Unlock()
	}
	if cache != nil {
		snapshot, supported, snapshotErr := protectedPolicyCachedSnapshot(ctx, ses, cached, min)
		if snapshotErr != nil {
			return "", snapshotErr
		}
		if supported && !snapshot.IsEmpty() {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if execution != nil {
				execution.protectedPolicyRaw = cached.value
				execution.protectedPolicyLoaded = true
				execution.authorizationSnapshot = snapshot
			}
			return cached.value, nil
		}
	}
	snapshot, err := protectedPolicyReadSnapshot(ctx, ses, min)
	if err != nil {
		return "", err
	}
	if cache != nil && cached.valid(ctx, ses, snapshot) {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		if execution != nil {
			execution.protectedPolicyRaw = cached.value
			execution.protectedPolicyLoaded = true
			execution.authorizationSnapshot = snapshot
		}
		return cached.value, nil
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true, readSnapshot: snapshot})
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return "", err
	}
	var proof protectedPolicyCertificate
	var reusable bool
	if cache != nil {
		proof, reusable = prepareProtectedPolicyCertificate(ctx, ses, bh, snapshot)
	}
	raw, err := readProtectedPolicy(ctx, ses, bh)
	if err == nil && reusable {
		reusable = proof.valid(ctx, ses, snapshot)
	}
	err = finishTxn(ctx, bh, err)
	if err != nil {
		return "", err
	}
	if cache != nil {
		cache.protectedPolicyMu.Lock()
		if reusable {
			proof.value = raw
			cache.protectedPolicy = proof
		} else {
			cache.protectedPolicy = protectedPolicyCertificate{}
		}
		cache.protectedPolicyMu.Unlock()
	}
	if execution != nil {
		execution.protectedPolicyRaw = raw
		execution.protectedPolicyLoaded = true
		execution.authorizationSnapshot = snapshot
	}
	return raw, nil
}

func readProtectedPolicy(ctx context.Context, ses *Session, bh BackgroundExec) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	account := ses.GetTenantInfo().GetTenantID()
	ctx = defines.AttachAccountId(ctx, account)
	sql := fmt.Sprintf("select variable_value from mo_catalog.mo_mysql_compatibility_mode where account_id = %d and system_variables = true and variable_name = 'protected_databases'", account)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return "", err
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil {
		return "", err
	}
	var result string
	found := false
	for _, row := range rows {
		for i := uint64(0); i < row.GetRowCount(); i++ {
			if found {
				return "", moerr.NewInternalError(ctx, "duplicate protected database policies")
			}
			found = true
			isNull, nullErr := row.ColumnIsNull(ctx, i, 0)
			if nullErr != nil {
				return "", nullErr
			}
			if isNull {
				return "", moerr.NewInternalError(ctx, "protected database policy is null")
			}
			if result, err = row.GetString(ctx, i, 0); err != nil {
				return "", err
			}
		}
	}
	return result, nil
}

// A derived user statement retains the caller's policy for its lifetime.
type derivedProtectedPolicy struct {
	owner *Session
	raw   string
}

// A procedure body runs on a background session. Check its caller's captured
// policy while resolving unqualified names against the procedure session.
func checkProcedureProtectedWrites(ctx context.Context, back *backSession, stmt tree.Statement, p *plan.Plan) error {
	if back == nil || back.protectedPolicy == nil || stmt == nil {
		return nil
	}
	owner := back.protectedPolicy.owner
	if !owner.GetFromRealUser() || canWriteProtectedDatabase(owner) {
		return nil
	}
	stmt = authorizationStatement(stmt)
	if stmt.GetQueryType() != tree.QueryTypeDML && stmt.GetQueryType() != tree.QueryTypeDDL {
		switch stmt.(type) {
		case *tree.CloneTable, *tree.CloneDatabase, *tree.DataBranchCreateTable,
			*tree.DataBranchDeleteTable, *tree.DataBranchMerge, *tree.DataBranchPick,
			*tree.DataBranchCreateDatabase, *tree.DataBranchDeleteDatabase:
		default:
			return nil
		}
	}
	priv := determinePrivilegeSetOfStatement(stmt)
	targets := append([]string(nil), priv.writeDatabaseTargets...)
	defaultDB := back.GetDatabaseName()
	if p != nil {
		if resolved := resolvedDDLWriteDatabase(p); resolved != "" {
			defaultDB = resolved
		}
		if p.GetQuery() != nil && priv.objectType() == objectTypeTable {
			requirement := derivePlanAuthorization(back, stmt, p, priv)
			targets = requirement.writeTargets
			if stmt.GetQueryType() == tree.QueryTypeDML && len(targets) == 0 {
				return moerr.NewInternalError(ctx, "cannot determine procedure write target")
			}
		}
	}
	for i := range targets {
		if targets[i] == "" {
			targets[i] = defaultDB
		}
	}
	protected := protectedDatabaseSetFromString(owner, back.protectedPolicy.raw)
	allowed := checkProtectedDatabaseWriteWithSet(ctx, owner, protected, targets...)
	if !allowed {
		return moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
	}
	return nil
}

// Every setter locks the stable account row, including before the first policy
// row exists. The policy is then read at a frontier newer than the preceding
// setter while retaining that lock through commit.
func lockAndValidateProtectedPolicy(ctx context.Context, ses *Session, bh BackgroundExec, value interface{}) error {
	raw, ok := value.(string)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid protected database policy value")
	}
	account := ses.GetTenantInfo().GetTenantID()
	txnSource, ok := bh.(interface{ GetTxnOperator() TxnOperator })
	if !ok {
		return moerr.NewInternalError(ctx, "missing protected database policy transaction")
	}
	txn := txnSource.GetTxnOperator()
	if txn == nil || !txn.Txn().IsPessimistic() || !txn.Txn().IsRCIsolation() || txn.GetWorkspace() == nil {
		return moerr.NewInternalError(ctx, "protected database policy requires a pessimistic RC transaction")
	}
	frontier, err := protectedPolicyFrontier(ctx, ses)
	if err != nil {
		return err
	}
	if err = txn.GetWorkspace().AdvanceSnapshot(ctx, frontier); err != nil {
		return err
	}
	if !txn.SnapshotTS().Greater(frontier) {
		return moerr.NewInternalError(ctx, "protected database policy setter snapshot did not advance")
	}
	if _, err = waitProtectedPolicySnapshot(ctx, ses, txn.SnapshotTS().Prev()); err != nil {
		return err
	}
	sysCtx := defines.AttachAccountId(ctx, sysAccountID)
	bh.ClearExecResultSet()
	if err := bh.Exec(sysCtx, fmt.Sprintf("select account_id from mo_catalog.mo_account where account_id = %d for update", account)); err != nil {
		return err
	}
	rows, err := getResultSet(sysCtx, bh)
	if err != nil {
		return err
	}
	var count uint64
	for _, row := range rows {
		count += row.GetRowCount()
	}
	if count != 1 {
		return moerr.NewInternalError(ctx, "cannot lock account for protected database policy")
	}
	frontier, err = protectedPolicyFrontier(ctx, ses)
	if err != nil {
		return err
	}
	if err = txn.GetWorkspace().AdvanceSnapshot(ctx, frontier); err != nil {
		return err
	}
	if !txn.SnapshotTS().Greater(frontier) {
		return moerr.NewInternalError(ctx, "protected database policy setter snapshot did not advance")
	}
	if _, err = waitProtectedPolicySnapshot(ctx, ses, txn.SnapshotTS().Prev()); err != nil {
		return err
	}
	previous, err := readProtectedPolicy(ctx, ses, bh)
	if err != nil {
		return err
	}
	if len(protectedDatabaseSetFromString(ses, raw)) == 0 && len(protectedDatabaseSetFromString(ses, previous)) != 0 {
		return moerr.NewInternalError(ctx, "protected_databases cannot be cleared directly")
	}
	return nil
}
