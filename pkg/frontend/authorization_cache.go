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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// These dependencies cover principal identity, all privilege derivations, and
// catalog object/name binding. Proofs retain scalar versions, never storage trees.
const authorizationDependencyCount = 8

type authorizationPrincipal struct {
	account, user, role             uint32
	accountName, userName, roleName string
	secondary                       bool
}

type authorizationCertificate struct {
	principal    authorizationPrincipal
	dependencies [authorizationDependencyCount]engine.TableContentDependency
	versions     [authorizationDependencyCount]engine.TableContentVersion
	reusableFrom timestamp.Timestamp
}

func currentAuthorizationPrincipal(ses *Session) authorizationPrincipal {
	tenant := ses.GetTenantInfo()
	tenant.mu.Lock()
	defer tenant.mu.Unlock()
	return authorizationPrincipal{account: tenant.TenantID, user: tenant.UserID, role: tenant.DefaultRoleID, accountName: tenant.Tenant, userName: tenant.User, roleName: tenant.DefaultRole, secondary: tenant.useAllSecondaryRole}
}

// authorizationReadSnapshot belongs to the existing execution context. Earlier
// native checks and old user SI transactions acquire the same visibility as a
// fresh RC statement, without opening a user transaction or workspace.
func authorizationReadSnapshot(ctx context.Context, ses *Session) (timestamp.Timestamp, error) {
	if err := ctx.Err(); err != nil {
		return timestamp.Timestamp{}, err
	}
	reader, ok := getPu(ses.GetService()).TxnClient.(client.ReadSnapshotClient)
	if !ok {
		return timestamp.Timestamp{}, nil
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
	if execution != nil && !execution.authorizationSnapshot.IsEmpty() {
		return execution.authorizationSnapshot, nil
	}
	snapshot, err := reader.ReadSnapshot(ctx, ses.getLastCommitTS())
	if err == nil && execution != nil {
		execution.authorizationSnapshot = snapshot
	}
	return snapshot, err
}

func newAuthorizationBackgroundExec(ctx context.Context, ses *Session) (BackgroundExec, timestamp.Timestamp, error) {
	snapshot, err := authorizationReadSnapshot(ctx, ses)
	if err != nil {
		return nil, snapshot, err
	}
	return ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true, readSnapshot: snapshot}), snapshot, nil
}

// One warm capture proves the fixed authorization view. The TN serial pre-WAL
// owner runs timestamp-consuming txnDB.PrePrepare before binding each catalog
// mutation's prepare time, so no later mutation can occupy appliedWatermark.Next().
// Later publications cannot change SQL at S; cold SQL still needs both captures
// because it may add new grant facts while loading catalog metadata.
func (proof *authorizationCertificate) valid(ctx context.Context, ses *Session, snapshot timestamp.Timestamp, cache *privilegeCache) bool {
	if proof.reusableFrom.IsEmpty() || snapshot.Less(proof.reusableFrom) || proof.principal != currentAuthorizationPrincipal(ses) {
		return false
	}
	reader, ok := getPu(ses.GetService()).StorageEngine.(engine.TableContentReader)
	if !ok {
		return false
	}
	if cache == nil {
		return false
	}
	// The existing session cache owns this fixed scratch buffer. A local array
	// passed through the engine interface would escape on every warm validation.
	return reader.ReadTableContentVersions(ctx, snapshot, proof.dependencies[:], cache.observedVersions[:]) && cache.observedVersions == proof.versions
}

// prepareAuthorizationCertificate runs only on a miss, in the independent
// read-only transaction already owned by authorization. A missing dependency
// disables reuse; the existing SQL evaluator remains responsible for admission.
func prepareAuthorizationCertificate(ctx context.Context, ses *Session, bh BackgroundExec, snapshot timestamp.Timestamp) (authorizationCertificate, bool, error) {
	var proof authorizationCertificate
	if snapshot.IsEmpty() {
		return proof, false, nil
	}
	txnSource, ok := bh.(interface{ GetTxnOperator() TxnOperator })
	if !ok {
		return proof, false, nil
	}
	op := txnSource.GetTxnOperator()
	if op == nil || !op.SnapshotTS().Equal(snapshot) || op.Txn().Isolation != pbtxn.TxnIsolation_SI || !op.TxnOptions().ReadOnly() {
		return proof, false, nil
	}
	storage := getPu(ses.GetService()).StorageEngine
	reader, ok := storage.(engine.TableContentReader)
	if !ok {
		return proof, false, nil
	}
	proof.principal = currentAuthorizationPrincipal(ses)
	proof.reusableFrom = snapshot
	names := [authorizationDependencyCount]string{catalog.MO_USER, "mo_user_grant", "mo_role", "mo_role_grant", "mo_role_privs", "mo_account", catalog.MO_DATABASE, catalog.MO_TABLES}
	for i, name := range names {
		account := proof.principal.account
		if i >= 5 {
			account = 0
		}
		tableCtx := defines.AttachAccountId(ctx, account)
		db, err := storage.Database(tableCtx, catalog.MO_CATALOG, op)
		if err != nil {
			return authorizationCertificate{}, false, ctx.Err()
		}
		dbID, err := strconv.ParseUint(db.GetDatabaseId(tableCtx), 10, 64)
		if err != nil {
			return authorizationCertificate{}, false, err
		}
		relation, err := db.Relation(tableCtx, name, nil)
		if err != nil || relation == nil {
			return authorizationCertificate{}, false, ctx.Err()
		}
		tableID := relation.GetTableID(tableCtx)
		if err := storage.TryToSubscribeTable(tableCtx, uint64(account), dbID, tableID, catalog.MO_CATALOG, name); err != nil {
			return authorizationCertificate{}, false, ctx.Err()
		}
		proof.dependencies[i] = engine.TableContentDependency{DatabaseID: dbID, TableID: tableID}
	}
	if !reader.ReadTableContentVersions(ctx, snapshot, proof.dependencies[:], proof.versions[:]) {
		return authorizationCertificate{}, false, nil
	}

	return proof, true, nil
}

// Login retirement versions remain owned by AccountRoutineManager. Internal
// sessions have no login record; real retained connections must match it.
func validateAuthorizationAccount(ctx context.Context, ses *Session, bh BackgroundExec) error {
	principal := currentAuthorizationPrincipal(ses)
	if principal.account != sysAccountID {
		manager, routine := ses.getRoutineManager(), ses.getRoutine()
		if manager == nil || manager.accountRoutine == nil || routine == nil {
			// Internal sessions have no authenticated connection retirement record.
			return nil
		}
		owner := manager.accountRoutine
		owner.accountRoutineMu.RLock()
		version, recorded := owner.accountId2Routine[int64(principal.account)][routine]
		owner.accountRoutineMu.RUnlock()
		if !recorded {
			return moerr.NewInternalError(ctx, "do not have privilege: authenticated account no longer active")
		}
		sysCtx := defines.AttachAccountId(ctx, sysAccountID)
		query := fmt.Sprintf("select version from mo_catalog.mo_account where account_id = %d and account_name = %s", principal.account, quoteSQLStringLiteral(principal.accountName))
		bh.ClearExecResultSet()
		if err := bh.Exec(sysCtx, query); err != nil {
			return err
		}
		rows, err := getResultSet(sysCtx, bh)
		if err != nil {
			return err
		}
		if !execResultArrayHasData(rows) {
			return moerr.NewInternalError(ctx, "do not have privilege: authenticated account no longer matches current catalog")
		}
		current, err := rows[0].GetUint64(sysCtx, 0, 0)
		if err != nil {
			return err
		}
		if current != version {
			return moerr.NewInternalError(ctx, "do not have privilege: authenticated account version changed")
		}
	}
	return nil
}
