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

package compile

import (
	"context"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type lifecycleDatabaseName struct {
	accountID uint32
	name      string
	mode      lock.LockMode
}

// admitBroadTableLifecycleRC enters the broad gate before COPY ALTER or
// TRUNCATE takes table locks. Re-resolve after the applied frontier: the
// relation read before admission cannot establish its current identity.
func (c *Compile) admitBroadTableLifecycleRC(database, table string, expectedID uint64) (engine.Database, engine.Relation, error) {
	ctx := c.proc.Ctx
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err = c.admitLifecycleRC([]lifecycleDatabaseName{{accountID: accountID, name: database, mode: lock.LockMode_Shared}}, true); err != nil {
		return nil, nil, err
	}
	db, err := c.e.Database(ctx, database, c.proc.GetTxnOperator())
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			return nil, nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		return nil, nil, err
	}
	rel, err := db.Relation(ctx, table, nil)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		return nil, nil, err
	}
	if rel.GetTableID(ctx) != expectedID {
		return nil, nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return db, rel, nil
}

// admitLifecycleRC takes the stable registry identity, the SNAPSHOT key, and
// complete database-name domain before the caller reads mutable catalog facts.
// All locks remain owned by the caller's transaction until its terminal path.
func (c *Compile) admitLifecycleRC(names []lifecycleDatabaseName, exclusiveSnapshotGate bool) error {
	ctx := c.proc.Ctx
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil || !txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		return moerr.NewInternalError(ctx, "lifecycle admission requires pessimistic RC")
	}
	for _, name := range names {
		if name.name == "" || (name.mode != lock.LockMode_Shared && name.mode != lock.LockMode_Exclusive) {
			return moerr.NewInternalError(ctx, "invalid lifecycle database lock")
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].accountID != names[j].accountID {
			return names[i].accountID < names[j].accountID
		}
		if names[i].name != names[j].name {
			return names[i].name < names[j].name
		}
		return names[i].mode == lock.LockMode_Exclusive && names[j].mode != lock.LockMode_Exclusive
	})
	systemCtx := context.WithValue(
		defines.AttachAccountId(ctx, catalog.System_Account),
		defines.LockWriterFairKey{}, false,
	)
	if err := c.lockLifecycleIdentityRC(); err != nil {
		return err
	}
	systemDB, err := c.e.Database(systemCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	registry, err := systemDB.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return err
	}
	registryKeys := batch.NewWithSize(1)
	registryKeys.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	if err = vector.AppendBytes(registryKeys.Vecs[0],
		[]byte(catalog.SnapshotLifecycleFeatureCode), false, c.proc.Mp()); err != nil {
		registryKeys.Vecs[0].Free(c.proc.Mp())
		return err
	}
	gateMode := lock.LockMode_Shared
	var waitPolicy []lock.WaitPolicy
	if exclusiveSnapshotGate {
		gateMode = lock.LockMode_Exclusive
		// An existing holder can create an upgrade wait cycle with its
		// retained database/component keys. Fresh owners can wait for readers
		// instead of failing ordinary concurrent DDL or DML.
		if txnOp.HasLockTable(registry.GetTableID(systemCtx)) {
			waitPolicy = []lock.WaitPolicy{lock.WaitPolicy_FastFail}
		}
	}
	_, err = lockop.LockRowsForAdmissionWithContext(systemCtx, c.e, c.proc,
		registry.GetTableID(systemCtx), registryKeys, 0, *registryKeys.Vecs[0].GetType(),
		gateMode, catalog.System_Account, waitPolicy...)
	registryKeys.Vecs[0].Free(c.proc.Mp())
	if err != nil {
		return err
	}

	if len(names) > 0 {
		dbRel, err := systemDB.Relation(systemCtx, catalog.MO_DATABASE, nil)
		if err != nil {
			return err
		}
		for i, name := range names {
			if i > 0 && name.accountID == names[i-1].accountID && name.name == names[i-1].name {
				continue
			}
			keys, err := getLockBatch(c.proc, name.accountID, []string{name.name})
			if err != nil {
				return err
			}
			_, err = lockop.LockRowsForAdmissionWithContext(ctx, c.e, c.proc,
				dbRel.GetTableID(systemCtx), keys, 0, *keys.Vecs[0].GetType(),
				name.mode, name.accountID)
			keys.Vecs[0].Free(c.proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	if err := c.advanceLifecycleAdmissionSnapshot(); err != nil {
		return err
	}
	// C excludes a concurrent replacement, but the transaction's earlier
	// snapshot can still resolve the retired registry after C was granted.
	currentDB, err := c.e.Database(systemCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	currentRegistry, err := currentDB.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return err
	}
	if currentRegistry.GetTableID(systemCtx) != registry.GetTableID(systemCtx) {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return c.verifyLifecycleRegistryRow()
}

// lockLifecycleIdentityRC pins the registry's physical relation before any
// caller resolves or locks its row. Broad owners also use this before G X.
func (c *Compile) lockLifecycleIdentityRC() error {
	ctx := context.WithValue(
		defines.AttachAccountId(c.proc.Ctx, catalog.System_Account),
		defines.LockWriterFairKey{}, false,
	)
	db, err := c.e.Database(ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := db.Relation(ctx, catalog.MO_TABLES, nil)
	if err != nil {
		return err
	}
	keys, err := getLockBatch(c.proc, catalog.System_Account,
		[]string{catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY})
	if err != nil {
		return err
	}
	defer keys.Vecs[0].Free(c.proc.Mp())
	_, err = lockop.LockRowsForAdmissionWithContext(ctx, c.e, c.proc,
		relation.GetTableID(ctx), keys, 0, *keys.Vecs[0].GetType(),
		lock.LockMode_Shared, catalog.System_Account)
	return err
}

func (c *Compile) advanceLifecycleAdmissionSnapshot() error {
	barrier, ok := getLogtailReadBarrier(c.e)
	if !ok {
		return moerr.NewInternalError(c.proc.Ctx, "lifecycle logtail barrier is unavailable")
	}
	frontier, err := barrier.AcquireLogtailReadBarrier(c.proc.Ctx)
	if err != nil {
		return err
	}
	workspace := c.proc.GetTxnOperator().GetWorkspace()
	if workspace == nil {
		return moerr.NewInternalError(c.proc.Ctx, "missing lifecycle transaction workspace")
	}
	return workspace.AdvanceSnapshot(c.proc.Ctx, frontier)
}

func (c *Compile) verifyLifecycleRegistryRow() error {
	res, err := c.runSqlWithResult(
		"select feature_code from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT'",
		int32(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer res.Close()
	count := 0
	res.ReadRows(func(rows int, _ []*vector.Vector) bool {
		count += rows
		return true
	})
	if count != 1 {
		return moerr.NewInternalError(c.proc.Ctx, "missing or duplicate SNAPSHOT lifecycle registry row")
	}
	return nil
}

// Scalar replacement consumes no external constraints or owned descendants.
// Inspect live constraints as well as the planner-facing definition.
func scalarReplacementShape(ctx context.Context, rel engine.Relation) (bool, error) {
	def := rel.GetTableDef(ctx)
	if def == nil || def.TableType != catalog.SystemOrdinaryRel || def.IsTemporary ||
		def.Partition != nil || len(def.Indexes) != 0 || len(def.Fkeys) != 0 || len(def.RefChildTbls) != 0 {
		return false, nil
	}
	extra := rel.GetExtraInfo()
	if extra == nil || extra.FeatureFlag != 0 || extra.ParentTableID != 0 || len(extra.IndexTables) != 0 {
		return false, nil
	}
	ct, err := GetConstraintDef(ctx, rel)
	if err != nil {
		return false, err
	}
	for _, constraint := range ct.Cts {
		switch value := constraint.(type) {
		case *engine.ForeignKeyDef:
			if len(value.Fkeys) != 0 {
				return false, nil
			}
		case *engine.RefChildTableDef:
			if len(value.Tables) != 0 {
				return false, nil
			}
		case *engine.IndexDef:
			if len(value.Indexes) != 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

// These nonlocking reads overlay the caller's workspace. G/D/K/T ownership and
// the applied frontier make the final negative result authoritative.
func (c *Compile) scalarReplacementProtected(database, table string, physicalID, logicalID uint64) (bool, error) {
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return false, err
	}
	accountName, err := c.lifecycleAccountName()
	if err != nil {
		return false, err
	}
	dbName, tableName := sqlquote.String(database), sqlquote.String(table)
	fkSQL := fmt.Sprintf("select 1 from mo_catalog.mo_foreign_keys where (db_name=%s and table_name=%s) or (refer_db_name=%s and refer_table_name=%s) limit 1", dbName, tableName, dbName, tableName)
	fk, err := alterDataBranchHistoricalSourceExists(func(sql string) (executor.Result, error) {
		return c.runSqlWithResultAndOptions(sql, int32(accountID), executor.StatementOption{}.WithDisableLog())
	}, []string{fkSQL})
	if err != nil || fk {
		return fk, err
	}
	// User snapshots are owned by the actor catalog. SYS also contains
	// cross-account history and the reserved branch protection snapshots.
	if accountID != catalog.System_Account {
		protected, err := alterDataBranchHistoricalSourceExists(func(sql string) (executor.Result, error) {
			return c.runSqlWithResultAndOptions(sql, int32(accountID), executor.StatementOption{}.WithDisableLog())
		}, []string{alterDataBranchHistoricalSnapshotSourceProbeSQL(accountName, database, table, physicalID, false, logicalID)})
		if err != nil || protected {
			return protected, err
		}
	}
	sqls := []string{
		alterDataBranchParticipationSQL(physicalID),
		alterDataBranchHistoricalSnapshotSourceProbeSQL(accountName, database, table, physicalID, false, logicalID),
		alterDataBranchHistoricalPitrSourceProbeSQL(accountName, database, table, physicalID, false, logicalID),
		fmt.Sprintf("select 1 from mo_catalog.mo_snapshots where kind='branch' and sname in (%s,%s) limit 1", sqlquote.String(fmt.Sprintf("__mo_branch_%d", physicalID)), sqlquote.String(fmt.Sprintf("__mo_branch_%d", logicalID))),
	}
	return alterDataBranchHistoricalSourceExists(func(sql string) (executor.Result, error) {
		return c.runSqlWithResultAndOptions(sql, int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog())
	}, sqls)
}

func (c *Compile) retryBroadReplacement() error {
	if err := c.admitLifecycleRC(nil, true); err != nil {
		return err
	}
	return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
}

func (c *Compile) admitScalarReplacementRC(database, table string, original engine.Relation) (engine.Database, engine.Relation, *dropLifecycleAdmission, error) {
	ctx := c.proc.Ctx
	eligible, err := scalarReplacementShape(ctx, original)
	if err != nil || !eligible {
		return nil, nil, nil, err
	}
	id := original.GetTableID(ctx)
	logicalID := plan2.SnapshotTableID(original.GetTableDef(ctx))
	protected, err := c.scalarReplacementProtected(database, table, id, logicalID)
	if err != nil || protected {
		return nil, nil, nil, err
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := c.admitLifecycleRC([]lifecycleDatabaseName{{accountID: accountID, name: database, mode: lock.LockMode_Shared}}, false); err != nil {
		return nil, nil, nil, err
	}
	dag, err := c.loadBranchReclaimComponentsRC([]uint64{id})
	if err != nil {
		return nil, nil, nil, err
	}
	if len(dag.Info) != 0 {
		return nil, nil, nil, c.retryBroadReplacement()
	}
	catalogRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return nil, nil, nil, err
	}
	keys, err := getLockBatch(c.proc, accountID, []string{database, table})
	if err != nil {
		return nil, nil, nil, err
	}
	_, err = lockop.LockRowsForAdmissionWithContext(ctx, c.e, c.proc,
		catalogRel.GetTableID(ctx), keys, 0, *keys.Vecs[0].GetType(), lock.LockMode_Exclusive, accountID)
	keys.Vecs[0].Free(c.proc.Mp())
	if err != nil {
		return nil, nil, nil, err
	}
	// Nested DROP borrows this destructive lock, including its definition-change fence.
	if err := lockTable(ctx, c.e, c.proc, original, database, true); err != nil {
		return nil, nil, nil, err
	}
	if err := c.advanceLifecycleAdmissionSnapshot(); err != nil {
		return nil, nil, nil, err
	}
	db, err := c.e.Database(ctx, database, c.proc.GetTxnOperator())
	if err != nil {
		return nil, nil, nil, err
	}
	rel, err := db.Relation(ctx, table, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	eligible, err = scalarReplacementShape(ctx, rel)
	if err != nil {
		return nil, nil, nil, err
	}
	if !eligible || rel.GetTableID(ctx) != id || rel.GetDBID(ctx) != original.GetDBID(ctx) || plan2.SnapshotTableID(rel.GetTableDef(ctx)) != logicalID {
		return nil, nil, nil, c.retryBroadReplacement()
	}
	protected, err = c.scalarReplacementProtected(database, table, id, logicalID)
	if err != nil {
		return nil, nil, nil, err
	}
	if protected {
		return nil, nil, nil, c.retryBroadReplacement()
	}
	admission := &dropLifecycleAdmission{rootID: id, accountID: accountID,
		root: dropLifecycleIdentity{database: database, table: table, databaseID: rel.GetDBID(ctx), logicalID: logicalID}}
	return db, rel, admission, nil
}

func (c *Compile) lifecycleAccountName() (string, error) {
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return "", err
	}
	account, err := c.runSqlWithResultAndOptions(
		fmt.Sprintf("select account_name from mo_catalog.mo_account where account_id=%d", accountID),
		int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog())
	if err != nil {
		account.Close()
		return "", err
	}
	accountName := ""
	account.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 0 {
			accountName = executor.GetStringRows(cols[0])[0]
		}
		return false
	})
	account.Close()
	if accountName == "" {
		return "", moerr.NewInternalError(c.proc.Ctx, "missing lifecycle account identity")
	}
	return accountName, nil
}
