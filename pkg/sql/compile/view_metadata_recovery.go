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

package compile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const viewRefreshStatusRunning = catalog.ViewRefreshStatusRunning
const viewRefreshStatusInvalid = catalog.ViewRefreshStatusInvalid
const viewRefreshStatusDiscovering = catalog.ViewRefreshStatusDiscovering

const viewMetadataRecoveryPageSize = 32
const viewMetadataRecoveryCallTimeout = 30 * time.Second

type viewMetadataRecoveryCommand struct {
	WorkerID   string `json:"worker_id"`
	Discover   bool   `json:"discover"`
	Revalidate bool   `json:"revalidate"`
}

// RequireViewMetadataRevalidation durably records that lifecycle DDL may be
// skipped while the cluster capability barrier is closed.
func RequireViewMetadataRevalidation(ctx context.Context, sqlExecutor executor.SQLExecutor) error {
	callCtx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancel()
	return sqlExecutor.ExecTxn(callCtx, func(txn executor.TxnExecutor) error {
		gate, err := txn.Exec(catalog.ViewMetadataLifecycleGateSQL, executor.StatementOption{})
		if err != nil {
			return err
		}
		gate.Close()

		inserted, err := txn.Exec(fmt.Sprintf(
			"insert into %s.%s (%s) select 0,0,0,0,'%s','%s',0,0,0,0,0,'','','','','%s','',0,null,0,1 "+
				"where not exists (select 1 from %s.%s where account_id=0 and target_relation_id=0 "+
				"and dependency_ordinal=0)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
			catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
			catalog.ViewRefreshStatusRevalidateRequired,
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES), executor.StatementOption{})
		if err != nil {
			return err
		}
		inserted.Close()
		markers, err := txn.Exec(fmt.Sprintf(
			"insert into %s.%s (%s) select a.account_id,0,0,0,'%s','%s',0,0,0,0,0,"+
				"'','','','','%s','',0,null,0,1 from %s.%s a where a.account_id<>0 and not exists "+
				"(select 1 from %s.%s d where d.account_id=a.account_id and d.target_relation_id=0 "+
				"and d.dependency_ordinal=0)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
			catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
			catalog.ViewRefreshStatusRevalidateRequired,
			catalog.MO_CATALOG, catalog.MOAccountTable,
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES), executor.StatementOption{})
		if err != nil {
			return err
		}
		markers.Close()
		result, err := txn.Exec(fmt.Sprintf(
			"update %s.%s set source_relation_kind='%s',dependency_generation=dependency_generation+1 "+
				"where target_relation_id=0 and dependency_ordinal=0 "+
				"and source_relation_kind in ('%s','%s')",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusRevalidateRequired,
			catalog.ViewRefreshStatusLegacyScan, catalog.ViewRefreshStatusRevalidateScan), executor.StatementOption{})
		if err != nil {
			return err
		}
		result.Close()
		return nil
	}, executor.Options{}.WithAccountID(catalog.System_Account))
}

// StartViewMetadataRevalidation starts one durable bounded pass over every
// CURRENT user View after the rolling-upgrade capability barrier reopens.
func StartViewMetadataRevalidation(ctx context.Context, sqlExecutor executor.SQLExecutor, workerID string) error {
	_ = workerID
	callCtx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
	defer cancel()
	return sqlExecutor.ExecTxn(callCtx, func(txn executor.TxnExecutor) error {
		gate, err := txn.Exec(catalog.ViewMetadataLifecycleGateSQL, executor.StatementOption{})
		if err != nil {
			return err
		}
		gate.Close()

		seeded, err := txn.Exec(fmt.Sprintf(
			"insert into %s.%s (%s) select a.account_id,0,0,0,'%s','%s',0,0,0,0,0,"+
				"'','','','','%s','',0,null,0,g.dependency_generation from %s.%s a join %s.%s g "+
				"on g.account_id=0 and g.target_relation_id=0 and g.dependency_ordinal=0 "+
				"and g.source_relation_kind='%s' where a.account_id<>0 and not exists "+
				"(select 1 from %s.%s d where d.account_id=a.account_id and d.target_relation_id=0 "+
				"and d.dependency_ordinal=0)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
			catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
			catalog.ViewRefreshStatusRevalidateRequired,
			catalog.MO_CATALOG, catalog.MOAccountTable,
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusRevalidateRequired,
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES), executor.StatementOption{})
		if err != nil {
			return err
		}
		seeded.Close()

		result, err := txn.Exec(fmt.Sprintf(
			"update %s.%s set source_account_id=0,source_database_name='',source_relation_name='',"+
				"source_relation_kind='%s',dependency_generation=dependency_generation+1 "+
				"where target_relation_id=0 and dependency_ordinal=0 and source_relation_kind='%s'",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusRevalidateScan,
			catalog.ViewRefreshStatusRevalidateRequired), executor.StatementOption{})
		if err != nil {
			return err
		}
		result.Close()
		return nil
	}, executor.Options{}.WithAccountID(catalog.System_Account))
}

// RunViewMetadataRecovery performs one bounded local-CN recovery tick. It is
// deliberately not a task-service executor: an older CN can never receive an
// executor code that it does not implement during a rolling upgrade.
func RunViewMetadataRecovery(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	workerID string,
) error {
	call := func(discover bool) (bool, error) {
		command, err := json.Marshal(viewMetadataRecoveryCommand{
			WorkerID: workerID,
			Discover: discover,
		})
		if err != nil {
			return false, err
		}
		callCtx, cancel := context.WithTimeout(ctx, viewMetadataRecoveryCallTimeout)
		defer cancel()
		result, err := sqlExecutor.Exec(callCtx, fmt.Sprintf(
			"select mo_ctl('CN','RefreshViewMetadata','%s')",
			sqlquote.EscapeString(string(command))),
			executor.Options{}.WithAccountID(catalog.System_Account))
		if err != nil {
			return false, err
		}
		processed := false
		result.ReadRows(func(rows int, columns []*vector.Vector) bool {
			if rows > 0 {
				var response struct {
					Result int `json:"result"`
				}
				if json.Unmarshal([]byte(columns[0].GetStringAt(0)), &response) == nil {
					processed = response.Result > 0
				}
			}
			return false
		})
		result.Close()
		return processed, nil
	}
	return runViewMetadataRecoveryPage(ctx,
		func() (bool, error) { return call(true) },
		func() (bool, error) { return call(false) })
}

func runViewMetadataRecoveryPage(
	ctx context.Context,
	discoverOne func() (bool, error),
	recoverOne func() (bool, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := discoverOne(); err != nil {
		return err
	}
	for range viewMetadataRecoveryPageSize - 1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		processed, err := recoverOne()
		if err != nil {
			return err
		}
		if !processed {
			return nil
		}
	}
	return nil
}

func init() {
	ctl.RegisterRefreshViewMetadataHandler("", recoverViewMetadataCommand)
}

func recoverViewMetadataCommand(proc *process.Process, parameter string) (int, error) {
	if !viewMetadataRefreshEnabled(proc.GetService()) {
		return 0, nil
	}
	if err := lockViewMetadataLifecycleGate(proc); err != nil {
		return 0, err
	}
	var command viewMetadataRecoveryCommand
	if err := json.Unmarshal([]byte(parameter), &command); err != nil {
		// Preserve compatibility with manually issued control calls.
		command.WorkerID = parameter
	}
	if command.Discover {
		return discoverLegacyViewMetadata(proc)
	}
	if command.Revalidate {
		return beginViewMetadataRevalidation(proc)
	}
	return recoverOnePendingViewMetadata(proc, command.WorkerID)
}

func beginViewMetadataRevalidation(proc *process.Process) (int, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return 0, moerr.NewInternalError(proc.Ctx, "internal SQL executor is unavailable")
	}
	result, err := v.(executor.SQLExecutor).Exec(proc.Ctx, fmt.Sprintf(
		"update %s.%s set source_account_id=0,source_database_name='',source_relation_name='',"+
			"source_relation_kind='%s',dependency_generation=dependency_generation+1 "+
			"where target_relation_id=0 and dependency_ordinal=0 and source_relation_kind='%s'",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
		catalog.ViewRefreshStatusRevalidateScan,
		catalog.ViewRefreshStatusRevalidateRequired), executor.Options{}.
		WithDisableIncrStatement().WithTxn(proc.GetTxnOperator()).WithAccountID(catalog.System_Account))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	return int(result.AffectedRows), nil
}

func lockViewMetadataLifecycleGate(proc *process.Process) error {
	if !viewMetadataRefreshEnabled(proc.GetService()) {
		return nil
	}
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "internal SQL executor is unavailable")
	}
	result, err := v.(executor.SQLExecutor).Exec(proc.Ctx, catalog.ViewMetadataLifecycleGateSQL,
		executor.Options{}.
			WithDisableIncrStatement().
			WithTxn(proc.GetTxnOperator()).
			WithAccountID(catalog.System_Account))
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

func discoverLegacyViewMetadata(proc *process.Process) (int, error) {
	if err := proc.Ctx.Err(); err != nil {
		return 0, err
	}
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return 0, moerr.NewInternalError(proc.Ctx, "internal SQL executor is unavailable")
	}
	sqlExecutor := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithAccountID(catalog.System_Account)
	return discoverLegacyViewPage(proc, sqlExecutor, opts)
}

type pendingViewRefresh struct {
	viewRefreshTarget
	leaseEpoch      uint64
	legacyDiscovery bool
	originalStatus  string
}

type legacyViewScanCursor struct {
	accountID    uint32
	databaseName string
	relationName string
	generation   uint64
	status       string
}

type legacyViewCandidate struct {
	accountID     uint32
	databaseID    uint64
	relationID    uint64
	logicalID     uint64
	databaseName  string
	relationName  string
	relationKind  string
	missingState  bool
	refreshStatus string
}

type recoveryCompilerContext struct {
	*compilerContext
	dependencies             []plan2.ViewDependency
	legacySubscriptions      map[string]*planpb.SubscriptionMeta
	legacySubscriptionLooked map[string]struct{}
	legacySnapshots          map[string]*plan2.Snapshot
}

func (c *recoveryCompilerContext) GetContext() context.Context { return c.compilerContext.ctx }

func (c *recoveryCompilerContext) GetAccountId() (uint32, error) {
	return defines.GetAccountId(c.compilerContext.ctx)
}

func (c *recoveryCompilerContext) withTopContext(ctx context.Context, fn func()) {
	original := c.proc.GetTopContext()
	c.proc.ReplaceTopCtx(ctx)
	defer c.proc.ReplaceTopCtx(original)
	fn()
}

func (c *recoveryCompilerContext) DatabaseExists(
	databaseName string,
	snapshot *plan2.Snapshot,
) bool {
	for _, dependency := range c.dependencies {
		bindingDatabase := dependency.BindingDatabaseName
		if bindingDatabase == "" {
			bindingDatabase = dependency.DatabaseName
			if dependency.SubscriptionName != "" {
				bindingDatabase = dependency.SubscriptionName
			}
		}
		if !viewBindingNameEqual(databaseName, bindingDatabase, dependency.LowerCaseTableNames) {
			continue
		}
		physicalContext := defines.AttachAccountId(c.GetContext(), dependency.AccountID)
		exists := false
		c.withTopContext(physicalContext, func() {
			exists = c.compilerContext.DatabaseExists(dependency.DatabaseName, snapshot)
		})
		return exists
	}
	exists := false
	c.withTopContext(c.GetContext(), func() {
		exists = c.compilerContext.DatabaseExists(databaseName, snapshot)
	})
	return exists
}

func (c *recoveryCompilerContext) Resolve(
	databaseName string,
	relationName string,
	snapshot *plan2.Snapshot,
) (*plan2.ObjectRef, *plan2.TableDef, error) {
	for _, dependency := range c.dependencies {
		bindingDatabase := dependency.BindingDatabaseName
		if bindingDatabase == "" {
			bindingDatabase = dependency.DatabaseName
			if dependency.SubscriptionName != "" {
				bindingDatabase = dependency.SubscriptionName
			}
		}
		bindingRelation := dependency.BindingRelationName
		if bindingRelation == "" {
			bindingRelation = dependency.RelationName
		}
		if !viewBindingNameEqual(databaseName, bindingDatabase, dependency.LowerCaseTableNames) ||
			!viewBindingNameEqual(relationName, bindingRelation, dependency.LowerCaseTableNames) {
			continue
		}
		if dependency.SubscriptionName != "" {
			subscription, subscriptionErr := c.GetSubscriptionMeta(bindingDatabase, snapshot)
			if subscriptionErr != nil {
				return nil, nil, subscriptionErr
			}
			if subscription == nil || uint32(subscription.AccountId) != dependency.AccountID ||
				!viewBindingNameEqual(subscription.DbName, dependency.DatabaseName, dependency.LowerCaseTableNames) ||
				!pubsub.InSubMetaTables(subscription, bindingRelation) {
				return nil, nil, &viewRefreshDependencyUnavailableError{cause: moerr.NewInternalErrorf(
					c.GetContext(), "subscription binding %q is unavailable", bindingDatabase)}
			}
		}
		physicalContext := defines.AttachAccountId(c.GetContext(), dependency.AccountID)
		var object *plan2.ObjectRef
		var tableDef *plan2.TableDef
		var err error
		c.withTopContext(physicalContext, func() {
			object, tableDef, err = c.compilerContext.Resolve(
				dependency.DatabaseName, dependency.RelationName, snapshot)
		})
		if err != nil {
			return nil, nil, err
		}
		if object == nil || tableDef == nil {
			return nil, nil, nil
		}
		object.SubscriptionName = dependency.SubscriptionName
		if dependency.SubscriptionName != "" {
			object.PubInfo = &planpb.PubInfo{TenantId: int32(dependency.AccountID)}
		}
		return object, tableDef, nil
	}
	subscription, err := c.GetSubscriptionMeta(databaseName, snapshot)
	if err != nil {
		return nil, nil, err
	}
	if subscription != nil {
		if !pubsub.InSubMetaTables(subscription, relationName) {
			return nil, nil, nil
		}
		physicalContext := defines.AttachAccountId(c.GetContext(), uint32(subscription.AccountId))
		var object *plan2.ObjectRef
		var tableDef *plan2.TableDef
		c.withTopContext(physicalContext, func() {
			object, tableDef, err = c.compilerContext.Resolve(subscription.DbName, relationName, snapshot)
		})
		if err != nil || object == nil || tableDef == nil {
			return object, tableDef, err
		}
		object.SubscriptionName = subscription.SubName
		object.PubInfo = &planpb.PubInfo{TenantId: subscription.AccountId}
		return object, tableDef, nil
	}
	var object *plan2.ObjectRef
	var tableDef *plan2.TableDef
	c.withTopContext(c.GetContext(), func() {
		object, tableDef, err = c.compilerContext.Resolve(databaseName, relationName, snapshot)
	})
	return object, tableDef, err
}

func (c *recoveryCompilerContext) GetSubscriptionMeta(
	databaseName string,
	_ *plan2.Snapshot,
) (*planpb.SubscriptionMeta, error) {
	key := databaseName
	if c.compilerContext.lower != 0 {
		key = strings.ToLower(databaseName)
	}
	if _, ok := c.legacySubscriptionLooked[key]; ok {
		return c.legacySubscriptions[key], nil
	}
	if c.legacySubscriptionLooked == nil {
		c.legacySubscriptionLooked = make(map[string]struct{})
	}
	c.legacySubscriptionLooked[key] = struct{}{}
	if c.legacySubscriptions == nil {
		c.legacySubscriptions = make(map[string]*planpb.SubscriptionMeta)
	}
	accountID, err := c.GetAccountId()
	if err != nil {
		return nil, err
	}
	result, err := c.execCatalogQuery(fmt.Sprintf(
		"select pub_account_id,pub_account_name,pub_name,pub_database,pub_tables "+
			"from %s.mo_subs where sub_account_id=%d and sub_name='%s' and status=0 limit 1",
		catalog.MO_CATALOG, accountID, sqlquote.EscapeString(databaseName)), catalog.System_Account)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows > 0 {
			c.legacySubscriptions[key] = &planpb.SubscriptionMeta{
				AccountId:   vector.MustFixedColNoTypeCheck[int32](columns[0])[0],
				AccountName: columns[1].GetStringAt(0), Name: columns[2].GetStringAt(0),
				DbName: columns[3].GetStringAt(0), SubName: databaseName,
				Tables: columns[4].GetStringAt(0),
			}
		}
		return false
	})
	return c.legacySubscriptions[key], nil
}

func (c *recoveryCompilerContext) ResolveById(
	relationID uint64,
	snapshot *plan2.Snapshot,
) (*plan2.ObjectRef, *plan2.TableDef, error) {
	for _, dependency := range c.dependencies {
		if dependency.RelationID == relationID {
			bindingDatabase := dependency.BindingDatabaseName
			if bindingDatabase == "" {
				bindingDatabase = dependency.DatabaseName
				if dependency.SubscriptionName != "" {
					bindingDatabase = dependency.SubscriptionName
				}
			}
			bindingRelation := dependency.BindingRelationName
			if bindingRelation == "" {
				bindingRelation = dependency.RelationName
			}
			return c.Resolve(bindingDatabase, bindingRelation, snapshot)
		}
	}
	var object *plan2.ObjectRef
	var tableDef *plan2.TableDef
	var err error
	c.withTopContext(c.GetContext(), func() {
		object, tableDef, err = c.compilerContext.ResolveById(relationID, snapshot)
	})
	return object, tableDef, err
}

func (c *recoveryCompilerContext) ResolveSnapshotWithSnapshotName(
	snapshotName string,
) (*plan2.Snapshot, error) {
	if snapshot := c.legacySnapshots[snapshotName]; snapshot != nil {
		return plan2.DeepCopySnapshot(snapshot), nil
	}
	accountID, err := c.GetAccountId()
	if err != nil {
		return nil, err
	}
	result, err := c.execCatalogQuery(fmt.Sprintf(
		"select ts,level,account_name,obj_id from %s.mo_snapshots "+
			"where sname='%s' and coalesce(kind,'')<>'branch' limit 1",
		catalog.MO_CATALOG, sqlquote.EscapeString(snapshotName)), accountID)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var snapshot *plan2.Snapshot
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		level := columns[1].GetStringAt(0)
		accountName := columns[2].GetStringAt(0)
		objectID := vector.MustFixedColNoTypeCheck[uint64](columns[3])[0]
		snapshotAccountID := uint32(0)
		if accountName != "" {
			snapshotAccountID = accountID
			if level == "account" {
				snapshotAccountID = uint32(objectID)
			}
		}
		snapshot = &plan2.Snapshot{
			TS: &timestamp.Timestamp{
				PhysicalTime: vector.MustFixedColNoTypeCheck[int64](columns[0])[0],
			},
			Tenant:    &planpb.SnapshotTenant{TenantName: accountName, TenantID: snapshotAccountID},
			ExtraInfo: &planpb.SnapshotExtraInfo{Level: level, ObjId: objectID, Name: snapshotName},
		}
		return false
	})
	if snapshot == nil {
		return nil, &viewRefreshDependencyUnavailableError{cause: moerr.NewInternalErrorf(
			c.GetContext(), "snapshot %q is unavailable", snapshotName)}
	}
	if c.legacySnapshots == nil {
		c.legacySnapshots = make(map[string]*plan2.Snapshot)
	}
	c.legacySnapshots[snapshotName] = snapshot
	return plan2.DeepCopySnapshot(snapshot), nil
}

func (c *recoveryCompilerContext) CheckTimeStampValid(ts int64) (bool, error) {
	for _, dependency := range c.dependencies {
		if dependency.Snapshot != nil && dependency.Snapshot.TS != nil &&
			dependency.Snapshot.TS.PhysicalTime == ts {
			return true, nil
		}
	}
	for _, snapshot := range c.legacySnapshots {
		if snapshot.TS != nil && snapshot.TS.PhysicalTime == ts {
			return true, nil
		}
	}
	return false, nil
}

func (c *recoveryCompilerContext) execCatalogQuery(
	query string,
	accountID uint32,
) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(c.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return executor.Result{}, moerr.NewInternalError(c.GetContext(), "internal SQL executor is unavailable")
	}
	return v.(executor.SQLExecutor).Exec(c.GetContext(), query,
		executor.Options{}.WithDisableIncrStatement().WithTxn(c.proc.GetTxnOperator()).
			WithAccountID(accountID))
}

func (c *recoveryCompilerContext) ResolveViewDependencyAccount(
	object *plan2.ObjectRef,
	tableDef *plan2.TableDef,
	_ *plan2.Snapshot,
) (uint32, error) {
	for _, dependency := range c.dependencies {
		if dependency.RelationID == tableDef.TblId ||
			(dependency.LogicalID != 0 && dependency.LogicalID == tableDef.LogicalId) ||
			(object != nil && viewBindingNameEqual(
				dependency.DatabaseName, object.SchemaName, dependency.LowerCaseTableNames) &&
				viewBindingNameEqual(
					dependency.RelationName, object.ObjName, dependency.LowerCaseTableNames)) {
			return dependency.AccountID, nil
		}
	}
	return c.GetAccountId()
}

// recoverOnePendingViewMetadata processes at most one View. The local CN worker
// invokes the command repeatedly, so every invocation owns one bounded
// transaction and one relation replacement.
func recoverOnePendingViewMetadata(proc *process.Process, workerID string) (int, error) {
	if err := proc.Ctx.Err(); err != nil {
		return 0, err
	}
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return 0, moerr.NewInternalError(proc.Ctx, "internal SQL executor is unavailable")
	}
	sqlExecutor := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithAccountID(catalog.System_Account)
	result, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"select account_id,target_database_id,target_relation_id,target_logical_id,"+
			"target_database_name,target_relation_name,target_generation,lease_epoch,status "+
			"from %s.%s where status in ('%s','%s') and (next_retry_at is null or next_retry_at<=now()) "+
			"order by next_retry_at,attempts,account_id,target_relation_id limit 1",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		viewRefreshStatusPending, viewRefreshStatusDiscovering), opts)
	if err != nil {
		return 0, err
	}
	defer result.Close()

	var pending *pendingViewRefresh
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		status := columns[8].GetStringAt(0)
		pending = &pendingViewRefresh{
			viewRefreshTarget: viewRefreshTarget{
				accountID:    vector.MustFixedColNoTypeCheck[uint32](columns[0])[0],
				databaseID:   vector.MustFixedColNoTypeCheck[uint64](columns[1])[0],
				relationID:   vector.MustFixedColNoTypeCheck[uint64](columns[2])[0],
				logicalID:    vector.MustFixedColNoTypeCheck[uint64](columns[3])[0],
				databaseName: columns[4].GetStringAt(0),
				relationName: columns[5].GetStringAt(0),
				generation:   vector.MustFixedColNoTypeCheck[uint64](columns[6])[0],
			},
			leaseEpoch:      vector.MustFixedColNoTypeCheck[uint64](columns[7])[0] + 1,
			legacyDiscovery: status == viewRefreshStatusDiscovering,
			originalStatus:  status,
		}
		return false
	})
	if pending == nil {
		return 0, nil
	}

	engineValue := proc.GetSessionInfo().StorageEngine
	if engineValue == nil {
		return 0, moerr.NewInternalError(proc.Ctx, "storage engine is unavailable")
	}
	runner := &Compile{proc: proc, e: engineValue, pn: &planpb.Plan{}}
	if err = runner.lockViewRefreshTarget(pending.viewRefreshTarget); err != nil {
		return 0, err
	}
	workerID = "'" + sqlquote.EscapeString(workerID) + "'"
	claim, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"update %s.%s set status='%s',lease_owner=%s,lease_epoch=%d,"+
			"lease_expires_at=date_add(now(),interval 60 second),attempts=attempts+1 "+
			"where account_id=%d and target_relation_id=%d and target_generation=%d "+
			"and lease_epoch=%d and status='%s'",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, viewRefreshStatusRunning, workerID,
		pending.leaseEpoch, pending.accountID, pending.relationID, pending.generation,
		pending.leaseEpoch-1, pending.originalStatus), opts)
	if err != nil {
		return 0, err
	}
	claimed := claim.AffectedRows
	claim.Close()
	if claimed != 1 {
		return 0, nil
	}

	wrote, refreshErr := refreshPendingView(proc, pending)
	if refreshErr == nil {
		return 1, nil
	}
	if wrote {
		// ReplaceDef, dependency edges, refresh state, and downstream enqueue
		// belong to one transaction. Never commit a failure marker after any of
		// those writes may have started.
		return 0, refreshErr
	}
	failure := classifyViewRefreshFailure(refreshErr)
	switch failure.disposition {
	case viewRefreshMarkInvalid:
		if err = updateViewRefreshFailure(proc, sqlExecutor, opts, pending,
			viewRefreshStatusInvalid, failure.code, false); err != nil {
			return 0, err
		}
		return 1, nil
	case viewRefreshRetry:
		if failure.code == viewRefreshFailureCanceled || failure.code == viewRefreshFailureTxnConflict {
			return 0, refreshErr
		}
		retryStatus := viewRefreshStatusPending
		if pending.legacyDiscovery {
			retryStatus = viewRefreshStatusDiscovering
		}
		if err = updateViewRefreshFailure(proc, sqlExecutor, opts, pending,
			retryStatus, failure.code, true); err != nil {
			return 0, err
		}
		return 1, nil
	default:
		return 0, refreshErr
	}
}

// discoverLegacyViewPage advances a durable keyset cursor over mo_tables'
// (account_id, reldatabase, relname) primary key. The cursor is a sentinel row
// in mo_view_dependencies with relation ID zero, which cannot collide with a catalog
// relation. One invocation scans and writes at most viewMetadataRecoveryPageSize
// catalog rows in one transaction.
func discoverLegacyViewPage(
	proc *process.Process,
	sqlExecutor executor.SQLExecutor,
	opts executor.Options,
) (int, error) {
	result, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"select source_account_id,source_database_name,source_relation_name,dependency_generation,"+
			"source_relation_kind from %s.%s where account_id=0 and target_relation_id=0 "+
			"and dependency_ordinal=0 for update",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES), opts)
	if err != nil {
		return 0, err
	}
	var cursor *legacyViewScanCursor
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return false
		}
		cursor = &legacyViewScanCursor{
			accountID:    vector.MustFixedColNoTypeCheck[uint32](columns[0])[0],
			databaseName: columns[1].GetStringAt(0),
			relationName: columns[2].GetStringAt(0),
			generation:   vector.MustFixedColNoTypeCheck[uint64](columns[3])[0],
			status:       columns[4].GetStringAt(0),
		}
		return false
	})
	result.Close()
	if cursor == nil {
		inserted, insertErr := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
			"insert into %s.%s (%s) values (0,0,0,0,'%s','%s',0,0,0,0,0,'','','','',"+
				"'%s','',0,null,0,1)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
			catalog.LegacyViewScanCursorDatabase, catalog.LegacyViewScanCursorRelation,
			catalog.ViewRefreshStatusLegacyScan), opts)
		if insertErr != nil {
			return 0, insertErr
		}
		inserted.Close()
		return 1, nil
	}
	if cursor.status != catalog.ViewRefreshStatusLegacyScan &&
		cursor.status != catalog.ViewRefreshStatusRevalidateScan {
		return 0, moerr.NewInternalErrorf(
			proc.Ctx, "invalid legacy View scan state %q", cursor.status)
	}

	escape := sqlquote.EscapeString
	page, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"select t.account_id,t.reldatabase_id,t.rel_id,t.rel_logical_id,t.reldatabase,t.relname,"+
			"t.relkind,r.target_relation_id,r.status from %s.%s t left join %s.%s r "+
			"on t.account_id=r.account_id and t.rel_id=r.target_relation_id where t.relkind='%s' "+
			"and t.reldatabase not in ('%s') and "+
			"(t.account_id>%d or (t.account_id=%d and t.reldatabase>'%s') or "+
			"(t.account_id=%d and t.reldatabase='%s' and t.relname>'%s')) "+
			"order by t.account_id,t.reldatabase,t.relname limit %d",
		catalog.MO_CATALOG, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		catalog.SystemViewRel, strings.Join(catalog.SystemDatabases, "','"),
		cursor.accountID, cursor.accountID, escape(cursor.databaseName),
		cursor.accountID, escape(cursor.databaseName), escape(cursor.relationName),
		viewMetadataRecoveryPageSize), opts)
	if err != nil {
		return 0, err
	}
	candidates := make([]legacyViewCandidate, 0, viewMetadataRecoveryPageSize)
	page.ReadRows(func(rows int, columns []*vector.Vector) bool {
		accounts := vector.MustFixedColNoTypeCheck[uint32](columns[0])
		databaseIDs := vector.MustFixedColNoTypeCheck[uint64](columns[1])
		relationIDs := vector.MustFixedColNoTypeCheck[uint64](columns[2])
		logicalIDs := vector.MustFixedColNoTypeCheck[uint64](columns[3])
		for row := range rows {
			refreshStatus := ""
			if !columns[8].IsNull(uint64(row)) {
				refreshStatus = columns[8].GetStringAt(row)
			}
			candidates = append(candidates, legacyViewCandidate{
				accountID: accounts[row], databaseID: databaseIDs[row], relationID: relationIDs[row],
				logicalID: logicalIDs[row], databaseName: columns[4].GetStringAt(row),
				relationName: columns[5].GetStringAt(row), relationKind: columns[6].GetStringAt(row),
				missingState: columns[7].IsNull(uint64(row)), refreshStatus: refreshStatus,
			})
		}
		return true
	})
	page.Close()

	for index := range candidates {
		candidate := &candidates[index]
		if candidate.relationKind != catalog.SystemViewRel {
			continue
		}
		if candidate.missingState {
			generation := uint64(proc.GetTxnOperator().SnapshotTS().PhysicalTime)
			if candidate.logicalID == 0 {
				candidate.logicalID = candidate.relationID
			}
			inserted, insertErr := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
				"insert into %s.%s (%s) values (%d,%d,%d,%d,'%s','%s',%d,0,'%s',"+
					"0,null,'',0,null,0)",
				catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
				candidate.accountID, candidate.databaseID, candidate.relationID, candidate.logicalID,
				escape(candidate.databaseName), escape(candidate.relationName), generation,
				viewRefreshStatusDiscovering), opts)
			if insertErr != nil {
				return 0, insertErr
			}
			inserted.Close()
			continue
		}
		if cursor.status == catalog.ViewRefreshStatusRevalidateScan &&
			candidate.refreshStatus == viewRefreshStatusCurrent {
			revalidated, updateErr := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
				"update %s.%s set target_generation=target_generation+1,status='%s',"+
					"failure_code=0,next_retry_at=null,lease_owner='',lease_epoch=lease_epoch+1,"+
					"lease_expires_at=null where account_id=%d and target_relation_id=%d and status='%s'",
				catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, viewRefreshStatusPending,
				candidate.accountID, candidate.relationID, viewRefreshStatusCurrent), opts)
			if updateErr != nil {
				return 0, updateErr
			}
			revalidated.Close()
		}
	}

	nextCursor, ok := nextLegacyViewScanCursor(*cursor, candidates)
	if !ok {
		return 0, moerr.NewInternalError(proc.Ctx, "legacy View scan generation overflow")
	}
	advanced, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"update %s.%s set source_account_id=%d,source_database_name='%s',source_relation_name='%s',"+
			"dependency_generation=%d,source_relation_kind='%s' where account_id=0 and target_relation_id=0 "+
			"and dependency_ordinal=0 and dependency_generation=%d and source_relation_kind='%s'",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, nextCursor.accountID,
		escape(nextCursor.databaseName), escape(nextCursor.relationName), nextCursor.generation,
		nextCursor.status,
		cursor.generation, cursor.status), opts)
	if err != nil {
		return 0, err
	}
	defer advanced.Close()
	if advanced.AffectedRows != 1 {
		return 0, moerr.NewTxnNeedRetryWithDefChanged(proc.Ctx)
	}
	if cursor.status == catalog.ViewRefreshStatusRevalidateScan && len(candidates) == 0 {
		completed, completeErr := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
			"update %s.%s set source_relation_kind='%s',dependency_generation=dependency_generation+1 "+
				"where account_id<>0 and target_relation_id=0 and dependency_ordinal=0 "+
				"and source_relation_kind='%s'",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
			catalog.ViewRefreshStatusLegacyScan, catalog.ViewRefreshStatusRevalidateScan), opts)
		if completeErr != nil {
			return 0, completeErr
		}
		completed.Close()
	}
	return 1, nil
}

func nextLegacyViewScanCursor(
	cursor legacyViewScanCursor,
	candidates []legacyViewCandidate,
) (legacyViewScanCursor, bool) {
	nextGeneration, ok := nextViewRefreshGeneration(cursor.generation, 0)
	if !ok {
		return legacyViewScanCursor{}, false
	}
	cursor.generation = nextGeneration
	if len(candidates) == 0 {
		// A completed pass wraps to the beginning. This makes discovery
		// continuous, so Views created by an older CN after a prior pass are
		// eventually captured without letting discovery monopolize one tick.
		cursor.accountID = 0
		cursor.databaseName = ""
		cursor.relationName = ""
		cursor.status = catalog.ViewRefreshStatusLegacyScan
		return cursor, true
	}
	last := candidates[len(candidates)-1]
	cursor.accountID = last.accountID
	cursor.databaseName = last.databaseName
	cursor.relationName = last.relationName
	return cursor, true
}

func refreshPendingView(proc *process.Process, pending *pendingViewRefresh) (bool, error) {
	engineValue := proc.GetSessionInfo().StorageEngine
	if engineValue == nil {
		return false, moerr.NewInternalError(proc.Ctx, "storage engine is unavailable")
	}
	runner := &Compile{proc: proc, e: engineValue, pn: &planpb.Plan{}}
	if err := runner.enqueueCurrentDependentViews(viewRelationMutation{
		accountID: pending.accountID, databaseID: pending.databaseID,
		relationID: pending.relationID, logicalID: pending.logicalID,
		databaseName: pending.databaseName, relationName: pending.relationName,
	}); err != nil {
		return false, err
	}
	originalTopContext := proc.GetTopContext()
	targetContext := defines.AttachAccountId(originalTopContext, pending.accountID)
	proc.ReplaceTopCtx(targetContext)
	defer proc.ReplaceTopCtx(originalTopContext)

	database, err := engineValue.Database(targetContext, pending.databaseName, proc.GetTxnOperator())
	if err != nil {
		return false, err
	}
	relation, err := database.Relation(targetContext, pending.relationName, nil)
	if err != nil {
		return false, err
	}
	if relation.GetTableID(targetContext) != pending.relationID {
		return false, &viewRefreshIdentityChangedError{
			cause: moerr.NewInternalErrorNoCtx("View relation identity changed"),
		}
	}
	currentDef := relation.CopyTableDef(targetContext)
	if currentDef == nil || currentDef.ViewSql == nil {
		return false, moerr.NewInternalError(targetContext, "target relation is not a View")
	}
	ownership, err := runner.loadViewCatalogOwnership(pending.accountID, pending.relationID)
	if err != nil {
		return false, err
	}
	regenerated, err := regenerateViewUsingPersistedEnvironment(
		proc, engineValue, targetContext, currentDef)
	if err != nil {
		return false, err
	}
	replacement := plan2.DeepCopyTableDef(currentDef, true)
	replacement.Cols = plan2.DeepCopyColDefList(regenerated.TableDef.Cols)
	replacement.ViewSql = regenerated.TableDef.ViewSql
	if err = relation.AlterTable(targetContext, nil, []*api.AlterTableReq{
		api.NewGuardedReplaceDefReq(
			pending.databaseID, pending.relationID, currentDef.Version,
			ownership.creator, ownership.owner, replacement),
	}); err != nil {
		return true, err
	}

	if err = runner.persistViewDependenciesWithContext(
		targetContext, database, pending.databaseName, replacement, pending.generation, false); err != nil {
		return true, err
	}
	return true, nil
}

func regenerateViewUsingPersistedEnvironment(
	proc *process.Process,
	engineValue engine.Engine,
	targetContext context.Context,
	currentDef *planpb.TableDef,
) (*plan2.RegeneratedViewDefinition, error) {
	originalTopContext := proc.GetTopContext()
	proc.ReplaceTopCtx(targetContext)
	defer proc.ReplaceTopCtx(originalTopContext)
	lower := int64(0)
	var persistedData plan2.ViewData
	if err := json.Unmarshal([]byte(currentDef.ViewSql.View), &persistedData); err != nil {
		return nil, err
	}
	if persistedData.LowerCaseTableNames != nil {
		lower = *persistedData.LowerCaseTableNames
	}
	compilerCtx := &recoveryCompilerContext{
		compilerContext: &compilerContext{
			ctx: targetContext, defaultDB: persistedData.DefaultDatabase,
			engine: engineValue, proc: proc, lower: lower,
		},
		dependencies: persistedData.Dependencies,
	}
	return plan2.RegenerateViewDefinition(compilerCtx, currentDef.ViewSql.View)
}

func (c *Compile) enqueueDependentViews(
	mutation viewRelationMutation,
	generation uint64,
	oldRelationID uint64,
	oldLogicalID uint64,
) error {
	return c.runSqlWithSystemTenant(fmt.Sprintf(
		"replace into %s.%s (%s) select distinct d.account_id,d.target_database_id,d.target_relation_id,"+
			"d.target_logical_id,d.target_database_name,d.target_relation_name,"+
			"coalesce(r.target_generation+1,%d),coalesce(r.completed_generation,0),'%s',"+
			"0,null,'',coalesce(r.lease_epoch,0)+1,null,coalesce(r.attempts,0) "+
			"from %s.%s d left join %s.%s r on d.account_id=r.account_id "+
			"and d.target_relation_id=r.target_relation_id where %s",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
		generation, viewRefreshStatusPending,
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		viewDependencyMutationPredicate(mutation, oldRelationID, oldLogicalID),
	))
}

func (c *Compile) enqueueCurrentDependentViews(mutation viewRelationMutation) error {
	return c.runSqlWithSystemTenant(fmt.Sprintf(
		"replace into %s.%s (%s) select distinct d.account_id,d.target_database_id,d.target_relation_id,"+
			"d.target_logical_id,d.target_database_name,d.target_relation_name,"+
			"coalesce(r.target_generation+1,d.dependency_generation),"+
			"coalesce(r.completed_generation,0),'%s',0,null,'',coalesce(r.lease_epoch,0)+1,null,"+
			"coalesce(r.attempts,0) from %s.%s d left join %s.%s r on d.account_id=r.account_id "+
			"and d.target_relation_id=r.target_relation_id where %s and "+
			"(r.target_relation_id is null or r.status='%s')",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
		viewRefreshStatusPending,
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		viewDependencyMutationPredicate(mutation, 0, 0), viewRefreshStatusCurrent))
}

func (c *Compile) enqueueViewsAfterDatabaseRemoval(
	accountID uint32,
	databaseID uint64,
	generation uint64,
) error {
	if (c.proc.GetSessionInfo().IsRestore && !restoreInvalidatesViewMetadata(c.proc.Ctx)) ||
		!viewMetadataRefreshEnabled(c.proc.GetService()) {
		return nil
	}
	return c.enqueueDependentViewClosure(fmt.Sprintf(
		"d.source_account_id=%d and d.source_database_id=%d", accountID, databaseID), generation)
}

func updateViewRefreshFailure(
	proc *process.Process,
	sqlExecutor executor.SQLExecutor,
	opts executor.Options,
	pending *pendingViewRefresh,
	status string,
	code viewRefreshFailureCode,
	retry bool,
) error {
	nextRetry := "null"
	if retry {
		nextRetry = fmt.Sprintf("date_add(now(),interval %d second)",
			min(300, 1<<min(pending.leaseEpoch, 8)))
	}
	result, err := sqlExecutor.Exec(proc.Ctx, fmt.Sprintf(
		"update %s.%s set status='%s',failure_code=%d,next_retry_at=%s,"+
			"lease_owner='',lease_expires_at=null where account_id=%d and target_relation_id=%d "+
			"and target_generation=%d and lease_epoch=%d",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, status, code, nextRetry,
		pending.accountID, pending.relationID, pending.generation, pending.leaseEpoch), opts)
	if err == nil {
		result.Close()
	}
	return err
}
