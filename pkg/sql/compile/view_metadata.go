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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const viewRefreshStatusCurrent = catalog.ViewRefreshStatusCurrent

const viewRefreshStatusPending = catalog.ViewRefreshStatusPending

type viewRelationMutation struct {
	accountID    uint32
	databaseID   uint64
	relationID   uint64
	logicalID    uint64
	databaseName string
	relationName string
}

type viewRefreshTarget struct {
	accountID    uint32
	databaseID   uint64
	relationID   uint64
	logicalID    uint64
	databaseName string
	relationName string
	generation   uint64
}

type viewCatalogOwnership struct {
	creator     uint32
	owner       uint32
	createdTime types.Timestamp
}

type persistedViewTarget struct {
	accountID  uint32
	databaseID uint64
	relationID uint64
	logicalID  uint64
}

type viewRefreshIdentityKey struct {
	accountID  uint32
	databaseID uint64
	logicalID  uint64
}

var viewMetadataRefreshEnabled = func(string) bool {
	return false
}

// ViewMetadataRefreshEnabled is intentionally false in this inactive lifecycle
// layer. A later prerequisite owns the durable membership epoch and admission
// fence; only the subsequent activation layer may replace this gate.
func ViewMetadataRefreshEnabled(string) bool {
	return false
}

// viewMetadataRefreshAvailable closes the capability-disabled window before a
// lifecycle DDL is allowed to skip incremental maintenance. The marker writes
// use the caller's DDL transaction, so a fast false-to-true capability change
// cannot make an untracked mutation visible without a durable revalidation.
func (c *Compile) viewMetadataRefreshAvailable() (bool, error) {
	if viewMetadataRefreshEnabled(c.proc.GetService()) {
		return true, nil
	}
	if err := c.requireViewMetadataRevalidationInTxn(); err != nil {
		// During an offset upgrade the SQL listener can become available before
		// the lifecycle tables. Preserve the pre-feature behavior only for this
		// typed catalog-readiness condition; every other failure aborts the DDL.
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
			moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return false, nil
		}
		return false, err
	}
	return false, nil
}

func (c *Compile) requireViewMetadataRevalidationInTxn() error {
	for _, statement := range viewMetadataRequireRevalidationSQL() {
		if err := c.runSqlWithSystemTenant(statement); err != nil {
			return err
		}
	}
	return nil
}

func restoreInvalidatesViewMetadata(ctx context.Context) bool {
	level, ok := ctx.Value(tree.CloneLevelCtxKey{}).(tree.CloneLevelType)
	return ok && (level == tree.RestoreCloneLevelTable || level == tree.RestoreCloneLevelDatabase)
}

func (c *Compile) persistViewDependencies(
	database engine.Database,
	databaseName string,
	viewDef *planpb.TableDef,
) error {
	if needSkipDbs[databaseName] || c.proc.GetSessionInfo().IsRestore {
		return nil
	}
	available, err := c.viewMetadataRefreshAvailable()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	return c.persistViewDependenciesWithContext(
		c.proc.Ctx,
		database,
		databaseName,
		viewDef,
		uint64(c.proc.GetTxnOperator().SnapshotTS().PhysicalTime),
		true,
	)
}

func (c *Compile) persistViewDependenciesWithContext(
	ctx context.Context,
	database engine.Database,
	databaseName string,
	viewDef *planpb.TableDef,
	generation uint64,
	createState bool,
) error {
	target, err := c.persistViewDependencyEdgesWithContext(
		ctx, database, databaseName, viewDef, generation)
	if err != nil {
		return err
	}
	escape := sqlquote.EscapeString
	if err = c.runSqlWithSystemTenant(fmt.Sprintf(
		"delete from %s.%s where account_id=%d and target_database_name='%s' and "+
			"target_relation_name='%s' and target_relation_id<>%d",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, target.accountID,
		escape(databaseName), escape(viewDef.Name), target.relationID,
	)); err != nil {
		return err
	}

	if createState {
		return c.runSqlWithSystemTenant(fmt.Sprintf(
			"replace into %s.%s (%s) values (%d,%d,%d,%d,'%s','%s',%d,%d,'%s',0,null,'',0,null,0)",
			catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
			target.accountID, target.databaseID, target.relationID, target.logicalID,
			escape(databaseName), escape(viewDef.Name), generation, generation, viewRefreshStatusCurrent,
		))
	}
	result, err := c.runSqlWithResult(fmt.Sprintf(
		"update %s.%s set target_database_id=%d,target_logical_id=%d,target_database_name='%s',"+
			"target_relation_name='%s',completed_generation=%d,status='%s',failure_code=0,"+
			"next_retry_at=null,lease_owner='',lease_expires_at=null where account_id=%d "+
			"and target_relation_id=%d and target_generation=%d",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		target.databaseID, target.logicalID, escape(databaseName), escape(viewDef.Name), generation,
		viewRefreshStatusCurrent, target.accountID, target.relationID, generation,
	), int32(catalog.System_Account))
	if err != nil {
		return err
	}
	defer result.Close()
	if result.AffectedRows != 1 {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return nil
}

func (c *Compile) persistViewDependencyEdgesWithContext(
	ctx context.Context,
	database engine.Database,
	databaseName string,
	viewDef *planpb.TableDef,
	generation uint64,
) (persistedViewTarget, error) {
	if viewDef == nil || viewDef.ViewSql == nil {
		return persistedViewTarget{}, moerr.NewInternalError(ctx, "missing persisted View definition")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return persistedViewTarget{}, err
	}
	relation, err := database.Relation(ctx, viewDef.Name, nil)
	if err != nil {
		return persistedViewTarget{}, err
	}
	targetRelationID := relation.GetTableID(ctx)
	targetDef := relation.GetTableDef(ctx)
	targetLogicalID := targetDef.GetLogicalId()
	if targetLogicalID == 0 {
		targetLogicalID = targetRelationID
	}
	targetDatabaseID, err := strconv.ParseUint(database.GetDatabaseId(ctx), 10, 64)
	if err != nil {
		return persistedViewTarget{}, err
	}
	target := persistedViewTarget{
		accountID: accountID, databaseID: targetDatabaseID,
		relationID: targetRelationID, logicalID: targetLogicalID,
	}

	var data plan2.ViewData
	if err = json.Unmarshal([]byte(viewDef.ViewSql.View), &data); err != nil {
		return persistedViewTarget{}, err
	}
	escape := sqlquote.EscapeString
	if err = c.runSqlWithSystemTenant(fmt.Sprintf(
		"delete from %s.%s where account_id = %d and target_database_name = '%s' and target_relation_name = '%s'",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, accountID,
		escape(databaseName), escape(viewDef.Name),
	)); err != nil {
		return persistedViewTarget{}, err
	}
	for ordinal, dependency := range data.Dependencies {
		if err = ctx.Err(); err != nil {
			return persistedViewTarget{}, err
		}
		snapshotData, marshalErr := json.Marshal(dependency.Snapshot)
		if marshalErr != nil {
			return persistedViewTarget{}, marshalErr
		}
		databaseNameKey := viewDependencyNameKey(
			dependency.DatabaseName, dependency.LowerCaseTableNames)
		relationNameKey := viewDependencyNameKey(
			dependency.RelationName, dependency.LowerCaseTableNames)
		if err = c.runSqlWithSystemTenant(fmt.Sprintf(
			"insert into %s.%s (%s) values (%d,%d,%d,%d,'%s','%s',%d,%d,%d,%d,%d,'%s','%s','%s','%s','%s','%s',%d,'%s',%d,%d)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, catalog.MoViewDependenciesColumns,
			accountID, targetDatabaseID, targetRelationID, targetLogicalID,
			escape(databaseName), escape(viewDef.Name), ordinal,
			dependency.AccountID, dependency.DatabaseID, dependency.RelationID, dependency.LogicalID,
			escape(dependency.DatabaseName), escape(dependency.RelationName),
			databaseNameKey, relationNameKey,
			escape(dependency.RelationKind), escape(dependency.SubscriptionName),
			dependency.PublisherAccount, escape(string(snapshotData)),
			dependency.LowerCaseTableNames, generation,
		)); err != nil {
			return persistedViewTarget{}, err
		}
	}
	return target, nil
}

func (c *Compile) refreshViewsAfterRelationMutation(
	databaseName string,
	relationName string,
	oldRelationID uint64,
	oldLogicalID uint64,
) error {
	if needSkipDbs[databaseName] {
		return nil
	}
	if c.proc.GetSessionInfo().IsRestore {
		// Table/database restore invalidates at the relation-removal boundary,
		// where the old physical and logical identities are still available.
		// Repeating invalidation after recreation would advance the generation
		// twice. Account/cluster restore rebuilds the lifecycle tables themselves.
		return nil
	}
	available, err := c.viewMetadataRefreshAvailable()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	database, err := c.e.Database(c.proc.Ctx, databaseName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := database.Relation(c.proc.Ctx, relationName, nil)
	if err != nil {
		return err
	}
	currentDef := relation.GetTableDef(c.proc.Ctx)
	databaseID, err := strconv.ParseUint(database.GetDatabaseId(c.proc.Ctx), 10, 64)
	if err != nil {
		return err
	}
	mutation := viewRelationMutation{
		accountID:    accountID,
		databaseID:   databaseID,
		relationID:   relation.GetTableID(c.proc.Ctx),
		logicalID:    currentDef.GetLogicalId(),
		databaseName: databaseName,
		relationName: relationName,
	}
	if mutation.logicalID == 0 {
		mutation.logicalID = oldLogicalID
	}

	generation := uint64(c.proc.GetTxnOperator().SnapshotTS().PhysicalTime)
	if err = c.enqueueDependentViewClosure(
		viewDependencyMutationPredicate(mutation, oldRelationID, oldLogicalID), generation); err != nil {
		return err
	}
	if viewMetadataSynchronousRefreshBudget == 0 {
		// Rebinding inside COPY/metadata DDL makes source success depend on View
		// ordering and multiplies catalog replacement work in one transaction.
		// The complete closure is already durable and fail-closed; recovery owns
		// every authoritative regeneration after the source transaction commits.
		return nil
	}
	queue := []viewRelationMutation{mutation}
	processedGenerations := make(map[[2]uint64]uint64)
	remainingSynchronous := viewMetadataSynchronousRefreshBudget
	for len(queue) > 0 {
		if err = c.proc.Ctx.Err(); err != nil {
			return err
		}
		current := queue[0]
		queue = queue[1:]
		targets, loadErr := c.loadDependentViews(current, oldRelationID, oldLogicalID)
		if loadErr != nil {
			return loadErr
		}
		synchronousTargets := synchronousViewRefreshCount(len(targets), remainingSynchronous)
		for index := 0; index < synchronousTargets; index++ {
			if err = c.lockViewRefreshTarget(targets[index]); err != nil {
				return err
			}
		}
		for index := range targets {
			key := [2]uint64{uint64(targets[index].accountID), targets[index].logicalID}
			if targets[index].logicalID == 0 {
				key[1] = targets[index].relationID
			}
			nextGeneration, ok := nextViewRefreshGeneration(
				targets[index].generation, processedGenerations[key])
			if !ok {
				return moerr.NewInternalError(c.proc.Ctx, "View refresh generation overflow")
			}
			targets[index].generation = nextGeneration
		}
		oldRelationID, oldLogicalID = 0, 0
		if remainingSynchronous == 0 {
			// Every View already refreshed synchronously must still publish its
			// downstream invalidation. The remaining queue contains only those
			// refreshed Views, so walking it is bounded by the synchronous budget
			// and performs no additional relation replacement.
			continue
		}
		for _, target := range targets[:synchronousTargets] {
			key := [2]uint64{uint64(target.accountID), target.logicalID}
			if target.logicalID == 0 {
				key[1] = target.relationID
			}
			// The budget bounds authoritative rebind attempts, not only successful
			// replacements. A target deferred because another dependency is absent
			// must not leave capacity for an unbounded downstream walk.
			remainingSynchronous--
			if err = c.refreshOneView(target, current); err != nil {
				failure := classifyViewRefreshFailure(err)
				switch failure.disposition {
				case viewRefreshMarkInvalid:
					if err = c.markSynchronousViewRefreshInvalid(target, failure.code); err != nil {
						return err
					}
				case viewRefreshRetry:
					if failure.code == viewRefreshFailureCanceled ||
						failure.code == viewRefreshFailureTxnConflict {
						return err
					}
					// The reverse-closure invalidation already made the target durable PENDING.
					// A failed rebind must not make source DDL success depend on
					// target ordering or the synchronous budget.
				default:
					return err
				}
			}
			processedGenerations[key] = target.generation
			queue = append(queue, viewRelationMutation{
				accountID: target.accountID, databaseID: target.databaseID,
				relationID: target.relationID, logicalID: target.logicalID,
				databaseName: target.databaseName, relationName: target.relationName,
			})
		}
	}
	return nil
}

func (c *Compile) markSynchronousViewRefreshInvalid(
	target viewRefreshTarget,
	code viewRefreshFailureCode,
) error {
	return c.runSqlWithSystemTenant(fmt.Sprintf(
		"update %s.%s set status='%s',failure_code=%d,next_retry_at=null,"+
			"lease_owner='',lease_expires_at=null where account_id=%d and target_relation_id=%d "+
			"and target_generation=%d",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, viewRefreshStatusInvalid, code,
		target.accountID, target.relationID, target.generation,
	))
}

func synchronousViewRefreshCount(targets, remaining int) int {
	if targets <= 0 || remaining <= 0 {
		return 0
	}
	return min(targets, remaining)
}

func (c *Compile) lockViewRefreshTarget(target viewRefreshTarget) error {
	originalContext := c.proc.Ctx
	originalTopContext := c.proc.GetTopContext()
	targetContext := defines.AttachAccountId(originalContext, target.accountID)
	c.proc.Ctx = targetContext
	c.proc.ReplaceTopCtx(targetContext)
	defer func() {
		c.proc.Ctx = originalContext
		c.proc.ReplaceTopCtx(originalTopContext)
	}()
	return lockMoTable(c, target.databaseName, target.relationName, lock.LockMode_Exclusive)
}

func nextViewRefreshGeneration(observed, processed uint64) (uint64, bool) {
	if processed > observed {
		observed = processed
	}
	if observed == ^uint64(0) {
		return 0, false
	}
	return observed + 1, true
}

func (c *Compile) enqueueViewsAfterRelationRemoval(
	databaseName string,
	relationName string,
	databaseID uint64,
	relationID uint64,
	logicalID uint64,
) error {
	if needSkipDbs[databaseName] ||
		(c.proc.GetSessionInfo().IsRestore && !restoreInvalidatesViewMetadata(c.proc.Ctx)) {
		return nil
	}
	available, err := c.viewMetadataRefreshAvailable()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	if logicalID == 0 {
		logicalID = relationID
	}
	mutation := viewRelationMutation{
		accountID: accountID, databaseID: databaseID,
		relationID: relationID, logicalID: logicalID,
		databaseName: databaseName, relationName: relationName,
	}
	return c.enqueueDependentViewClosure(
		viewDependencyMutationPredicate(mutation, 0, 0),
		uint64(c.proc.GetTxnOperator().SnapshotTS().PhysicalTime),
		viewRefreshKeyForMutation(mutation))
}

func (c *Compile) deleteDroppedViewMetadata(databaseName string, relationID uint64) error {
	if needSkipDbs[databaseName] ||
		(c.proc.GetSessionInfo().IsRestore && !restoreInvalidatesViewMetadata(c.proc.Ctx)) {
		return nil
	}
	available, err := c.viewMetadataRefreshAvailable()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	// Recovery and catalog cleanup must acquire locks in the same order. In
	// particular, never take a range lock on mo_view_refresh before the stable
	// lifecycle gate: recovery owns the gate while claiming a refresh row.
	if err := lockViewMetadataLifecycleGate(c.proc); err != nil {
		return err
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	for _, tableName := range []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH} {
		if err = c.runSqlWithSystemTenant(fmt.Sprintf(
			"delete from %s.%s where account_id=%d and target_relation_id=%d",
			catalog.MO_CATALOG, tableName, accountID, relationID)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) deleteDroppedDatabaseViewMetadata(
	accountID uint32,
	databaseID uint64,
	databaseName string,
) error {
	if needSkipDbs[databaseName] ||
		(c.proc.GetSessionInfo().IsRestore && !restoreInvalidatesViewMetadata(c.proc.Ctx)) {
		return nil
	}
	available, err := c.viewMetadataRefreshAvailable()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	if err := lockViewMetadataLifecycleGate(c.proc); err != nil {
		return err
	}
	for _, tableName := range []string{catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH} {
		if err := c.runSqlWithSystemTenant(fmt.Sprintf(
			"delete from %s.%s where account_id=%d and "+
				"(target_database_id=%d or target_database_name='%s')",
			catalog.MO_CATALOG, tableName, accountID, databaseID,
			sqlquote.EscapeString(databaseName))); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) loadDependentViews(
	mutation viewRelationMutation,
	oldRelationID uint64,
	oldLogicalID uint64,
) ([]viewRefreshTarget, error) {
	query := fmt.Sprintf(
		"select d.account_id,d.target_database_id,d.target_relation_id,d.target_logical_id,"+
			"d.target_database_name,d.target_relation_name,max(r.target_generation) from %s.%s d "+
			"join %s.%s r on d.account_id=r.account_id and d.target_relation_id=r.target_relation_id "+
			"where %s "+
			"group by d.account_id,d.target_database_id,d.target_relation_id,d.target_logical_id,"+
			"d.target_database_name,d.target_relation_name "+
			"order by d.account_id,d.target_database_id,d.target_relation_id limit %d",
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		viewDependencyMutationPredicate(mutation, oldRelationID, oldLogicalID),
		viewMetadataSynchronousRefreshBudget+1,
	)
	result, err := c.runSqlWithResult(query, int32(catalog.System_Account))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	targets := make([]viewRefreshTarget, 0)
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		accounts := vector.MustFixedColNoTypeCheck[uint32](columns[0])
		databaseIDs := vector.MustFixedColNoTypeCheck[uint64](columns[1])
		relationIDs := vector.MustFixedColNoTypeCheck[uint64](columns[2])
		logicalIDs := vector.MustFixedColNoTypeCheck[uint64](columns[3])
		generations := vector.MustFixedColNoTypeCheck[uint64](columns[6])
		for row := range rows {
			targets = append(targets, viewRefreshTarget{
				accountID: accounts[row], databaseID: databaseIDs[row],
				relationID: relationIDs[row], logicalID: logicalIDs[row],
				databaseName: columns[4].GetStringAt(row),
				relationName: columns[5].GetStringAt(row),
				generation:   generations[row],
			})
		}
		return true
	})
	return targets, nil
}

func viewDependencyNameKey(name string, lowerCaseTableNames int64) string {
	if lowerCaseTableNames != 0 {
		name = strings.ToLower(name)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
}

func viewBindingNameEqual(left, right string, lowerCaseTableNames int64) bool {
	if lowerCaseTableNames == 0 {
		return left == right
	}
	return strings.EqualFold(left, right)
}

func viewDependencyMutationPredicate(
	mutation viewRelationMutation,
	oldRelationID uint64,
	oldLogicalID uint64,
) string {
	databaseNameKeyExact := viewDependencyNameKey(mutation.databaseName, 0)
	relationNameKeyExact := viewDependencyNameKey(mutation.relationName, 0)
	databaseNameKeyFolded := viewDependencyNameKey(mutation.databaseName, 1)
	relationNameKeyFolded := viewDependencyNameKey(mutation.relationName, 1)
	return fmt.Sprintf(
		"d.source_account_id=%d and ("+
			"(d.source_database_id=%d and d.source_relation_id<>0 and d.source_relation_id in (%d,%d)) or "+
			"(d.source_database_id=%d and d.source_logical_id<>0 and d.source_logical_id in (%d,%d)) or "+
			"((d.lower_case_table_names=0 and d.source_database_name_key='%s' and d.source_relation_name_key='%s') or "+
			"(d.lower_case_table_names<>0 and d.source_database_name_key='%s' and d.source_relation_name_key='%s')))",
		mutation.accountID, mutation.databaseID, mutation.relationID, oldRelationID,
		mutation.databaseID, mutation.logicalID, oldLogicalID,
		databaseNameKeyExact, relationNameKeyExact, databaseNameKeyFolded, relationNameKeyFolded,
	)
}

func viewRefreshKeyForMutation(mutation viewRelationMutation) viewRefreshIdentityKey {
	logicalID := mutation.logicalID
	if logicalID == 0 {
		logicalID = mutation.relationID
	}
	return viewRefreshIdentityKey{
		accountID: mutation.accountID, databaseID: mutation.databaseID, logicalID: logicalID,
	}
}

// enqueueDependentViewClosure invalidates the complete reverse dependency DAG
// without requiring any View in that DAG to rebind successfully. The queries
// follow exact persisted dependency identities, so work is proportional to the
// affected graph rather than the size of mo_tables.
func (c *Compile) enqueueDependentViewClosure(
	initialPredicate string,
	generation uint64,
	seed ...viewRefreshIdentityKey,
) error {
	if err := c.proc.Ctx.Err(); err != nil {
		return err
	}
	oldCtx := c.proc.Ctx
	c.proc.Ctx = process.WithSystemCTELimits(
		context.WithValue(oldCtx, defines.TenantIDKey{}, uint32(catalog.System_Account)))
	defer func() { c.proc.Ctx = oldCtx }()
	for offset := 0; ; offset += viewMetadataClosureWritePageSize {
		result, err := c.runSqlWithResultAndOptions(
			viewMetadataClosureInvalidationPageSQL(
				initialPredicate, generation, offset, viewMetadataClosureWritePageSize, seed...),
			int32(catalog.System_Account), executor.StatementOption{}.WithDisableLog())
		if err != nil {
			return err
		}
		affected := result.AffectedRows
		result.Close()
		if affected < viewMetadataClosureWritePageSize {
			return nil
		}
	}
}

func viewMetadataClosureInvalidationSQL(
	initialPredicate string,
	generation uint64,
	seed ...viewRefreshIdentityKey,
) string {
	return viewMetadataClosureInvalidationPageSQL(initialPredicate, generation, -1, 0, seed...)
}

func viewMetadataClosureInvalidationPageSQL(
	initialPredicate string,
	generation uint64,
	offset int,
	pageSize int,
	seed ...viewRefreshIdentityKey,
) string {
	exclusions := make([]string, 0, len(seed))
	for _, key := range seed {
		exclusions = append(exclusions, fmt.Sprintf(
			"(a.account_id=%d and a.target_database_id=%d and "+
				"coalesce(nullif(a.target_logical_id,0),a.target_relation_id)=%d)",
			key.accountID, key.databaseID, key.logicalID))
	}
	finalPredicate := "true"
	if len(exclusions) > 0 {
		finalPredicate = "not (" + strings.Join(exclusions, " or ") + ")"
	}
	pageClause := ""
	if offset >= 0 && pageSize > 0 {
		pageClause = fmt.Sprintf(
			" order by a.account_id,a.target_relation_id limit %d offset %d", pageSize, offset)
	}
	return fmt.Sprintf(
		"replace into %s.%s (%s) with recursive affected "+
			"(account_id,target_database_id,target_relation_id,target_logical_id,"+
			"target_database_name,target_relation_name) as ("+
			"select d.account_id,d.target_database_id,d.target_relation_id,"+
			"d.target_logical_id,d.target_database_name,d.target_relation_name from %s.%s d "+
			"where d.target_relation_id<>0 and (%s) "+
			"union select d.account_id,d.target_database_id,d.target_relation_id,"+
			"d.target_logical_id,d.target_database_name,d.target_relation_name from %s.%s d "+
			"join affected a on d.target_relation_id<>0 and d.source_account_id=a.account_id and ("+
			"(d.source_database_id=a.target_database_id and d.source_relation_id<>0 "+
			"and d.source_relation_id=a.target_relation_id) or "+
			"(d.source_database_id=a.target_database_id and d.source_logical_id<>0 "+
			"and d.source_logical_id=coalesce(nullif(a.target_logical_id,0),a.target_relation_id)) or "+
			"(d.lower_case_table_names=0 and d.source_database_name=a.target_database_name "+
			"and d.source_relation_name=a.target_relation_name) or "+
			"(d.lower_case_table_names<>0 and lower(d.source_database_name)=lower(a.target_database_name) "+
			"and lower(d.source_relation_name)=lower(a.target_relation_name)))) "+
			"select a.account_id,a.target_database_id,a.target_relation_id,a.target_logical_id,"+
			"a.target_database_name,a.target_relation_name,coalesce(r.target_generation+1,%d),"+
			"coalesce(r.completed_generation,0),'%s',0,null,'',coalesce(r.lease_epoch,0)+1,null,"+
			"coalesce(r.attempts,0) from affected a left join %s.%s r on "+
			"a.account_id=r.account_id and a.target_relation_id=r.target_relation_id where %s%s",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, initialPredicate,
		catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES,
		generation, viewRefreshStatusPending, catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
		finalPredicate, pageClause)
}

// SnapshotViewMetadataInvalidationSQL returns one atomic reverse-closure
// invalidation statement for Views bound to the exact persisted snapshot. A
// timestamp-only binding is invalidated only when this is the last snapshot
// retaining that timestamp.
func SnapshotViewMetadataInvalidationSQL(
	snapshotData string,
	snapshotTS int64,
	invalidateTimestampBindings bool,
	generation uint64,
) string {
	predicate := fmt.Sprintf("d.snapshot_data='%s'", sqlquote.EscapeString(snapshotData))
	if invalidateTimestampBindings {
		predicate += fmt.Sprintf(" or (json_extract(d.snapshot_data,'$.ExtraInfo.Name') is null and "+
			"cast(json_unquote(json_extract(d.snapshot_data,'$.TS.PhysicalTime')) as bigint)=%d)", snapshotTS)
	}
	return viewMetadataClosureInvalidationSQL(predicate, generation)
}

// PublicationViewMetadataInvalidationSQL invalidates subscription Views bound
// to a publication's physical publisher database. This deliberately includes
// sibling publications of the same database: over-invalidation is safe and
// avoids depending on mutable publication names which are not relation identity.
func PublicationViewMetadataInvalidationSQL(
	publisherAccountID uint32,
	publisherDatabaseID uint64,
	generation uint64,
) string {
	return viewMetadataClosureInvalidationSQL(fmt.Sprintf(
		"d.publisher_account_id=%d and d.source_account_id=%d%s",
		publisherAccountID, publisherAccountID, func() string {
			if publisherDatabaseID == 0 {
				return ""
			}
			return fmt.Sprintf(" and d.source_database_id=%d", publisherDatabaseID)
		}()), generation)
}

// AccountViewMetadataInvalidationSQL invalidates the complete reverse closure
// of Views bound to relations owned by an account being restored.
func AccountViewMetadataInvalidationSQL(sourceAccountID uint32, generation uint64) string {
	return viewMetadataClosureInvalidationSQL(fmt.Sprintf(
		"d.source_account_id=%d", sourceAccountID), generation)
}

// SubscriptionViewMetadataInvalidationSQL invalidates Views owned by one
// subscriber which bind through the exact subscription database name.
func SubscriptionViewMetadataInvalidationSQL(
	subscriberAccountID uint32,
	subscriptionName string,
	generation uint64,
) string {
	return viewMetadataClosureInvalidationSQL(fmt.Sprintf(
		"d.account_id=%d and d.subscription_name='%s'",
		subscriberAccountID, sqlquote.EscapeString(subscriptionName)), generation)
}

// SeedMissingViewMetadataSQL makes restored user Views fail closed before the
// restore transaction commits. Recovery subsequently regenerates them through
// the same authoritative View schema path.
func SeedMissingViewMetadataSQL(generation uint64) string {
	return fmt.Sprintf(
		"insert into %s.%s (%s) select t.account_id,t.reldatabase_id,t.rel_id,"+
			"coalesce(nullif(t.rel_logical_id,0),t.rel_id),t.reldatabase,t.relname,%d,0,'%s',"+
			"0,null,'',0,null,0 from %s.%s t left join %s.%s r on "+
			"t.account_id=r.account_id and t.rel_id=r.target_relation_id where t.relkind='%s' "+
			"and t.reldatabase not in ('%s') and r.target_relation_id is null",
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
		generation, catalog.ViewRefreshStatusDiscovering,
		catalog.MO_CATALOG, catalog.MO_TABLES,
		catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.SystemViewRel,
		strings.Join(catalog.SystemDatabases, "','"))
}

// ReconcileAccountViewMetadataSQL removes lifecycle rows whose target View no
// longer exists after an account restore, then seeds restored Views that have
// no refresh row. Sentinel rows are intentionally preserved.
func ReconcileAccountViewMetadataSQL(accountID uint32, generation uint64) []string {
	existingView := fmt.Sprintf(
		"select 1 from %s.%s t where t.account_id=%d and t.rel_id=target_relation_id and t.relkind='%s'",
		catalog.MO_CATALOG, catalog.MO_TABLES, accountID, catalog.SystemViewRel)
	return []string{
		fmt.Sprintf("delete from %s.%s where account_id=%d and target_relation_id<>0 and not exists (%s)",
			catalog.MO_CATALOG, catalog.MO_VIEW_DEPENDENCIES, accountID, existingView),
		fmt.Sprintf("delete from %s.%s where account_id=%d and target_relation_id<>0 and not exists (%s)",
			catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, accountID, existingView),
		fmt.Sprintf(
			"insert into %s.%s (%s) select t.account_id,t.reldatabase_id,t.rel_id,"+
				"coalesce(nullif(t.rel_logical_id,0),t.rel_id),t.reldatabase,t.relname,%d,0,'%s',"+
				"0,null,'',0,null,0 from %s.%s t left join %s.%s r on "+
				"t.account_id=r.account_id and t.rel_id=r.target_relation_id where t.account_id=%d "+
				"and t.relkind='%s' and t.reldatabase not in ('%s') and r.target_relation_id is null",
			catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH, catalog.MoViewRefreshColumns,
			generation, catalog.ViewRefreshStatusDiscovering,
			catalog.MO_CATALOG, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_VIEW_REFRESH,
			accountID, catalog.SystemViewRel, strings.Join(catalog.SystemDatabases, "','")),
	}
}

func (c *Compile) refreshOneView(
	target viewRefreshTarget,
	source viewRelationMutation,
) error {
	targetContext := defines.AttachAccountId(c.proc.Ctx, target.accountID)
	database, err := c.e.Database(targetContext, target.databaseName, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := database.Relation(targetContext, target.relationName, nil)
	if err != nil {
		return err
	}
	if relation.GetTableID(targetContext) != target.relationID {
		return moerr.NewInternalErrorf(
			targetContext, "View identity changed while refreshing %s.%s",
			target.databaseName, target.relationName)
	}
	currentDef := relation.CopyTableDef(targetContext)
	if currentDef == nil || currentDef.ViewSql == nil {
		return moerr.NewInternalErrorf(
			targetContext, "relation %s.%s is no longer a View",
			target.databaseName, target.relationName)
	}
	ownership, err := c.loadViewCatalogOwnership(target.accountID, target.relationID)
	if err != nil {
		return err
	}
	regenerated, err := regenerateViewUsingPersistedEnvironment(
		c.proc, c.e, targetContext, currentDef)
	if err != nil {
		return err
	}
	if source.logicalID != 0 {
		dependencies := append([]plan2.ViewDependency(nil), regenerated.Dependencies...)
		changed := false
		for index := range dependencies {
			dependency := &dependencies[index]
			if dependency.LogicalID != 0 || dependency.AccountID != source.accountID {
				continue
			}
			// Dependency membership was established by the authoritative rebind.
			// Names are used only to correlate that result with the mutation event
			// when COPY has changed the physical relation ID in this transaction.
			matchesPhysicalID := dependency.DatabaseID == source.databaseID &&
				dependency.RelationID == source.relationID
			matchesQualifiedName := viewBindingNameEqual(
				dependency.DatabaseName, source.databaseName, dependency.LowerCaseTableNames) &&
				viewBindingNameEqual(
					dependency.RelationName, source.relationName, dependency.LowerCaseTableNames)
			if matchesPhysicalID || matchesQualifiedName {
				dependency.LogicalID = source.logicalID
				changed = true
			}
		}
		if changed {
			if err = plan2.ReplaceRegeneratedViewDependencies(regenerated, dependencies); err != nil {
				return err
			}
		}
	}
	replacement := plan2.DeepCopyTableDef(currentDef, true)
	replacement.Cols = plan2.DeepCopyColDefList(regenerated.TableDef.Cols)
	replacement.ViewSql = regenerated.TableDef.ViewSql
	if err = relation.AlterTable(targetContext, nil, []*api.AlterTableReq{
		api.NewGuardedReplaceDefReq(
			target.databaseID, target.relationID, currentDef.Version,
			ownership.creator, ownership.owner, int64(ownership.createdTime), replacement),
	}); err != nil {
		return err
	}
	return c.persistViewDependenciesWithContext(
		targetContext, database, target.databaseName, replacement, target.generation, false)
}

func (c *Compile) loadViewCatalogOwnership(
	accountID uint32,
	relationID uint64,
) (viewCatalogOwnership, error) {
	result, err := c.runSqlWithResult(fmt.Sprintf(
		"select creator,owner,created_time from %s.%s where account_id=%d and rel_id=%d and relkind='%s' limit 1",
		catalog.MO_CATALOG, catalog.MO_TABLES, accountID, relationID, catalog.SystemViewRel,
	), int32(catalog.System_Account))
	if err != nil {
		return viewCatalogOwnership{}, err
	}
	defer result.Close()
	var ownership viewCatalogOwnership
	found := false
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows > 0 {
			ownership.creator = vector.MustFixedColNoTypeCheck[uint32](columns[0])[0]
			ownership.owner = vector.MustFixedColNoTypeCheck[uint32](columns[1])[0]
			ownership.createdTime = vector.MustFixedColNoTypeCheck[types.Timestamp](columns[2])[0]
			found = true
		}
		return false
	})
	if !found {
		return viewCatalogOwnership{}, &viewRefreshIdentityChangedError{
			cause: moerr.NewInternalErrorNoCtx("View relation identity changed"),
		}
	}
	return ownership, nil
}
