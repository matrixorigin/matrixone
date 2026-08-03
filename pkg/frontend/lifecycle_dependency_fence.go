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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// ignoreMissingLifecycleCatalog keeps ordinary management operations usable
// while a tenant's asynchronous upgrade has not created Lifecycle tables yet.
// Absence means that no Lifecycle state can exist; all other errors remain
// fail-closed. Lifecycle commands themselves do not use this helper.
func ignoreMissingLifecycleCatalog(err error) error {
	if err == nil || moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
		return nil
	}
	return err
}

// lockLifecycleFeatureConfiguration serializes SET LIFECYCLE with changes to
// the Lifecycle release gate and deployment certification. It is deliberately
// not used by Snapshot, PITR, Clone, Branch, Publication, ordinary DDL, DML,
// query, or Merge paths.
func lockLifecycleFeatureConfiguration(
	ctx context.Context,
	background BackgroundExec,
) error {
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	background.ClearExecResultSet()
	return background.Exec(
		systemCtx,
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'",
	)
}

// lifecycleArchiveRestoreScope describes one source or target scope checked by
// Snapshot/PITR restore. snapshotTS == 0 means the current Catalog. Database
// and table restore checks are Archive-only. Direct Account restore also
// rejects TTL Bindings because restoring the tenant Lifecycle Catalog would
// reactivate a Binding that refers to the old physical table identity.
type lifecycleArchiveRestoreScope struct {
	level             tree.RestoreLevel
	accountID         uint32
	databaseName      string
	tableName         string
	snapshotTS        int64
	rejectTTLBindings bool
}

type lifecycleArchiveRestoreProbe struct {
	accountID uint32
	sql       string
}

func lifecycleArchiveRestoreProbes(
	scope lifecycleArchiveRestoreScope,
) ([]lifecycleArchiveRestoreProbe, error) {
	if scope.level == tree.RESTORELEVELCLUSTER {
		return nil, moerr.NewInvalidInputNoCtx(
			"cluster Lifecycle Archive restore scope must be expanded by account",
		)
	}

	timeTravel := ""
	if scope.snapshotTS != 0 {
		timeTravel = fmt.Sprintf(" {MO_TS = %d}", scope.snapshotTS)
	}
	tablePredicate, bindingPredicate, err := lifecycleArchiveRestorePredicates(
		scope,
		timeTravel,
	)
	if err != nil {
		return nil, err
	}

	tenantPrefix := fmt.Sprintf("account_id=%d", scope.accountID)
	rootPrefix := fmt.Sprintf("owner_account_id=%d", scope.accountID)
	if bindingPredicate != "" {
		bindingPredicate = " and " + bindingPredicate
	}
	if tablePredicate != "" {
		tablePredicate = " and " + tablePredicate
	}
	bindingAction := " and action='ARCHIVE'"
	if scope.rejectTTLBindings {
		bindingAction = ""
	}

	return []lifecycleArchiveRestoreProbe{
		{
			accountID: scope.accountID,
			sql: fmt.Sprintf(
				`select binding_id from mo_catalog.mo_lifecycle_bindings%s
where %s%s and state in ('ACTIVE','PAUSED','BLOCKED')%s limit 1`,
				timeTravel,
				tenantPrefix,
				bindingAction,
				bindingPredicate,
			),
		},
		{
			accountID: scope.accountID,
			sql: fmt.Sprintf(
				`select dataset_id from mo_catalog.mo_lifecycle_datasets%s
where %s and state<>'PURGED'%s limit 1`,
				timeTravel,
				tenantPrefix,
				tablePredicate,
			),
		},
		{
			accountID: catalog.System_Account,
			sql: fmt.Sprintf(
				`select root_id from mo_catalog.mo_lifecycle_cleanup_roots%s
where %s and mode in ('ARCHIVE_WHOLE','ARCHIVE_REWRITE') and state<>'CLEANED'%s limit 1`,
				timeTravel,
				rootPrefix,
				tablePredicate,
			),
		},
	}, nil
}

func lifecycleArchiveRestorePredicates(
	scope lifecycleArchiveRestoreScope,
	timeTravel string,
) (tablePredicate string, bindingPredicate string, err error) {
	switch scope.level {
	case tree.RESTORELEVELACCOUNT:
		return "", "", nil

	case tree.RESTORELEVELDATABASE:
		if scope.databaseName == "" {
			return "", "", moerr.NewInvalidInputNoCtx(
				"database restore scope has no database name",
			)
		}
		predicate := fmt.Sprintf(
			`logical_table_id in (select rel_logical_id from mo_catalog.mo_tables%s
where account_id=%d and reldatabase=%s)`,
			timeTravel,
			scope.accountID,
			quoteSQLStringLiteral(scope.databaseName),
		)
		return predicate, predicate, nil

	case tree.RESTORELEVELTABLE:
		if scope.databaseName == "" || scope.tableName == "" {
			return "", "", moerr.NewInvalidInputNoCtx(
				"table restore scope has no database or table name",
			)
		}
		predicate := fmt.Sprintf(
			`logical_table_id in (select rel_logical_id from mo_catalog.mo_tables%s
where account_id=%d and reldatabase=%s and relname=%s)`,
			timeTravel,
			scope.accountID,
			quoteSQLStringLiteral(scope.databaseName),
			quoteSQLStringLiteral(scope.tableName),
		)
		return predicate, predicate, nil

	default:
		return "", "", moerr.NewInvalidInputf(
			context.Background(),
			"unknown Lifecycle Archive restore scope %d",
			scope.level,
		)
	}
}

// rejectLifecycleArchiveRestoreScope is a read-only management-path guard.
// Phase 1 does not restore Archive Dataset/Root metadata together with active
// table data, so a matching Archive owner makes Snapshot/PITR restore
// unsupported. It adds no lock or state machine to ordinary MO paths.
// Phase 1 requires operators to disable and drain Lifecycle data jobs before
// Snapshot/PITR restore; concurrent SET/finalization is intentionally outside
// the certified contract instead of reintroducing a global feature barrier.
func rejectLifecycleArchiveRestoreScope(
	ctx context.Context,
	background BackgroundExec,
	scope lifecycleArchiveRestoreScope,
	operation string,
) error {
	probes, err := lifecycleArchiveRestoreProbes(scope)
	if err != nil {
		return err
	}
	for _, probe := range probes {
		probeCtx := defines.AttachAccountId(ctx, probe.accountID)
		background.ClearExecResultSet()
		if err = background.Exec(probeCtx, probe.sql); err != nil {
			if err = ignoreMissingLifecycleCatalog(err); err != nil {
				return err
			}
			continue
		}
		results, resultErr := getResultSet(probeCtx, background)
		if resultErr != nil {
			return resultErr
		}
		if execResultArrayHasData(results) {
			state := "Lifecycle Archive state"
			if scope.rejectTTLBindings {
				state = "Lifecycle state"
			}
			return moerr.NewNotSupportedf(
				ctx,
				"%s while the target scope contains %s",
				operation,
				state,
			)
		}
	}
	return nil
}

func lifecycleArchiveRootProbeSQL(snapshotTS int64) string {
	timeTravel := ""
	if snapshotTS != 0 {
		timeTravel = fmt.Sprintf(" {MO_TS = %d}", snapshotTS)
	}
	return fmt.Sprintf(
		`select root_id from mo_catalog.mo_lifecycle_cleanup_roots%s
where mode in ('ARCHIVE_WHOLE','ARCHIVE_REWRITE') and state<>'CLEANED' limit 1`,
		timeTravel,
	)
}

func rejectLifecycleArchiveClusterRestore(
	ctx context.Context,
	ses *Session,
	background BackgroundExec,
	ownerName string,
	snapshotTS int64,
	operation string,
) error {
	// A dropped account can leave a system-owned Cleanup Root after its tenant
	// Catalog is gone. Account enumeration cannot discover that owner, so guard
	// the cluster-wide Root set explicitly before restoring system metadata.
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	for _, rootSnapshotTS := range []int64{0, snapshotTS} {
		background.ClearExecResultSet()
		if err := background.Exec(
			systemCtx,
			lifecycleArchiveRootProbeSQL(rootSnapshotTS),
		); err != nil {
			if err = ignoreMissingLifecycleCatalog(err); err != nil {
				return err
			}
			continue
		}
		results, err := getResultSet(systemCtx, background)
		if err != nil {
			return err
		}
		if execResultArrayHasData(results) {
			return moerr.NewNotSupportedf(
				ctx,
				"%s while the target scope contains Lifecycle Archive state",
				operation,
			)
		}
	}

	currentAccounts, err := getRestoreAcurrentExistsAccount(
		ctx,
		ses.GetService(),
		background,
		ownerName,
	)
	if err != nil {
		return err
	}
	pastAccounts, err := getPastExistsAccounts(
		ctx,
		ses.GetService(),
		background,
		ownerName,
		snapshotTS,
	)
	if err != nil {
		return err
	}
	for _, account := range currentAccounts {
		if err = rejectLifecycleArchiveRestoreScope(
			ctx,
			background,
			lifecycleArchiveRestoreScope{
				level:     tree.RESTORELEVELACCOUNT,
				accountID: uint32(account.accountId),
			},
			operation,
		); err != nil {
			return err
		}
	}
	for _, account := range pastAccounts {
		if err = rejectLifecycleArchiveRestoreScope(
			ctx,
			background,
			lifecycleArchiveRestoreScope{
				level:      tree.RESTORELEVELACCOUNT,
				accountID:  uint32(account.accountId),
				snapshotTS: snapshotTS,
			},
			operation,
		); err != nil {
			return err
		}
	}
	return nil
}
