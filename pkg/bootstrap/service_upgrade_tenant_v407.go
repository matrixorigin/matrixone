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

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type tenantUpgradeSelection struct {
	upgrade versions.VersionUpgrade
	taskID  uint64
	tenants []int32
}

func (s *service) upgradeV407TenantRange(ctx context.Context, selected tenantUpgradeSelection) error {
	for _, accountID := range selected.tenants {
		if err := s.upgradeV407Tenant(ctx, selected.upgrade, accountID); err != nil {
			return err
		}
	}
	return s.finishV407TenantTask(ctx, selected)
}

func (s *service) upgradeV407Tenant(ctx context.Context, upgrade versions.VersionUpgrade, accountID int32) error {
	for index := 0; index < v4_0_7.Handler.TenantUpgradeEntryCount(); index++ {
		var cursor v4_0_7.TenantPageCursor
		for {
			next := cursor
			var more bool
			active, err := s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
				var err error
				more, err = v4_0_7.Handler.UpgradeTenantEntryPage(ctx, accountID, index, tx, s.upgrade.catalogObjectLimit, &next)
				return err
			})
			if err != nil {
				return catalogOperationError(err, fmt.Sprintf("tenant %d", accountID), fmt.Sprintf("entry %d", index), s.upgrade.catalogObjectLimit, true)
			}
			if !active {
				return nil
			}
			cursor = next
			if !more {
				break
			}
		}
	}
	var after v4_0_7.CatalogTableKey
	for {
		var tables []v4_0_7.CatalogTableKey
		active, err := s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
			var err error
			tables, err = v4_0_7.Handler.ListForeignKeyTables(tx, accountID, after, CatalogUpgradePageBytes)
			return err
		})
		if err != nil {
			return catalogOperationError(err, fmt.Sprintf("tenant %d", accountID), "enumerating foreign-key tables", CatalogUpgradePageBytes, false)
		}
		if !active {
			return nil
		}
		for _, table := range tables {
			active, err = s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
				return v4_0_7.Handler.UpgradeForeignKeyTable(ctx, accountID, tx, table, s.upgrade.catalogObjectLimit)
			})
			if err != nil {
				return catalogOperationError(err, fmt.Sprintf("tenant %d", accountID), "foreign-key table "+table.Database+"."+table.Table, s.upgrade.catalogObjectLimit, true)
			}
			if !active {
				return nil
			}
			after = table
		}
		if len(tables) < v4_0_7.CatalogDataPageSize {
			break
		}
	}
	var afterIndex v4_0_7.IndexMetadataKey
	for {
		var tables []v4_0_7.IndexMetadataKey
		active, err := s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
			var err error
			tables, err = v4_0_7.Handler.ListIndexMetadataTables(tx, uint32(accountID), afterIndex, CatalogUpgradePageBytes)
			return err
		})
		if err != nil {
			return catalogOperationError(err, fmt.Sprintf("tenant %d", accountID), "enumerating index metadata tables", CatalogUpgradePageBytes, false)
		}
		if !active {
			return nil
		}
		for _, table := range tables {
			active, err = s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
				return v4_0_7.Handler.UpgradeIndexMetadataTable(ctx, accountID, tx, table)
			})
			if err != nil {
				return catalogOperationError(err, fmt.Sprintf("tenant %d", accountID), "index metadata table "+table[0]+"."+table[1], s.upgrade.catalogObjectLimit, true)
			}
			if !active {
				return nil
			}
			afterIndex = table
		}
		if len(tables) < v4_0_7.CatalogDataPageSize {
			break
		}
	}
	_, err := s.withV407Account(ctx, upgrade, accountID, func(tx executor.TxnExecutor) error {
		current, err := versions.GetUpgradeVersionForUpdateByID(upgrade.ID, tx)
		if err != nil {
			return err
		}
		if current.State != versions.StateUpgradingTenant || current.ToVersion != upgrade.ToVersion {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("tenant upgrade step changed before account completion")}
		}
		if current.ReadyTenant < 0 || current.TotalTenant <= current.ReadyTenant {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("invalid tenant completion counts")}
		}
		if err := versions.UpgradeTenantVersion(accountID, upgrade.ToVersion, tx); err != nil {
			return err
		}
		current.ReadyTenant++
		return versions.UpdateVersionUpgradeTasks(current, tx)
	})
	return err
}

// Each operation reacquires the account lock and checks the authoritative
// create_version. A concurrent worker that finished this account makes later
// pages no-ops, including after an uncertain commit result.
func (s *service) withV407Account(ctx context.Context, upgrade versions.VersionUpgrade, accountID int32, work func(executor.TxnExecutor) error) (bool, error) {
	opCtx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTenantTask)
	defer cancel()
	active := false
	err := s.exec.ExecTxn(opCtx, func(tx executor.TxnExecutor) error {
		version, err := versions.GetTenantCreateVersionForUpdate(accountID, tx)
		if err != nil {
			return err
		}
		if !shouldRunTenantUpgrade(version, upgrade) {
			return nil
		}
		active = true
		return work(tx)
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).WithMinCommittedTS(s.now()).WithWaitCommittedLogApplied().WithTimeZone(time.Local))
	return active, err
}

func (s *service) finishV407TenantTask(ctx context.Context, selected tenantUpgradeSelection) error {
	opCtx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTenantTask)
	defer cancel()
	return s.exec.ExecTxn(opCtx, func(tx executor.TxnExecutor) error {
		from, to, upgradeID, ready, err := versions.GetUpgradeTenantTask(selected.taskID, tx)
		if err != nil {
			return err
		}
		if upgradeID != selected.upgrade.ID {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("tenant task upgrade identity changed")}
		}
		if ready == versions.Yes {
			return nil
		}
		sql := fmt.Sprintf("select account_id, create_version from mo_account where account_id >= %d and account_id <= %d for update", from, to)
		rows, err := tx.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		defer rows.Close()
		incomplete := false
		rows.ReadRows(func(n int, cols []*vector.Vector) bool {
			for i := 0; i < n; i++ {
				if shouldRunTenantUpgrade(cols[1].GetStringAt(i), selected.upgrade) {
					incomplete = true
					return false
				}
			}
			return true
		})
		if incomplete {
			return nil
		}
		current, err := versions.GetUpgradeVersionForUpdateByID(selected.upgrade.ID, tx)
		if err != nil {
			return err
		}
		if current.State == versions.StateReady {
			return nil
		}
		if current.State != versions.StateUpgradingTenant {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("tenant upgrade step is not accepting task completion")}
		}
		return versions.UpdateUpgradeTenantTaskState(selected.taskID, versions.Yes, tx)
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).WithMinCommittedTS(s.now()).WithWaitCommittedLogApplied())
}

func catalogOperationError(err error, scope, operation string, limit uint64, adjustable bool) error {
	if mpool.IsMPoolCapacityFailure(err) {
		return fmt.Errorf("%s %s exceeded engine memory capacity; the upgrade will retry, provision more memory if pressure persists: %w", scope, operation, err)
	}
	var indexConflict *v4_0_7.IndexConflict
	if errors.As(err, &indexConflict) {
		return &terminalUpgradeError{fmt.Errorf("%s %s: %w", scope, operation, err)}
	}
	var resultLimit *executor.ResultLimitError
	if errors.As(err, &resultLimit) {
		if !adjustable {
			return &terminalUpgradeError{fmt.Errorf("%s %s exceeds the %d-byte catalog page limit: %w", scope, operation, limit, err)}
		}
		return &terminalUpgradeError{fmt.Errorf("%s %s exceeds %d-byte catalog input limit; provision memory, raise cn.upgrade-catalog-object-limit on all CNs and restart: %w", scope, operation, limit, err)}
	}
	return err
}
