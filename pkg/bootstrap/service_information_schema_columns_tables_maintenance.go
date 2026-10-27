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

package bootstrap

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"

	"go.uber.org/zap"
)

// informationSchemaColumnsTablesMaintenanceState is process-local progress.
// The persisted view definitions are the durable pending-work markers, so a
// fresh process can start at zero and a failed transaction can safely retry
// the same page without losing work.
type informationSchemaColumnsTablesMaintenanceState struct {
	accountCursor int32
}

// informationSchemaColumnsTablesUpgradeEntries returns the shared current
// definition refreshes (issue #31160 + #31163). Tenants that already reached
// the final catalog version before these definitions changed still hold the
// old binary-selector COLUMNS and the create-time-fabricating TABLES: no
// version UpgradeEntry re-runs for them, so the periodic maintenance owner
// converges them here, one bounded transactional page per invocation. The
// exact-equality CheckFunc in each entry makes the repair fire exactly once
// per account and then stay idle.
func informationSchemaColumnsTablesUpgradeEntries() []versions.UpgradeEntry {
	return []versions.UpgradeEntry{
		versions.InformationSchemaColumnsUpgradeEntry(),
		versions.InformationSchemaTablesUpgradeEntry(),
	}
}

// maintainInformationSchemaColumnsTables repairs the COLUMNS and TABLES
// definitions for every account, including tenants already at the final
// version. It mirrors the VIEWS maintenance contract: protocol-gated,
// transactional pages, durable persisted-definition idempotence.
func (s *service) maintainInformationSchemaColumnsTables(ctx context.Context) error {
	if !s.upgrade.informationSchemaColumnsTablesMaintenanceRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer s.upgrade.informationSchemaColumnsTablesMaintenanceRunning.Store(false)

	current := s.upgrade.informationSchemaColumnsTablesMaintenanceState
	next := current
	options := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)

	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)

		// Same gate as the sibling VIEWS maintenance: a NotSupported result is a
		// normal rolling-upgrade retry. The COLUMNS/TABLES definitions require a
		// lower common protocol than this gate enforces, so passing it is always
		// sufficient for publishing them.
		if ready, err := informationSchemaViewsProtocolReady(txn); err != nil {
			return err
		} else if !ready {
			return nil
		}

		accountIDs, err := loadInformationSchemaViewsMaintenanceAccounts(
			txn, current.accountCursor)
		if err != nil {
			return err
		}
		if len(accountIDs) == 0 {
			// Account IDs are monotonic. Starting the next pass at zero makes
			// late-created accounts and a restarted process discoverable.
			next = current
			next.accountCursor = 0
			return nil
		}

		candidateCursor := nextInformationSchemaViewsMaintenanceCursor(accountIDs[len(accountIDs)-1])
		entries := informationSchemaColumnsTablesUpgradeEntries()
		for _, accountID := range accountIDs {
			repaired := false
			for i := range entries {
				done, err := entries[i].CheckFunc(txn, uint32(accountID))
				if err != nil {
					if informationSchemaViewsAccountGone(accountID, err) {
						break
					}
					return err
				}
				if done {
					continue
				}
				// Recheck immediately before the transactional entry; the gate
				// error rolls the whole page back rather than being staged.
				if ready, err := informationSchemaViewsProtocolReady(txn); err != nil {
					return err
				} else if !ready {
					return errInformationSchemaViewsProtocolUnavailable
				}
				if err := entries[i].UpgradeAfterProtocolCheck(txn, uint32(accountID)); err != nil {
					if informationSchemaViewsAccountGone(accountID, err) {
						break
					}
					return err
				}
				repaired = true
			}
			if repaired {
				s.logger.Info("information schema columns/tables maintenance repaired account",
					zap.Int32("accountId", accountID))
			}
		}

		next = current
		next.accountCursor = candidateCursor
		return nil
	}, options)
	if err != nil {
		if isOnlyInformationSchemaViewsProtocolGateError(err) {
			return nil
		}
		return err
	}

	s.upgrade.informationSchemaColumnsTablesMaintenanceState = next
	return nil
}
