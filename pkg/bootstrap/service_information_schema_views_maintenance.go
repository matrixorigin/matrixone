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
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

const informationSchemaViewsMaintenancePageSize = 32

// informationSchemaPartitionsMaintenanceEntry keeps the PARTITIONS
// reconciliation on the same bounded page, cursor and transaction as VIEWS.
// The indirection only exists so the pre-existing VIEWS scenario tests can
// pin an already-current PARTITIONS entry and keep exercising exactly their
// original subjects; production always reconciles the live PARTITIONS
// definition against sysview's current DDL.
var informationSchemaPartitionsMaintenanceEntry = versions.InformationSchemaPartitionsUpgradeEntry

var errInformationSchemaViewsProtocolUnavailable = moerr.NewNotSupportedNoCtx(
	"information schema views maintenance protocol is unavailable")

// informationSchemaViewsMaintenanceState is process-local progress. The
// persisted VIEWS definition is the durable pending-work marker, so a fresh
// process can start at zero and a failed transaction can safely retry the same
// page without losing work.
type informationSchemaViewsMaintenanceState struct {
	accountCursor int32
}

// maintainInformationSchemaViews repairs the system VIEWS and PARTITIONS
// definitions for every account, including tenants already at v4.0.6, v4.0.7,
// or the final version. It processes one bounded page per invocation after the
// v126 gate; the standalone repair does not alter any historical handler
// identity, and the page cursor publishes only when every definition staged
// on that page committed together.
func (s *service) maintainInformationSchemaViews(ctx context.Context) error {
	if !s.upgrade.informationSchemaViewsMaintenanceRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer s.upgrade.informationSchemaViewsMaintenanceRunning.Store(false)

	current := s.upgrade.informationSchemaViewsMaintenanceState
	next := current
	options := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)

	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		txn.Use(catalog.MO_CATALOG)

		// Do not even enumerate tenants until the writer protocol is known to
		// be available everywhere. A NotSupported result is a normal rolling-
		// upgrade retry; actual catalog or transaction errors remain visible.
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
		partitionsEntry := informationSchemaPartitionsMaintenanceEntry()
		for _, accountID := range accountIDs {
			exists, viewDefinition, err := v4_0_7.CheckInformationSchemaViewsDefinition(txn, uint32(accountID))
			if err != nil {
				if informationSchemaViewsAccountGone(accountID, err) {
					// DROP ACCOUNT removes the account-local mo_user and
					// information_schema objects while the system-account page
					// is being scanned. The account is no longer a valid repair
					// target; keep the page transactional and let the next pass
					// discover any surviving accounts.
					continue
				}
				return err
			}
			viewsCurrent := exists && sysview.InformationSchemaViewDefinitionMatches(
				viewDefinition, sysview.InformationSchemaViewsDDL)

			// The same ready account can carry a current VIEWS definition next
			// to a stale PARTITIONS one; a VIEWS match must never skip the
			// unrepaired sibling. Reconcile both on this bounded page.
			partitionsCurrent, err := partitionsEntry.CheckFunc(txn, uint32(accountID))
			if err != nil {
				gone, goneErr := informationSchemaViewsPartitionsAccountGone(txn, accountID, err)
				if goneErr != nil {
					return goneErr
				}
				if gone {
					continue
				}
				return err
			}
			if viewsCurrent && partitionsCurrent {
				continue
			}

			// Recheck immediately before the transactional entries. If a peer
			// disappears after this check, the DDL error must escape so the
			// transaction rolls back rather than being mistaken for a gate miss.
			if ready, err := informationSchemaViewsProtocolReady(txn); err != nil {
				return err
			} else if !ready {
				return errInformationSchemaViewsProtocolUnavailable
			}
			if !viewsCurrent {
				if err := v4_0_7.UpgradeInformationSchemaViewsAfterProtocolCheck(
					txn, uint32(accountID)); err != nil {
					if informationSchemaViewsAccountGone(accountID, err) {
						continue
					}
					return err
				}
			}
			// A real PARTITIONS DDL error rolls back the entire page and keeps
			// the cursor, so the next pass retries every reconciliation in it.
			if !partitionsCurrent {
				if err := partitionsEntry.UpgradeAfterProtocolCheck(txn, uint32(accountID)); err != nil {
					gone, goneErr := informationSchemaViewsPartitionsAccountGone(txn, accountID, err)
					if goneErr != nil {
						return goneErr
					}
					if gone {
						continue
					}
					return err
				}
			}
		}

		next = current
		next.accountCursor = candidateCursor
		return nil
	}, options)
	if err != nil {
		// A protocol miss after staging an earlier replacement must be returned
		// from the transaction so ExecTxn rolls back the whole page. Swallow
		// only the gate error after a successful rollback; a joined rollback
		// error remains visible to the caller.
		if isOnlyInformationSchemaViewsProtocolGateError(err) {
			return nil
		}
		return err
	}

	// Only a successful ExecTxn publishes the closure-local cursor. For an
	// unsupported capability the closure deliberately leaves next unchanged.
	s.upgrade.informationSchemaViewsMaintenanceState = next
	return nil
}

func informationSchemaViewsAccountGone(accountID int32, err error) bool {
	return uint32(accountID) != catalog.System_Account &&
		(moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
			moerr.IsMoErrCode(err, moerr.ErrBadDB))
}

// informationSchemaAccountDroppedInTxn positively confirms inside the page
// transaction that the account no longer exists in mo_catalog.mo_account.
// A typed ErrNoSuchTable/ErrBadDB alone cannot prove a tenant disappeared: a
// live account can hit those codes through a missing dependency, and skipping
// it would silently drop its PARTITIONS work while committing the staged
// VIEWS replacement of the same page.
func informationSchemaAccountDroppedInTxn(
	txn executor.TxnExecutor,
	accountID uint32,
) (bool, error) {
	sql := fmt.Sprintf(
		"select account_id from mo_catalog.mo_account where account_id = %d",
		accountID)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(catalog.System_Account))
	if err != nil {
		return false, err
	}
	defer res.Close()
	found := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && len(cols) > 0 {
			found = true
		}
		return false
	})
	return !found, nil
}

// informationSchemaViewsPartitionsAccountGone upgrades the code-only
// account-gone suspicion of the PARTITIONS branches into an in-transaction
// fact: only an account confirmed absent may keep the historical skip; a
// surviving tenant's typed error must roll the whole page back.
func informationSchemaViewsPartitionsAccountGone(
	txn executor.TxnExecutor,
	accountID int32,
	err error,
) (bool, error) {
	if !informationSchemaViewsAccountGone(accountID, err) {
		return false, nil
	}
	dropped, probeErr := informationSchemaAccountDroppedInTxn(txn, uint32(accountID))
	if probeErr != nil {
		return false, probeErr
	}
	return dropped, nil
}

func informationSchemaViewsProtocolReady(txn executor.TxnExecutor) (bool, error) {
	if !v4_0_7.InformationSchemaViewsAuthoringProtocolReady(txn) {
		return false, nil
	}
	err := versions.CheckCommonProtocolVersion(txn, defines.MORPCVersion126)
	if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func isOnlyInformationSchemaViewsProtocolGateError(err error) bool {
	if err == nil || !errors.Is(err, errInformationSchemaViewsProtocolUnavailable) {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		return len(causes) == 1 && errors.Is(causes[0], errInformationSchemaViewsProtocolUnavailable)
	}
	return true
}

func loadInformationSchemaViewsMaintenanceAccounts(
	txn executor.TxnExecutor,
	accountCursor int32,
) ([]int32, error) {
	sql := fmt.Sprintf(
		"select account_id from mo_catalog.mo_account where account_id >= %d order by account_id limit %d",
		accountCursor, informationSchemaViewsMaintenancePageSize)
	res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(catalog.System_Account))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	accountIDs := make([]int32, 0, informationSchemaViewsMaintenancePageSize)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) == 0 {
			return false
		}
		for i := 0; i < rows; i++ {
			accountIDs = append(accountIDs, vector.GetFixedAtWithTypeCheck[int32](cols[0], i))
		}
		return true
	})
	return accountIDs, nil
}

func nextInformationSchemaViewsMaintenanceCursor(accountID int32) int32 {
	const maxInt32 = int32(^uint32(0) >> 1)
	if accountID == maxInt32 {
		return 0
	}
	return accountID + 1
}
