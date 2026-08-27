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

package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type ExpiredRestoreAttempt struct {
	AccountID          uint32
	RestoreID          string
	TargetDatabaseName string
}

type ExpiredRestoreCursor struct {
	AccountID uint32
	RestoreID string
}

// SQLExpiredRestorePager finds only deadline-expired hidden-table owners.
// Cleanup itself remains an ordinary tenant transaction in SQLRestoreRepository.
type SQLExpiredRestorePager struct {
	Executor executor.SQLExecutor
}

func (pager SQLExpiredRestorePager) Next(
	ctx context.Context,
	cursor ExpiredRestoreCursor,
	now time.Time,
	maxAccounts int,
	maxAttempts int,
) ([]ExpiredRestoreAttempt, ExpiredRestoreCursor, error) {
	if pager.Executor == nil ||
		now.IsZero() ||
		maxAccounts <= 0 ||
		maxAttempts <= 0 {
		return nil, cursor, moerr.NewInternalErrorNoCtxf(
			"Lifecycle expired Restore pager is incomplete",
		)
	}
	attempts := make([]ExpiredRestoreAttempt, 0, min(maxAttempts, 64))
	next := cursor
	accountsRemaining := maxAccounts
	if cursor.AccountID != 0 && cursor.RestoreID != "" {
		page, err := pager.readExpiredRestores(
			ctx,
			cursor.AccountID,
			cursor.RestoreID,
			now,
			maxAttempts,
		)
		if err != nil {
			return nil, cursor, err
		}
		attempts = append(attempts, page...)
		if len(page) > 0 {
			next = ExpiredRestoreCursor{
				AccountID: cursor.AccountID,
				RestoreID: page[len(page)-1].RestoreID,
			}
		}
		if len(attempts) == maxAttempts {
			return attempts, next, nil
		}
		accountsRemaining--
		next = ExpiredRestoreCursor{AccountID: cursor.AccountID}
	}
	if accountsRemaining == 0 {
		return attempts, next, nil
	}
	accounts, err := SQLMetadataCompactor(pager).
		listAccounts(ctx, cursor.AccountID, accountsRemaining)
	if err != nil {
		return nil, cursor, err
	}
	if len(accounts) == 0 && cursor.AccountID != 0 {
		accounts, err = SQLMetadataCompactor(pager).
			listAccounts(ctx, 0, accountsRemaining)
		if err != nil {
			return nil, cursor, err
		}
		next = ExpiredRestoreCursor{}
	}
	for _, accountID := range accounts {
		remaining := maxAttempts - len(attempts)
		if remaining == 0 {
			break
		}
		page, queryErr := pager.readExpiredRestores(
			ctx,
			accountID,
			"",
			now,
			remaining,
		)
		if queryErr != nil {
			return nil, cursor, queryErr
		}
		attempts = append(attempts, page...)
		if len(page) > 0 {
			next = ExpiredRestoreCursor{
				AccountID: accountID,
				RestoreID: page[len(page)-1].RestoreID,
			}
			if len(attempts) == maxAttempts {
				return attempts, next, nil
			}
		}
		next = ExpiredRestoreCursor{AccountID: accountID}
	}
	return attempts, next, nil
}

func (pager SQLExpiredRestorePager) readExpiredRestores(
	ctx context.Context,
	accountID uint32,
	afterRestoreID string,
	now time.Time,
	limit int,
) ([]ExpiredRestoreAttempt, error) {
	predicate := ""
	if afterRestoreID != "" {
		encoded, err := lifecycleSQLUUID(afterRestoreID)
		if err != nil {
			return nil, err
		}
		predicate = fmt.Sprintf(" and a.restore_id>unhex('%s')", encoded)
	}
	result, err := pager.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select hex(a.restore_id),coalesce(d.datname,'')
from mo_catalog.mo_lifecycle_restore_attempts a
left join mo_catalog.mo_database d on d.dat_id=a.staging_database_id
where a.state in ('IMPORTING','PUBLISHING') and
(a.deadline<=%s or not exists (
  select 1 from mo_catalog.mo_tables h
  where h.rel_id=a.staging_table_id
    and h.reldatabase_id=a.staging_database_id
    and h.relname=a.hidden_name))%s
order by a.restore_id limit %d`,
			lifecycleSQLTime(now),
			predicate,
			limit,
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	attempts := make([]ExpiredRestoreAttempt, 0, min(limit, 64))
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 2 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle expired Restore query is invalid")
			return false
		}
		for row := 0; row < rows; row++ {
			restoreID, idErr := lifecycleUUIDFromHex(columns[0].GetStringAt(row))
			if idErr != nil {
				decodeErr = idErr
				return false
			}
			attempts = append(attempts, ExpiredRestoreAttempt{
				AccountID:          accountID,
				RestoreID:          restoreID,
				TargetDatabaseName: columns[1].GetStringAt(row),
			})
		}
		return true
	})
	return attempts, decodeErr
}
