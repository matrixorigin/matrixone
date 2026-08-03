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
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const maxUnknownTTLRewriteRoots = 4096

// SQLMetadataCompactor bounds only terminal Lifecycle metadata. It is paged
// by account and every DELETE has a row cap, so cleanup never becomes a
// cluster-wide transaction or enters ordinary MO paths.
type SQLMetadataCompactor struct {
	Executor executor.SQLExecutor
}

func (compactor SQLMetadataCompactor) CompactPage(
	ctx context.Context,
	afterAccountID uint32,
	now time.Time,
	retention time.Duration,
	maxAccounts int,
	maxRows int,
) (nextAccountID uint32, wrapped bool, err error) {
	if compactor.Executor == nil ||
		now.IsZero() ||
		retention <= 0 ||
		maxAccounts <= 0 ||
		maxRows <= 0 {
		return afterAccountID, false, fmt.Errorf(
			"Lifecycle metadata compactor is incomplete",
		)
	}
	accounts, err := compactor.listAccounts(
		ctx,
		afterAccountID,
		maxAccounts,
	)
	if err != nil {
		return afterAccountID, false, err
	}
	if len(accounts) == 0 && afterAccountID != 0 {
		accounts, err = compactor.listAccounts(ctx, 0, maxAccounts)
		if err != nil {
			return afterAccountID, false, err
		}
		wrapped = true
	}
	terminalCutoff := lifecycleSQLTime(now.Add(-retention))
	datasetRetention := retention * 3
	if datasetRetention <= 0 {
		return afterAccountID, false, fmt.Errorf(
			"Lifecycle Dataset metadata retention overflow",
		)
	}
	datasetCutoff := lifecycleSQLTime(now.Add(-datasetRetention))
	for _, accountID := range accounts {
		protectedTTLAttempts, checkErr := compactor.unknownTTLRewriteAttempts(
			ctx,
			accountID,
		)
		if checkErr != nil {
			return afterAccountID, wrapped, checkErr
		}
		for _, sql := range terminalLifecycleMetadataDeletes(
			terminalCutoff,
			datasetCutoff,
			maxRows,
			protectedTTLAttempts,
		) {
			result, execErr := compactor.Executor.Exec(
				ctx,
				sql,
				executor.Options{}.WithAccountID(accountID),
			)
			if execErr != nil {
				return afterAccountID, wrapped, execErr
			}
			result.Close()
		}
		nextAccountID = accountID
	}
	result, err := compactor.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_cleanup_roots
where state='CLEANED' and updated_at<%s limit %d`,
			terminalCutoff,
			maxRows,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return afterAccountID, wrapped, err
	}
	result.Close()
	if len(accounts) == 0 {
		nextAccountID = afterAccountID
	}
	return nextAccountID, wrapped, nil
}

// unknownTTLRewriteAttempts returns the exact TTL Receipt owners which must
// remain available while a final transaction is unresolved. Protecting the
// entire tenant would let one long-lived unknown Root prevent unrelated
// Receipts from ever reaching their retention endpoint.
func (compactor SQLMetadataCompactor) unknownTTLRewriteAttempts(
	ctx context.Context,
	accountID uint32,
) ([]string, error) {
	result, err := compactor.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select concat(hex(root_id),hex(attempt_id)) from mo_catalog.mo_lifecycle_cleanup_roots
where owner_account_id=%d and mode='TTL_REWRITE'
and state='COMMIT_UNKNOWN' order by root_id limit %d`,
			accountID,
			maxUnknownTTLRewriteRoots+1,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	protected := make([]string, 0)
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 1 {
			decodeErr = fmt.Errorf(
				"Lifecycle unknown TTL Root query is invalid",
			)
			return false
		}
		for row := 0; row < rows; row++ {
			identity := strings.ToLower(columns[0].GetStringAt(row))
			decoded, decodeIdentityErr := hex.DecodeString(identity)
			if decodeIdentityErr != nil || len(decoded) != 32 {
				decodeErr = fmt.Errorf(
					"Lifecycle unknown TTL Root identity is invalid",
				)
				return false
			}
			protected = append(protected, identity)
			if len(protected) > maxUnknownTTLRewriteRoots {
				decodeErr = fmt.Errorf(
					"RESOURCE_BLOCKED: Lifecycle unknown TTL Root limit exceeded",
				)
				return false
			}
		}
		return true
	})
	return protected, decodeErr
}

func (compactor SQLMetadataCompactor) listAccounts(
	ctx context.Context,
	afterAccountID uint32,
	limit int,
) ([]uint32, error) {
	result, err := compactor.Executor.Exec(
		ctx,
		fmt.Sprintf(
			`select cast(account_id as bigint unsigned)
from mo_catalog.mo_account where account_id > %d
order by account_id limit %d`,
			afterAccountID,
			limit,
		),
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return readLifecycleAccountIDs(result)
}

func terminalLifecycleMetadataDeletes(
	terminalCutoff string,
	datasetCutoff string,
	limit int,
	protectedTTLAttempts []string,
) []string {
	deletes := []string{
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_restore_chunks
where restore_id in (
  select restore_id from mo_catalog.mo_lifecycle_restore_attempts
  where state in ('DONE','FAILED') and updated_at<%s
) limit %d`,
			terminalCutoff,
			limit,
		),
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_restore_attempts
where state in ('DONE','FAILED') and updated_at<%s
and not exists (
  select 1 from mo_catalog.mo_lifecycle_restore_chunks c
  where c.restore_id=mo_catalog.mo_lifecycle_restore_attempts.restore_id
) limit %d`,
			terminalCutoff,
			limit,
		),
	}
	ttlDelete := fmt.Sprintf(
		`delete from mo_catalog.mo_lifecycle_ttl_receipts
where created_at<%s`,
		terminalCutoff,
	)
	if len(protectedTTLAttempts) > 0 {
		var protected strings.Builder
		protected.WriteString(
			"\nand (root_id is null or attempt_id is null or not (\n",
		)
		for index, identity := range protectedTTLAttempts {
			if index > 0 {
				protected.WriteString("  or ")
			} else {
				protected.WriteString("  ")
			}
			fmt.Fprintf(
				&protected,
				"(root_id=unhex('%s') and attempt_id=unhex('%s'))\n",
				identity[:32],
				identity[32:],
			)
		}
		protected.WriteString("))")
		ttlDelete += protected.String()
	}
	ttlDelete += fmt.Sprintf(" limit %d", limit)
	deletes = append(deletes, ttlDelete)
	return append(deletes,
		fmt.Sprintf(
			`delete from mo_catalog.mo_lifecycle_datasets
where state='PURGED' and updated_at<%s limit %d`,
			datasetCutoff,
			limit,
		),
	)
}
