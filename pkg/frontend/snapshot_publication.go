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

package frontend

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// This file implements the publication-routed snapshot lifecycle protocol of
// docs/design/20261023-subscription-table-branch-snapshot-routing.md (v7,
// approved in pre-pr-review-1792751823194.json) for issue #30949: the
// split-identity admission (design §4.4) and the scope-bound deletion
// (design §4.5, I7). Two accounts participate in a publication-routed
// CREATE: the physical source owner P_src carries the catalog keys, quota,
// and the recorded artifact, while the mo_pubs lock key and the coverage
// read belong to the publication account P_pub. For ordinary publications
// P_src == P_pub and every emitted lock key and query is byte-identical to
// the pre-change single-account flow.

// snapshotPublicationRow is one mo_pubs row plus the subscriber membership
// verdict, as read by resolveAccountFromPublication (provisional) and by the
// post-frontier revalidation (the row's shared lock is already held then).
type snapshotPublicationRow struct {
	accountID    uint32 // publication owner account (P_pub)
	accountName  string
	pubName      string
	databaseName string
	databaseID   uint64
	tableList    string
	accountList  string
}

// databaseScoped reports the C9 coverage shape: a row with a persisted
// database_id binds exactly one physical database.
func (r *snapshotPublicationRow) databaseScoped() bool {
	return r.databaseID != 0
}

// readSnapshotPublicationRow performs the exact mo_pubs read of
// resolveAccountFromPublication and returns every column. The statement
// account must still be in account_list; missingAsNil controls the
// missing-row outcome so the post-frontier revalidation can classify drift.
func readSnapshotPublicationRow(
	ctx context.Context,
	bh BackgroundExec,
	pubAccountName string,
	pubName string,
	currentAccount string,
	lock bool,
	missingAsNil bool,
) (row *snapshotPublicationRow, err error) {
	// Query mo_pubs to get publication info and verify permission
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	queryPubSQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, strings.ReplaceAll(pubAccountName, "'", "''"), strings.ReplaceAll(pubName, "'", "''"))
	if lock {
		// ALTER/DROP PUBLICATION updates this row. Hold its lock through the
		// snapshot transaction so membership cannot change after authorization.
		queryPubSQL += " for update"
	}

	bh.ClearExecResultSet()
	err = bh.Exec(systemCtx, queryPubSQL)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query publication info: %v", err)
	}

	erArray, err := getResultSet(systemCtx, bh)
	if err != nil {
		return nil, err
	}

	if !execResultArrayHasData(erArray) {
		if missingAsNil {
			return nil, nil
		}
		return nil, moerr.NewInternalErrorf(ctx, "publication %s.%s does not exist", pubAccountName, pubName)
	}

	row = &snapshotPublicationRow{}
	var accountList string
	var accountID uint64
	for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
		if accountID, err = erArray[0].GetUint64(ctx, i, 0); err != nil {
			return nil, err
		}
		if row.accountName, err = erArray[0].GetString(ctx, i, 1); err != nil {
			return nil, err
		}
		if row.pubName, err = erArray[0].GetString(ctx, i, 2); err != nil {
			return nil, err
		}
		if row.databaseName, err = erArray[0].GetString(ctx, i, 3); err != nil {
			return nil, err
		}
		if row.databaseID, err = erArray[0].GetUint64(ctx, i, 4); err != nil {
			return nil, err
		}
		if row.tableList, err = erArray[0].GetString(ctx, i, 5); err != nil {
			return nil, err
		}
		if accountList, err = erArray[0].GetString(ctx, i, 6); err != nil {
			return nil, err
		}
	}
	if accountID > uint64(^uint32(0)) {
		return nil, moerr.NewInternalErrorf(ctx, "publication %s.%s has invalid account id %d", pubAccountName, pubName, accountID)
	}
	row.accountID = uint32(accountID)
	row.accountList = accountList

	// Check if current account has permission to access this publication
	pubInfo := &pubsub.PubInfo{
		SubAccountsStr: accountList,
	}
	if !pubInfo.InSubAccounts(currentAccount) {
		return nil, moerr.NewInternalErrorf(ctx, "account %s does not have permission to access publication %s.%s", currentAccount, pubAccountName, pubName)
	}

	return row, nil
}

// resolveAccountFromPublication keeps its historical signature; the row read
// and membership check are shared with the snapshot publication protocol.
func resolveAccountFromPublication(ctx context.Context, bh BackgroundExec, pubAccountName string, pubName string, currentAccount string, lock bool) (accountID uint64, accountName string, err error) {
	row, err := readSnapshotPublicationRow(ctx, bh, pubAccountName, pubName, currentAccount, lock, false)
	if err != nil {
		return 0, "", err
	}
	return uint64(row.accountID), row.accountName, nil
}

// deriveSnapshotPublicationSource resolves the physical source owner P_src
// from a publication row (design §4.4 step 1) and applies the shared
// foreign-source gate (design §4.4 step 2, finding 10): database-scoped rows
// resolve through the persisted (D, database_id); account-covering rows bind
// the publisher and admit no foreign source by construction.
func deriveSnapshotPublicationSource(
	ctx context.Context,
	bh BackgroundExec,
	row *snapshotPublicationRow,
) (physID uint32, err error) {
	if !row.databaseScoped() {
		return row.accountID, nil
	}
	physID, _, err = getDbAccountIdAndTypeById(ctx, bh, row.databaseName, row.databaseID)
	if err != nil {
		return 0, err
	}
	if physID != row.accountID {
		authorized := row.accountID == sysAccountID
		if !authorized {
			admitted, admErr := publicationTargetsAdmitForeignSource(
				ctx, bh, &pubsub.PubInfo{SubAccountsStr: row.accountList}, physID)
			if admErr != nil {
				return 0, admErr
			}
			authorized = admitted
		}
		if !authorized {
			return 0, publicationForeignSourceUnauthorizedError(ctx, row.accountID, physID, row.databaseName)
		}
	}
	return physID, nil
}

// resolveAccountNameByID resolves one account's name through the system
// catalog (clone_database_source.go precedent).
func resolveAccountNameByID(ctx context.Context, bh BackgroundExec, accountID uint32) (string, error) {
	sql := fmt.Sprintf("select account_name from mo_catalog.mo_account where account_id = %d", accountID)
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(systemCtx, sql); err != nil {
		return "", err
	}
	results, err := getResultSet(systemCtx, bh)
	if err != nil {
		return "", err
	}
	if !execResultArrayHasData(results) {
		return "", moerr.NewInternalErrorf(ctx, "account %d does not exist", accountID)
	}
	return results[0].GetString(systemCtx, 0, 0)
}

// snapshotPublicationBinding carries the §4.4 provisional facts of a
// publication-routed CREATE: the locked publication row, the resolved
// physical owner, and, for the subscription alias form, the subscriber
// binding facts that the post-frontier revalidation compares (finding 12).
type snapshotPublicationBinding struct {
	row      *snapshotPublicationRow
	physID   uint32 // P_src
	physName string

	subscriberID   uint32
	subscriberName string
	// aliasDb/aliasDbID describe the subscriber database N (alias form only):
	// its spelling and its persisted database id under S.
	aliasDb   string
	aliasDbID uint64
}

func (b *snapshotPublicationBinding) aliasForm() bool {
	return b.aliasDb != ""
}

// resolveSnapshotPublicationBinding is the §4.4 step-1 provisional
// resolution: membership and P_pub via the mo_pubs row read, the shape-aware
// physical owner, and the shared foreign-source gate when the source differs
// from the publisher. Both identities stay provisional until the
// post-frontier revalidation.
func resolveSnapshotPublicationBinding(
	ctx context.Context,
	bh BackgroundExec,
	subscriberID uint32,
	subscriberName string,
	pubAccountName string,
	pubName string,
) (*snapshotPublicationBinding, error) {
	row, err := readSnapshotPublicationRow(ctx, bh, pubAccountName, pubName, subscriberName, false, false)
	if err != nil {
		return nil, err
	}
	physID, err := deriveSnapshotPublicationSource(ctx, bh, row)
	if err != nil {
		return nil, err
	}
	physName := row.accountName
	if physID != row.accountID {
		if physName, err = resolveAccountNameByID(ctx, bh, physID); err != nil {
			return nil, err
		}
	}
	return &snapshotPublicationBinding{
		row:            row,
		physID:         physID,
		physName:       physName,
		subscriberID:   subscriberID,
		subscriberName: subscriberName,
	}, nil
}

// revalidateSnapshotPublicationBinding is the §4.4 step-5 post-frontier
// revalidation. The mo_pubs shared lock has been held since admission, so
// the re-read sees the locked row; every authorization fact is re-derived
// from it. Drift fails closed with a retry-class error before any write;
// for the alias form the subscriber binding re-reads (finding 12) use the
// same discipline while the (S, N) key is held.
func revalidateSnapshotPublicationBinding(
	ctx context.Context,
	bh BackgroundExec,
	b *snapshotPublicationBinding,
) error {
	reread, err := readSnapshotPublicationRow(
		ctx, bh, b.row.accountName, b.row.pubName, b.subscriberName, false, true)
	if err != nil {
		return err
	}
	if reread == nil ||
		reread.accountID != b.row.accountID ||
		reread.databaseName != b.row.databaseName ||
		reread.databaseID != b.row.databaseID ||
		reread.tableList != b.row.tableList {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	physID, err := deriveSnapshotPublicationSource(ctx, bh, reread)
	if err != nil {
		return err
	}
	if physID != b.physID {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}

	if b.aliasForm() {
		aliasID, _, err := getDbIdAndTypeForAccount(ctx, bh, b.aliasDb, b.subscriberID)
		if err != nil || aliasID != b.aliasDbID {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		subCtx := defines.AttachAccountId(ctx, b.subscriberID)
		infos, _, err := getSubInfosFromSubWithOptions(subCtx, bh, b.aliasDb, true, 2)
		if err != nil {
			return err
		}
		if len(infos) != 1 ||
			infos[0].PubAccountName != b.row.accountName ||
			infos[0].PubName != b.row.pubName {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
	}
	return nil
}

// admitPublicationLifecycleRC is the two-identity variant of
// admitLocalLifecycleRC (design §4.4 step 3): the D/T/Q keys bind the
// physical source owner, the mo_pubs key binds the publication account, and
// the alias form prepends the subscriber database key (S, N) at the head of
// the lock order (finding 12). With aliasDb == "" and
// physAccountID == publisherAccountID the emitted lock-key sequence equals
// admitLocalLifecycleRC's byte for byte.
func admitPublicationLifecycleRC(
	ctx context.Context, ses *Session, bh BackgroundExec,
	subscriberAccountID uint32, aliasDb string,
	physAccountID uint32, publisherAccountID uint32,
	databaseName, snapshotScope string, databaseMode lock.LockMode,
	publicationName, tableName string,
) error {
	return admitLifecycleRCSplit(
		ctx, ses, bh, subscriberAccountID, aliasDb,
		physAccountID, publisherAccountID, databaseName, snapshotScope,
		databaseMode, publicationName, tableName,
	)
}

// lockPublicationSnapshotOwner resolves the publication-routed DROP owner
// (design §4.5 step 1): today's lockAccountFromPublication spine (session
// generation plus the FOR UPDATE membership row), then the shape-aware
// physical owner and the shared gate on the locked row.
func lockPublicationSnapshotOwner(
	ctx context.Context,
	bh BackgroundExec,
	pubAccountName string,
	pubName string,
	subscriberName string,
	subscriberID uint32,
) (row *snapshotPublicationRow, physID uint32, err error) {
	if err = checkPublicationSubscriberGeneration(ctx, bh, subscriberName, subscriberID); err != nil {
		return nil, 0, err
	}
	row, err = readSnapshotPublicationRow(ctx, bh, pubAccountName, pubName, subscriberName, true, false)
	if err != nil {
		return nil, 0, err
	}
	physID, err = deriveSnapshotPublicationSource(ctx, bh, row)
	if err != nil {
		return nil, 0, err
	}
	return row, physID, nil
}

// bindPublicationSnapshotRecord gates a publication-routed DROP SNAPSHOT by
// the record's own identity and the publication's coverage (design §4.5 step
// 2, I7, findings 4/7/8/9/10/11/13/14/15). Any record failing a rule fails
// with the scope error and is left intact; nothing is written.
func bindPublicationSnapshotRecordForDrop(
	ctx context.Context,
	bh BackgroundExec,
	row *snapshotPublicationRow,
	physID uint32,
	record *snapshotRecord,
) error {
	reject := func() error {
		return moerr.NewInternalErrorf(ctx,
			"snapshot %s is outside the publication's scope", record.snapshotName)
	}

	physName := row.accountName
	if physID != row.accountID {
		var err error
		if physName, err = resolveAccountNameByID(ctx, bh, physID); err != nil {
			return err
		}
	}
	if record.accountName != physName {
		return reject()
	}
	physCtx := defines.AttachAccountId(ctx, physID)

	// resolveAnchoredDatabaseID implements the account-covering identity read
	// shared by the DATABASE and TABLE rules: only a database that currently
	// exists with the recorded identity in the publisher's catalog binds.
	resolveAnchoredDatabaseID := func(dbName string) (uint64, error) {
		dbID, _, err := getDbIdAndTypeForAccount(ctx, bh, dbName, row.accountID)
		if err != nil {
			return 0, reject()
		}
		return dbID, nil
	}

	switch record.level {
	case tree.SNAPSHOTLEVELDATABASE.String():
		var anchoredDbID uint64
		if row.databaseScoped() {
			// Coverage first (finding 11): the deletion-side counterpart of
			// the creation boundary — a table-limited publication never
			// deletes a database snapshot.
			if row.tableList != pubsub.TableAll {
				return reject()
			}
			anchoredDbID = row.databaseID
		} else {
			var err error
			if anchoredDbID, err = resolveAnchoredDatabaseID(record.databaseName); err != nil {
				return err
			}
		}
		if record.objId != anchoredDbID {
			return reject()
		}
		return nil

	case tree.SNAPSHOTLEVELTABLE.String():
		// Coverage: the recorded table must be inside the publication's list
		// and anchored to the publication's database (the record's own
		// qualifier for account-covering rows).
		if row.tableList != pubsub.TableAll &&
			!slices.Contains(strings.Split(row.tableList, pubsub.Sep), record.tableName) {
			return reject()
		}
		anchoredDbName := row.databaseName
		var anchoredDbID uint64
		if row.databaseScoped() {
			anchoredDbID = row.databaseID
		} else {
			anchoredDbName = record.databaseName
			var err error
			if anchoredDbID, err = resolveAnchoredDatabaseID(record.databaseName); err != nil {
				return err
			}
		}
		if record.databaseName != anchoredDbName {
			return reject()
		}

		// Identity (findings 13/14, C10): bind the record's logical table id
		// against the current catalog row behind the published name; COPY
		// ALTER keeps the logical id, drop/recreate mints a new one.
		curSQL, err := getSqlForCheckDatabaseTableWithSnapshot(
			ctx, anchoredDbName, record.tableName, physID, 0)
		if err != nil {
			return err
		}
		bh.ClearExecResultSet()
		if err = bh.Exec(physCtx, curSQL); err != nil {
			return err
		}
		curRows, err := getResultSet(physCtx, bh)
		if err != nil {
			return err
		}
		if execResultArrayHasData(curRows) {
			if curRows[0].GetRowCount() != 1 {
				return reject()
			}
			logicalID, err := curRows[0].GetUint64(physCtx, 0, 0)
			if err != nil {
				return err
			}
			if record.objId != logicalID {
				return reject()
			}
			return nil
		}

		// The authorized name no longer resolves. Name-only lists cannot
		// testify which logical identity they authorized (finding 15): the
		// historical fallback is admissible only under whole-database
		// coverage; table-limited records in this state fail closed and
		// revert to the record owner's own DROP SNAPSHOT.
		if row.tableList != pubsub.TableAll {
			return reject()
		}
		histSQL := fmt.Sprintf(
			"select reldatabase_id from mo_catalog.mo_tables {MO_TS = %d} where rel_logical_id = %d and account_id = %d;",
			record.ts, record.objId, physID,
		)
		bh.ClearExecResultSet()
		if err = bh.Exec(physCtx, histSQL); err != nil {
			return err
		}
		histRows, err := getResultSet(physCtx, bh)
		if err != nil {
			return err
		}
		if !execResultArrayHasData(histRows) || histRows[0].GetRowCount() != 1 {
			return reject()
		}
		histDbID, err := histRows[0].GetUint64(physCtx, 0, 0)
		if err != nil {
			return err
		}
		if histDbID != anchoredDbID {
			return reject()
		}
		return nil

	case tree.SNAPSHOTLEVELACCOUNT.String():
		// Only account-covering publications bind account snapshots, and only
		// of the publisher's own account.
		if row.databaseScoped() || record.objId != uint64(row.accountID) {
			return reject()
		}
		return nil

	default:
		// CLUSTER and unknown levels are never deletable through a
		// publication.
		return reject()
	}
}
