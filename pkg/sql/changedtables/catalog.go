// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package changedtables

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlutil "github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	watchDisabled = "changed-table watch is disabled or not provisioned"

	// Keep the internal result bounded independently of the number of rows.
	watchCatalogMaxResultBytes     = 128 << 20
	watchPartitionIDBatchSize      = 128
	watchPartitionLookupResultSize = 64 << 10
)

type watchCatalog struct {
	binding     Binding
	grants      []Grant
	sourceNames map[uint32]string
}

func readCatalog(
	ctx context.Context,
	exec executor.SQLExecutor,
	caller callerIdentity,
	observerName *string,
	cut timestamp.Timestamp,
) (watchCatalog, error) {
	if err := validateWatchPrincipal(ctx, caller, observerName); err != nil {
		return watchCatalog{}, err
	}
	if cut.PhysicalTime <= 0 {
		return watchCatalog{}, moerr.NewInternalError(ctx, "invalid changed-table catalog watermark")
	}

	// exec owns a new transaction and its workspace. A parent snapshot
	// strictly after cut forces the planner and operators to bind the exact
	// historical timestamp. The catalog cache and the data reader include
	// entries committed at cut and exclude commits at cut.Next().
	hint := fmt.Sprintf("{MO_TS = '%s'}", cut.DebugString())
	systemCtx := defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	opts := executor.Options{}.
		WithAccountID(catalog.System_Account).
		WithTxnIsolation(txn.TxnIsolation_SI).
		WithMinCommittedTS(cut)
	var result watchCatalog
	err := exec.ExecTxn(systemCtx, func(te executor.TxnExecutor) error {
		if te.Txn() == nil || !te.Txn().SnapshotTS().Greater(cut) {
			return moerr.NewInternalError(ctx, "changed-table catalog transaction did not start after its read watermark")
		}
		if err := checkWatchRole(ctx, te, caller, hint); err != nil {
			return err
		}
		epoch, err := readWatchEpoch(ctx, te, hint)
		if err != nil {
			return err
		}
		observer, err := readWatchObserver(ctx, te, caller.accountID, observerName, hint)
		if err != nil {
			return err
		}
		result.binding = Binding{Epoch: epoch, Observer: observer}
		result.grants, result.sourceNames, err = readWatchGrants(ctx, te, observer.AccountID, hint)
		return err
	}, opts)
	if err != nil {
		return watchCatalog{}, err
	}
	return result, nil
}

func validateWatchPrincipal(ctx context.Context, caller callerIdentity, observerName *string) error {
	if caller.accountID == catalog.System_Account {
		if caller.roleID != catalog.System_Role {
			return moerr.NewInvalidInput(ctx, "permission denied for mo_changed_tables")
		}
		if observerName == nil || *observerName == "" {
			return moerr.NewInvalidInput(ctx, "mo_changed_tables requires an observer name for sys callers")
		}
		return nil
	}
	// Tenant bootstrap assigns accountadmin ID 2. The catalog check below also
	// verifies the role name and a live grant to the authenticated user.
	if caller.roleID != 2 || observerName != nil {
		return moerr.NewInvalidInput(ctx, "permission denied for mo_changed_tables")
	}
	return nil
}

func checkWatchRole(ctx context.Context, te executor.TxnExecutor, caller callerIdentity, hint string) error {
	roleName := "accountadmin"
	if caller.accountID == catalog.System_Account {
		roleName = "moadmin"
	}
	query := fmt.Sprintf(`select r.role_id
from mo_catalog.mo_role %[1]s r
join mo_catalog.mo_user_grant %[1]s g on g.role_id = r.role_id
join mo_catalog.mo_user %[1]s u on u.user_id = g.user_id
where r.role_id = ? and binary r.role_name = binary ? and u.user_id = ?
limit 2`, hint)
	opts := executor.StatementOption{}.
		WithAccountID(caller.accountID).
		WithDisableLog().
		WithParams([]string{watchAccountID(caller.roleID), roleName, watchAccountID(caller.userID)})
	res, err := te.Exec(query, opts)
	if err != nil {
		return err
	}
	defer res.Close()
	if watchResultRows(res) != 1 {
		return moerr.NewInvalidInput(ctx, "permission denied for mo_changed_tables")
	}
	return nil
}

func readWatchEpoch(ctx context.Context, te executor.TxnExecutor, hint string) ([16]byte, error) {
	query := fmt.Sprintf(`select recovery_epoch, enabled from mo_change_watch.state %s where singleton = 1 limit 2`, hint)
	res, err := te.Exec(query, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) || moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return [16]byte{}, moerr.NewNotSupported(ctx, watchDisabled)
		}
		return [16]byte{}, err
	}
	defer res.Close()

	if watchResultRows(res) == 0 {
		return [16]byte{}, moerr.NewNotSupported(ctx, watchDisabled)
	}
	columns, err := watchSingletonColumns(ctx, res, types.T_uuid, types.T_bool)
	if err != nil {
		return [16]byte{}, err
	}
	if !vector.GetFixedAtNoTypeCheck[bool](columns[1], 0) {
		return [16]byte{}, moerr.NewNotSupported(ctx, watchDisabled)
	}
	epoch := vector.GetFixedAtNoTypeCheck[types.Uuid](columns[0], 0)
	if epoch == ([16]byte{}) {
		return [16]byte{}, moerr.NewInvalidInput(ctx, "changed-table watch recovery epoch must be nonzero")
	}
	return epoch, nil
}

func readWatchObserver(
	ctx context.Context,
	te executor.TxnExecutor,
	accountID uint32,
	name *string,
	hint string,
) (Observer, error) {
	where, value := "account_id = ?", watchAccountID(accountID)
	if name != nil {
		where, value = "binary account_name = binary ?", *name
	}
	query := fmt.Sprintf(`select account_id, created_time from mo_catalog.mo_account %s
where status = 'open' and account_id > 0 and %s limit 2`, hint, where)
	res, err := te.Exec(query, executor.StatementOption{}.WithDisableLog().WithParams([]string{value}))
	if err != nil {
		return Observer{}, err
	}
	defer res.Close()
	if watchResultRows(res) == 0 {
		return Observer{}, moerr.NewInvalidInput(ctx, "mo_changed_tables observer account is unavailable")
	}
	columns, err := watchSingletonColumns(ctx, res, types.T_int32, types.T_timestamp)
	if err != nil {
		return Observer{}, err
	}
	id := vector.GetFixedAtNoTypeCheck[int32](columns[0], 0)
	created := vector.GetFixedAtNoTypeCheck[types.Timestamp](columns[1], 0)
	if id <= 0 || created <= 0 {
		return Observer{}, moerr.NewInvalidInput(ctx, "invalid changed-table observer account identity")
	}
	return Observer{AccountID: uint32(id), Incarnation: uint64(created)}, nil
}

func readWatchGrants(
	ctx context.Context,
	te executor.TxnExecutor,
	observerID uint32,
	hint string,
) ([]Grant, map[uint32]string, error) {
	// Match canonical bytes, retain missing tables for their baseline and
	// exclude inactive sources. Repeat the system database predicate on both
	// catalog scans so the planner can push it through the outer joins.
	systemDatabases := strings.Join(catalog.SystemDatabases, "','")
	query := fmt.Sprintf(`select w.source_account_id, s.account_name, w.database_name, w.table_name,
       w.grant_id, d.dat_id, t.rel_id, t.partitioned, t.extra_info
from mo_change_watch.watch %[1]s w
join mo_catalog.mo_account %[1]s s on s.account_id = w.source_account_id and s.status = 'open'
left join mo_catalog.mo_database %[1]s d
  on d.account_id = w.source_account_id
 and binary d.datname = binary w.database_name
 and coalesce(d.dat_type, '') <> 'subscription'
 and lower(d.datname) not in ('%[3]s')
 and (d.account_id <> 0 or lower(d.datname) <> 'mo_change_watch')
left join mo_catalog.mo_tables %[1]s t
  on t.account_id = w.source_account_id and t.reldatabase_id = d.dat_id
 and binary t.relname = binary w.table_name and t.relkind = 'r'
 and lower(t.reldatabase) not in ('%[3]s')
where w.observer_account_id = ?
limit %[2]d`, hint, MaxTables+1, systemDatabases)
	opts := executor.StatementOption{}.WithDisableLog().
		WithMaxResultBytes(watchCatalogMaxResultBytes).
		WithParams([]string{watchAccountID(observerID)})
	res, err := te.Exec(query, opts)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) || moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return nil, nil, moerr.NewNotSupported(ctx, watchDisabled)
		}
		return nil, nil, err
	}
	grants, names, err := decodeWatchGrants(ctx, res)
	res.Close()
	if err != nil {
		return nil, nil, err
	}
	if err := checkWatchLegacyPartitions(ctx, te, hint, grants); err != nil {
		return nil, nil, err
	}
	return grants, names, nil
}

func checkWatchLegacyPartitions(ctx context.Context, te executor.TxnExecutor, hint string, grants []Grant) error {
	// Names matching the historical convention can also be created by ordinary
	// DDL. Only relations that survived the durable kind and flag checks need
	// this lookup in the source account's partition catalog.
	var bySource map[uint32][]int
	for i, grant := range grants {
		if grant.TableID == 0 {
			continue
		}
		if candidate, _, _ := sqlutil.SplitNameOfPartitionTable(grant.TableName); !candidate {
			continue
		}
		if bySource == nil {
			bySource = make(map[uint32][]int)
		}
		bySource[grant.AccountID] = append(bySource[grant.AccountID], i)
	}

	for account, indexes := range bySource {
		for candidates := range slices.Chunk(indexes, watchPartitionIDBatchSize) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := filterWatchPartitionIDs(ctx, te, hint, account, grants, candidates); err != nil {
				return err
			}
		}
	}
	return nil
}

func filterWatchPartitionIDs(
	ctx context.Context,
	te executor.TxnExecutor,
	hint string,
	account uint32,
	grants []Grant,
	indexes []int,
) error {
	query := fmt.Sprintf("select partition_id from mo_catalog.%s %s where partition_id in (%scast(? as unsigned)) limit %d",
		catalog.MOPartitionTables, hint, strings.Repeat("cast(? as unsigned), ", len(indexes)-1), len(indexes)+1)
	params := make([]string, len(indexes))
	matched := make(map[uint64]bool, len(indexes))
	for i, index := range indexes {
		id := grants[index].TableID
		params[i] = strconv.FormatUint(id, 10)
		matched[id] = false
	}
	opts := executor.StatementOption{}.WithAccountID(account).WithDisableLog().
		WithMaxResultBytes(watchPartitionLookupResultSize).WithParams(params)
	res, err := te.Exec(query, opts)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) || moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return moerr.NewNotSupportedf(ctx, "mo_changed_tables cannot verify partition identity for source %d: mo_catalog.mo_partition_tables is unavailable", account)
		}
		return err
	}
	defer res.Close()
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if !watchResultTypes(cols, types.T_uint64) {
			err = moerr.NewInternalError(ctx, "unexpected changed-table partition catalog result")
			return false
		}
		for row := range rows {
			if cols[0].IsNull(uint64(row)) {
				err = moerr.NewInternalError(ctx, "NULL changed-table partition identity")
				return false
			}
			id := vector.GetFixedAtNoTypeCheck[uint64](cols[0], row)
			alreadyMatched, requested := matched[id]
			if !requested || alreadyMatched {
				err = moerr.NewInternalError(ctx, "unexpected changed-table partition identity")
				return false
			}
			matched[id] = true
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if matched[grants[index].TableID] {
			grants[index].TableID = 0
		}
	}
	return nil
}

func decodeWatchGrants(ctx context.Context, res executor.Result) ([]Grant, map[uint32]string, error) {
	numRows := watchResultRows(res)
	if numRows > MaxTables {
		return nil, nil, moerr.NewInvalidInput(ctx, "mo_changed_tables exceeds the registered table limit")
	}
	grants := make([]Grant, 0, numRows)
	sources := make(map[uint32]string)
	var decodeErr error
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if len(cols) != 9 || !watchResultTypes(cols[:8], types.T_uint32, types.T_varchar, types.T_varchar,
			types.T_varchar, types.T_uuid, types.T_uint64, types.T_uint64, types.T_int8) ||
			cols[8] == nil || (cols[8].GetType().Oid != types.T_varchar && cols[8].GetType().Oid != types.T_text) {
			decodeErr = moerr.NewInvalidInput(ctx, "invalid changed-table registration schema")
			return false
		}
		for row := range n {
			grant, name, err := decodeWatchGrant(ctx, cols, row)
			if err != nil {
				decodeErr = err
				return false
			}
			if prior, exists := sources[grant.AccountID]; !exists {
				if len(sources) >= MaxSources {
					decodeErr = moerr.NewInvalidInput(ctx, "mo_changed_tables exceeds the registered source limit")
					return false
				}
				sources[grant.AccountID] = strings.Clone(name)
			} else if prior != name {
				decodeErr = moerr.NewInvalidInput(ctx, "inconsistent changed-table source identity")
				return false
			}
			grants = append(grants, grant)
		}
		return true
	})
	if decodeErr != nil {
		return nil, nil, decodeErr
	}
	return grants, sources, nil
}

func decodeWatchGrant(ctx context.Context, cols []*vector.Vector, row int) (Grant, string, error) {
	for _, v := range cols[:5] {
		if v.IsNull(uint64(row)) {
			return Grant{}, "", moerr.NewInvalidInput(ctx, "NULL changed-table registration field")
		}
	}
	grant := Grant{
		AccountID:    vector.GetFixedAtNoTypeCheck[uint32](cols[0], row),
		DatabaseName: cols[2].GetStringAt(row),
		TableName:    cols[3].GetStringAt(row),
		GrantID:      vector.GetFixedAtNoTypeCheck[types.Uuid](cols[4], row),
	}
	sourceName := cols[1].UnsafeGetStringAt(row)
	if sourceName == "" || grant.DatabaseName == "" || grant.TableName == "" || grant.GrantID == ([16]byte{}) {
		return Grant{}, "", moerr.NewInvalidInput(ctx, "invalid changed-table registration field")
	}
	if !cols[5].IsNull(uint64(row)) {
		grant.DatabaseID = vector.GetFixedAtNoTypeCheck[uint64](cols[5], row)
	}
	if cols[6].IsNull(uint64(row)) {
		if !cols[7].IsNull(uint64(row)) || !cols[8].IsNull(uint64(row)) {
			return Grant{}, "", invalidWatchRelation(ctx, grant)
		}
		return grant, sourceName, nil
	}
	tableID := vector.GetFixedAtNoTypeCheck[uint64](cols[6], row)
	if tableID == 0 || grant.DatabaseID == 0 || cols[7].IsNull(uint64(row)) {
		return Grant{}, "", invalidWatchRelation(ctx, grant)
	}
	legacyPartitioned := vector.GetFixedAtNoTypeCheck[int8](cols[7], row)
	if legacyPartitioned != 0 && legacyPartitioned != 1 {
		return Grant{}, "", invalidWatchRelation(ctx, grant)
	}
	var extra api.SchemaExtra
	// Old ordinary catalog rows may have NULL or empty extra metadata.
	if !cols[8].IsNull(uint64(row)) {
		if err := extra.Unmarshal(cols[8].GetBytesAt(row)); err != nil {
			return Grant{}, "", invalidWatchRelation(ctx, grant)
		}
	}
	partitioned := legacyPartitioned == 1 || features.IsPartitioned(extra.FeatureFlag)
	child := features.IsPartition(extra.FeatureFlag)
	internal := catalog.IsHiddenTable(grant.TableName) ||
		catalog.RequiresPrivilegedRelationAccess(grant.TableName, catalog.SystemOrdinaryRel, extra.FeatureFlag)
	if internal {
		return grant, sourceName, nil
	}
	if partitioned && (child || extra.ParentTableID != 0) {
		return Grant{}, "", invalidWatchRelation(ctx, grant)
	}
	if child && (extra.ParentTableID == 0 || extra.ParentTableID == tableID) {
		return Grant{}, "", invalidWatchRelation(ctx, grant)
	}
	if child {
		return grant, sourceName, nil
	}
	if extra.ParentTableID != 0 {
		return Grant{}, "", invalidWatchRelation(ctx, grant)
	}
	if partitioned {
		return Grant{}, "", moerr.NewNotSupportedf(ctx,
			"mo_changed_tables cannot watch partitioned source %d granted %q.%q; remove the partitioned table registration to continue",
			grant.AccountID, grant.DatabaseName, grant.TableName)
	}
	grant.TableID = tableID
	return grant, sourceName, nil
}

func invalidWatchRelation(ctx context.Context, grant Grant) error {
	return moerr.NewInvalidInputf(ctx, "inconsistent changed-table relation identity for source %d granted %q.%q",
		grant.AccountID, grant.DatabaseName, grant.TableName)
}

func watchResultRows(res executor.Result) int {
	n := 0
	res.ReadRows(func(rows int, _ []*vector.Vector) bool {
		n += rows
		return true
	})
	return n
}

func watchSingletonColumns(ctx context.Context, res executor.Result, types ...types.T) ([]*vector.Vector, error) {
	var cols []*vector.Vector
	res.ReadRows(func(rows int, batchCols []*vector.Vector) bool {
		if rows > 0 {
			cols = batchCols
			return false
		}
		return true
	})
	if watchResultRows(res) != 1 || !watchResultTypes(cols, types...) {
		return nil, moerr.NewInvalidInput(ctx, "unexpected changed-table metadata result")
	}
	for _, col := range cols {
		if col.IsNull(0) {
			return nil, moerr.NewInvalidInput(ctx, "NULL changed-table metadata field")
		}
	}
	return cols, nil
}

func watchResultTypes(cols []*vector.Vector, oids ...types.T) bool {
	if len(cols) != len(oids) {
		return false
	}
	for i, oid := range oids {
		if cols[i] == nil || cols[i].GetType().Oid != oid {
			return false
		}
	}
	return true
}

func watchAccountID(id uint32) string {
	return strconv.FormatUint(uint64(id), 10)
}
