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

package compile

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the D152/D155-A owner-coverage contract of the
// drop-lifecycle funnels: coverage is ONE predicate evaluated over three
// disjoint row sources (the tenant's own snapshots, SYS-owned snapshots,
// and active mo_pitr rows), every probe failure keeps the links (fail
// ALIVE), and the keep/delete verdicts retire exactly the right link
// ranges. Every mock is SQL-shape driven on purpose: the same test file
// runs against the pre-extension tree (where the sys-owned halves do not
// exist and the same scenarios retire the links — the documented RED) and
// the post-extension tree (where they keep the links — GREEN), which is
// what makes the pre-fix failure evidence meaningful.

// pkMoveMockHandler answers any executed SQL whose text starts with
// prefix; unmatched SQLs fall through to an empty success, mirroring the
// long-standing drop-lifecycle harness convention.
type pkMoveMockHandler struct {
	prefix string
	fn     func(sql string) (executor.Result, error)
}

func newPkMoveOwnerMockCompile(
	t *testing.T, ctrl *gomock.Controller, handlers []pkMoveMockHandler,
) (c *Compile, gotSql *[]string) {
	t.Helper()
	got := []string{}
	exec := &dropDDLExecutor{}
	exec.exec = func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		got = append(got, sql)
		for _, h := range handlers {
			if strings.HasPrefix(sql, h.prefix) {
				return h.fn(sql)
			}
		}
		return executor.Result{}, nil
	}
	c, _ = newDropDDLCompile(t, ctrl, exec)
	return c, &got
}

// Result builders. All builders take parallel plain-Go columns so the file
// stays free of coverage-row struct literals (the struct's field naming
// differs across the pre/post trees; SQL shapes do not).

func pkMoveStrColResult(t *testing.T, c *Compile, vals []string) executor.Result {
	t.Helper()
	memRes := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, c.proc.Mp())
	memRes.NewBatchWithRowCount(len(vals))
	if len(vals) > 0 {
		require.NoError(t, executor.AppendStringRows(memRes, 0, vals))
	}
	return memRes.GetResult()
}

func pkMoveStrPairResult(t *testing.T, c *Compile, col1, col2 []string) executor.Result {
	t.Helper()
	memRes := executor.NewMemResult(
		[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, c.proc.Mp())
	memRes.NewBatchWithRowCount(len(col1))
	if len(col1) > 0 {
		require.NoError(t, executor.AppendStringRows(memRes, 0, col1))
		require.NoError(t, executor.AppendStringRows(memRes, 1, col2))
	}
	return memRes.GetResult()
}

// pkMoveIdentityResult builds one (database, table, logical id) identity
// projection — the shape of both the wide retention-ledger lookup and the
// mo_tables by-id identity probe (the third column answers the probe's
// coalesce(nullif(rel_logical_id,0),rel_id) expression). A nil logicals
// pads the legacy/unresolved zero marker.
func pkMoveIdentityResult(t *testing.T, c *Compile, dbs, tbls []string, logicals []uint64) executor.Result {
	t.Helper()
	n := len(dbs)
	require.Len(t, tbls, n, "parallel identity columns must match the row count")
	logicals = pkMovePadUints(t, logicals, n)
	memRes := executor.NewMemResult(
		[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()},
		c.proc.Mp())
	memRes.NewBatchWithRowCount(n)
	if n > 0 {
		require.NoError(t, executor.AppendStringRows(memRes, 0, dbs))
		require.NoError(t, executor.AppendStringRows(memRes, 1, tbls))
		require.NoError(t, executor.AppendFixedRows(memRes, 2, logicals))
	}
	return memRes.GetResult()
}

// pkMoveMissingLedgerColumnErr is the missing-column capability signal a
// pre-upgrade catalog answers the wide ledger shape with.
func pkMoveMissingLedgerColumnErr() error {
	return moerr.NewBadFieldError(context.Background(), "logical_id", "mo_data_branch_link_retention")
}

func pkMovePadStrs(t *testing.T, in []string, n int) []string {
	t.Helper()
	if len(in) == 0 {
		return make([]string, n)
	}
	require.Len(t, in, n, "parallel string columns must match the row count")
	return in
}

func pkMovePadUints(t *testing.T, in []uint64, n int) []uint64 {
	t.Helper()
	if len(in) == 0 {
		return make([]uint64, n)
	}
	require.Len(t, in, n, "parallel uint64 columns must match the row count")
	return in
}

// pkMoveSnapshotRowsResult builds a mo_snapshots owner-row result: the
// account-local shape (level, database_name, table_name, obj_id) when
// withAccount is false, the sys-catalog shape (level, account_name,
// database_name, table_name, obj_id) when true. Empty columns pad with
// zero values for convenience in cluster/account rows.
func pkMoveSnapshotRowsResult(
	t *testing.T, c *Compile, withAccount bool,
	levels, accounts, dbs, tbls []string, objs []uint64,
) executor.Result {
	t.Helper()
	n := len(levels)
	accounts, dbs, tbls = pkMovePadStrs(t, accounts, n), pkMovePadStrs(t, dbs, n), pkMovePadStrs(t, tbls, n)
	objs = pkMovePadUints(t, objs, n)
	tys := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}
	if withAccount {
		tys = []types.Type{
			types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_uint64.ToType()}
	}
	memRes := executor.NewMemResult(tys, c.proc.Mp())
	memRes.NewBatchWithRowCount(n)
	if n > 0 {
		require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
		off := 0
		if withAccount {
			require.NoError(t, executor.AppendStringRows(memRes, 1, accounts))
			off = 1
		}
		require.NoError(t, executor.AppendStringRows(memRes, 1+off, dbs))
		require.NoError(t, executor.AppendStringRows(memRes, 2+off, tbls))
		require.NoError(t, executor.AppendFixedRows(memRes, 3+off, objs))
	}
	return memRes.GetResult()
}

// pkMovePitrRowsResult builds the active-mo_pitr owner-row result
// (level, account_id, account_name, database_name, table_name, obj_id).
func pkMovePitrRowsResult(
	t *testing.T, c *Compile,
	levels []string, accountIds []uint64, accounts, dbs, tbls []string, objs []uint64,
) executor.Result {
	t.Helper()
	n := len(levels)
	accountIds, objs = pkMovePadUints(t, accountIds, n), pkMovePadUints(t, objs, n)
	accounts, dbs, tbls = pkMovePadStrs(t, accounts, n), pkMovePadStrs(t, dbs, n), pkMovePadStrs(t, tbls, n)
	tys := []types.Type{
		types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}
	memRes := executor.NewMemResult(tys, c.proc.Mp())
	memRes.NewBatchWithRowCount(n)
	if n > 0 {
		require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
		require.NoError(t, executor.AppendFixedRows(memRes, 1, accountIds))
		require.NoError(t, executor.AppendStringRows(memRes, 2, accounts))
		require.NoError(t, executor.AppendStringRows(memRes, 3, dbs))
		require.NoError(t, executor.AppendStringRows(memRes, 4, tbls))
		require.NoError(t, executor.AppendFixedRows(memRes, 5, objs))
	}
	return memRes.GetResult()
}

const (
	pkMoveLocalSnapshotsSQL        = "select level, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind != 'branch'"
	pkMoveSysSnapshotsSQL          = "select level, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind != 'branch'"
	pkMoveActivePitrsSQL           = "select level, account_id, account_name, database_name, table_name, obj_id from mo_catalog.mo_pitr where pitr_status = 1"
	pkMoveAccountNameSQL           = "select account_name from mo_catalog.mo_account where account_id = "
	pkMoveRetainedTablesSQL        = "select reldatabase, relname, coalesce(nullif(rel_logical_id,0),rel_id) from mo_catalog.mo_tables where rel_id = "
	pkMoveLinkDeletesSQL           = "delete from mo_catalog.mo_data_branch_update_link"
	pkMoveBranchChildrenSQL        = "select table_id, table_deleted from mo_catalog.mo_branch_metadata where p_table_id = "
	pkMoveRetentionLookupSQL       = "select database_name, table_name, logical_id from mo_catalog.mo_data_branch_link_retention where row_kind = 0 and table_id = "
	pkMoveRetentionLookupLegacySQL = "select database_name, table_name from mo_catalog.mo_data_branch_link_retention where table_id = "
)

func countSqlWithPrefix(sqls []string, prefix string) int {
	n := 0
	for _, sql := range sqls {
		if strings.HasPrefix(sql, prefix) {
			n++
		}
	}
	return n
}

func collectSqlWithPrefix(sqls []string, prefix string) []string {
	var out []string
	for _, sql := range sqls {
		if strings.HasPrefix(sql, prefix) {
			out = append(out, sql)
		}
	}
	return out
}

// branchSnapshotCoversTable: the D155-A sys-owned snapshot halves and the
// D152 mo_pitr halves, plus the failure contract of every side.
func TestBranchSnapshotCoversTableOwnerHalves(t *testing.T) {
	ctrl := gomock.NewController(t)
	probeErr := fmt.Errorf("owner catalog unavailable")

	type snapshotSpec struct {
		levels, accounts, dbs, tbls []string
		objs                        []uint64
	}
	type pitrSpec struct {
		levels, accounts, dbs, tbls []string
		accountIds, objs            []uint64
	}
	cases := []struct {
		name                      string
		sys                       *snapshotSpec
		pitr                      *pitrSpec
		localErr, sysErr, pitrErr error
		// nameLookupFails forces the lazy mo_account name lookup to
		// return an error (which must degrade to id-only matching
		// without ever deciding the verdict by itself).
		nameLookupFails bool
		want            bool
		wantErr         bool
		wantNameLookups int
	}{
		{name: "sys cluster snapshot keeps",
			sys:  &snapshotSpec{levels: []string{"cluster"}, accounts: []string{""}, objs: []uint64{^uint64(0)}},
			want: true, wantNameLookups: 0},
		{name: "sys account row keeps by recorded id",
			sys:  &snapshotSpec{levels: []string{"account"}, accounts: []string{"tenant7"}, objs: []uint64{7}},
			want: true, wantNameLookups: 0,
		},
		{name: "sys account row keeps by lazily-resolved name",
			sys:  &snapshotSpec{levels: []string{"account"}, accounts: []string{"tenant7"}, objs: []uint64{0}},
			want: true, wantNameLookups: 1,
		},
		{name: "foreign sys account row does not extend coverage",
			sys:  &snapshotSpec{levels: []string{"account"}, accounts: []string{"other"}, objs: []uint64{8}},
			want: false, wantNameLookups: 1,
		},
		{name: "sys database and table rows are never attributed",
			sys: &snapshotSpec{
				levels:   []string{"database", "table"},
				accounts: []string{"", ""},
				dbs:      []string{"db1", "db1"},
				tbls:     []string{"", "t800"},
				objs:     []uint64{0, 800},
			},
			want: false, wantNameLookups: 0,
		},
		{name: "sys snapshot probe failure keeps (fail ALIVE)",
			sysErr:  probeErr,
			want:    true,
			wantErr: true, wantNameLookups: 0,
		},
		{name: "missing local catalog reads as empty, sys half still covers",
			localErr: moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_snapshots"),
			sys:      &snapshotSpec{levels: []string{"cluster"}, accounts: []string{""}, objs: []uint64{^uint64(0)}},
			want:     true, wantNameLookups: 0,
		},
		{name: "account-name lookup failure degrades to id-only match",
			sys:             &snapshotSpec{levels: []string{"account"}, accounts: []string{"tenant7"}, objs: []uint64{0}},
			nameLookupFails: true,
			want:            false, wantNameLookups: 1,
		},
		{name: "active cluster PITR keeps",
			pitr: &pitrSpec{levels: []string{"cluster"}, accountIds: []uint64{0}, objs: []uint64{^uint64(0)}},
			want: true, wantNameLookups: 0,
		},
		{name: "active account PITR keeps by recorded id",
			pitr: &pitrSpec{levels: []string{"account"}, accountIds: []uint64{7}, accounts: []string{"tenant7"}, objs: []uint64{7}},
			want: true, wantNameLookups: 0,
		},
		{name: "active database PITR scopes to the owning account and database",
			pitr: &pitrSpec{levels: []string{"database"}, accountIds: []uint64{7}, dbs: []string{"db1"}},
			want: true, wantNameLookups: 0,
		},
		{name: "active database PITR of a foreign account does not ground",
			pitr: &pitrSpec{levels: []string{"database"}, accountIds: []uint64{8}, dbs: []string{"db1"}},
			want: false, wantNameLookups: 0,
		},
		{name: "active table PITR keeps by recorded obj id across renames",
			pitr: &pitrSpec{levels: []string{"table"}, accountIds: []uint64{7}, dbs: []string{"old"}, tbls: []string{"old"}, objs: []uint64{800}},
			want: true, wantNameLookups: 0,
		},
		{name: "active table PITR keeps by case-insensitive scope names",
			pitr: &pitrSpec{levels: []string{"table"}, accountIds: []uint64{7}, dbs: []string{"DB1"}, tbls: []string{"T800"}, objs: []uint64{0}},
			want: true, wantNameLookups: 0,
		},
		{name: "pitr probe failure keeps (fail ALIVE)",
			pitrErr: probeErr,
			want:    true,
			wantErr: true, wantNameLookups: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c *Compile
			handlers := []pkMoveMockHandler{}
			if tc.localErr != nil {
				err := tc.localErr
				handlers = append(handlers, pkMoveMockHandler{pkMoveLocalSnapshotsSQL,
					func(string) (executor.Result, error) { return executor.Result{}, err }})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveLocalSnapshotsSQL, func(string) (executor.Result, error) {
					return pkMoveSnapshotRowsResult(t, c, false, nil, nil, nil, nil, nil), nil
				}})
			}
			if tc.sysErr != nil {
				err := tc.sysErr
				handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL,
					func(string) (executor.Result, error) { return executor.Result{}, err }})
			} else if tc.sys != nil {
				sp := tc.sys
				handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
					return pkMoveSnapshotRowsResult(t, c, true, sp.levels, sp.accounts, sp.dbs, sp.tbls, sp.objs), nil
				}})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
					return pkMoveSnapshotRowsResult(t, c, true, nil, nil, nil, nil, nil), nil
				}})
			}
			if tc.pitrErr != nil {
				err := tc.pitrErr
				handlers = append(handlers, pkMoveMockHandler{pkMoveActivePitrsSQL,
					func(string) (executor.Result, error) { return executor.Result{}, err }})
			} else if tc.pitr != nil {
				pp := tc.pitr
				handlers = append(handlers, pkMoveMockHandler{pkMoveActivePitrsSQL, func(string) (executor.Result, error) {
					return pkMovePitrRowsResult(t, c, pp.levels, pp.accountIds, pp.accounts, pp.dbs, pp.tbls, pp.objs), nil
				}})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveActivePitrsSQL, func(string) (executor.Result, error) {
					return pkMovePitrRowsResult(t, c, nil, nil, nil, nil, nil, nil), nil
				}})
			}
			handlers = append(handlers, pkMoveMockHandler{pkMoveAccountNameSQL, func(string) (executor.Result, error) {
				if tc.nameLookupFails {
					return executor.Result{}, probeErr
				}
				// Every case that resolves a name resolves the probing
				// account's own name: id-keyed rows never reach this
				// lookup, and the foreign-name case must still miss.
				return pkMoveStrColResult(t, c, []string{"tenant7"}), nil
			}})
			cNew, got := newPkMoveOwnerMockCompile(t, ctrl, handlers)
			c = cNew
			got2, err := branchSnapshotCoversTable(c, 7, 800, 0, "db1", "t800")
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got2)
			require.Equal(t, tc.wantNameLookups, countSqlWithPrefix(*got, pkMoveAccountNameSQL),
				"lazy account-name resolutions run at most once per probe")
		})
	}
}

// The unbranched-drop funnel honors sys-owned owners exactly like local
// ones: a tenant protected solely by a SYS-catalog cluster snapshot (or an
// active account PITR recorded under SYS) keeps its dropped table's links;
// owners of OTHER accounts never extend that protection.
func TestReclaimUnbranchedDropPkMoveLinksSysOwnedOwners(t *testing.T) {
	ctrl := gomock.NewController(t)
	cases := []struct {
		name       string
		sysRows    func(c *Compile) executor.Result
		pitrRows   func(c *Compile) executor.Result
		wantDelete bool
	}{
		{name: "sys-owned cluster snapshot keeps",
			sysRows: func(c *Compile) executor.Result {
				return pkMoveSnapshotRowsResult(t, c, true,
					[]string{"cluster"}, []string{""}, []string{""}, []string{""}, []uint64{^uint64(0)})
			},
			wantDelete: false},
		{name: "sys-created account snapshot targeting the tenant keeps",
			sysRows: func(c *Compile) executor.Result {
				return pkMoveSnapshotRowsResult(t, c, true,
					[]string{"account"}, []string{"tenant7"}, []string{""}, []string{""}, []uint64{7})
			},
			wantDelete: false},
		{name: "active account PITR targeting the tenant keeps",
			pitrRows: func(c *Compile) executor.Result {
				return pkMovePitrRowsResult(t, c,
					[]string{"account"}, []uint64{7}, []string{"tenant7"}, []string{""}, []string{""}, []uint64{7})
			},
			wantDelete: false},
		{name: "sys-created account snapshot of another tenant reclaims",
			sysRows: func(c *Compile) executor.Result {
				return pkMoveSnapshotRowsResult(t, c, true,
					[]string{"account"}, []string{"other"}, []string{""}, []string{""}, []uint64{8})
			},
			wantDelete: true},
		{name: "foreign table PITR reclaims",
			pitrRows: func(c *Compile) executor.Result {
				return pkMovePitrRowsResult(t, c,
					[]string{"table"}, []uint64{8}, []string{"other"}, []string{"db1"}, []string{"t800"}, []uint64{800})
			},
			wantDelete: true},
		{name: "no owners reclaim",
			wantDelete: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c *Compile
			handlers := []pkMoveMockHandler{}
			handlers = append(handlers, pkMoveMockHandler{pkMoveLocalSnapshotsSQL, func(string) (executor.Result, error) {
				return pkMoveSnapshotRowsResult(t, c, false, nil, nil, nil, nil, nil), nil
			}})
			if tc.sysRows != nil {
				fn := tc.sysRows
				handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL,
					func(string) (executor.Result, error) { return fn(c), nil }})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
					return pkMoveSnapshotRowsResult(t, c, true, nil, nil, nil, nil, nil), nil
				}})
			}
			if tc.pitrRows != nil {
				fn := tc.pitrRows
				handlers = append(handlers, pkMoveMockHandler{pkMoveActivePitrsSQL,
					func(string) (executor.Result, error) { return fn(c), nil }})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveActivePitrsSQL, func(string) (executor.Result, error) {
					return pkMovePitrRowsResult(t, c, nil, nil, nil, nil, nil, nil), nil
				}})
			}
			// The name fallback resolves this account as tenant7 (never
			// reached in id-keyed cases; reachable in the foreign-name
			// case above, which must miss regardless).
			handlers = append(handlers, pkMoveMockHandler{pkMoveAccountNameSQL, func(string) (executor.Result, error) {
				return pkMoveStrColResult(t, c, []string{"tenant7"}), nil
			}})
			cNew, got := newPkMoveOwnerMockCompile(t, ctrl, handlers)
			c = cNew
			require.NoError(t, reclaimUnbranchedDropPkMoveLinks(c, 7, 800, 0, "db1", "t800"))
			linkDeletes := collectSqlWithPrefix(*got, pkMoveLinkDeletesSQL)
			if tc.wantDelete {
				require.Len(t, linkDeletes, 1)
			} else {
				require.Empty(t, linkDeletes,
					"a sys-owned owner covering the dropped table keeps its links")
			}
		})
	}
}

// The protect-snapshot reclaim funnel applies the same shared predicate: a
// member or orphan parent protected only by a SYS-owned account snapshot
// keeps its links, and the uncovered twin still retires.
func TestDeletePkMoveUpdateLinksForReclaimedSubtreesSysOwnedOwner(t *testing.T) {
	ctrl := gomock.NewController(t)

	mk := func(withSysOwner, withPitrOwner bool) []string {
		var c *Compile
		children := map[uint64]map[uint64]bool{
			800: {2: true, 6: true, 9: true, 12: true},
			2:   nil,
			6:   nil,
			9:   nil,
			12:  nil,
		}
		handlers := []pkMoveMockHandler{
			// Orphan parent 800 carries genuine links in this scenario;
			// the funnel's presence probe (r51 review F2) must report one
			// row so the owner-coverage KEEP/retire decisions stay live.
			{pkMoveRetentionProbeSQL, func(string) (executor.Result, error) {
				return pkMoveStrColResult(t, c, []string{"1"}), nil
			}},
			{pkMoveLocalSnapshotsSQL, func(string) (executor.Result, error) {
				return pkMoveSnapshotRowsResult(t, c, false, nil, nil, nil, nil, nil), nil
			}},
			// The reclaim member-name capture pass (r23 F2) reads
			// branch-protect snapshot scopes under SYS; answer it with no
			// rows so earlier-retired members stay identity-unresolved.
			{"select obj_id, database_name, table_name from mo_catalog.mo_snapshots", func(string) (executor.Result, error) {
				memRes := executor.NewMemResult(
					[]types.Type{types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
					c.proc.Mp())
				memRes.NewBatchWithRowCount(0)
				return memRes.GetResult(), nil
			}},
			{pkMoveActivePitrsSQL, func(string) (executor.Result, error) {
				if withPitrOwner {
					return pkMovePitrRowsResult(t, c,
						[]string{"account"}, []uint64{7}, []string{"tenant7"}, []string{""}, []string{""}, []uint64{7}), nil
				}
				return pkMovePitrRowsResult(t, c, nil, nil, nil, nil, nil, nil), nil
			}},
			{pkMoveAccountNameSQL, func(string) (executor.Result, error) {
				return pkMoveStrColResult(t, c, []string{"tenant7"}), nil
			}},
			{pkMoveBranchChildrenSQL, func(sql string) (executor.Result, error) {
				var pid uint64
				_, err := fmt.Sscanf(sql, pkMoveBranchChildrenSQL+"%d", &pid)
				require.NoError(t, err)
				kids := children[pid]
				memRes := executor.NewMemResult(
					[]types.Type{types.T_uint64.ToType(), types.T_bool.ToType()}, c.proc.Mp())
				memRes.NewBatchWithRowCount(len(kids))
				ids := make([]uint64, 0, len(kids))
				dels := make([]bool, 0, len(kids))
				for id, deleted := range kids {
					ids = append(ids, id)
					dels = append(dels, deleted)
				}
				require.NoError(t, executor.AppendFixedRows(memRes, 0, ids))
				require.NoError(t, executor.AppendFixedRows(memRes, 1, dels))
				return memRes.GetResult(), nil
			}},
			{pkMoveRetainedTablesSQL, func(sql string) (executor.Result, error) {
				var relID uint64
				_, err := fmt.Sscanf(sql, pkMoveRetainedTablesSQL+"%d", &relID)
				require.NoError(t, err)
				return pkMoveIdentityResult(t, c,
					[]string{"db1"}, []string{fmt.Sprintf("t%d", relID)}, []uint64{relID}), nil
			}},
		}
		if withSysOwner {
			handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
				return pkMoveSnapshotRowsResult(t, c, true,
					[]string{"account"}, []string{"tenant7"}, []string{""}, []string{""}, []uint64{7}), nil
			}})
		} else {
			handlers = append(handlers, pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
				return pkMoveSnapshotRowsResult(t, c, true, nil, nil, nil, nil, nil), nil
			}})
		}
		cNew, got := newPkMoveOwnerMockCompile(t, ctrl, handlers)
		c = cNew

		rows := []databranchutils.DataBranchMetadata{
			{TableID: 2, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
			{TableID: 6, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
			{TableID: 9, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
			{TableID: 12, PTableID: 800, Creator: 7, Level: "table", TableDeleted: true},
		}
		dag := databranchutils.NewBranchReclaimDag(rows)
		require.NoError(t, deletePkMoveUpdateLinksForReclaimedSubtrees(c, dag, []uint64{2, 6, 9, 12},
			map[uint64]pkMoveTableIdentity{2: {db: "db1", tbl: "t2"}, 6: {db: "db1", tbl: "t6"}}))
		return collectSqlWithPrefix(*got, pkMoveLinkDeletesSQL)
	}

	t.Run("sys-owned account snapshot keeps members and orphan parents", func(t *testing.T) {
		linkDeletes := mk(true, false)
		require.Empty(t, linkDeletes,
			"the sys-created account snapshot targeting the tenant grounds every retained identity")
	})
	t.Run("active account pitr keeps members and orphan parents", func(t *testing.T) {
		linkDeletes := mk(false, true)
		require.Empty(t, linkDeletes)
	})
	t.Run("uncovered members and parents still retire", func(t *testing.T) {
		linkDeletes := mk(false, false)
		// Members 9 and 12 are identity-unresolved and always keep; 2, 6
		// retire by plan identity and orphan parent 800 by the mo_tables
		// fallback identity.
		require.Len(t, linkDeletes, 3)
		lowerBounds := make(map[uint64]bool, len(linkDeletes))
		for _, sql := range linkDeletes {
			var lo uint64
			_, err := fmt.Sscanf(sql, pkMoveLinkDeletesSQL+" where link_id >= x'%016x'", &lo)
			require.NoError(t, err, sql)
			lowerBounds[lo] = true
		}
		require.Equal(t, map[uint64]bool{2: true, 6: true, 800: true}, lowerBounds)
	})
}

// The empty-names branch of the by-id grounded probe consults the
// retention ledger (D154-B): a stored identity keeps coverage decidable
// after mo_tables lost the name, an uncovered stored identity finally
// retires the links, and everything else keeps fail-closed.
func TestBranchHasSnapshotGroundedLineageRetentionLedgerConsult(t *testing.T) {
	ctrl := gomock.NewController(t)
	probeErr := fmt.Errorf("ledger catalog read failed")
	cases := []struct {
		name string
		// ledgerRow: nil pair slices = no row (or error below);
		// non-nil = one (db, tbl) row, possibly empty names.
		ledgerDbs, ledgerTbls []string
		// ledgerLogical is the durable logical id the wide lookup
		// projection carries alongside the names.
		ledgerLogical uint64
		ledgerErr     error
		localCovers   bool
		want          bool
		wantErr       bool
		// The identity handed back for KEEP records (D154-B/D): the
		// resolved names and logical id, or the empty pair for the O1
		// residue class.
		wantDb, wantTbl string
		wantLogical     uint64
	}{
		{name: "stored covered identity keeps", ledgerDbs: []string{"db1"}, ledgerTbls: []string{"t800"},
			ledgerLogical: 800,
			localCovers:   true, want: true, wantDb: "db1", wantTbl: "t800", wantLogical: 800},
		{name: "stored uncovered identity finally retires", ledgerDbs: []string{"db1"}, ledgerTbls: []string{"t800"},
			ledgerLogical: 800,
			localCovers:   false, want: false, wantDb: "db1", wantTbl: "t800", wantLogical: 800},
		{name: "empty-name residue keeps", ledgerDbs: []string{""}, ledgerTbls: []string{""}, want: true},
		{name: "ledger miss keeps", want: true},
		{name: "ledger probe error keeps fail-ALIVE", ledgerErr: probeErr, want: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c *Compile
			handlers := []pkMoveMockHandler{}
			handlers = append(handlers,
				pkMoveMockHandler{pkMoveRetainedTablesSQL, func(string) (executor.Result, error) {
					// mo_tables no longer names the retired table.
					return pkMoveIdentityResult(t, c, nil, nil, nil), nil
				}},
				pkMoveMockHandler{pkMoveLocalSnapshotsSQL, func(string) (executor.Result, error) {
					if tc.localCovers {
						return pkMoveSnapshotRowsResult(t, c, false,
							[]string{"table"}, nil, []string{"db1"}, []string{"t800"}, []uint64{0}), nil
					}
					return pkMoveSnapshotRowsResult(t, c, false, nil, nil, nil, nil, nil), nil
				}},
				pkMoveMockHandler{pkMoveSysSnapshotsSQL, func(string) (executor.Result, error) {
					return pkMoveSnapshotRowsResult(t, c, true, nil, nil, nil, nil, nil), nil
				}},
				pkMoveMockHandler{pkMoveActivePitrsSQL, func(string) (executor.Result, error) {
					return pkMovePitrRowsResult(t, c, nil, nil, nil, nil, nil, nil), nil
				}},
			)
			if tc.ledgerErr != nil {
				err := tc.ledgerErr
				handlers = append(handlers, pkMoveMockHandler{pkMoveRetentionLookupSQL,
					func(string) (executor.Result, error) { return executor.Result{}, err }})
			} else {
				handlers = append(handlers, pkMoveMockHandler{pkMoveRetentionLookupSQL, func(string) (executor.Result, error) {
					var logicals []uint64
					if tc.ledgerDbs != nil {
						logicals = []uint64{tc.ledgerLogical}
					}
					return pkMoveIdentityResult(t, c, tc.ledgerDbs, tc.ledgerTbls, logicals), nil
				}})
			}
			cNew, _ := newPkMoveOwnerMockCompile(t, ctrl, handlers)
			c = cNew
			gotDb, gotTbl, gotLogical, grounded, err := branchHasSnapshotGroundedLineage(c, 7, 800)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, grounded)
			require.Equal(t, tc.wantDb, gotDb)
			require.Equal(t, tc.wantTbl, gotTbl)
			require.Equal(t, tc.wantLogical, gotLogical,
				"the wide lookup's logical id travels to the KEEP records")
		})
	}
}
