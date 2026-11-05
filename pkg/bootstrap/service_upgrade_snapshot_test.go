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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestScanSnapshotDecisions(t *testing.T) {
	cases := []struct {
		name           string
		kind           string
		manifest       string
		snapshotName   string
		expectedVisits int
		wantError      string
	}{
		{name: "no ambiguity or file", kind: "branch"},
		{name: "no decision", kind: "user", wantError: "administrator decision"},
		{name: "explicit branch", kind: "user", manifest: testDecision, expectedVisits: 1},
		{name: "explicit user", kind: "user", manifest: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"user"`, 1), expectedVisits: 1},
		{name: "case alias cannot apply", kind: "user", manifest: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"user","Decision":"branch"`, 1), wantError: "noncanonical JSON key"},
		{name: "already applied", kind: "branch", manifest: testDecision, expectedVisits: 1},
		{name: "cannot reverse", kind: "branch", manifest: strings.Replace(testDecision, `"decision":"branch"`, `"decision":"user"`, 1), wantError: "reverse"},
		{name: "stale uuid", kind: "user", manifest: strings.Replace(testDecision, `01a17d34-05f1-752c-acbd-52c7a8fe04fe`, `01a17d34-05f1-752c-acbd-52c7a8fe04ff`, 1), wantError: "preimage changed"},
		{name: "unknown record", kind: "user", manifest: testDecision, snapshotName: "__mo_branch_44", wantError: "unknown or stale"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				if tc.manifest != "" {
					data := []byte(testDecisionHeader + "\n" + tc.manifest + "\n")
					require.NoError(t, fs.Write(t.Context(), fileservice.IOVector{FilePath: SnapshotUpgradeDecisionPath, Entries: []fileservice.IOEntry{{Size: int64(len(data)), Data: data}}}))
				}
				name := tc.snapshotName
				if name == "" {
					name = "__mo_branch_42"
				}
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "from mo_catalog.mo_snapshots where sname >"):
						return testCatalogSnapshotPage(t, name, tc.kind), nil
					case strings.Contains(sql, "from mo_catalog.mo_branch_metadata where table_id"):
						return testCatalogSnapshotEdge(t), nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {}, WithUpgradeDecisionFileService(fs))
				t.Cleanup(s.stopper.Stop)
				visits := 0
				err = s.scanSnapshotDecisions(context.Background(), func(snapshotDecisionRecord) error { visits++; return nil })
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.expectedVisits, visits)
			})
		})
	}
}

func TestApplySnapshotDecisionPageRechecksOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     int32
		kind      string
		decision  string
		gate      bool
		snapshot  bool
		wantWrite bool
		wantError string
	}{
		{name: "branch", state: versions.StateCreated, kind: "user", decision: "branch", gate: true, snapshot: true, wantWrite: true},
		{name: "user", state: versions.StateCreated, kind: "user", decision: "user", gate: true, snapshot: true},
		{name: "already applied", state: versions.StateCreated, kind: "branch", decision: "branch", gate: true, snapshot: true},
		{name: "conflicting reversal", state: versions.StateCreated, kind: "branch", decision: "user", gate: true, snapshot: true, wantError: "reverse"},
		{name: "missing gate", state: versions.StateCreated, kind: "user", decision: "branch", snapshot: true, wantError: "gate"},
		{name: "missing snapshot", state: versions.StateCreated, kind: "user", decision: "branch", gate: true, wantError: "disappeared"},
		{name: "step advanced", state: versions.StateUpgradingTenant, kind: "user", decision: "branch", gate: true, snapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				reader, err := newSnapshotDecisionReader(strings.NewReader(testDecisionHeader + "\n" + testDecision))
				require.NoError(t, err)
				d, err := reader.next()
				require.NoError(t, err)
				d.Decision = tc.decision
				writes := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "select state from mo_version"):
						return testCatalogUpgradeState(t, versions.StateCreated), nil
					case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where id = 7 for update"):
						res := buildUpgradeVersionResult(7, tc.state, "4.0.6", "4.0.7", 41, 0, versions.Yes, versions.Yes, 0, 0)
						t.Cleanup(func() { mpool.DeleteMPool(res.Mp) })
						return res, nil
					case sql == catalog.SnapshotLifecycleGateSQL:
						if tc.gate {
							return newBootstrapStringResult("SNAPSHOT"), nil
						}
						return executor.Result{}, nil
					case strings.Contains(sql, "from mo_catalog.mo_snapshots where sname ="):
						if tc.snapshot {
							return testCatalogSnapshotPage(t, "__mo_branch_42", tc.kind), nil
						}
						return executor.Result{}, nil
					case strings.Contains(sql, "from mo_catalog.mo_branch_metadata where table_id"):
						return testCatalogSnapshotEdge(t), nil
					case strings.HasPrefix(sql, "update mo_catalog.mo_snapshots set kind"):
						writes++
						return executor.Result{AffectedRows: 1}, nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(*service) {})
				t.Cleanup(s.stopper.Stop)
				err = s.applySnapshotDecisionPage(t.Context(), versions.VersionUpgrade{ID: 7, ToVersion: "4.0.7", FinalVersion: "4.0.7", FinalVersionOffset: 41}, []snapshotDecisionRecord{*d})
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
				} else {
					require.NoError(t, err)
				}
				if tc.wantWrite {
					require.Equal(t, 1, writes)
				} else {
					require.Zero(t, writes)
				}
			})
		})
	}
}

func testCatalogUpgradeState(t *testing.T, state int32) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	r := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
	r.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(r, 0, []int32{state}))
	return r.GetResult()
}

func testCatalogSnapshotPage(t *testing.T, name, kind string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	typs := make([]types.Type, 9)
	for i := range typs {
		typs[i] = types.T_varchar.ToType()
	}
	r := executor.NewMemResult(typs, mp)
	r.NewBatchWithRowCount(1)
	values := []string{"01a17d34-05f1-752c-acbd-52c7a8fe04fe", name, "123", "table", "sys", "db", "t", "9", kind}
	for i, v := range values {
		require.NoError(t, executor.AppendStringRows(r, i, []string{v}))
	}
	return r.GetResult()
}
func testCatalogSnapshotEdge(t *testing.T) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	r := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_bool.ToType()}, mp)
	r.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(r, 0, []uint64{42}))
	require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{9}))
	require.NoError(t, executor.AppendFixedRows(r, 2, []int64{123}))
	require.NoError(t, executor.AppendFixedRows(r, 3, []uint64{0}))
	require.NoError(t, executor.AppendStringRows(r, 4, []string{"table"}))
	require.NoError(t, executor.AppendFixedRows(r, 5, []bool{false}))
	return r.GetResult()
}
