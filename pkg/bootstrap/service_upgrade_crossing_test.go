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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// crossingFakeDB models published-source route creation and restart admission.
type crossingFakeDB struct {
	executor.SQLExecutor
	t *testing.T

	mu          sync.Mutex
	txns        int
	afterCommit func(n int)

	latest       versions.Version
	latestReady  string
	finalExists  bool
	finalState   int32
	finalSteps   []versions.VersionUpgrade
	retainedStep []versions.VersionUpgrade
	cdcTasks     [][]string
	cdcForbidden bool
	cdcReads     int
	cdcWrites    int
	log          []string
	strict       bool
}

const crossingFinalDDLKey = "'4.0.7'"

func crossingVersion(metadata versions.Version) versions.Version {
	return metadata
}

func newCrossingFakeDB(t *testing.T) *crossingFakeDB {
	t.Helper()
	return &crossingFakeDB{
		t:           t,
		latest:      crossingVersion(versions.Version{Version: "4.0.6", VersionOffset: 5, State: versions.StateReady}),
		latestReady: "4.0.6",
		finalState:  versions.StateCreated,
		strict:      true,
	}
}

func (db *crossingFakeDB) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.txns++
	err := fn(executor.NewMemTxnExecutor(db.execLogged, &testTxnOperator{}))
	if err != nil {
		return err
	}
	if db.afterCommit != nil {
		db.afterCommit(db.txns)
	}
	return nil
}

func (db *crossingFakeDB) execLogged(sql string) (executor.Result, error) {
	sql = strings.Join(strings.Fields(sql), " ")
	db.log = append(db.log, sql)
	return db.exec(sql)
}

func (db *crossingFakeDB) exec(sql string) (executor.Result, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
		return newBootstrapStringResult("mo_catalog"), nil
	case sql == versions.LatestVersionSQL:
		v := db.latest
		if db.finalExists {
			v = versions.Version{Version: "4.0.7", VersionOffset: v4_0_7.Handler.Metadata().VersionOffset, State: db.finalState}
		}
		return recoveryVersionResult(db.t, v), nil
	case strings.HasPrefix(sql, "select state from mo_version"):
		if strings.Contains(sql, crossingFinalDDLKey) {
			if !db.finalExists {
				return executor.Result{}, nil
			}
			return crossingStateResult(db.t, db.finalState), nil
		}
		if strings.Contains(sql, "'"+db.latest.Version+"'") {
			return crossingStateResult(db.t, db.latest.State), nil
		}
		return executor.Result{}, fmt.Errorf("unexpected version state read: %s", sql)
	case strings.Contains(sql, "mo_upgrade_tenant"):
		// Seeded shapes carry no pending tenant ranges either way.
		return executor.Result{}, nil
	case strings.Contains(sql, "from mo_upgrade "):
		if strings.Contains(sql, crossingFinalDDLKey) {
			return ownRecoveryResult(db.t, recoveryRouteResult(db.t, db.finalSteps)), nil
		}
		return ownRecoveryResult(db.t, recoveryRouteResult(db.t, db.retainedStep)), nil
	case strings.Contains(sql, "mo_cdc_task") && (strings.HasPrefix(sql, "insert ") || strings.HasPrefix(sql, "update ") || strings.HasPrefix(sql, "delete ")):
		db.cdcWrites++
		return executor.Result{}, nil
	case strings.Contains(sql, "from mo_catalog.mo_cdc_task"):
		db.cdcReads++
		if db.cdcForbidden {
			return executor.Result{}, fmt.Errorf("mo_cdc_task read issued where the crossing gate must skip")
		}
		mp := mpool.MustNewZeroNoFixed()
		db.t.Cleanup(func() { mpool.DeleteMPool(mp) })
		res := executor.NewMemResult([]types.Type{
			types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_varchar.ToType(),
		}, mp)
		for _, row := range db.cdcTasks {
			res.NewBatchWithRowCount(1)
			for col, value := range row {
				executor.AppendStringRows(res, col, []string{value})
			}
		}
		return res.GetResult(), nil
	case strings.HasPrefix(sql, "SELECT mo_ctl("):
		return newBootstrapStringResult(`{"result": "fake-cn:126"}`), nil
	case strings.HasPrefix(sql, "insert into mo_version"):
		db.finalExists = true
		db.finalState = versions.StateCreated
		return executor.Result{}, nil
	case strings.HasPrefix(sql, "insert into mo_upgrade "):
		step, err := parseCrossingInsertedStep(sql)
		if err != nil {
			return executor.Result{}, err
		}
		db.finalSteps = append(db.finalSteps, step)
		return executor.Result{}, nil
	case strings.HasPrefix(sql, "update mo_upgrade set state"):
		return executor.Result{}, db.flipCrossingStep(sql)
	case strings.HasPrefix(sql, "update mo_upgrade set total_tenant"):
		return executor.Result{}, nil
	case strings.HasPrefix(sql, "update mo_version set state"):
		if strings.Contains(sql, crossingFinalDDLKey) {
			db.finalState = versions.StateReady
		} else {
			db.latest.State = versions.StateReady
		}
		return executor.Result{}, nil
	case strings.HasPrefix(sql, "select version from mo_version where state = 2"):
		return newBootstrapStringResult(db.latestReady), nil
	case db.strict:
		return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
	default:
		return executor.Result{}, nil
	}
}

func crossingStateResult(t *testing.T, state int32) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
	res.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(res, 0, []int32{state}))
	return res.GetResult()
}

func parseCrossingInsertedStep(sql string) (versions.VersionUpgrade, error) {
	start := strings.Index(sql, "values (")
	if start < 0 {
		return versions.VersionUpgrade{}, fmt.Errorf("cannot parse inserted step: %s", sql)
	}
	inner := sql[start+len("values ("):]
	if end := strings.LastIndex(inner, ")"); end >= 0 {
		inner = inner[:end]
	}
	parts := strings.Split(inner, ", ")
	if len(parts) < 10 {
		return versions.VersionUpgrade{}, fmt.Errorf("cannot parse inserted step: %s", sql)
	}
	unquote := func(s string) string { return strings.Trim(s, "'") }
	atoi := func(s string) int32 {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return -1
		}
		return int32(n)
	}
	return versions.VersionUpgrade{
		FromVersion:        unquote(parts[0]),
		ToVersion:          unquote(parts[1]),
		FinalVersion:       unquote(parts[2]),
		FinalVersionOffset: uint32(atoi(parts[3])),
		State:              atoi(parts[4]),
		UpgradeCluster:     atoi(parts[5]),
		UpgradeTenant:      atoi(parts[6]),
		UpgradeOrder:       atoi(parts[7]),
		TotalTenant:        atoi(parts[8]),
		ReadyTenant:        atoi(parts[9]),
	}, nil
}

func (db *crossingFakeDB) flipCrossingStep(sql string) error {
	idx := strings.LastIndex(sql, "upgrade_order = ")
	if idx < 0 {
		return fmt.Errorf("cannot parse step state flip: %s", sql)
	}
	order, err := strconv.Atoi(strings.TrimSpace(sql[idx+len("upgrade_order = "):]))
	if err != nil {
		return fmt.Errorf("cannot parse step state flip: %s", sql)
	}
	steps := db.finalSteps
	if !strings.Contains(sql, crossingFinalDDLKey) {
		steps = db.retainedStep
	}
	for i := range steps {
		if steps[i].UpgradeOrder == int32(order) {
			steps[i].State = versions.StateReady
			return nil
		}
	}
	return fmt.Errorf("step with upgrade_order %d not found: %s", order, sql)
}

func (db *crossingFakeDB) mutations() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	var out []string
	for _, sql := range db.log {
		for _, prefix := range []string{"insert ", "update ", "create ", "drop ", "alter ", "replace ", "delete ", "truncate "} {
			if strings.HasPrefix(sql, prefix) {
				out = append(out, sql)
				break
			}
		}
	}
	return out
}

func newCrossingService(t *testing.T, db *crossingFakeDB) *service {
	t.Helper()
	s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, db,
		func(s *service) { s.initUpgrade() })
	t.Cleanup(s.stopper.Stop)
	return s
}

func crossingFinalSteps() []versions.VersionUpgrade {
	v407 := v4_0_7.Handler.Metadata()
	return []versions.VersionUpgrade{{ID: 10, FromVersion: "4.0.6", ToVersion: v407.Version,
		FinalVersion: v407.Version, FinalVersionOffset: v407.VersionOffset, State: versions.StateCreated,
		UpgradeCluster: v407.UpgradeCluster, UpgradeTenant: v407.UpgradeTenant}}
}

func requireCrossingTerminalLegacyTask(t *testing.T, err error, account, taskID, state string) {
	t.Helper()
	var terminal *terminalUpgradeError
	require.ErrorAs(t, err, &terminal, "legacy CDC task must be a terminal pre-mutation rejection")
	require.ErrorContains(t, err, "account="+account)
	require.ErrorContains(t, err, "task_id="+taskID)
	require.ErrorContains(t, err, fmt.Sprintf("state=%q", state))
}

// (vi) legacy matrix: {READY 4.0.5, READY 4.0.6, interrupted 4.0.7} x
// {one legacy task incl. paused/failed; empty-task control}. Task legs must
// be terminal, name the account/task/state, and precede any catalog
// mutation; control legs pass the preflight and form (or retain) routes.
func TestCrossingLegacyCDCGateMatrix(t *testing.T) {
	v407Offset := v4_0_7.Handler.Metadata().VersionOffset
	sources := []struct {
		name string
		// latest is the persisted latest version row of the staged catalog;
		// offsets are the published historical values pinned by the preserved
		// fixtures (4.0.6 offset five) and the frozen handler metadata.
		latest   versions.Version
		retained []versions.VersionUpgrade
	}{
		{
			name:   "ready-4.0.5",
			latest: versions.Version{Version: "4.0.5", VersionOffset: 5, State: versions.StateReady},
		},
		{
			name:   "ready-4.0.6",
			latest: versions.Version{Version: "4.0.6", VersionOffset: 5, State: versions.StateReady},
		},
		{
			name:   "interrupted-4.0.7",
			latest: versions.Version{Version: "4.0.7", VersionOffset: v407Offset, State: versions.StateCreated},
			retained: []versions.VersionUpgrade{
				{ID: 10, FromVersion: "4.0.6", ToVersion: "4.0.7", FinalVersion: "4.0.7", FinalVersionOffset: v407Offset,
					State: versions.StateCreated, UpgradeOrder: 0, UpgradeCluster: versions.Yes, UpgradeTenant: versions.Yes},
			},
		},
	}
	for _, src := range sources {
		for _, taskState := range []string{"", "paused", "failed"} {
			t.Run(src.name+"/"+map[bool]string{true: "empty-tasks", false: "task-" + taskState}[taskState == ""], func(t *testing.T) {
				runtime.RunTest("", func(runtime.Runtime) {
					db := newCrossingFakeDB(t)
					db.latest = src.latest
					db.latestReady = src.latest.Version
					db.retainedStep = src.retained
					if src.latest.Version == "4.0.7" {
						db.finalExists, db.finalSteps = true, src.retained
					}
					db.strict = taskState != ""
					if taskState != "" {
						db.cdcTasks = [][]string{{"10001", "task-uuid", "old-task", taskState}}
					}
					s := newCrossingService(t, db)
					err := s.doCheckUpgrade(t.Context())
					if taskState != "" {
						requireCrossingTerminalLegacyTask(t, err, "10001", "task-uuid", taskState)
						require.Equal(t, 1, db.cdcReads)
						require.Empty(t, db.mutations(), "CDC admission must precede any catalog mutation: %v", db.mutations())
						return
					}
					require.NoError(t, err)
					switch src.name {
					case "ready-4.0.5":
						require.Equal(t, 1, db.cdcReads, "the creation preflight runs once on an eligible source")
						requireCrossingRouteShape(t, db.finalSteps, [][2]string{{"4.0.5", "4.0.6"}, {"4.0.6", "4.0.7"}})
					case "ready-4.0.6":
						require.Equal(t, 1, db.cdcReads, "the creation preflight runs once on an eligible source")
						requireCrossingRouteShape(t, db.finalSteps, [][2]string{{"4.0.6", "4.0.7"}})
					case "interrupted-4.0.7":
						require.Equal(t, 1, db.cdcReads, "the current-target recovery checks CDC admission once")
						require.Len(t, db.finalSteps, 1, "an interrupted route is retained, not recreated")
						require.Empty(t, db.mutations(), "recovery validation is read-only")
					}
				})
			})
		}
	}
}

func requireCrossingRouteShape(t *testing.T, steps []versions.VersionUpgrade, want [][2]string) {
	t.Helper()
	require.Len(t, steps, len(want))
	for i, pair := range want {
		require.Equal(t, pair[0], steps[i].FromVersion)
		require.Equal(t, pair[1], steps[i].ToVersion)
		require.Equal(t, "4.0.7", steps[i].FinalVersion)
		require.Equal(t, v4_0_7.Handler.Metadata().VersionOffset, steps[i].FinalVersionOffset)
		require.Equal(t, versions.StateCreated, steps[i].State)
		require.Equal(t, int32(i), steps[i].UpgradeOrder)
	}
}

// An interrupted 4.0.6 -> 4.0.7 route rejects legacy tasks before mutation.
func TestCrossingRestartMidCrossingRejectsLegacyTask(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		db := newCrossingFakeDB(t)
		db.finalExists = true
		db.finalSteps = crossingFinalSteps()
		db.cdcTasks = [][]string{{"10001", "task-uuid", "old-task", "paused"}}
		db.latest = versions.Version{Version: "4.0.7", VersionOffset: v4_0_7.Handler.Metadata().VersionOffset, State: versions.StateCreated}
		s := newCrossingService(t, db)
		err := s.doCheckUpgrade(t.Context())
		requireCrossingTerminalLegacyTask(t, err, "10001", "task-uuid", "paused")
		require.Equal(t, 1, db.cdcReads)
		require.Empty(t, db.mutations(), "rejection must be pre-mutation: %v", db.mutations())
	})
}

// (vi) recheck arm: a worker completing a retained READY-4.0.6 target heads
// for route creation through newUpgradePass's retained-target completion
// recheck while a competing CN has already created the final route in a
// non-READY state. Mid-crossing shapes reject the seeded legacy task
// pre-mutation; post-crossing shapes skip the preflight and report
// not-completed without touching task rows.
func TestCrossingRecheckArm(t *testing.T) {
	for _, tc := range []struct {
		name        string
		prepare     func(steps []versions.VersionUpgrade) []versions.VersionUpgrade
		wantReject  bool
		legacyTasks [][]string
	}{
		{
			name: "mid-crossing",
			prepare: func(steps []versions.VersionUpgrade) []versions.VersionUpgrade {
				return steps
			},
			wantReject:  true,
			legacyTasks: [][]string{{"10001", "task-uuid", "old-task", "failed"}},
		},
		{
			name: "post-crossing",
			prepare: func(steps []versions.VersionUpgrade) []versions.VersionUpgrade {
				steps[0].State = versions.StateReady
				return steps
			},
			wantReject:  false,
			legacyTasks: [][]string{{"10001", "task-uuid", "new-task", "running"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				db := newCrossingFakeDB(t)
				db.cdcTasks = tc.legacyTasks
				db.cdcForbidden = !tc.wantReject
				db.afterCommit = func(n int) {
					if n == 1 {
						// The competing CN publishes its route between our
						// retained-target commit and our recheck.
						db.finalExists = true
						db.finalState = versions.StateCreated
						db.finalSteps = tc.prepare(crossingFinalSteps())
						if !tc.wantReject {
							db.finalState = versions.StateReady
						}
					}
				}
				s := newCrossingService(t, db)
				completed, err := s.newUpgradePass(t.Context())()
				if tc.wantReject {
					requireCrossingTerminalLegacyTask(t, err, "10001", "task-uuid", "failed")
					require.Equal(t, 1, db.cdcReads)
				} else {
					require.NoError(t, err)
					require.False(t, completed, "the retained-target pass yields after rechecking the current target")
					require.True(t, s.upgrade.finalVersionCompleted.Load())
					require.Equal(t, 0, db.cdcReads, "post-crossing recheck skips the preflight")
				}
				require.Empty(t, db.mutations(), "the recheck arm performs no catalog mutation: %v", db.mutations())
			})
		})
	}
}
