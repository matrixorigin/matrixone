// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package bootstrap

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Historical 4.0.7 recovery tests need their original final-version fixture.
func initUpgradeThroughV407(s *service) {
	s.initUpgrade()
	s.handles = s.handles[:len(s.handles)-1]
}

func TestV408FrozenOffsetAdmission(t *testing.T) {
	ready := versions.Version{Version: "4.0.7", VersionOffset: 41, State: versions.StateReady}
	final := versions.Version{Version: "4.0.8"}
	require.True(t, canUpgradeFrozenV407Offset(ready, final, 44))
	for _, offset := range []uint32{0, 40, 42, 43, 44, 45} {
		candidate := ready
		candidate.VersionOffset = offset
		require.False(t, canUpgradeFrozenV407Offset(candidate, final, 44), "offset %d", offset)
	}
	unfinished := ready
	unfinished.State = versions.StateCreated
	require.False(t, canUpgradeFrozenV407Offset(unfinished, final, 44))
	require.False(t, canUpgradeFrozenV407Offset(ready, versions.Version{Version: "4.0.7"}, 44))
	require.False(t, canUpgradeFrozenV407Offset(ready, final, 45))
}

// The ready 4.0.7 offset-41 snapshot must produce a semantic-version upgrade
// route; the migration must never treat it as an already-current catalog.
func TestV408CreatesRouteFromV407Offset41(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		var routes []string
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
				return newBootstrapStringResult("mo_catalog"), nil
			case sql == versions.LatestVersionSQL:
				return recoveryVersionResult(t, versions.Version{Version: "4.0.7", VersionOffset: 41, State: versions.StateReady}), nil
			case strings.HasPrefix(sql, "select state from mo_version"):
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "insert into mo_version"):
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "select version from mo_version where state = 2"):
				return newBootstrapStringResult("4.0.7"), nil
			case strings.HasPrefix(sql, "insert into mo_upgrade "):
				routes = append(routes, sql)
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
			}
		}, &testTxnOperator{})
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
		t.Cleanup(s.stopper.Stop)
		require.NoError(t, s.doCheckUpgrade(t.Context()))
		require.Len(t, routes, 1)
		require.Contains(t, routes[0], "'4.0.7'")
		require.Contains(t, routes[0], "'4.0.8'")
		require.False(t, s.upgrade.finalVersionCompleted.Load())
	})
}
