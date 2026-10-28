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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// oldColumnsSelectorDefinition re-derives the pre-#31160 COLUMNS definition
// from the current one, the inverse of the planner replay tolerance.
func oldColumnsSelectorDefinition() string {
	return strings.NewReplacer(
		"WHEN 2 then NULL", "WHEN 2 then 'binary'",
	).Replace(sysview.InformationSchemaColumnsDDL)
}

// oldTablesV118Definition re-derives the pre-#31163 TABLES definition that
// fabricated UPDATE_TIME/CHECK_TIME from created_time.
func oldTablesV118Definition() string {
	return strings.NewReplacer(
		"cast(null as timestamp) AS UPDATE_TIME,", "if(relkind = 'v', NULL, created_time) AS UPDATE_TIME,",
		"cast(null as timestamp) AS CHECK_TIME,", "if(relkind = 'v', NULL, created_time) AS CHECK_TIME,",
	).Replace(sysview.InformationSchemaTablesV118DDL)
}

type columnsTablesMaintenanceFixture struct {
	// definitions maps "accountID/viewName" -> persisted definition; "" means missing.
	definitions  map[string]string
	writes       []string
	protocol     string
	failProtocol bool
	// failDrop makes every DROP VIEW fail once: the whole transactional page
	// must roll back and the cursor must not advance.
	failDrop bool
	// protocolEntered, when non-nil, receives a signal each time the mock
	// protocol probe runs; protocolBlock, when non-nil, parks the probe
	// until the channel is closed. The protocol probe executes strictly
	// inside the maintenance CAS window (after CompareAndSwap, before any
	// repair write), so concurrent-pass tests get a deterministic overlap
	// barrier instead of relying on sleep ordering.
	protocolEntered chan<- struct{}
	protocolBlock   <-chan struct{}
}

func informationSchemaColumnTablesKey(accountID uint32, view string) string {
	return fmt.Sprintf("%d/%s", accountID, strings.ToUpper(view))
}

func (f *columnsTablesMaintenanceFixture) mocker(sql string) (executor.Result, error) {
	switch {
	case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
		if f.protocolEntered != nil {
			select {
			case f.protocolEntered <- struct{}{}:
			default:
			}
		}
		if f.protocolBlock != nil {
			<-f.protocolBlock
		}
		if f.failProtocol {
			return executor.Result{}, fmt.Errorf("injected protocol query failure")
		}
		return newBootstrapStringResult(fmt.Sprintf(`{"result":"cn:%s"}`, f.protocol)), nil
	case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
		result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mpool.MustNewZero())
		seen := map[int32]bool{}
		var accounts []int32
		for k := range f.definitions {
			var id uint32
			var view string
			if _, err := fmt.Sscanf(k, "%d/%s", &id, &view); err != nil {
				continue
			}
			if !seen[int32(id)] {
				seen[int32(id)] = true
				accounts = append(accounts, int32(id))
			}
		}
		result.NewBatchWithRowCount(len(accounts))
		executor.AppendFixedRows(result, 0, accounts)
		return result.GetResult(), nil
	case strings.HasPrefix(sql, "DROP VIEW IF EXISTS information_schema."):
		if f.failDrop {
			return executor.Result{}, fmt.Errorf("injected drop view failure")
		}
		f.writes = append(f.writes, sql)
		return executor.Result{}, nil
	case sql == sysview.InformationSchemaColumnsDDL || sql == sysview.InformationSchemaTablesV118DDL:
		f.writes = append(f.writes, sql)
		return executor.Result{}, nil
	default:
		return executor.Result{}, fmt.Errorf("unexpected sql: %.200s", sql)
	}
}

func TestMaintainInformationSchemaColumnsTablesRepairsFinalVersionTenants(t *testing.T) {
	fixture := &columnsTablesMaintenanceFixture{
		protocol: "126",
		definitions: map[string]string{
			// account 0 (sys): fully current, must not be touched
			"0/COLUMNS": sysview.InformationSchemaColumnsDDL,
			"0/TABLES":  sysview.InformationSchemaTablesV118DDL,
			// account 10: already at final version with pre-fix definitions
			"10/COLUMNS": oldColumnsSelectorDefinition(),
			"10/TABLES":  oldTablesV118Definition(),
			// account 20: tenant whose COLUMNS never got a fix but TABLES is fine
			"20/COLUMNS": "CREATE VIEW information_schema.COLUMNS AS SELECT 1",
			"20/TABLES":  sysview.InformationSchemaTablesV118DDL,
		},
	}

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		accountID uint32,
		_ string,
		viewName string,
	) (bool, string, error) {
		definition := fixture.definitions[informationSchemaColumnTablesKey(accountID, viewName)]
		return definition != "", definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor2(fixture.mocker, txnOperator),
		func(*service) {},
	)

	// pass 1: repairs exactly the stale definitions
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Len(t, fixture.writes, 6, "expected DROP+CREATE for 10/COLUMNS, 10/TABLES, 20/COLUMNS")
	require.Contains(t, fixture.writes, "DROP VIEW IF EXISTS information_schema.COLUMNS;")
	require.Contains(t, fixture.writes, "DROP VIEW IF EXISTS information_schema.TABLES;")
	fixture.definitions["10/COLUMNS"] = sysview.InformationSchemaColumnsDDL
	fixture.definitions["10/TABLES"] = sysview.InformationSchemaTablesV118DDL
	fixture.definitions["20/COLUMNS"] = sysview.InformationSchemaColumnsDDL

	// pass 2: nothing stale left; persisted definitions are the idempotence marker
	fixture.writes = nil
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	// cursor already advanced past all accounts; second pass only wraps it
	require.Empty(t, fixture.writes)
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Empty(t, fixture.writes)
}

func TestMaintainInformationSchemaColumnsTablesProtocolGate(t *testing.T) {
	fixture := &columnsTablesMaintenanceFixture{
		protocol: "125", // one below the 126 gate required before publishing
		definitions: map[string]string{
			"10/COLUMNS": oldColumnsSelectorDefinition(),
			"10/TABLES":  sysview.InformationSchemaTablesV118DDL,
		},
	}

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		accountID uint32,
		_ string,
		viewName string,
	) (bool, string, error) {
		definition := fixture.definitions[informationSchemaColumnTablesKey(accountID, viewName)]
		return definition != "", definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor2(fixture.mocker, txnOperator),
		func(*service) {},
	)

	// protocol not ready: maintenance must not touch any definition
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Empty(t, fixture.writes)
	require.Equal(t, oldColumnsSelectorDefinition(), fixture.definitions["10/COLUMNS"])
}

func TestInformationSchemaTablesUpgradeEntryMatchesVersionedContract(t *testing.T) {
	entry := versions.InformationSchemaTablesUpgradeEntry()
	require.Equal(t, sysview.InformationSchemaTablesV118DDL, entry.UpgSql)
	require.Equal(t, defines.MORPCVersion118, entry.RequiredProtocolVersion)
	require.Equal(t, "DROP VIEW IF EXISTS information_schema.TABLES;", entry.PreSql)
	// the old definition must never pass the equality check
	oldDef := oldTablesV118Definition()
	require.NotEqual(t, entry.UpgSql, oldDef)
	require.Contains(t, entry.UpgSql, "cast(null as timestamp) AS UPDATE_TIME")
	require.Contains(t, entry.UpgSql, "cast(null as timestamp) AS CHECK_TIME")
	require.NotContains(t, entry.UpgSql, "created_time) AS UPDATE_TIME")
}

// swapColumnsTablesCheckViewDefinition routes view-definition reads to the
// fixture for the duration of this test.
func swapColumnsTablesCheckViewDefinition(t *testing.T, fixture *columnsTablesMaintenanceFixture) {
	t.Helper()
	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		accountID uint32,
		_ string,
		viewName string,
	) (bool, string, error) {
		definition := fixture.definitions[informationSchemaColumnTablesKey(accountID, viewName)]
		return definition != "", definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })
}

func newColumnsTablesMaintenanceService(t *testing.T, fixture *columnsTablesMaintenanceFixture) *service {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor2(fixture.mocker, txnOperator),
		func(*service) {},
	)
}

// A failing protocol probe must abort the transactional page, keep every
// persisted definition untouched, and never advance the process-local cursor:
// the next periodic pass retries the same page.
func TestMaintainInformationSchemaColumnsTablesProtocolQueryErrorRollsBack(t *testing.T) {
	fixture := &columnsTablesMaintenanceFixture{
		protocol:     "126",
		failProtocol: true,
		definitions: map[string]string{
			"10/COLUMNS": oldColumnsSelectorDefinition(),
			"10/TABLES":  oldTablesV118Definition(),
		},
	}
	swapColumnsTablesCheckViewDefinition(t, fixture)
	service := newColumnsTablesMaintenanceService(t, fixture)

	before := service.upgrade.informationSchemaColumnsTablesMaintenanceState
	require.Error(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Empty(t, fixture.writes, "no write must escape an aborted transactional page")
	require.Equal(t, before, service.upgrade.informationSchemaColumnsTablesMaintenanceState)
}

// A mid-page statement failure must roll the whole page back and hold the
// cursor; clearing the fault lets the next pass repair the same accounts.
func TestMaintainInformationSchemaColumnsTablesDropFailureRollsBackAndRetries(t *testing.T) {
	fixture := &columnsTablesMaintenanceFixture{
		protocol: "126",
		failDrop: true,
		definitions: map[string]string{
			"10/COLUMNS": oldColumnsSelectorDefinition(),
			"10/TABLES":  oldTablesV118Definition(),
		},
	}
	swapColumnsTablesCheckViewDefinition(t, fixture)
	service := newColumnsTablesMaintenanceService(t, fixture)

	before := service.upgrade.informationSchemaColumnsTablesMaintenanceState
	require.Error(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Empty(t, fixture.writes)
	require.Equal(t, before, service.upgrade.informationSchemaColumnsTablesMaintenanceState)

	fixture.failDrop = false
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Len(t, fixture.writes, 4, "retry must re-issue DROP+CREATE for both stale views")
	require.NotEqual(t, before, service.upgrade.informationSchemaColumnsTablesMaintenanceState)
}

// A second pass overlapping an in-flight one must be a CAS no-op and must not
// duplicate repairs. The overlap is pinned by a deterministic barrier rather
// than sleep ordering: the first pass signals from inside the mock protocol
// probe — which runs strictly after the maintenance CompareAndSwap and before
// any repair write — and stays parked there until the test releases it. The
// second invocation therefore provably executes while the CAS is held. Every
// exit path releases the barrier, and the goroutine's result channel is
// buffered, so no failure path can leak a goroutine.
func TestMaintainInformationSchemaColumnsTablesCASBlocksConcurrentPass(t *testing.T) {
	entered := make(chan struct{}, 8)
	proceed := make(chan struct{})
	fixture := &columnsTablesMaintenanceFixture{
		protocol:        "126",
		protocolEntered: entered,
		protocolBlock:   proceed,
		definitions: map[string]string{
			"10/COLUMNS": oldColumnsSelectorDefinition(),
			"10/TABLES":  sysview.InformationSchemaTablesV118DDL,
		},
	}
	swapColumnsTablesCheckViewDefinition(t, fixture)
	service := newColumnsTablesMaintenanceService(t, fixture)

	errCh := make(chan error, 1)
	go func() {
		errCh <- service.maintainInformationSchemaColumnsTables(t.Context())
	}()

	// done marks that the happy path already consumed pass 1's result. On
	// any failure exit the guard releases the parked probe and drains the
	// goroutine, so no assertion failure can leak it; on the happy path it
	// is a no-op and never double-drains.
	done := false
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(proceed) }) }
	defer func() {
		if done {
			return
		}
		release()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("first pass failed while draining on teardown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("first pass did not finish after barrier release")
		}
	}()

	// Deterministic overlap: pass 1 is parked inside the protocol probe,
	// i.e. strictly inside the CAS window, and has issued no writes yet.
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first pass never reached the protocol probe")
	}

	// Second pass must bounce off the CAS: nil error, zero repairs.
	require.NoError(t, service.maintainInformationSchemaColumnsTables(t.Context()))
	require.Empty(t, fixture.writes, "overlapping pass must be a no-op")

	release()
	var pass1Err error
	select {
	case pass1Err = <-errCh:
		done = true
	case <-time.After(10 * time.Second):
		t.Fatal("first pass did not finish after barrier release")
	}
	require.NoError(t, pass1Err)
	require.Len(t, fixture.writes, 2, "exactly one pass repaired the stale COLUMNS view")
}
