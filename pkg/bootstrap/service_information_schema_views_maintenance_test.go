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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func TestMaintainInformationSchemaViewsRetriesFailedReplacement(t *testing.T) {
	const accountID = int32(10)
	viewDefinition := sysview.InformationSchemaViewsLegacyDDL
	failReplacement := true
	var successfulReplacements int
	var executed []string
	var accountLookups int

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		id uint32,
		schema string,
		viewName string,
	) (bool, string, error) {
		require.Equal(t, uint32(accountID), id)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "VIEWS", viewName)
		return true, viewDefinition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newInformationSchemaViewsMaintenanceTestService(t, func(sql string) (executor.Result, error) {
		switch {
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return newBootstrapStringResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`), nil
		case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
			accountLookups++
			require.Contains(t, sql, "limit 32")
			if accountLookups == 1 || accountLookups == 2 || accountLookups == 4 {
				require.Contains(t, sql, "account_id >= 0")
				return buildInformationSchemaViewsMaintenanceAccountRows(accountID), nil
			}
			require.Equal(t, 3, accountLookups)
			require.Contains(t, sql, "account_id >= 11")
			return executor.Result{}, nil
		case sql == sysview.InformationSchemaViewsDDL:
			executed = append(executed, sql)
			if failReplacement {
				failReplacement = false
				return executor.Result{}, errors.New("replace VIEWS failed")
			}
			viewDefinition = sysview.InformationSchemaViewsDDL
			successfulReplacements++
			return executor.Result{}, nil
		case strings.HasPrefix(sql, "DROP VIEW IF EXISTS information_schema.VIEWS"):
			executed = append(executed, sql)
			return executor.Result{}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	})

	err := service.maintainInformationSchemaViews(t.Context())
	require.ErrorContains(t, err, "replace VIEWS failed")
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, viewDefinition,
		"a failed transaction must leave the legacy definition as the retry marker")
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor,
		"a failed replacement must not publish the page cursor")

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, sysview.InformationSchemaViewsDDL, viewDefinition)
	require.Equal(t, int32(accountID+1),
		service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
	require.Equal(t, 1, successfulReplacements)
	require.Equal(t, 4, len(executed), "the failed attempt and the committed retry each issue DROP and CREATE")

	// The persisted definition is now the idempotence marker. A subsequent
	// pass first observes an empty page, then a wrapped scan rediscovers the
	// account and must still not replace it again.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, 1, successfulReplacements)
	require.Equal(t, 4, len(executed))
}

func TestMaintainInformationSchemaViewsDoesNotSwallowReplacementNotSupported(t *testing.T) {
	const accountID = int32(10)
	state := &transactionalInformationSchemaViewsState{
		definition:     sysview.InformationSchemaViewsLegacyDDL,
		replacementErr: moerr.NewNotSupportedNoCtx("VIEWS replacement is unavailable"),
	}

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		txn executor.TxnExecutor,
		id uint32,
		schema string,
		viewName string,
	) (bool, string, error) {
		require.Equal(t, uint32(accountID), id)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "VIEWS", viewName)
		definition := state.definition
		if reader, ok := txn.(interface {
			informationSchemaViewsDefinition() string
		}); ok {
			definition = reader.informationSchemaViewsDefinition()
		}
		return definition != "", definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)
	service.upgrade.informationSchemaViewsMaintenanceState.accountCursor = 0

	err := service.maintainInformationSchemaViews(t.Context())
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, state.definition,
		"a DDL NotSupported error must roll back the staged DROP")
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, sysview.InformationSchemaViewsDDL, state.definition)
	require.Equal(t, int32(accountID+1),
		service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
}

func TestMaintainInformationSchemaViewsRollsBackOnMidPageProtocolLoss(t *testing.T) {
	state := &transactionalInformationSchemaViewsState{
		definition: sysview.InformationSchemaViewsLegacyDDL,
		accountIDs: []int32{10, 20},
		protocolResponses: []string{
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`,
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`,
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:75"}`,
		},
	}

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		id uint32,
		schema string,
		viewName string,
	) (bool, string, error) {
		require.Contains(t, []uint32{10, 20}, id)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "VIEWS", viewName)
		return true, sysview.InformationSchemaViewsLegacyDDL, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, int32(3), state.protocolCalls.Load())
	require.Equal(t, int32(1), state.replacementCalls.Load(),
		"the first tenant was staged before the protocol loss")
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, state.definition,
		"a mid-page gate miss must roll back the staged replacement")
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)

	// The gate miss is retryable. The next page attempt sees v76 and commits
	// both tenants, while the first failed attempt left no persisted progress.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, int32(6), state.protocolCalls.Load())
	require.Equal(t, int32(3), state.replacementCalls.Load())
	require.Equal(t, sysview.InformationSchemaViewsDDL, state.definition)
	require.Equal(t, int32(21), service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
}

func TestMaintainInformationSchemaViewsRetriesCommitFailureAfterRestart(t *testing.T) {
	const accountID = int32(10)
	state := &transactionalInformationSchemaViewsState{
		definition: sysview.InformationSchemaViewsLegacyDDL,
		commitErr:  errors.New("commit VIEWS replacement failed"),
	}
	installTransactionalInformationSchemaViewsCheck(t, state, accountID)

	firstService := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)
	err := firstService.maintainInformationSchemaViews(t.Context())
	require.ErrorContains(t, err, "commit VIEWS replacement failed")
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, state.definition)
	require.Zero(t, firstService.upgrade.informationSchemaViewsMaintenanceState.accountCursor)

	// The process-local cursor is intentionally discarded. A fresh service
	// starts from zero and uses the persisted legacy definition as its marker.
	restartedService := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)
	require.NoError(t, restartedService.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, sysview.InformationSchemaViewsDDL, state.definition)
	require.Equal(t, int32(accountID+1),
		restartedService.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
}

func TestMaintainInformationSchemaViewsSerializesConcurrentCallAndRetries(t *testing.T) {
	const accountID = int32(10)
	state := &transactionalInformationSchemaViewsState{
		definition:      sysview.InformationSchemaViewsLegacyDDL,
		commitErr:       errors.New("commit VIEWS replacement failed"),
		protocolEntered: make(chan struct{}),
		protocolRelease: make(chan struct{}),
	}
	installTransactionalInformationSchemaViewsCheck(t, state, accountID)
	service := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)

	firstDone := make(chan error, 1)
	go func() { firstDone <- service.maintainInformationSchemaViews(t.Context()) }()
	select {
	case <-state.protocolEntered:
	case <-time.After(time.Second):
		t.Fatal("first maintenance call did not reach the protocol barrier")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- service.maintainInformationSchemaViews(t.Context()) }()
	select {
	case err := <-secondDone:
		require.NoError(t, err, "the concurrent call should be a no-op")
	case <-time.After(time.Second):
		t.Fatal("concurrent maintenance call did not return")
	}

	close(state.protocolRelease)
	require.ErrorContains(t, <-firstDone, "commit VIEWS replacement failed")
	require.Equal(t, int32(1), state.execTxnCalls.Load())
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, state.definition)

	// A failed owner releases the guard; the next invocation can retry.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, int32(2), state.execTxnCalls.Load())
	require.Equal(t, sysview.InformationSchemaViewsDDL, state.definition)
}

func TestMaintainInformationSchemaViewsWaitsForProtocol(t *testing.T) {
	const accountID = int32(10)
	var accountLookups int
	var viewChecks int
	var replacements int

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		_ uint32,
		_ string,
		_ string,
	) (bool, string, error) {
		viewChecks++
		return true, sysview.InformationSchemaViewsLegacyDDL, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newInformationSchemaViewsMaintenanceTestService(t, func(sql string) (executor.Result, error) {
		switch {
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return newBootstrapStringResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:74"}`), nil
		case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
			accountLookups++
			return buildInformationSchemaViewsMaintenanceAccountRows(accountID), nil
		case sql == sysview.InformationSchemaViewsDDL:
			replacements++
			return executor.Result{}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	})

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Zero(t, accountLookups, "unsupported capability must not scan tenants")
	require.Zero(t, viewChecks)
	require.Zero(t, replacements)
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
}

func TestMaintainInformationSchemaViewsTransitionsLegacyDefinitionForPublicConsumers(t *testing.T) {
	const accountID = int32(10)
	state := &transactionalInformationSchemaViewsState{
		definition: sysview.InformationSchemaViewsLegacyDDL,
		protocolResponses: []string{
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:74"}`,
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`,
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`,
			`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`,
		},
	}
	installTransactionalInformationSchemaViewsCheck(t, state, accountID)
	service := newTransactionalInformationSchemaViewsMaintenanceTestService(t, state)

	// A tenant created while capability discovery is unavailable keeps the
	// predecessor definition, which is safe for every public VIEWS consumer.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, sysview.InformationSchemaViewsLegacyDDL, state.definition)
	require.Zero(t, state.replacementCalls.Load())
	require.Contains(t, state.definition, "tbl.rel_createsql AS `VIEW_DEFINITION`")
	require.Contains(t, state.definition, "'YES' AS `IS_UPDATABLE`")

	// Once all-CN capability is available, maintenance replaces the same
	// persisted definition transactionally and exposes the current contract.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, sysview.InformationSchemaViewsDDL, state.definition)
	require.Equal(t, int32(1), state.replacementCalls.Load())
	require.Contains(t, state.definition, "mo_view_definition(tbl.viewdef)")
	require.Contains(t, state.definition, "cast('NO' as varchar(3)) AS `IS_UPDATABLE`")

	// The current definition is the durable idempotence marker; a later pass
	// must not replace it again.
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, int32(1), state.replacementCalls.Load())
}

func TestInformationSchemaViewsProtocolGateClassifier(t *testing.T) {
	rollbackErr := errors.New("transaction rollback failed")
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "gate only", err: errInformationSchemaViewsProtocolUnavailable, want: true},
		{name: "different not supported", err: moerr.NewNotSupportedNoCtx("different capability"), want: false},
		{name: "gate with rollback error", err: errors.Join(errInformationSchemaViewsProtocolUnavailable, rollbackErr), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isOnlyInformationSchemaViewsProtocolGateError(test.err))
		})
	}
}

func TestMaintainInformationSchemaViewsSkipsAccountDroppedDuringScan(t *testing.T) {
	const droppedAccountID = int32(10)
	const survivingAccountID = int32(20)
	var replacements int

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		id uint32,
		_ string,
		_ string,
	) (bool, string, error) {
		switch int32(id) {
		case droppedAccountID:
			return false, "", moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_user")
		case survivingAccountID:
			return true, sysview.InformationSchemaViewsLegacyDDL, nil
		default:
			t.Fatalf("unexpected account %d", id)
			return false, "", nil
		}
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newInformationSchemaViewsMaintenanceTestService(t, func(sql string) (executor.Result, error) {
		switch {
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return newBootstrapStringResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`), nil
		case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
			require.Contains(t, sql, "account_id >= 0")
			return buildInformationSchemaViewsMaintenanceAccountRows(droppedAccountID, survivingAccountID), nil
		case sql == sysview.InformationSchemaViewsDDL:
			replacements++
			return executor.Result{}, nil
		case strings.HasPrefix(sql, "DROP VIEW IF EXISTS information_schema.VIEWS"):
			return executor.Result{}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	})

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, 1, replacements)
	require.Equal(t, int32(survivingAccountID+1),
		service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)
}

func TestMaintainInformationSchemaViewsFindsLateAccountAfterWrap(t *testing.T) {
	definitions := map[int32]string{
		10: sysview.InformationSchemaViewsDDL,
		20: sysview.InformationSchemaViewsDDL,
	}
	var accountLookups int
	var replacements int

	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		_ executor.TxnExecutor,
		accountID uint32,
		_ string,
		_ string,
	) (bool, string, error) {
		definition, exists := definitions[int32(accountID)]
		return exists, definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })

	service := newInformationSchemaViewsMaintenanceTestService(t, func(sql string) (executor.Result, error) {
		switch {
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return newBootstrapStringResult(`{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`), nil
		case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
			accountLookups++
			switch accountLookups {
			case 1:
				require.Contains(t, sql, "account_id >= 0")
				return buildInformationSchemaViewsMaintenanceAccountRows(10, 20), nil
			case 2:
				require.Contains(t, sql, "account_id >= 21")
				return executor.Result{}, nil
			case 3:
				require.Contains(t, sql, "account_id >= 0")
				require.Contains(t, sql, "limit 32")
				return buildInformationSchemaViewsMaintenanceAccountRows(10, 20, 30), nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected account lookup %d", accountLookups)
			}
		case sql == sysview.InformationSchemaViewsDDL:
			replacements++
			definitions[30] = sysview.InformationSchemaViewsDDL
			return executor.Result{}, nil
		case strings.HasPrefix(sql, "DROP VIEW IF EXISTS information_schema.VIEWS"):
			return executor.Result{}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	})

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, int32(21), service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)

	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Zero(t, service.upgrade.informationSchemaViewsMaintenanceState.accountCursor)

	// The new account appears after the first finite scan. Once the cursor
	// wraps, it is rediscovered and repaired in the same bounded page.
	definitions[30] = sysview.InformationSchemaViewsLegacyDDL
	require.NoError(t, service.maintainInformationSchemaViews(t.Context()))
	require.Equal(t, 1, replacements)
	require.Equal(t, sysview.InformationSchemaViewsDDL, definitions[30])
}

func newInformationSchemaViewsMaintenanceTestService(
	t *testing.T,
	mocker func(string) (executor.Result, error),
) *service {
	t.Helper()
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor2(mocker, txnOperator),
		func(*service) {},
	)
}

func buildInformationSchemaViewsMaintenanceAccountRows(accountIDs ...int32) executor.Result {
	result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mpool.MustNewZero())
	result.NewBatchWithRowCount(len(accountIDs))
	executor.AppendFixedRows(result, 0, accountIDs)
	return result.GetResult()
}

func installTransactionalInformationSchemaViewsCheck(
	t *testing.T,
	state *transactionalInformationSchemaViewsState,
	accountID int32,
) {
	t.Helper()
	oldCheck := versions.CheckViewDefinition
	versions.CheckViewDefinition = func(
		txn executor.TxnExecutor,
		id uint32,
		schema string,
		viewName string,
	) (bool, string, error) {
		require.Equal(t, uint32(accountID), id)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "VIEWS", viewName)
		definition := state.definition
		if reader, ok := txn.(interface {
			informationSchemaViewsDefinition() string
		}); ok {
			definition = reader.informationSchemaViewsDefinition()
		}
		return definition != "", definition, nil
	}
	t.Cleanup(func() { versions.CheckViewDefinition = oldCheck })
}

type transactionalInformationSchemaViewsState struct {
	definition        string
	replacementErr    error
	commitErr         error
	accountIDs        []int32
	protocolResponses []string
	protocolCalls     atomic.Int32
	replacementCalls  atomic.Int32
	protocolEntered   chan struct{}
	protocolRelease   chan struct{}
	protocolOnce      sync.Once
	execTxnCalls      atomic.Int32
}

type transactionalInformationSchemaViewsExecutor struct {
	state       *transactionalInformationSchemaViewsState
	txnOperator client.TxnOperator
}

func newTransactionalInformationSchemaViewsMaintenanceTestService(
	t *testing.T,
	state *transactionalInformationSchemaViewsState,
) *service {
	t.Helper()
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		&transactionalInformationSchemaViewsExecutor{
			state:       state,
			txnOperator: txnOperator,
		},
		func(*service) {},
	)
}

func (e *transactionalInformationSchemaViewsExecutor) Exec(
	context.Context,
	string,
	executor.Options,
) (executor.Result, error) {
	return executor.Result{}, errors.New("unexpected non-transactional execution")
}

func (e *transactionalInformationSchemaViewsExecutor) ExecTxn(
	_ context.Context,
	execFunc func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	e.state.execTxnCalls.Add(1)
	txn := &transactionalInformationSchemaViewsTxn{
		state:       e.state,
		definition:  e.state.definition,
		txnOperator: e.txnOperator,
	}
	if err := execFunc(txn); err != nil {
		return err
	}
	if e.state.commitErr != nil {
		err := e.state.commitErr
		e.state.commitErr = nil
		return err
	}
	e.state.definition = txn.definition
	return nil
}

type transactionalInformationSchemaViewsTxn struct {
	state       *transactionalInformationSchemaViewsState
	definition  string
	txnOperator client.TxnOperator
}

func (txn *transactionalInformationSchemaViewsTxn) Use(string) {}

func (txn *transactionalInformationSchemaViewsTxn) LockTable(string) error { return nil }

func (txn *transactionalInformationSchemaViewsTxn) Txn() client.TxnOperator {
	return txn.txnOperator
}

func (txn *transactionalInformationSchemaViewsTxn) informationSchemaViewsDefinition() string {
	return txn.definition
}

func (txn *transactionalInformationSchemaViewsTxn) Exec(
	sql string,
	_ executor.StatementOption,
) (executor.Result, error) {
	switch {
	case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
		if txn.state.protocolEntered != nil && txn.state.protocolRelease != nil {
			txn.state.protocolOnce.Do(func() { close(txn.state.protocolEntered) })
			<-txn.state.protocolRelease
		}
		response := `{"method":"GETPROTOCOLVERSION","result":"cn-a:76"}`
		call := int(txn.state.protocolCalls.Add(1)) - 1
		if call < len(txn.state.protocolResponses) {
			response = txn.state.protocolResponses[call]
		}
		return newBootstrapStringResult(response), nil
	case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
		accountIDs := txn.state.accountIDs
		if len(accountIDs) == 0 {
			accountIDs = []int32{10}
		}
		return buildInformationSchemaViewsMaintenanceAccountRows(accountIDs...), nil
	case strings.HasPrefix(sql, "DROP VIEW IF EXISTS information_schema.VIEWS"):
		txn.definition = ""
		return executor.Result{}, nil
	case sql == sysview.InformationSchemaViewsDDL:
		txn.state.replacementCalls.Add(1)
		if txn.state.replacementErr != nil {
			err := txn.state.replacementErr
			txn.state.replacementErr = nil
			return executor.Result{}, err
		}
		txn.definition = sysview.InformationSchemaViewsDDL
		return executor.Result{}, nil
	default:
		return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
	}
}
