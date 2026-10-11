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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	frontendmock "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	mysql "github.com/matrixorigin/mysql"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type futureCDCAdmissionCatalog struct {
	mu            sync.Mutex
	owner, source uint64
	watermark     string
	statements    []string
	errMsg        string
}

type futureCDCTargetStateResult struct{ *claimLossWatermarkResult }

func (r *futureCDCTargetStateResult) Value(_ context.Context, _, column uint64) (interface{}, error) {
	if column == 3 && r.rows[0][3] != "" {
		return r.rows[0][3], nil
	}
	return nil, nil
}

type cdcSourceKeyCatalog struct {
	rows    [][]string
	err     error
	queries int
}

func (*cdcSourceKeyCatalog) Exec(context.Context, string, ie.SessionOverrideOptions) error {
	return nil
}
func (c *cdcSourceKeyCatalog) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	c.queries++
	return &claimLossWatermarkResult{rows: c.rows, err: c.err}
}
func (*cdcSourceKeyCatalog) ApplySessionOverride(ie.SessionOverrideOptions) {}

func (c *futureCDCAdmissionCatalog) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, sql)
	if match := regexp.MustCompile(`'([^']*)' AS err_msg`).FindStringSubmatch(sql); match != nil {
		owner := regexp.MustCompile(`([0-9]+) AS owner_generation`).FindStringSubmatch(sql)
		if owner != nil && owner[1] == strconv.FormatUint(c.owner, 10) {
			c.errMsg = match[1]
		}
	}
	return nil
}

func (c *futureCDCAdmissionCatalog) Query(_ context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasPrefix(sql, "SELECT snapshot_epoch") {
		return &claimLossWatermarkResult{}
	}
	if strings.HasPrefix(sql, "SELECT account_id, task_id, db_name, table_name, watermark") {
		return &claimLossWatermarkResult{rows: [][]string{{"1", "task", "db", "t", c.watermark}}}
	}
	if strings.Contains(sql, "SELECT err_msg") {
		return &claimLossWatermarkResult{rows: [][]string{{c.errMsg}}}
	}
	if strings.HasPrefix(sql, "SELECT owner_generation, source_table_id, pending_source_table_id, target_identity") {
		identity := ""
		if c.source != 0 {
			identity = "mo:" + strconv.FormatUint(c.source, 10)
		}
		return &futureCDCTargetStateResult{&claimLossWatermarkResult{rows: [][]string{{
			strconv.FormatUint(c.owner, 10), strconv.FormatUint(c.source, 10), "", identity,
		}}}}
	}
	if !strings.HasPrefix(sql, "SELECT owner_generation, watermark, source_table_id") {
		return &claimLossWatermarkResult{err: errors.New("unexpected catalog read")}
	}
	return &claimLossWatermarkResult{rows: [][]string{{
		strconv.FormatUint(c.owner, 10), c.watermark, strconv.FormatUint(c.source, 10),
	}}}
}

func (*futureCDCAdmissionCatalog) ApplySessionOverride(ie.SessionOverrideOptions) {}

func TestCDCFutureStartDefersWithoutSpendingErrorBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		noFull    bool
		storedID  uint64
		currentID uint64
		watermark string
	}{
		{"first NoFull", true, 0, 10, "20-0"},
		{"first explicit full", false, 0, 10, "20-0"},
		{"replacement", true, 9, 10, "30-0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fence := cdc.NewOwnerFenceForGeneration(time.Unix(100, 0), func(context.Context) error { return nil })
			catalog := &futureCDCAdmissionCatalog{
				owner: fence.GenerationToken(), source: tc.storedID, watermark: tc.watermark,
			}
			executor := &CDCTaskExecutor{
				watermarkUpdater: cdc.NewCDCWatermarkUpdater(t.Name(), catalog),
				noFull:           tc.noFull, explicitStart: true, startTs: types.BuildTS(20, 0),
			}
			ctrl := gomock.NewController(t)
			txn := frontendmock.NewMockTxnOperator(ctrl)
			txn.EXPECT().SnapshotTS().Return(types.BuildTS(10, 0).ToTimestamp()).AnyTimes()
			key := &cdc.WatermarkKey{AccountId: 1, TaskId: "t", DBName: "db", TableName: "src"}
			for attempt := 0; attempt < 5; attempt++ {
				state, err := executor.prepareGenerationAdmission(context.Background(), key, tc.currentID, "db", "dst", txn, fence)
				require.NoError(t, err)
				require.True(t, state.deferred)
				require.False(t, state.targetReady)
			}
			for _, sql := range catalog.statements {
				require.Contains(t, sql, "mo_cdc_watermark")
				require.NotContains(t, sql, "mo_cdc_snapshot")
			}
		})
	}
}

func TestCDCEndTsAbsentGenerationRequiresDurableOldCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		watermark string
		canCheck  bool
	}{
		{"old progress behind EndTs", "19-0", false},
		{"old progress at EndTs", "20-0", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fence := cdc.NewOwnerFenceForGeneration(time.Unix(101, 0), func(context.Context) error { return nil })
			catalog := &futureCDCAdmissionCatalog{
				owner: fence.GenerationToken(), source: 9, watermark: tc.watermark,
			}
			ctrl := gomock.NewController(t)
			current := frontendmock.NewMockTxnOperator(ctrl)
			current.EXPECT().SnapshotTS().Return(types.BuildTS(30, 0).ToTimestamp()).AnyTimes()
			historical := frontendmock.NewMockTxnOperator(ctrl)
			historical.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp())
			historical.EXPECT().Rollback(gomock.Any()).Return(nil)
			client := frontendmock.NewMockTxnClient(ctrl)
			client.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(historical, nil)
			storage := frontendmock.NewMockEngine(ctrl)
			storage.EXPECT().New(gomock.Any(), historical).Return(nil)
			storage.EXPECT().GetRelationById(gomock.Any(), historical, uint64(10)).Return(
				"", "", nil, moerr.NewNoSuchTablef(context.Background(), "can not find table by id 10: accountId: 1"))
			storage.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second})
			executor := &CDCTaskExecutor{
				watermarkUpdater: cdc.NewCDCWatermarkUpdater(t.Name(), catalog),
				cnTxnClient:      client, cnEngine: storage, noFull: true, endTs: types.BuildTS(20, 0),
			}
			key := &cdc.WatermarkKey{AccountId: 1, TaskId: "t", DBName: "db", TableName: "src"}
			state, err := executor.prepareGenerationAdmission(context.Background(), key, 10, "db", "dst", current, fence)
			if tc.canCheck {
				require.NoError(t, err)
				require.True(t, state.completeAfterCheck)
			} else {
				require.ErrorContains(t, err, "prior durable progress")
				require.False(t, state.completeAfterCheck)
			}
			for _, sql := range catalog.statements {
				require.NotContains(t, sql, "mo_cdc_snapshot")
			}
		})
	}
}

func TestCDCEndTsFirstAdmissionChecksHistoricalGenerationBeforeTarget(t *testing.T) {
	for _, tc := range []struct {
		name, watermark                string
		noFull, explicitStart, visible bool
		cause                          error
	}{
		{"initial full absent", "0-0", false, false, false, nil},
		{"NoFull absent", "10-0", true, false, false, nil},
		{"explicit start absent", "10-0", false, true, false, nil},
		{"transient historical read", "0-0", false, false, false, moerr.NewRPCTimeoutNoCtx()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fence := cdc.NewOwnerFenceForGeneration(time.Unix(102, 0), func(context.Context) error { return nil })
			catalog := &futureCDCAdmissionCatalog{
				owner: fence.GenerationToken(), watermark: tc.watermark,
			}
			ctrl := gomock.NewController(t)
			current := frontendmock.NewMockTxnOperator(ctrl)
			current.EXPECT().SnapshotTS().Return(types.BuildTS(30, 0).ToTimestamp()).AnyTimes()
			historical := frontendmock.NewMockTxnOperator(ctrl)
			historical.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp())
			historical.EXPECT().Rollback(gomock.Any()).Return(nil)
			client := frontendmock.NewMockTxnClient(ctrl)
			client.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(historical, nil)
			storage := frontendmock.NewMockEngine(ctrl)
			storage.EXPECT().New(gomock.Any(), historical).Return(nil)
			if tc.cause != nil {
				storage.EXPECT().GetRelationById(gomock.Any(), historical, uint64(10)).Return("", "", nil, tc.cause)
			} else if tc.visible {
				relation := frontendmock.NewMockRelation(ctrl)
				relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(10))
				storage.EXPECT().GetRelationById(gomock.Any(), historical, uint64(10)).Return("db", "src", relation, nil)
			} else {
				storage.EXPECT().GetRelationById(gomock.Any(), historical, uint64(10)).Return(
					"", "", nil, moerr.NewNoSuchTablef(context.Background(), "can not find table by id 10: accountId: 1"))
			}
			storage.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second})
			startTs := types.BuildTS(10, 0)
			if !tc.noFull && !tc.explicitStart {
				startTs = types.TS{}
			}
			executor := &CDCTaskExecutor{
				watermarkUpdater: cdc.NewCDCWatermarkUpdater(t.Name(), catalog),
				cnTxnClient:      client, cnEngine: storage, noFull: tc.noFull,
				explicitStart: tc.explicitStart, startTs: startTs, endTs: types.BuildTS(20, 0),
			}
			key := &cdc.WatermarkKey{AccountId: 1, TaskId: "t", DBName: "db", TableName: "src"}
			state, err := executor.prepareGenerationAdmission(context.Background(), key, 10, "db", "dst", current, fence)
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
				retryable, _ := cdc.ClassifyRetryableError(err)
				require.True(t, retryable)
			} else if tc.visible {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "was absent at EndTs")
				require.ErrorContains(t, err, "first admission cannot prove an empty target")
			}
			require.False(t, state.targetReady)
			require.False(t, state.complete)
			for _, sql := range catalog.statements {
				require.NotContains(t, sql, "mo_cdc_snapshot")
			}
		})
	}
}

func TestCDCMode2CaseOnlyRenameBlocksNewWatermarkKey(t *testing.T) {
	key := &cdc.WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "T"}
	for _, tc := range []struct {
		name      string
		rows      [][]string
		queryErr  error
		ambiguous bool
	}{
		{"same raw key", [][]string{{"db", "T"}}, nil, false},
		{"case-only rename", [][]string{{"db", "t"}}, nil, true},
		{"unrelated table", [][]string{{"db", "other"}}, nil, false},
		{"backend timeout", nil, context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &CDCTaskExecutor{
				ie:     &cdcSourceKeyCatalog{rows: tc.rows, err: tc.queryErr},
				tables: cdc.PatternTuples{SourceCaseMode: 2},
			}
			ambiguous, err := exec.rejectAmbiguousSourceKey(context.Background(), key, &cdcSourceKeyIndex{})
			require.Equal(t, tc.ambiguous, ambiguous)
			if tc.ambiguous {
				require.ErrorContains(t, err, "case-only rename")
			} else if tc.queryErr != nil {
				require.ErrorIs(t, err, tc.queryErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCDCMode2SourceKeyIndexReadsOnceAndTracksNewKeys(t *testing.T) {
	catalog := &cdcSourceKeyCatalog{rows: [][]string{{"db", "existing"}, {"DB", "EXISTING"}}}
	exec := &CDCTaskExecutor{ie: catalog, tables: cdc.PatternTuples{SourceCaseMode: 2}}
	index := &cdcSourceKeyIndex{}
	ctx := context.Background()
	for _, name := range []string{"first", "second", "third"} {
		key := &cdc.WatermarkKey{DBName: "db", TableName: name}
		ambiguous, err := exec.rejectAmbiguousSourceKey(ctx, key, index)
		require.NoError(t, err)
		require.False(t, ambiguous)
		index.add(key) // The callback inserted this watermark after the initial read.
	}
	require.Equal(t, 1, catalog.queries)
	ambiguous, err := exec.rejectAmbiguousSourceKey(ctx,
		&cdc.WatermarkKey{DBName: "db", TableName: "existing"}, index)
	require.True(t, ambiguous, "the first raw spelling must still see the second alias")
	require.ErrorContains(t, err, "case-only rename")
	alias := &cdc.WatermarkKey{DBName: "DB", TableName: "FIRST"}
	ambiguous, err = exec.rejectAmbiguousSourceKey(ctx, alias, index)
	require.True(t, ambiguous)
	require.ErrorContains(t, err, "case-only rename")
	require.Equal(t, 1, catalog.queries)
}

// The guard owns retry classification before its caller persists a table error.
// Wrapping at a backend boundary must preserve both retryability and cause.
func TestCDCSourceGuardRetryContract(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		retry bool
	}{
		{"snapshot advanced", moerr.NewTxnNeedRetryNoCtx(), true},
		{"definition changed", moerr.NewTxnNeedRetryWithDefChangedNoCtx(), true},
		{"rpc timeout", moerr.NewRPCTimeoutNoCtx(), true},
		{"target wire timeout", &gomysql.MySQLError{Number: 1159, Message: "communication timeout"}, true},
		{"deadline", context.DeadlineExceeded, true},
		{"service unavailable", moerr.NewServiceUnavailableNoCtx("temporary"), true},
		{"unknown", errors.New("unclassified failure"), false},
		{"unsupported", moerr.NewNotSupportedNoCtx("guard unavailable"), false},
		{"cancelled", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := frontendmock.NewMockTxnClient(ctrl)
			client.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("backend: %w", tc.cause))
			exec := &CDCTaskExecutor{cnTxnClient: client}
			err := exec.withCDCSourceGenerationGuard(context.Background(), 1, "db", "t", 42, func() error {
				t.Fatal("failed source guard must never acknowledge target identity")
				return nil
			})
			require.ErrorIs(t, err, tc.cause)
			require.Equal(t, tc.retry, cdc.IsRetryableSnapshotEpochError(err))
			if errors.Is(err, context.Canceled) {
				return // Caller cancellation is filtered before persistence.
			}
			// Follow the admission error through the real owned watermark writer
			// and the next callback's catalog consumer, without a live cluster.
			catalog := &futureCDCAdmissionCatalog{owner: 123}
			updater := cdc.NewCDCWatermarkUpdater(t.Name(), catalog)
			updater.Start()
			defer updater.Stop()
			fence := cdc.NewOwnerFenceForGeneration(time.UnixMicro(123), func(context.Context) error { return nil })
			key := &cdc.WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "t"}
			retryable, _ := cdc.ClassifyRetryableError(err)
			require.NoError(t, updater.UpdateWatermarkErrMsg(
				cdc.WithWatermarkOwnerFence(context.Background(), fence, 0), key,
				err.Error(), &cdc.ErrorContext{IsRetryable: retryable}))
			require.NotEmpty(t, catalog.errMsg)
			require.Len(t, catalog.statements, 1)
			require.Contains(t, catalog.statements[0], "w.owner_generation = v.owner_generation")
			hasError, readErr := GetTableErrMsg(context.Background(), 1, catalog, "task", &cdc.DbTableInfo{SourceDbName: "db", SourceTblName: "t"})
			require.NoError(t, readErr)
			require.Equal(t, !tc.retry, hasError, "a transient admission failure must allow the next callback")
		})
	}
}

// Exercise the real callback, factory, owned diagnostic writer and next-callback gate.
// Only the target connection and source engine dependencies are simulated.
func TestCDCTargetSetupRetryAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		retry bool
	}{
		{"production wire timeout", &mysql.MySQLError{Number: 2013, Message: "target setup connection lost"}, true},
		{"production wire syntax", &mysql.MySQLError{Number: 1064, Message: "target setup syntax error"}, false},
		{"release deadline", context.DeadlineExceeded, true},
		{"cancelled callback", &mysql.MySQLError{Number: 2013}, true},
		{"obsolete callback", &mysql.MySQLError{Number: 2013}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubs := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
				return nil, nil
			})
			defer stubs.Reset()
			stubs.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
			stubs.Stub(&cdc.GetTableDef, func(context.Context, client.TxnOperator, engine.Engine, uint64) (*plan.TableDef, error) {
				return &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Default: &plan.Default{}, Typ: plan.Type{Id: int32(types.T_int32)}}}, Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}}, Name2ColIndex: map[string]int32{"id": 0}}, nil
			})
			opens := 0
			var exec *CDCTaskExecutor
			stubs.Stub(&cdc.OpenDbConn, func(context.Context, string, string, string, int, string) (*sql.DB, error) {
				opens++
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(expected, actual string) error {
					// Cancel after the capability/identity transactions have ended;
					// database/sql's automatic rollback must not race fixture cleanup.
					if tc.name == "cancelled callback" && strings.Contains(actual, "SELECT RELEASE_LOCK") {
						exec.callbackCancel()
					}
					if tc.name == "obsolete callback" && strings.Contains(actual, "CALL mo_cdc_target_guard_capability") {
						exec.callbackMu.Lock()
						exec.callbackGeneration.Add(1)
						exec.callbackMu.Unlock()
					}
					return sqlmock.QueryMatcherRegexp.Match(expected, actual)
				})))
				require.NoError(t, err)
				t.Cleanup(func() {
					defer db.Close()
					require.NoError(t, mock.ExpectationsWereMet())
					require.Zero(t, db.Stats().OpenConnections)
				})
				mock.ExpectQuery("SELECT GET_LOCK").WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))
				mock.ExpectBegin()
				if tc.name != "release deadline" && tc.name != "cancelled callback" {
					mock.ExpectExec("CALL mo_cdc_target_guard_capability").WillReturnError(tc.cause)
					mock.ExpectRollback()
					mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(1))
					mock.ExpectClose()
					return db, nil
				}
				mock.ExpectExec("CALL mo_cdc_target_guard_capability").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectRollback()
				mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectBegin()
				mock.ExpectQuery("CALL mo_cdc_target_identity").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(42))
				mock.ExpectQuery("SELECT column_name, column_type").WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type", "collation_name", "numeric_scale"}).AddRow("id", "int", nil, nil))
				mock.ExpectQuery("SELECT index_name, non_unique").WillReturnRows(sqlmock.NewRows([]string{"index_name", "non_unique", "seq_in_index", "column_name", "sub_part"}).AddRow("PRIMARY", 0, 1, "id", nil))
				mock.ExpectRollback()
				mock.ExpectQuery("SELECT RELEASE_LOCK").WillReturnError(tc.cause)
				mock.ExpectClose()
				return db, nil
			})
			ctrl := gomock.NewController(t)
			eng := frontendmock.NewMockEngine(ctrl)
			eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			fence := cdc.NewOwnerFenceForGeneration(time.UnixMicro(123), func(context.Context) error { return nil })
			catalog := &futureCDCAdmissionCatalog{owner: fence.GenerationToken(), source: 42, watermark: "10-0"}
			updater := cdc.NewCDCWatermarkUpdater(t.Name(), catalog)
			updater.Start()
			defer updater.Stop()
			exec = &CDCTaskExecutor{
				spec:     &task.CreateCdcDetails{TaskId: "task", TaskName: "task", Accounts: []*task.Account{{Id: 1}}},
				tables:   cdc.PatternTuples{Pts: []*cdc.PatternTuple{{Source: cdc.PatternTable{Database: "db", Table: "t"}, Sink: cdc.PatternTable{Database: "sink", Table: "t"}}}},
				cnEngine: eng, ie: catalog, watermarkUpdater: updater, claimFence: fence,
				additionalConfig: map[string]any{cdc.CDCTaskExtraOptions_MaxSqlLength: float64(cdc.CDCDefaultTaskExtra_MaxSQLLen), cdc.CDCTaskExtraOptions_SendSqlTimeout: cdc.CDCDefaultSendSqlTimeout},
				noFull:           true, startTs: types.BuildTS(10, 0), sinkUri: cdc.UriInfo{SinkTyp: cdc.CDCSinkType_MO},
				activeRoutine: cdc.NewCdcActiveRoutine(), runningReaders: &sync.Map{}, holdCh: make(chan int, 1), stateMachine: NewExecutorStateMachine(),
			}
			require.NoError(t, exec.stateMachine.Transition(TransitionStart))
			require.NoError(t, exec.stateMachine.Transition(TransitionStartSuccess))
			tables := map[uint32]cdc.TblMap{1: {"db.t": {SourceDbName: "db", SourceTblName: "t", SourceTblId: 42, HasUserPrimaryKey: true}}}
			if tc.name == "cancelled callback" || tc.name == "obsolete callback" {
				require.Error(t, exec.handleNewTables(tables))
				require.Equal(t, 1, opens)
				catalog.mu.Lock()
				diagnostic, source, watermark := catalog.errMsg, catalog.source, catalog.watermark
				catalog.mu.Unlock()
				require.Empty(t, diagnostic)
				require.Equal(t, uint64(42), source)
				require.Equal(t, "10-0", watermark)
				return
			}
			attempts := 1
			if tc.retry {
				attempts = 4
			}
			for attempt := 1; attempt <= attempts; attempt++ {
				require.Error(t, exec.handleNewTables(tables))
				require.Equal(t, attempt, opens)
				catalog.mu.Lock()
				diagnostic := catalog.errMsg
				watermark := catalog.watermark
				catalog.mu.Unlock()
				require.Contains(t, diagnostic, tc.cause.Error())
				require.Equal(t, "10-0", watermark)
				if tc.retry && attempt < 4 {
					require.True(t, strings.HasPrefix(diagnostic, fmt.Sprintf("R:%d:", attempt)), diagnostic)
				} else {
					require.True(t, strings.HasPrefix(diagnostic, "N:"), diagnostic)
				}
				require.Equal(t, StateRunning, exec.stateMachine.State())
				exec.runningReaders.Range(func(_, _ any) bool { t.Fatal("failed setup published a reader"); return false })
			}
			require.Error(t, exec.handleNewTables(tables))
			require.Equal(t, attempts, opens, "permanent gate must precede target setup")
			require.Equal(t, StateFailed, exec.stateMachine.State())
		})
	}
}
