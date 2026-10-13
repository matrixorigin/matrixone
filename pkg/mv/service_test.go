// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package mv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewSourceIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer require.Zero(t, mp.CurrNB())
	st := &State{Key: Key{7, 99}, Database: "db", Definition: catalog.MVDefinition{Sources: []catalog.MVSource{{AccountID: 7, DatabaseID: 2, TableID: 3, LogicalID: 4, Version: 5, Database: "db", Table: "t"}}}}
	for _, version := range []uint32{5, 6} {
		tx := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			require.Contains(t, sql, "account_id=7 and reldatabase_id=2")
			r := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint32.ToType()}, mp)
			r.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(r, 0, []uint64{3}))
			require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{4}))
			require.NoError(t, executor.AppendFixedRows(r, 2, []uint32{version}))
			return r.GetResult(), nil
		}, nil)
		err := ValidateSources(tx, st)
		if version == 5 {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "MV_INVALID_DEFINITION")
		}
	}
	st.Definition.Sources[0].AccountID = 8
	tx := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
		t.Fatal("invalid account reached SQL")
		return executor.Result{}, nil
	}, nil)
	require.Error(t, ValidateSources(tx, st))
}
func TestMaterializedViewTaskIdentity(t *testing.T) {
	a := runMetadata(Key{7, 99}, 2, 10)
	require.LessOrEqual(t, len(a.ID), 50, "task storage metadata ID width")
	require.Equal(t, a, runMetadata(Key{7, 99}, 2, 10))
	require.NotEqual(t, a.ID, runMetadata(Key{7, 100}, 2, 10).ID)
	require.NotEqual(t, a.ID, runMetadata(Key{7, 99}, 3, 10).ID)
	require.NotEqual(t, a.ID, runMetadata(Key{7, 99}, 2, 11).ID)
	var spec TaskSpec
	require.NoError(t, json.Unmarshal(a.Context, &spec))
	require.Equal(t, uint64(99), spec.Key.ID)
	require.Len(t, spec.RunID, 64)
	s := NewService(executor.NewMemExecutor(func(string) (executor.Result, error) {
		t.Fatal("cancelled admission reached SQL")
		return executor.Result{}, nil
	}))
	s.accounts[7] <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Refresh(ctx, Key{7, 99}, 0), context.Canceled)
}

type refreshExecutor struct {
	executor.SQLExecutor
	t                  *testing.T
	op                 client.TxnOperator
	mp                 *mpool.MPool
	state              State
	failInsert         bool
	failPublish        bool
	conflictOnce       bool
	changeEpoch        bool
	loseCommitResponse bool
	prior              *refreshRun
	lastPublication    string
	failureRecords     int
	dataWrites         int
	publications       int
	commits, rollbacks int
}

func (e *refreshExecutor) ExecTxn(ctx context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
	if e.conflictOnce {
		e.conflictOnce = false
		return moerr.NewTxnWWConflictNoCtx(99, "injected conflict")
	}
	_, bounded := ctx.Deadline()
	require.True(e.t, bounded, "native transaction operations require a deadline")
	tx := executor.NewMemTxnExecutor(e.statement, e.op)
	err := f(tx)
	if err != nil {
		e.rollbacks++
	} else {
		e.commits++
		if e.changeEpoch && e.commits == 1 {
			e.state.Epoch++
		}
		if e.loseCommitResponse && e.publications > 0 {
			e.loseCommitResponse = false
			e.state.LastRun = e.lastPublication
			return errors.New("lost commit response")
		}
	}
	return err
}
func (e *refreshExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	if strings.HasPrefix(sql, "delete from `db`.`stored`") || strings.HasPrefix(sql, "insert into `db`.`stored`") {
		require.Same(e.t, e.op, opts.Txn())
		require.True(e.t, defines.CanWriteMaterializedView(ctx, 7, 99, e.op.Txn().ID))
		e.dataWrites++
		if e.failInsert && strings.HasPrefix(sql, "insert") {
			return executor.Result{}, errors.New("injected source/write failure")
		}
	}
	return e.statement(sql)
}
func (e *refreshExecutor) statement(sql string) (executor.Result, error) {
	if strings.Contains(sql, "set status='FAILED'") {
		e.failureRecords++
	}
	if strings.HasPrefix(sql, "select sequence,") && e.prior != nil {
		r := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType()}, e.mp)
		r.NewBatchWithRowCount(1)
		require.NoError(e.t, executor.AppendFixedRows(r, 0, []uint64{e.prior.sequence}))
		require.NoError(e.t, executor.AppendFixedRows(r, 1, []uint64{e.prior.epoch}))
		require.NoError(e.t, executor.AppendStringRows(r, 2, []string{e.prior.status}))
		return r.GetResult(), nil
	}
	if strings.HasPrefix(sql, "select database_name,") {
		return e.stateResult(), nil
	}
	if strings.HasPrefix(sql, "select rel_id,") {
		r := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint32.ToType()}, e.mp)
		r.NewBatchWithRowCount(1)
		require.NoError(e.t, executor.AppendFixedRows(r, 0, []uint64{3}))
		require.NoError(e.t, executor.AppendFixedRows(r, 1, []uint64{4}))
		require.NoError(e.t, executor.AppendFixedRows(r, 2, []uint32{5}))
		return r.GetResult(), nil
	}
	if strings.HasPrefix(sql, "update mo_catalog.mo_materialized_views set populated=true") {
		if e.failPublish {
			return executor.Result{}, errors.New("injected publication failure")
		}
		e.publications++
		e.lastPublication = strings.SplitN(strings.SplitN(sql, "last_run='", 2)[1], "'", 2)[0]
		require.Contains(e.t, sql, "source_physical=123,source_logical=17")
	}
	return executor.Result{}, nil
}
func (e *refreshExecutor) stateResult() executor.Result {
	data, err := json.Marshal(e.state.Definition)
	require.NoError(e.t, err)
	r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_text.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_bool.ToType(), types.T_varchar.ToType(), types.T_int64.ToType(), types.T_uint32.ToType(), types.T_int64.ToType()}, e.mp)
	r.NewBatchWithRowCount(1)
	for i, s := range []string{"db", "stored", string(data)} {
		require.NoError(e.t, executor.AppendStringRows(r, i, []string{s}))
	}
	require.NoError(e.t, executor.AppendFixedRows(r, 3, []uint64{e.state.Epoch}))
	require.NoError(e.t, executor.AppendFixedRows(r, 4, []uint64{e.state.Requested}))
	require.NoError(e.t, executor.AppendFixedRows(r, 5, []uint64{e.state.Successful}))
	require.NoError(e.t, executor.AppendFixedRows(r, 6, []bool{true}))
	require.NoError(e.t, executor.AppendStringRows(r, 7, []string{e.state.LastRun}))
	require.NoError(e.t, executor.AppendFixedRows(r, 8, []int64{5}))
	require.NoError(e.t, executor.AppendFixedRows(r, 9, []uint32{6}))
	require.NoError(e.t, executor.AppendFixedRows(r, 10, []int64{e.state.Interval}))
	return r.GetResult()
}
func TestMaterializedViewRefreshTransaction(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fail  bool
		epoch uint64
	}{{"success", false, 2}, {"insert failure", true, 2}, {"obsolete epoch", false, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 17}).AnyTimes()
			op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("refresh-txn")}).AnyTimes()
			e := &refreshExecutor{t: t, op: op, mp: mp, failInsert: tc.fail, state: State{Key: Key{7, 99}, Epoch: 2, Requested: 10, Successful: 9, Definition: catalog.MVDefinition{FormatVersion: 1, Database: "db", Query: "select * from t", TimeZone: "UTC", Sources: []catalog.MVSource{{AccountID: 7, DatabaseID: 2, TableID: 3, LogicalID: 4, Version: 5, Database: "db", Table: "t"}}}}}
			s := NewService(e)
			err := s.Refresh(context.Background(), Key{7, 99}, tc.epoch)
			require.Zero(t, len(s.slots))
			require.Zero(t, len(s.accounts[7]))
			switch tc.name {
			case "success":
				require.NoError(t, err)
				require.Equal(t, 2, e.dataWrites)
				require.Equal(t, 1, e.publications)
				require.Equal(t, 2, e.commits)
			case "insert failure":
				require.ErrorContains(t, err, "injected")
				require.Equal(t, 2, e.dataWrites)
				require.Zero(t, e.publications)
				require.Equal(t, 1, e.rollbacks)
			case "obsolete epoch":
				require.ErrorContains(t, err, "obsolete")
				require.Zero(t, e.dataWrites)
				require.Zero(t, e.publications)
			}
		})
	}
}

func TestMaterializedViewRefreshFailureBoundaries(t *testing.T) {
	for _, scenario := range []string{"metadata publication failure", "source snapshot drift", "definition changed", "lost commit response", "retry conflict", "task replay", "obsolete replay epoch", "invalid task account"} {
		t.Run(scenario, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("refresh-txn")}).AnyTimes()
			snapshots := 0
			op.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp {
				snapshots++
				ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 17}
				if scenario == "source snapshot drift" && snapshots > 1 {
					ts.LogicalTime++
				}
				return ts
			}).AnyTimes()
			e := &refreshExecutor{t: t, op: op, mp: mp, state: State{Key: Key{7, 99}, Epoch: 2, Requested: 10, Successful: 9, LastRun: "last-good", Definition: catalog.MVDefinition{FormatVersion: 1, Database: "db", Query: "select * from t", TimeZone: "UTC", SQLMode: "NO_UNSIGNED_SUBTRACTION", Sources: []catalog.MVSource{{AccountID: 7, DatabaseID: 2, TableID: 3, LogicalID: 4, Version: 5, Database: "db", Table: "t"}}}}}
			runID := strings.Repeat("a", 64)
			ctx := context.Background()
			switch scenario {
			case "metadata publication failure":
				e.failPublish = true
			case "definition changed":
				e.changeEpoch = true
			case "lost commit response":
				e.loseCommitResponse = true
			case "retry conflict":
				e.conflictOnce = true
			case "task replay":
				e.prior = &refreshRun{11, 2, "SUCCEEDED"}
				e.state.LastRun = runID
				e.state.Successful = 11
				ctx = defines.WithMaterializedViewTask(ctx, defines.MaterializedViewTask{Account: 7, Table: 99, Epoch: 2, RunID: runID})
			case "obsolete replay epoch":
				e.prior = &refreshRun{11, 1, "RUNNING"}
				ctx = defines.WithMaterializedViewTask(ctx, defines.MaterializedViewTask{Account: 7, Table: 99, Epoch: 2, RunID: runID})
			case "invalid task account":
				ctx = defines.WithMaterializedViewTask(ctx, defines.MaterializedViewTask{Account: 8, Table: 99, Epoch: 2, RunID: runID})
			}
			service := NewService(e)
			err := service.Refresh(ctx, Key{7, 99}, 2)
			switch scenario {
			case "retry conflict", "lost commit response":
				require.NoError(t, err)
				require.Equal(t, 2, e.dataWrites)
				require.Equal(t, 1, e.publications)
				require.Zero(t, e.failureRecords)
			case "task replay":
				require.NoError(t, err)
				require.Zero(t, e.dataWrites)
				require.Zero(t, e.publications)
			default:
				require.Error(t, err)
				require.Zero(t, e.publications)
				if scenario == "source snapshot drift" || scenario == "metadata publication failure" {
					require.Equal(t, 2, e.dataWrites)
				} else {
					require.Zero(t, e.dataWrites)
				}
			}
			require.Zero(t, len(service.slots))
			require.Zero(t, len(service.accounts[7]))
		})
	}
}

func TestMaterializedViewBoundedAdmission(t *testing.T) {
	s := NewService(executor.NewMemExecutor(func(string) (executor.Result, error) {
		t.Fatal("full admission reached SQL")
		return executor.Result{}, nil
	}))
	s.pending[7] <- struct{}{}
	s.pending[7] <- struct{}{}
	require.ErrorContains(t, s.Refresh(context.Background(), Key{7, 99}, 0), "MV_REFRESH_BUSY")
	require.Equal(t, 2, len(s.pending[7]))
	require.Zero(t, len(s.accounts[7]))
	require.Zero(t, len(s.slots))
}

func TestMaterializedViewCapturedTimeZone(t *testing.T) {
	for _, zone := range []struct {
		name   string
		offset int32
	}{{"+08:00", 28800}, {"UTC", 0}, {"Local", -18000}} {
		t.Run(zone.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("refresh-txn")}).AnyTimes()
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 17}).AnyTimes()
			e := &refreshExecutor{t: t, op: op, mp: mp, state: State{Key: Key{7, 99}, Epoch: 1, Definition: catalog.MVDefinition{FormatVersion: 1, Database: "db", Query: "select * from t", TimeZone: zone.name, TimeZoneOffset: zone.offset, SQLMode: "PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES", Sources: []catalog.MVSource{{AccountID: 7, DatabaseID: 2, TableID: 3, LogicalID: 4, Version: 5, Database: "db", Table: "t"}}}}}
			wrapped := &zoneExecutor{refreshExecutor: e, offset: int(zone.offset)}
			require.NoError(t, NewService(wrapped).Refresh(context.Background(), Key{7, 99}, 0))
		})
	}
}

type zoneExecutor struct {
	*refreshExecutor
	offset int
}

func (e *zoneExecutor) ExecTxn(ctx context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
	require.Empty(e.t, opts.ParserSQLMode(), "metadata SQL must retain its own literal/parser contract")
	return e.refreshExecutor.ExecTxn(ctx, f, opts)
}

func (e *zoneExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	if strings.HasPrefix(sql, "insert into `db`.`stored`") {
		require.Equal(e.t, e.state.Definition.SQLMode, opts.ParserSQLMode())
		_, offset := time.Unix(0, 0).In(opts.GetTimeZone()).Zone()
		require.Equal(e.t, e.offset, offset)
	}
	return e.refreshExecutor.Exec(ctx, sql, opts)
}
