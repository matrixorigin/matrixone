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

package cdc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	mysql "github.com/matrixorigin/mysql"
	"github.com/stretchr/testify/require"
)

// Model the documented InnoDB error boundary: a usable physical session can
// lose its transaction without notifying database/sql.Tx. Only two keys are
// needed to distinguish complete replay from a successful B-only retry.
type abortedTarget struct {
	sync.Mutex
	active             bool
	pending, committed map[int]bool
	cause              error
	wholeRollback      bool
	failed             bool
	bCalls, commits    int
}

func (s *abortedTarget) rows() map[int]bool {
	s.Lock()
	defer s.Unlock()
	return maps.Clone(s.committed)
}

type abortedTargetConnector struct{ state *abortedTarget }

func (c abortedTargetConnector) Connect(context.Context) (driver.Conn, error) {
	return &abortedTargetConn{c.state}, nil
}
func (c abortedTargetConnector) Driver() driver.Driver { return abortedTargetDriver{} }

type abortedTargetDriver struct{}

func (abortedTargetDriver) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

type abortedTargetConn struct{ state *abortedTarget }

func (*abortedTargetConn) Prepare(string) (driver.Stmt, error)      { return nil, driver.ErrSkip }
func (*abortedTargetConn) Close() error                             { return nil }
func (*abortedTargetConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (c *abortedTargetConn) Begin() (driver.Tx, error) {
	s := c.state
	s.Lock()
	defer s.Unlock()
	s.active = true
	s.pending = maps.Clone(s.committed)
	return abortedTargetTx{s}, nil
}
func (c *abortedTargetConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	s := c.state
	s.Lock()
	defer s.Unlock()
	q := string(args[0].Value.([]byte))
	if strings.Contains(q, "VALUES (2)") {
		s.bCalls++
		if !s.failed {
			s.failed = true
			if s.wholeRollback {
				s.active = false
				s.pending = nil
			}
			return nil, s.cause
		}
	}
	rows := s.committed
	if s.active {
		rows = s.pending
	}
	if strings.Contains(q, "DELETE FROM") {
		delete(rows, 1)
	} else if strings.Contains(q, "VALUES (2)") {
		rows[2] = true
	} else {
		rows[1] = true
	}
	return driver.RowsAffected(1), nil
}

type abortedTargetTx struct{ state *abortedTarget }

func (x abortedTargetTx) Commit() error {
	s := x.state
	s.Lock()
	defer s.Unlock()
	s.commits++
	if s.active {
		s.committed = s.pending
	}
	s.active = false
	s.pending = nil
	return nil
}
func (x abortedTargetTx) Rollback() error {
	s := x.state
	s.Lock()
	defer s.Unlock()
	s.active = false
	s.pending = nil
	return nil
}

// signatureGuardTarget answers the REAL guarded identity and generation
// signature routines with known rows and records every SQL statement seen on
// the connection. The real mysqlSinker2 handleBegin runs against it, so the
// projection fence is observed at its true owner instead of an injected
// error: a disabled projection fence would let the mismatched signature
// pass, BeginTransaction would succeed and the negative assertions below
// would fail - the test is discriminating.
type signatureGuardTarget struct {
	mu           sync.Mutex
	identity     uint64
	sawIdentity  bool
	sawSignature bool
	signature    [][]driver.Value
	begins       int
	commits      int
	rollbacks    int
	exec         int
	unknownSQL   []string
}

type signatureGuardConnector struct{ state *signatureGuardTarget }

func (c signatureGuardConnector) Connect(context.Context) (driver.Conn, error) {
	return &signatureGuardConn{state: c.state}, nil
}
func (c signatureGuardConnector) Driver() driver.Driver { return signatureGuardDriver{} }

type signatureGuardDriver struct{}

func (signatureGuardDriver) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

type signatureGuardConn struct{ state *signatureGuardTarget }

func (*signatureGuardConn) Prepare(string) (driver.Stmt, error)      { return nil, driver.ErrSkip }
func (*signatureGuardConn) Close() error                             { return nil }
func (*signatureGuardConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (c signatureGuardConn) Begin() (driver.Tx, error) {
	c.state.begins++
	return signatureGuardTx{state: c.state}, nil
}

func (c signatureGuardConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "mo_cdc_target_identity"):
		c.state.mu.Lock()
		c.state.sawIdentity = true
		c.state.mu.Unlock()
		return &guardResultRows{cols: []string{"id"}, rows: [][]driver.Value{{c.state.identity}}}, nil
	case strings.Contains(query, "mo_cdc_target_generation_signature"):
		c.state.mu.Lock()
		c.state.sawSignature = true
		c.state.mu.Unlock()
		return &guardResultRows{cols: []string{"column_ordinal", "column_name", "generation_class", "not_null", "typed_digest"}, rows: c.state.signature}, nil
	}
	c.state.mu.Lock()
	c.state.unknownSQL = append(c.state.unknownSQL, query)
	c.state.mu.Unlock()
	return nil, fmt.Errorf("unexpected query: %s", query)
}

func (c signatureGuardConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.exec++
	return guardExecResult{}, nil
}

type signatureGuardTx struct{ state *signatureGuardTarget }

func (x signatureGuardTx) Commit() error {
	x.state.commits++
	return nil
}
func (x signatureGuardTx) Rollback() error {
	x.state.rollbacks++
	return nil
}

type guardExecResult struct{}

func (guardExecResult) LastInsertId() (int64, error) { return 0, nil }
func (guardExecResult) RowsAffected() (int64, error) { return 1, nil }

type guardResultRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *guardResultRows) Columns() []string { return r.cols }
func (r *guardResultRows) Close() error      { return nil }
func (r *guardResultRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

// TestSinkerGenerationFenceRejectedRangeIsNeverAckd drives the production
// TransactionManager against a real mysqlSinker2 whose handleBegin observes
// an actual mismatched generation signature on the sink transaction. The
// rejected interval must leave the watermark owner untouched: no DML SQL is
// executed, the sink transaction rolls back, COMMIT is never called and the
// tracked watermark stays at the prior fromTs. The positive control feeds
// the exact matching signature rows through the same real path and the
// watermark advances to toTs through a real commit.
func TestSinkerGenerationFenceRejectedRangeIsNeverAckd(t *testing.T) {
	ordinary := [][]driver.Value{
		{int64(0), "id", "O", int64(1), int64(7)},
		{int64(1), "v", "O", int64(1), int64(8)},
	}
	mismatched := [][]driver.Value{
		{int64(0), "id", "O", int64(1), int64(7)},
		{int64(1), "v", "S", int64(1), int64(9)},
	}

	run := func(rows [][]driver.Value, expectFence bool) *signatureGuardTarget {
		state := &signatureGuardTarget{identity: 42, signature: rows}
		db := sql.OpenDB(signatureGuardConnector{state})
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		executor := &Executor{conn: db, retryTimes: 1, retryDuration: time.Minute}
		executor.initRetryPolicy()
		executor.retryPolicy.Backoff = nil
		def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}}}, Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}}, Name2ColIndex: map[string]int32{"id": 0}}
		builder, err := NewCDCStatementBuilder("sink", "t", def, 1024, false, omitGeneratedContractFor(def))
		require.NoError(t, err)
		ar := NewCdcActiveRoutine()
		dbTbl := &DbTableInfo{
			SourceDbName: "src", SourceTblName: "t",
			SinkDbName: "sink", SinkTblName: "t",
			TargetSinkType: CDCSinkType_MO,
			TargetIdentity: "mo:42",
			GenerationContract: &CDCTargetGenerationContract{
				Copy:                []bool{true, true},
				TargetClass:         []byte{CDCColumnOrdinary, CDCColumnOrdinary},
				TargetNames:         []string{"id", "v"},
				TargetDigest:        []uint64{7, 8},
				ProjectionSensitive: true,
			},
		}
		sinker := NewMysqlSinker2(executor, 1, "task", dbTbl, nil, builder, ar)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		t.Cleanup(cancel)
		mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
		require.NoError(t, err)
		t.Cleanup(func() { mpool.DeleteMPool(mp) })
		done := make(chan struct{})
		go func() { defer close(done); sinker.Run(ctx, ar) }()
		t.Cleanup(func() { sinker.Close(); cancel(); <-done })
		updater := newMockWatermarkUpdater()
		tm := NewTransactionManager(sinker, updater, 1, "task", "src", "t")
		from, to := types.BuildTS(10, 0), types.BuildTS(20, 0)
		updater.watermarks[updater.keyString(tm.watermarkKey)] = from

		// The queue barrier may surface the fence at EITHER boundary the
		// real owner owns: the caller's early BEGIN drain (a root-sanctioned
		// legal outcome) or the first dispatch inside the batch's opened
		// transaction. Both paths must leave identical rejected-range
		// invariants: no DML SQL, no COMMIT, rollback, untouched watermark.
		beginErr := tm.BeginTransaction(ctx, from, to)
		if expectFence {
			fenceErr := beginErr
			if fenceErr == nil {
				sinker.sendCommand(NewInsertBatchCommand(buildBatch(t, mp, []int32{1}, to), mp, from, to))
				sinker.SendDummy()
				fenceErr = sinker.Error()
			}
			require.ErrorContains(t, fenceErr, "changed schema without being replaced",
				"the real guarded revalidation must fence the mismatched signature before any write")
			commitErr := tm.CommitTransaction(ctx)
			if commitErr != nil {
				require.ErrorContains(t, commitErr, "changed schema without being replaced",
					"a sticky fence never admits the rejected range to the watermark owner")
			}
			require.NoError(t, tm.EnsureCleanup(ctx))
			state.mu.Lock()
			commits, execs, rollbacks, begins, unknown := state.commits, state.exec, state.rollbacks, state.begins, append([]string(nil), state.unknownSQL...)
			state.mu.Unlock()
			require.Zero(t, commits, "a rejected range must never call COMMIT")
			require.Zero(t, execs, "a rejected range must execute no DML SQL")
			require.Empty(t, unknown, "only the guarded routines may be read")
			require.GreaterOrEqual(t, rollbacks, 1, "the fenced sink transaction must roll back")
			require.GreaterOrEqual(t, begins, 1)
			require.True(t, state.sawIdentity, "the real identity guard must observe the live table ID")
			require.True(t, state.sawSignature, "the real signature read must observe the drifted rows")
			require.False(t, updater.updateCalled, "the watermark owner must not be reached for the rejected toTs")
			require.Equal(t, from, updater.watermarks[updater.keyString(tm.watermarkKey)],
				"the tracked watermark must stay at the prior interval start")
			return state
		}

		require.NoError(t, beginErr, "the exact matching signature must pass the real guarded begin")
		sinker.sendCommand(NewInsertBatchCommand(buildBatch(t, mp, []int32{1}, to), mp, from, to))
		sinker.SendDummy()
		require.NoError(t, sinker.Error(), "the real DML path must succeed for a matching signature")
		require.NoError(t, tm.CommitTransaction(ctx))
		state.mu.Lock()
		commits, execs := state.commits, state.exec
		state.mu.Unlock()
		require.GreaterOrEqual(t, execs, 1, "the matching signature must reach real DML SQL")
		require.GreaterOrEqual(t, commits, 1, "a matching range commits through the real path")
		require.True(t, state.sawIdentity, "the matching control also observes the live identity read")
		require.True(t, state.sawSignature, "the matching control also observes the signature read")
		require.True(t, updater.updateCalled, "a matching range acks through the real watermark owner")
		require.Equal(t, to, updater.watermarks[updater.keyString(tm.watermarkKey)],
			"the watermark must advance to the committed interval")
		return state
	}

	run(mismatched, true)
	run(ordinary, false)
}

func TestMysqlSinkerTransactionErrorReplaysWholeRange(t *testing.T) {
	for _, backend := range []string{"production", "upstream"} {
		for _, fault := range []struct {
			name  string
			code  uint16
			whole bool
		}{
			{"deadlock", 1213, true}, {"MO deadlock", moerr.ErrDeadLockDetected, true},
			{"timeout statement rollback", 1205, false}, {"timeout transaction rollback", 1205, true},
		} {
			for _, mode := range []string{"snapshot", "incremental"} {
				t.Run(backend+"/"+fault.name+"/"+mode, func(t *testing.T) {
					var cause error = &mysql.MySQLError{Number: fault.code, Message: "controlled transaction failure"}
					if backend == "upstream" {
						cause = &gomysql.MySQLError{Number: fault.code, Message: "controlled transaction failure"}
					}
					state := &abortedTarget{committed: map[int]bool{}, cause: cause, wholeRollback: fault.whole}
					if mode == "incremental" {
						state.committed[1] = true
					}
					db := sql.OpenDB(abortedTargetConnector{state})
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					executor := &Executor{conn: db, retryTimes: 1, retryDuration: time.Minute}
					executor.initRetryPolicy()
					executor.retryPolicy.Backoff = nil
					def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}}}, Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}}, Name2ColIndex: map[string]int32{"id": 0}}
					builder, err := NewCDCStatementBuilder("sink", "t", def, 1024, false, omitGeneratedContractFor(def))
					require.NoError(t, err)
					ar := NewCdcActiveRoutine()
					sinker := NewMysqlSinker2(executor, 1, "task", &DbTableInfo{SourceDbName: "src", SourceTblName: "t"}, nil, builder, ar)
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					t.Cleanup(cancel)
					mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
					require.NoError(t, err)
					t.Cleanup(func() { mpool.DeleteMPool(mp) })
					done := make(chan struct{})
					go func() { defer close(done); sinker.Run(ctx, ar) }()
					t.Cleanup(func() { sinker.Close(); cancel(); <-done })
					updater := newMockWatermarkUpdater()
					tm := NewTransactionManager(sinker, updater, 1, "task", "src", "t")
					from, to := types.BuildTS(10, 0), types.BuildTS(20, 0)
					updater.watermarks[updater.keyString(tm.watermarkKey)] = from
					send := func() {
						if mode == "snapshot" {
							for _, id := range []int32{1, 2} {
								sinker.sendCommand(NewInsertBatchCommand(buildBatch(t, mp, []int32{id}, to), mp, from, to))
							}
						} else {
							insert, deleteBatch := NewAtomicBatch(mp), NewAtomicBatch(mp)
							packer := types.NewPacker()
							defer packer.Close()
							insert.Append(packer, buildBatch(t, mp, []int32{2}, to), 1, 0)
							deleteBatch.Append(packer, buildBatch(t, mp, []int32{1}, to), 1, 0)
							sinker.sendCommand(NewInsertDeleteBatchCommand(insert, deleteBatch, from, to))
						}
						sinker.SendDummy()
					}
					require.NoError(t, tm.BeginTransaction(ctx, from, to))
					send()
					require.ErrorIs(t, sinker.Error(), cause)
					require.True(t, (&TableChangeStream{}).determineRetryable(sinker.Error()), "normalization must preserve automatic recovery")
					require.ErrorIs(t, tm.CommitTransaction(ctx), cause)
					state.Lock()
					bCalls, commits := state.bCalls, state.commits
					state.Unlock()
					require.Equal(t, 1, bCalls)
					require.Zero(t, commits, "sticky sinker error must suppress COMMIT")
					require.False(t, updater.updateCalled)
					require.Equal(t, from, updater.watermarks[updater.keyString(tm.watermarkKey)])
					wantBefore := map[int]bool{}
					if mode == "incremental" {
						wantBefore[1] = true
					}
					require.Equal(t, wantBefore, state.rows())
					require.NoError(t, tm.EnsureCleanup(ctx))
					require.NoError(t, tm.BeginTransaction(ctx, from, to))
					send()
					require.NoError(t, tm.CommitTransaction(ctx))
					want := map[int]bool{2: true}
					if mode == "snapshot" {
						want[1] = true
					}
					require.Equal(t, want, state.rows())
					require.True(t, updater.updateCalled)
					require.Equal(t, to, updater.watermarks[updater.keyString(tm.watermarkKey)])
				})
			}
		}
	}
}

func TestExecutorTransactionalFailureAndStandaloneRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		retry bool
	}{
		{"success", nil, false},
		{"wire loss", &mysql.MySQLError{Number: 2013}, true}, {"EOF", io.EOF, true}, {"deadline", context.DeadlineExceeded, true},
		{"syntax", &mysql.MySQLError{Number: 1064}, false}, {"permission", &mysql.MySQLError{Number: 1045}, false},
		{"cancelled", context.Canceled, false}, {"owner lost", &OwnerFenceLostError{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			e := &Executor{conn: db, retryTimes: 2, retryDuration: time.Minute}
			e.initRetryPolicy()
			e.retryPolicy.Backoff = nil
			mock.ExpectBegin()
			require.NoError(t, e.BeginTx(t.Context()))
			defer func() {
				mock.ExpectRollback()
				require.NoError(t, e.RollbackTx(context.Background()))
				require.NoError(t, mock.ExpectationsWereMet())
			}()
			wantErrors := float64(0)
			if tc.cause == nil {
				mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 1))
			} else {
				mock.ExpectExec("fakeSql").WillReturnError(tc.cause)
				wantErrors = 1
			}
			before := readCounterValue(t, v2.CdcMysqlSinkErrorCounter)
			err = e.ExecSQL(t.Context(), nil, []byte("     INSERT INTO t VALUES (1)"), true)
			if tc.cause == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.cause)
			}
			require.Equal(t, wantErrors, readCounterValue(t, v2.CdcMysqlSinkErrorCounter)-before)
			s := &mysqlSinker2{}
			s.SetError(err)
			require.Equal(t, tc.retry, (&TableChangeStream{}).determineRetryable(s.Error()))
			if tc.retry || errors.Is(tc.cause, context.Canceled) || IsOwnerFenceLostError(tc.cause) {
				require.ErrorIs(t, s.Error(), tc.cause)
			}
		})
	}
	for _, tc := range []struct {
		name      string
		needRetry bool
		failures  int
	}{
		{"standalone success", true, 0},
		{"standalone retries", true, 1},
		{"standalone retries twice", true, 2},
		{"single attempt success", false, 0},
		{"single attempt failure", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			e := &Executor{conn: db, retryTimes: 2, retryDuration: time.Minute}
			e.initRetryPolicy()
			e.retryPolicy.Backoff = nil
			cause := &mysql.MySQLError{Number: 1213}
			for range tc.failures {
				mock.ExpectExec("fakeSql").WillReturnError(cause)
			}
			if tc.needRetry || tc.failures == 0 {
				mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			before := readCounterValue(t, v2.CdcMysqlSinkErrorCounter)
			err = e.ExecSQL(t.Context(), nil, []byte("     SELECT 1"), tc.needRetry)
			if tc.needRetry || tc.failures == 0 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, float64(tc.failures), readCounterValue(t, v2.CdcMysqlSinkErrorCounter)-before)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
	for _, control := range []string{"context", "pause", "cancel"} {
		t.Run("stopped/"+control, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			e := &Executor{conn: db}
			mock.ExpectBegin()
			require.NoError(t, e.BeginTx(t.Context()))
			defer func() {
				mock.ExpectRollback()
				require.NoError(t, e.RollbackTx(context.Background()))
				require.NoError(t, mock.ExpectationsWereMet())
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ar := NewCdcActiveRoutine()
			switch control {
			case "context":
				cancel()
			case "pause":
				close(ar.Pause)
			case "cancel":
				close(ar.Cancel)
			}
			before := readCounterValue(t, v2.CdcMysqlSinkErrorCounter)
			require.Error(t, e.ExecSQL(ctx, ar, []byte("     SELECT 1"), true))
			require.Equal(t, before, readCounterValue(t, v2.CdcMysqlSinkErrorCounter))
		})
	}
}

func TestMysqlSinkerPreservesClassifiedErrorCauses(t *testing.T) {
	for _, cause := range []error{
		&mysql.MySQLError{Number: 1213}, &gomysql.MySQLError{Number: 1205},
		moerr.NewDeadLockDetectedNoCtx(),
		&mysql.MySQLError{Number: moerr.ErrDeadLockDetected}, &gomysql.MySQLError{Number: moerr.ErrDeadLockDetected},
		errors.Join(context.Canceled, context.DeadlineExceeded),
		errors.Join(&OwnerFenceLostError{}, &mysql.MySQLError{Number: 2013}),
	} {
		for _, set := range []func(*mysqlSinker2, error){(*mysqlSinker2).SetError, (*mysqlSinker2).setErrorIfNil} {
			s := &mysqlSinker2{}
			set(s, cause)
			require.ErrorIs(t, s.Error(), cause)
			want, _ := ClassifyRetryableError(cause)
			require.Equal(t, want, (&TableChangeStream{}).determineRetryable(s.Error()))
		}
	}
	unknown := errors.New("unknown sink failure")
	require.IsType(t, &moerr.Error{}, normalizeMysqlSinkerError(unknown))
	require.Nil(t, normalizeMysqlSinkerError(nil))
}
