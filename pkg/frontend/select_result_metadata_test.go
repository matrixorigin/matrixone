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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type selectMetadataTestCompile struct {
	mockCompile
	freeze func()
}

func (c *selectMetadataTestCompile) FreezeResultMetadata() { c.freeze() }

type selectMetadataTestResponder struct {
	Responser
	before func(*ExecCtx, any) error
	finish func(*ExecCtx) error
}

func (r *selectMetadataTestResponder) RespPreMeta(ec *ExecCtx, columns any) error {
	return r.before(ec, columns)
}

func (r *selectMetadataTestResponder) finalizeQueryResult(ec *ExecCtx) error {
	return r.finish(ec)
}

func TestSelectResultMetadataUsesExecutedPlan(t *testing.T) {
	for _, rows := range []int{0, 2} {
		t.Run(map[int]string{0: "empty", 2: "rows"}[rows], func(t *testing.T) {
			original := newResultColumnTestPlan(1)
			original.GetQuery().Nodes[0].ProjectList[0].Typ.Id = int32(types.T_int32)
			final := newResultColumnTestPlan(1)
			stmt := &tree.Select{}
			ses := &Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}}
			ec := &ExecCtx{
				reqCtx: context.Background(), ses: ses, stmt: stmt,
				cw:            &TxnComputationWrapper{stmt: stmt, plan: original},
				prepareColDef: [][]byte{{0x42}},
			}
			var headers, freezes, written, finalized int
			runFinished := false
			runner := &selectMetadataTestCompile{freeze: func() { freezes++ }}
			runner.getPlanFunc = func() *plan.Plan { return final }
			runner.planGenerationRebuilt = true
			ec.runner = runner
			ec.resper = &selectMetadataTestResponder{
				before: func(ec *ExecCtx, columns any) error {
					headers++
					require.Equal(t, 1, freezes)
					require.Nil(t, ec.prepareColDef)
					require.Equal(t, defines.MYSQL_TYPE_LONGLONG, columns.([]any)[0].(Column).ColumnType())
					require.Equal(t, int32(types.T_int64), ses.rs.ResultCols[0].Typ.Id)
					require.Equal(t, rows == 0, runFinished)
					return nil
				},
				finish: func(*ExecCtx) error {
					require.True(t, runFinished)
					require.Equal(t, 1, headers)
					finalized++
					return nil
				},
			}
			ses.outputCallback = func(_ FeSession, _ *ExecCtx, bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat != nil && bat.RowCount() > 0 {
					require.Equal(t, 1, headers, "metadata must precede every row")
					written += bat.RowCount()
				}
				return nil
			}
			callback := ses.GetOutputCallback(ec)
			runner.runFunc = func(uint64) (*util.RunResult, error) {
				// A failed attempt can emit terminal/empty callbacks before retry.
				require.NoError(t, callback(nil, nil))
				require.NoError(t, callback(batch.NewWithSize(0), nil))
				require.Zero(t, headers)
				require.Zero(t, freezes)
				for range rows {
					bat := batch.NewWithSize(0)
					bat.SetRowCount(1)
					require.NoError(t, callback(bat, nil))
				}
				runFinished = true
				return &util.RunResult{}, nil
			}
			require.NoError(t, executeResultRowStmt(ses, ec))
			require.Equal(t, rows, written)
			require.Equal(t, 1, headers)
			require.Equal(t, 1, freezes)
			require.Equal(t, 1, finalized)
		})
	}
}

func TestSelectResultMetadataFailureBoundaries(t *testing.T) {
	wantErr := errors.New("metadata publication failed")
	for _, tc := range []struct {
		name        string
		runErr      error
		metadataErr error
		finalizeErr error
	}{
		{name: "run failure", runErr: wantErr},
		{name: "cancellation", runErr: context.Canceled},
		{name: "metadata failure", metadataErr: wantErr},
		{name: "saved metadata failure", finalizeErr: wantErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}}
			stmt := &tree.Select{}
			ec := &ExecCtx{reqCtx: context.Background(), ses: ses, stmt: stmt,
				cw: &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(1)}}
			headers, written, finalized := 0, 0, 0
			ec.resper = &selectMetadataTestResponder{
				before: func(*ExecCtx, any) error { headers++; return tc.metadataErr },
				finish: func(*ExecCtx) error { finalized++; return tc.finalizeErr },
			}
			ses.outputCallback = func(FeSession, *ExecCtx, *batch.Batch, *perfcounter.CounterSet) error {
				written++
				return nil
			}
			callback := ses.GetOutputCallback(ec)
			ec.runner = &mockCompile{runFunc: func(uint64) (*util.RunResult, error) {
				if tc.runErr != nil {
					return nil, tc.runErr
				}
				if tc.finalizeErr != nil {
					return &util.RunResult{}, nil
				}
				bat := batch.NewWithSize(0)
				bat.SetRowCount(1)
				first := callback(bat, nil)
				require.ErrorIs(t, callback(bat, nil), first)
				return nil, first
			}, getPlanFunc: func() *plan.Plan { return ec.cw.Plan() }}
			err := executeResultRowStmt(ses, ec)
			if tc.runErr != nil {
				require.ErrorIs(t, err, tc.runErr)
				require.Zero(t, headers)
			} else if tc.metadataErr != nil {
				require.ErrorIs(t, err, tc.metadataErr)
				require.Equal(t, 1, headers)
			} else {
				require.ErrorIs(t, err, tc.finalizeErr)
				require.Equal(t, 1, headers)
			}
			require.Zero(t, written)
			if tc.finalizeErr != nil {
				require.Equal(t, 1, finalized)
			} else {
				require.Zero(t, finalized)
			}
			// A later statement must never inherit this publisher's cached error.
			ec.beginStatementGeneration(&UserInput{})
			require.Nil(t, ec.resultMetadata)
			require.NoError(t, callback(batch.NewWithSize(0), nil))
			require.Equal(t, 1, written)
		})
	}
}

func TestSelectResultMetadataConcurrentCallbacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var headers, rows atomic.Int32
	var published atomic.Bool
	ec := &ExecCtx{resultMetadata: &deferredResultMetadata{initialize: func() error {
		headers.Add(1)
		close(started)
		select {
		case <-release:
			published.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}}
	ses := &Session{}
	ses.outputCallback = func(FeSession, *ExecCtx, *batch.Batch, *perfcounter.CounterSet) error {
		if !published.Load() {
			return errors.New("row preceded metadata")
		}
		rows.Add(1)
		return nil
	}
	callback := ses.GetOutputCallback(ec)
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)
	var workers sync.WaitGroup
	var errs [2]error
	workers.Go(func() { errs[0] = callback(bat, nil) })
	select {
	case <-started:
	case <-ctx.Done():
		workers.Wait()
		t.Fatal(ctx.Err())
	}
	workers.Go(func() { errs[1] = callback(bat, nil) })
	close(release)
	workers.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), headers.Load())
	require.Equal(t, int32(2), rows.Load())
}

func TestSelectResultSaverFinalizesAfterRun(t *testing.T) {
	wantErr := errors.New("saved metadata write failed")
	saver := &performTestBinaryWriter{err: wantErr}
	resper := &MysqlResp{binWr: saver}
	ec := &ExecCtx{stmt: &tree.Select{}, resultMetadata: &deferredResultMetadata{}}
	// A retry/reset nil callback cannot finalize saved metadata prematurely.
	require.NoError(t, resper.RespResult(ec, nil, nil))
	require.Zero(t, saver.calls)
	ec.resper = resper
	require.ErrorIs(t, finalizeQueryResult(ec), wantErr)
	require.Equal(t, 1, saver.calls)
}

func TestSelectResultFailureResetsSavedState(t *testing.T) {
	for _, wantErr := range []error{errors.New("late producer failure"), context.Canceled} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			ses := &Session{feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}}}
			stmt := &tree.Select{}
			ec := &ExecCtx{reqCtx: context.Background(), ses: ses, stmt: stmt,
				cw: &TxnComputationWrapper{stmt: stmt, plan: newResultColumnTestPlan(1)}}
			ec.resper = &selectMetadataTestResponder{
				before: func(*ExecCtx, any) error { return nil },
				finish: func(*ExecCtx) error { t.Fatal("failed Run must not finalize saved metadata"); return nil },
			}
			ses.outputCallback = func(FeSession, *ExecCtx, *batch.Batch, *perfcounter.CounterSet) error {
				// Model the session state owned by an already persisted batch.
				ses.blockIdx = 1
				ses.p = ec.cw.Plan()
				ses.curResultSize = 1
				ses.savedRowCount = 1
				ses.queryRowCount = 1
				return nil
			}
			callback := ses.GetOutputCallback(ec)
			ec.runner = &mockCompile{runFunc: func(uint64) (*util.RunResult, error) {
				bat := batch.NewWithSize(0)
				bat.SetRowCount(1)
				require.NoError(t, callback(bat, nil))
				require.Equal(t, 1, ses.blockIdx)
				return nil, wantErr
			}, getPlanFunc: func() *plan.Plan { return ec.cw.Plan() }}
			require.ErrorIs(t, executeResultRowStmt(ses, ec), wantErr)
			requirePerformResultStateReset(t, ses)
		})
	}
}
