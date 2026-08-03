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
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

type returningProtocolRecorder struct {
	testMysqlWriter
	events   *[]string
	writeErr error
}

func (r *returningProtocolRecorder) WriteLengthEncodedNumber(value uint64) error {
	*r.events = append(*r.events, fmt.Sprintf("column-count:%d", value))
	return nil
}

func (r *returningProtocolRecorder) WriteColumnDef(context.Context, Column, int) error {
	*r.events = append(*r.events, "column")
	return nil
}

func (r *returningProtocolRecorder) WriteEOFIFAndNoFlush(uint16, uint16) error {
	*r.events = append(*r.events, "column-eof")
	return nil
}

func (r *returningProtocolRecorder) Write(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error {
	*r.events = append(*r.events, "rows")
	return r.writeErr
}

func (r *returningProtocolRecorder) WriteEOFOrOKWithAffectedRows(affectedRows uint64, _ uint16, _ uint16) error {
	*r.events = append(*r.events, fmt.Sprintf("result-eof:%d", affectedRows))
	return nil
}

type returningStageRecorder struct {
	events     *[]string
	publishErr error
}

func (*returningStageRecorder) Stage(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error {
	return nil
}

func (*returningStageRecorder) FinishStage(*ExecCtx) error { return nil }

func (r *returningStageRecorder) Publish(*ExecCtx) error {
	*r.events = append(*r.events, "publish")
	return r.publishErr
}

func (r *returningStageRecorder) Abort(*ExecCtx) error {
	*r.events = append(*r.events, "abort")
	return nil
}

func returningTestBatch(t *testing.T, ses *Session, values ...int64) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, ses.proc.Mp()))
	}
	bat.SetRowCount(len(values))
	return bat
}

func TestReturningSpoolGenerationAndReplay(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	spool := &returningSpool{}
	budget, err := ses.proc.GetHashBuildBudget()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spool.Close())
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
	}()

	retried := returningTestBatch(t, ses, 1, 2)
	defer retried.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, retried, nil))
	require.ErrorContains(t, spool.Write(1, retried, nil), "generation mismatch")
	require.NoError(t, spool.AbortAttempt(0, nil))
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())

	final := returningTestBatch(t, ses, 7, 8, 9)
	defer final.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 1, ses.proc))
	require.NoError(t, spool.Write(1, final, nil))
	require.NoError(t, spool.SealAttempt(1))
	require.Equal(t, uint64(3), spool.RowCount())
	require.Zero(t, budget.Used(), "RETURNING Go buffers are outside the exact MPool allocation ledger")
	require.Positive(t, budget.SpillDiskUsed())
	require.Equal(t, uint64(1), budget.SpillFDUsed())

	var got []int64
	for i := 0; i < 2; i++ {
		got = got[:0]
		require.NoError(t, spool.Replay(ctx, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
			got = append(got, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])...)
			return nil
		}))
		require.Equal(t, []int64{7, 8, 9}, got)
		require.Zero(t, budget.Used(), "replay must not create an estimated HashBuild charge")
	}
}

func TestDeferredReturningResponseStartsAfterPublish(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.txnHandler = &TxnHandler{}
	ses.SetMysqlResultSet(&MysqlResultSet{})

	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	bat := returningTestBatch(t, ses, 7, 8, 9)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))

	column := &MysqlColumn{}
	column.SetName("v")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	var events []string
	state := &returningState{
		spool:        spool,
		columns:      []any{column},
		affectedRows: 3,
		stagedSaver:  &returningStageRecorder{events: &events},
	}
	execCtx := &ExecCtx{
		reqCtx:     ctx,
		ses:        ses,
		proc:       ses.proc,
		isLastStmt: true,
		returning:  state,
	}
	resper := &MysqlResp{mysqlRrWr: &returningProtocolRecorder{events: &events}}
	require.NoError(t, resper.respDeferredResultRow(ses, execCtx))
	require.Equal(t, []string{
		"publish",
		"column-count:1",
		"column",
		"column-eof",
		"rows",
		"result-eof:3",
	}, events)
}

func TestDeferredReturningZeroRowsStillSendsMetadata(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.txnHandler = &TxnHandler{}
	ses.SetMysqlResultSet(&MysqlResultSet{})

	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.SealAttempt(0))

	column := &MysqlColumn{}
	column.SetName("v")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	var events []string
	execCtx := &ExecCtx{
		reqCtx:     ctx,
		ses:        ses,
		proc:       ses.proc,
		isLastStmt: true,
		returning: &returningState{
			spool:        spool,
			columns:      []any{column},
			affectedRows: 0,
		},
	}
	resper := &MysqlResp{mysqlRrWr: &returningProtocolRecorder{events: &events}}
	require.NoError(t, resper.respDeferredResultRow(ses, execCtx))
	require.Equal(t, []string{"column-count:1", "column", "column-eof", "result-eof:0"}, events)
}

func TestDeferredReturningPublishFailureDoesNotHideCommittedRows(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.txnHandler = &TxnHandler{}
	ses.SetMysqlResultSet(&MysqlResultSet{})

	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	bat := returningTestBatch(t, ses, 42)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))

	column := &MysqlColumn{}
	column.SetName("v")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	var events []string
	execCtx := &ExecCtx{
		reqCtx:     ctx,
		ses:        ses,
		proc:       ses.proc,
		isLastStmt: true,
		returning: &returningState{
			spool:        spool,
			columns:      []any{column},
			affectedRows: 1,
			stagedSaver: &returningStageRecorder{
				events:     &events,
				publishErr: context.DeadlineExceeded,
			},
		},
	}
	resper := &MysqlResp{mysqlRrWr: &returningProtocolRecorder{events: &events}}
	require.NoError(t, resper.respDeferredResultRow(ses, execCtx))
	require.Equal(t, []string{
		"publish", "abort", "column-count:1", "column", "column-eof", "rows", "result-eof:1",
	}, events)
}

func TestReturningCommitFailureAbortsStageBeforeProtocol(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	var events []string
	state := &returningState{
		spool:       &returningSpool{},
		stagedSaver: &returningStageRecorder{events: &events},
	}
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, proc: ses.proc, returning: state}
	commitErr := moerr.NewTxnUnknown(ctx, "commit outcome unknown")
	err := abortStagedReturning(execCtx, commitErr)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, []string{"abort"}, events)
	require.NoError(t, state.Close())
}

func TestDeferredReturningClientDisconnectCleansSpool(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.txnHandler = &TxnHandler{}
	ses.SetMysqlResultSet(&MysqlResultSet{})
	budget, err := ses.proc.GetHashBuildBudget()
	require.NoError(t, err)

	spool := &returningSpool{}
	bat := returningTestBatch(t, ses, 42)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))

	column := &MysqlColumn{}
	column.SetName("v")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	var events []string
	execCtx := &ExecCtx{
		reqCtx: ctx,
		ses:    ses,
		proc:   ses.proc,
		returning: &returningState{
			spool:        spool,
			columns:      []any{column},
			affectedRows: 1,
		},
	}
	resper := &MysqlResp{mysqlRrWr: &returningProtocolRecorder{events: &events, writeErr: context.Canceled}}
	require.ErrorIs(t, resper.respDeferredResultRow(ses, execCtx), context.Canceled)
	require.Equal(t, []string{"column-count:1", "column", "column-eof", "rows"}, events)
	require.NoError(t, execCtx.returning.Close())
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
}

func TestReturningQueryResultStagesPhysicalZeroRowBlock(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, TenantID: sysAccountID})
	ses.SetStmtId(uuid.New())
	ses.limitResultSize = 1
	ses.createdTime = time.Now()
	ses.expiredTime = ses.createdTime.Add(time.Hour)
	ses.rs = &plan.ResultColDef{ResultCols: []*plan.ColDef{{
		Name: "v",
		Typ:  plan.Type{Id: int32(types.T_int64)},
	}}}
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, proc: ses.proc}
	saver := &QueryResult{}

	require.NoError(t, saver.FinishStage(execCtx))
	require.Equal(t, 1, saver.stagedBlocks)
	require.Zero(t, ses.savedRowCount)
	require.Zero(t, ses.queryRowCount)
	require.NoError(t, saver.Abort(execCtx))
	require.Zero(t, ses.blockIdx)
}

func TestReturningSpoolRejectsTruncatedRecord(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	bat := returningTestBatch(t, ses, 1, 2)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))
	info, err := spool.file.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(4))
	require.NoError(t, spool.file.Truncate(info.Size()-4))
	require.Error(t, spool.Replay(ctx, func(*batch.Batch, *perfcounter.CounterSet) error { return nil }))
}

func TestReturningSpoolRejectsCorruptMagic(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	bat := returningTestBatch(t, ses, 1)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))
	require.NoError(t, spool.file.Sync())
	require.NoError(t, func() error {
		if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		payloadSize, err := types.ReadUint64(spool.file)
		if err != nil {
			return err
		}
		if _, err = spool.file.Seek(int64(payloadSize), io.SeekCurrent); err != nil {
			return err
		}
		_, err = spool.file.Write(make([]byte, 8))
		return err
	}())
	require.ErrorContains(t, spool.Replay(ctx, func(*batch.Batch, *perfcounter.CounterSet) error { return nil }), "corrupted")
}

func TestReturningSpoolReplayCancellationReleasesDecodeBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ses := newValidateSession(t)
	budget, err := ses.proc.GetHashBuildBudget()
	require.NoError(t, err)
	spool := &returningSpool{}
	bat := returningTestBatch(t, ses, 1)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))
	cancel()
	require.ErrorIs(t, spool.Replay(ctx, func(*batch.Batch, *perfcounter.CounterSet) error { return nil }), context.Canceled)
	require.Zero(t, budget.Used())
	require.NoError(t, spool.Close())
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
}

func TestReturningSpoolConcurrentTerminalTransitions(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	budget, err := ses.proc.GetHashBuildBudget()
	require.NoError(t, err)
	for i := 0; i < 16; i++ {
		spool := &returningSpool{}
		bat := returningTestBatch(t, ses, int64(i))
		require.NoError(t, spool.BeginAttempt(ctx, uint64(i), ses.proc))
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			_ = spool.Write(uint64(i), bat, nil)
		}()
		go func() {
			defer wg.Done()
			_ = spool.AbortAttempt(uint64(i), context.Canceled)
		}()
		go func() {
			defer wg.Done()
			_ = spool.Close()
		}()
		wg.Wait()
		require.NoError(t, spool.Close())
		bat.Clean(ses.proc.Mp())
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
	}
}
