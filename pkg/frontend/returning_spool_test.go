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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
)

type returningProtocolRecorder struct {
	testMysqlWriter
	events     *[]string
	writeErr   error
	outputConn *Conn
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

func (r *returningProtocolRecorder) Write(_ *ExecCtx, counter *perfcounter.CounterSet, _ *batch.Batch) error {
	*r.events = append(*r.events, "rows")
	if r.outputConn != nil {
		return r.outputConn.withOutputCounter(counter, func() error {
			tracker := r.outputConn.responseOutputWait.Load()
			operatorCounter := r.outputConn.outputCounter.Load()
			if tracker != nil {
				tracker.totalNS.Add(19)
			}
			if operatorCounter != nil {
				operatorCounter.ProtocolOutputWaitNS.Add(19)
				if tracker != nil {
					tracker.operatorNS.Add(19)
				}
			}
			return r.writeErr
		})
	}
	return r.writeErr
}

func (r *returningProtocolRecorder) setResponseOutputWaitTracker(tracker *responseOutputWaitTracker) {
	if r.outputConn != nil {
		r.outputConn.setResponseOutputWaitTracker(tracker)
	}
}

func (r *returningProtocolRecorder) WriteEOFOrOKWithAffectedRows(affectedRows uint64, _ uint16, _ uint16) error {
	*r.events = append(*r.events, fmt.Sprintf("result-eof:%d", affectedRows))
	return nil
}

type returningStageRecorder struct {
	events     *[]string
	publishErr error
	published  bool
	aborted    bool
}

type returningAccountingFileService struct {
	fileservice.FileService
	failWrites  bool
	failDeletes int
}

func (f *returningAccountingFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
		for _, entry := range vector.Entries {
			counter.FileService.S3WriteSize.Add(int64(len(entry.Data)))
		}
	})
	if f.failWrites {
		return errors.New("injected query-result write failure")
	}
	return f.FileService.Write(ctx, vector)
}

func (f *returningAccountingFileService) Delete(ctx context.Context, paths ...string) error {
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	})
	if f.failDeletes > 0 {
		f.failDeletes--
		return errors.New("injected query-result delete failure")
	}
	return f.FileService.Delete(ctx, paths...)
}

func installReturningAccountingFileService(t *testing.T, ses *Session) *returningAccountingFileService {
	t.Helper()
	ses.SetMemPool(ses.proc.Mp())
	pu := getPu(ses.GetService())
	original := pu.FileService
	accountingFS := &returningAccountingFileService{FileService: original}
	pu.FileService = accountingFS
	t.Cleanup(func() { pu.FileService = original })
	return accountingFS
}

func (*returningStageRecorder) Stage(*ExecCtx, *perfcounter.CounterSet, *batch.Batch) error {
	return nil
}

func (*returningStageRecorder) FinishStage(*ExecCtx) error { return nil }

func (r *returningStageRecorder) Publish(*ExecCtx) error {
	if r.aborted || r.published {
		return nil
	}
	*r.events = append(*r.events, "publish")
	if r.publishErr == nil {
		r.published = true
	}
	return r.publishErr
}

func (r *returningStageRecorder) Abort(*ExecCtx) error {
	if r.published || r.aborted {
		return nil
	}
	*r.events = append(*r.events, "abort")
	r.aborted = true
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

func TestDeferredReturningReplayOutputWaitStaysRootOwned(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.txnHandler = &TxnHandler{}
	ses.SetMysqlResultSet(&MysqlResultSet{})

	spool := &returningSpool{}
	defer func() { require.NoError(t, spool.Close()) }()
	bat := returningTestBatch(t, ses, 7)
	defer bat.Clean(ses.proc.Mp())
	require.NoError(t, spool.BeginAttempt(ctx, 0, ses.proc))
	require.NoError(t, spool.Write(0, bat, nil))
	require.NoError(t, spool.SealAttempt(0))

	column := &MysqlColumn{}
	column.SetName("v")
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	var events []string
	writer := &returningProtocolRecorder{events: &events, outputConn: &Conn{}}
	resper := NewMysqlResp(writer)
	ses.respr = resper
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
	execCtx := &ExecCtx{
		reqCtx:     ctx,
		ses:        ses,
		proc:       ses.proc,
		isLastStmt: true,
		returning: &returningState{
			spool:        spool,
			columns:      []any{column},
			affectedRows: 1,
		},
	}

	ses.beginResponseAccounting()
	require.NoError(t, resper.respDeferredResultRow(ses, execCtx))
	ses.finishResponseAccounting(ctx, nil, false)

	require.Equal(t, uint64(19), root.PreResponseSummary().Usage.WaitNS[resource.WaitOutput])
	require.Nil(t, writer.outputConn.outputCounter.Load())
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
	require.NoError(t, state.Close(execCtx))
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
	require.NoError(t, execCtx.returning.Close(execCtx))
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
}

func TestReturningQueryResultStagesPhysicalZeroRowBlock(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	installReturningAccountingFileService(t, ses)
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
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
	require.Zero(t, root.PreResponseSummary().Usage.S3Requests[resource.S3Put])
	require.NoError(t, saver.Abort(execCtx))
	require.Zero(t, ses.blockIdx)
	summary := root.PreResponseSummary()
	require.Positive(t, summary.Usage.S3Requests[resource.S3Put])
	require.Equal(t, uint64(1), summary.Usage.S3Requests[resource.S3DeleteMulti])
}

func TestReturningQueryResultPublishesStageAndMetadataAccounting(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	installReturningAccountingFileService(t, ses)
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
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
	bat := returningTestBatch(t, ses, 7)
	defer bat.Clean(ses.proc.Mp())

	require.NoError(t, saver.Stage(execCtx, nil, bat))
	require.Zero(t, root.PreResponseSummary().Usage.S3Requests[resource.S3Put])
	stagePuts := saver.stageCounter.FileService.S3.Put.Load()
	require.Positive(t, stagePuts)
	require.NoError(t, saver.Publish(execCtx))
	summary := root.PreResponseSummary()
	require.Greater(t, summary.Usage.S3Requests[resource.S3Put], uint64(stagePuts),
		"metadata publication must share the staged-result terminal owner")
	require.Positive(t, summary.Usage.S3WriteBytes)
	require.NoError(t, saver.Publish(execCtx))
	require.Equal(t, summary, root.PreResponseSummary(), "terminal accounting must merge exactly once")
}

func TestReturningQueryResultPublishFailureAccountsAbort(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	accountingFS := installReturningAccountingFileService(t, ses)
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
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
	bat := returningTestBatch(t, ses, 7)
	defer bat.Clean(ses.proc.Mp())

	require.NoError(t, saver.Stage(execCtx, nil, bat))
	accountingFS.failWrites = true
	require.ErrorContains(t, saver.Publish(execCtx), "injected query-result write failure")
	require.Zero(t, root.PreResponseSummary().Usage.S3Requests[resource.S3Put])
	require.NoError(t, saver.Abort(execCtx))
	summary := root.PreResponseSummary()
	require.Positive(t, summary.Usage.S3Requests[resource.S3Put])
	require.Equal(t, uint64(1), summary.Usage.S3Requests[resource.S3DeleteMulti])
}

func TestReturningQueryResultStageFailureAccountsAbort(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	accountingFS := installReturningAccountingFileService(t, ses)
	accountingFS.failWrites = true
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
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
	bat := returningTestBatch(t, ses, 7)
	defer bat.Clean(ses.proc.Mp())

	require.ErrorContains(t, saver.Stage(execCtx, nil, bat), "injected query-result write failure")
	require.Zero(t, root.PreResponseSummary().Usage.S3Requests[resource.S3Put])
	require.NoError(t, saver.Abort(execCtx))
	summary := root.PreResponseSummary()
	require.Positive(t, summary.Usage.S3Requests[resource.S3Put])
	require.Equal(t, uint64(1), summary.Usage.S3Requests[resource.S3DeleteMulti])
}

func TestReturningQueryResultAbortRetryAccountsEveryAttempt(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	accountingFS := installReturningAccountingFileService(t, ses)
	root := resource.NewRoot(resource.ConnExternal)
	ctx = resource.ContextWithRoot(ctx, root)
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
	bat := returningTestBatch(t, ses, 7)
	defer bat.Clean(ses.proc.Mp())

	require.NoError(t, saver.Stage(execCtx, nil, bat))
	accountingFS.failDeletes = 1
	require.ErrorContains(t, saver.Abort(execCtx), "injected query-result delete failure")
	require.False(t, saver.aborted, "failed cleanup must remain retryable")
	require.Equal(t, uint64(1), root.PreResponseSummary().Usage.S3Requests[resource.S3DeleteMulti])
	state := &returningState{spool: &returningSpool{}, stagedSaver: saver}
	require.NoError(t, state.Close(execCtx))
	require.True(t, saver.aborted)
	require.Equal(t, uint64(2), root.PreResponseSummary().Usage.S3Requests[resource.S3DeleteMulti])
	require.NoError(t, state.Close(execCtx))
	require.Equal(t, uint64(2), root.PreResponseSummary().Usage.S3Requests[resource.S3DeleteMulti],
		"terminal cleanup must not issue or publish another delete")
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
