// Copyright 2024 Matrix Origin
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

package motrace

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

// This file covers the statement_info spool through the real pipeline
// machinery (the pipe impl, the content buffer, and the batch handler), not
// through mocks of the spool itself. It replays the timeline of issue
// #30812: a committed statement is reported, the process dies before the
// periodic export, and the restart replay must still land the audit record.
// It also pins down the controls: the graceful path, the normal
// post-flush path, and the failed-statement path.

// spoolLifecycleEnv wires a spool into the tracer provider buildable from
// this package, plus a statement pipe over a capturing writer factory.
type spoolLifecycleEnv struct {
	spool      *statementInfoSpool
	pipe       PipeImpl
	captured   *bytes.Buffer
	oldSpool   *statementInfoSpool
	oldFactory table.WriterFactory
}

func setupSpoolLifecycle(t *testing.T, dir string) *spoolLifecycleEnv {
	t.Helper()
	provider := GetTracerProvider()
	// a spool with zero ack lag: an acked segment can be removed immediately,
	// keeping assertions deterministic.
	spool, err := newStatementInfoSpool(dir, 0, 0, 0)
	require.NoError(t, err)
	captured := &bytes.Buffer{}
	factory := &spoolCapturableFactory{rowWriter: &spoolCaptureWriter{storage: captured, needBuf: true}}

	env := &spoolLifecycleEnv{
		spool:      spool,
		captured:   captured,
		oldSpool:   provider.stmtInfoSpool,
		oldFactory: provider.writerFactory,
	}
	provider.stmtInfoSpool = spool
	provider.writerFactory = factory
	// spool lifecycle jobs run with the real janitor so close() behaves
	// exactly like the production shutdown path.
	spool.startJanitorOnly()
	env.pipe = NewBufferPipe2CSVWorker(BufferWithReminder(bp.NewConstantClock(time.Hour)))
	t.Cleanup(func() {
		spool.close()
		provider.stmtInfoSpool = env.oldSpool
		provider.writerFactory = env.oldFactory
	})
	return env
}

func (env *spoolLifecycleEnv) newStatement(marker string, failed bool) *StatementInfo {
	stm := &StatementInfo{
		Statement:     []byte("insert /* " + marker + " */ into exp_stmtinfo_durable_1022.t values(201,201.25)"),
		StatementType: "Insert",
		QueryType:     "DML",
		Status:        StatementStatusSuccess,
		Account:       "sys",
		User:          "root",
		ConnType:      1,
		RequestAt:     time.Now(),
		Database:      "exp_stmtinfo_durable_1022",
		SqlSourceType: "external",
	}
	if failed {
		stm.Status = StatementStatusFailed
		stm.Error = io.ErrClosedPipe
	}
	return stm
}

// reportOne runs the production report path of one statement: spool append +
// collector hand-off, recreated as ReportStatement does it, with the real
// statement pipe instead of a full collector.
func (env *spoolLifecycleEnv) reportOne(t *testing.T, stm *StatementInfo, buf Buffer) {
	t.Helper()
	spoolStatementInfo(context.Background(), stm)
	buf.Add(stm)
}

func (env *spoolLifecycleEnv) simulateKill() {
	// an abrupt exit drops every in-memory structure without any lifecycle
	// handling. What must remain is only the on-disk spool.
	spoolKillFile(env.spool)
}

func TestStatementInfoSpoolCrashBetweenReportAndExport(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	env := setupSpoolLifecycle(t, dir)

	buf := env.pipe.NewItemBuffer(SingleStatementTable.GetName()).(Buffer)
	marker := "si_lifecycle_kill_30812"
	env.reportOne(t, env.newStatement(marker, false), buf)

	// the crash happens between the report and the periodic export: the
	// in-memory buffer still holds the record when the process dies.
	env.simulateKill()
	files := spoolDirFiles(t, dir)
	require.NotEmpty(t, files)

	// boot replay must land the record exactly once at the destination.
	stub := stubSpoolSQLFlush(t)
	{
		restarted, err := newStatementInfoSpool(dir, 0, 0, 0)
		require.NoError(t, err)
		defer restarted.close()
		restarted.replayOnce(ctx)
		assert.False(t, restarted.hasPendingOrphans())
	}
	assert.Contains(t, string(stub.bytes()), marker)

	// a second boot after the successful replay has nothing left: no
	// duplicate audit row can be produced.
	{
		stub2 := stubSpoolSQLFlush(t)
		restarted, err := newStatementInfoSpool(dir, 0, 0, 0)
		require.NoError(t, err)
		defer restarted.close()
		restarted.replayOnce(ctx)
		assert.NotContains(t, string(stub2.bytes()), marker)
	}
}

func TestStatementInfoSpoolPostFlushLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	env := setupSpoolLifecycle(t, dir)

	buf := env.pipe.NewItemBuffer(SingleStatementTable.GetName()).(Buffer)
	marker := "si_lifecycle_flush_30812"
	env.reportOne(t, env.newStatement(marker, false), buf)

	// regular periodic export through the wrapped buffer and the real batch
	// handler: it must land at the writer AND clear the spool.
	handle := env.pipe.NewItemBatchHandler(ctx)
	handle(buf.GetBatch(ctx, new(bytes.Buffer)))
	assert.Contains(t, env.captured.String(), marker)

	// with zero ack lag the ack can drop the segment right away; every
	// remaining spool file must carry no statement record.
	require.Eventually(t, func() bool {
		for _, name := range spoolDirFiles(t, dir) {
			content, err := os.ReadFile(dir + string(os.PathSeparator) + name)
			require.NoError(t, err)
			if len(content) > 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)

	// a crash after the flush replays nothing: no duplicate of the already
	// exported statement.
	env.simulateKill()
	stub := stubSpoolSQLFlush(t)
	{
		restarted, err := newStatementInfoSpool(dir, 0, 0, 0)
		require.NoError(t, err)
		defer restarted.close()
		restarted.replayOnce(ctx)
	}
	assert.NotContains(t, string(stub.bytes()), marker)
}

// The pipeline-gap regression: a statement is reported (and spooled) while a
// batch that does NOT contain it still fires and succeeds (the record is
// still draining through the collector, e.g. inside the aggregation window
// or the collector queue). The successful batch must not turn the spooled
// record into a lost one: its segment stays un-acked, and the boot replay
// must still land the record.
func TestStatementInfoSpoolPipelineGapReplayed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	env := setupSpoolLifecycle(t, dir)

	buf := env.pipe.NewItemBuffer(SingleStatementTable.GetName()).(Buffer)
	marker := "si_lifecycle_gap_30812"
	// spool the record but keep it OUT of the buffer the batch drains: this
	// models the record sitting in the collector pipeline when the periodic
	// (or shutdown) export of OTHER content fires.
	spoolStatementInfo(context.Background(), env.newStatement(marker, false))

	// an empty-buffer export tick produces no batch at all (the production
	// caller skips the batch handler then) — no spool ack may happen, the
	// record's segment is not touched.
	assert.Nil(t, buf.GetBatch(ctx, new(bytes.Buffer)))
	assert.NotContains(t, env.captured.String(), marker)

	env.simulateKill()

	stub := stubSpoolSQLFlush(t)
	{
		restarted, err := newStatementInfoSpool(dir, 0, 0, 0)
		require.NoError(t, err)
		defer restarted.close()
		restarted.replayOnce(ctx)
	}
	assert.Contains(t, string(stub.bytes()), marker)
}

func TestStatementInfoSpoolFailedStatementIsAudited(t *testing.T) {
	dir := t.TempDir()
	env := setupSpoolLifecycle(t, dir)

	buf := env.pipe.NewItemBuffer(SingleStatementTable.GetName()).(Buffer)
	marker := "si_lifecycle_failed_30812"
	env.reportOne(t, env.newStatement(marker, true), buf)
	env.simulateKill()

	stub := stubSpoolSQLFlush(t)
	{
		restarted, err := newStatementInfoSpool(dir, 0, 0, 0)
		require.NoError(t, err)
		defer restarted.close()
		restarted.replayOnce(context.Background())
	}
	// failed statements are audit records, too: the crash must not swallow
	// the attempt and the replayed row keeps status Failed.
	assert.Contains(t, string(stub.bytes()), marker)
	assert.Contains(t, string(stub.bytes()), "Failed")
}
