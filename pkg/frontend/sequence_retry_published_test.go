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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The publication seam of tier-2 admission (design §4 D, G1/F1): the single
// publication path marks the per-statement flag; statements without a retry
// state (background/internal) and nil inputs are safe no-ops.
func TestMarkSequenceRetryPublishedSetsPerStatementFlag(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	execCtx := &ExecCtx{proc: proc}
	// No state installed: nothing to mark, no panic.
	markSequenceRetryPublished(execCtx)
	markSequenceRetryPublished(nil)
	markSequenceRetryPublished(&ExecCtx{})

	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState
	markSequenceRetryPublished(execCtx)
	require.True(t, retryState.Published(), "the first client batch marks the statement published")

	// A second batch is idempotent: the flag stays set.
	markSequenceRetryPublished(execCtx)
	require.True(t, retryState.Published())
}

// publicationOrderingWriter stands in for the wire writer at the single
// production publication point (resp_client.writeClientBatch; design §4 D,
// G1/F1). Its Write records whether the per-statement flag was ALREADY set
// when the first byte would leave: the publication must precede the bytes,
// never follow them.
type publicationOrderingWriter struct {
	*testMysqlWriter
	state              *process.SequenceRetryState
	writes             int
	publishedWhenWrote []bool
}

func (w *publicationOrderingWriter) Write(_ *ExecCtx, _ *perfcounter.CounterSet, _ *batch.Batch) error {
	w.writes++
	w.publishedWhenWrote = append(w.publishedWhenWrote, w.state.Published())
	return nil
}

// Real-path publication oracle (executed integration proof, not a helper
// check): RespResult drives saveResultBatch + writeClientBatch for an
// ordinary SELECT batch, so publication happens through the exact production
// call chain and the wire writer observes the flag already set. A nil batch
// (early-out arm) publishes nothing.
func TestWriteClientBatchPublishesBeforeClientBytes(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	retryState := process.NewSequenceRetryState()
	proc.GetSessionInfo().SeqRetry = retryState

	wr := &publicationOrderingWriter{testMysqlWriter: &testMysqlWriter{}, state: retryState}
	resper := &MysqlResp{mysqlRrWr: wr}
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		proc:   proc,
		stmt:   &tree.Select{},
		ses:    &Session{},
	}

	// Nil-batch early-out: nothing enters the write path, nothing publishes.
	require.NoError(t, resper.RespResult(execCtx, nil, nil))
	require.Equal(t, 0, wr.writes)
	require.False(t, retryState.Published(), "an empty early-out never publishes")

	// The first real batch: publication lands BEFORE the bytes reach the
	// writer, exactly once, through the production call chain.
	require.NoError(t, resper.RespResult(execCtx, nil, batch.NewWithSize(0)))
	require.Equal(t, 1, wr.writes, "the batch reached the wire writer through the real path")
	require.Equal(t, []bool{true}, wr.publishedWhenWrote,
		"the writer observed the flag already set before receiving the first byte")
	require.True(t, retryState.Published())

	// Subsequent batches keep the flag set; each write still observes it.
	require.NoError(t, resper.RespResult(execCtx, nil, batch.NewWithSize(0)))
	require.Equal(t, 2, wr.writes)
	require.Equal(t, []bool{true, true}, wr.publishedWhenWrote)
}
