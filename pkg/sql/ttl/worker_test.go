// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// Deliberately fails before invoking the callback: retry must open a new whole
// transaction, be bounded, preserve permanent failures and respect cancellation.
type retryExecutor struct {
	executor.SQLExecutor
	calls  int
	err    error
	cancel context.CancelFunc
}

func (e *retryExecutor) ExecTxn(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
	e.calls++
	if e.cancel != nil {
		e.cancel()
	}
	return e.err
}
func validRetryCandidate(t *testing.T) Candidate {
	t.Helper()
	p := Policy{Version: 1, Column: "ts", Seconds: 1}
	c := engine.ConstraintDef{Cts: []engine.Constraint{&engine.StreamConfigsDef{Configs: []*plan.Property{{Key: PropertyKey, Value: Encode(&p)}}}}}
	data, err := c.MarshalBinary()
	require.NoError(t, err)
	return Candidate{TableID: 1, Database: "d", Table: "t", Policy: p, Constraint: data}
}
func TestTTLRetryBoundsAndCancellation(t *testing.T) {
	c := validRetryCandidate(t)
	retry := &retryExecutor{err: errors.Join(moerr.NewTxnNeedRetryNoCtx())}
	_, err := DeleteBatch(t.Context(), retry, c, time.Now(), 1)
	require.Error(t, err)
	require.Equal(t, 8, retry.calls)
	permanent := errors.New("permanent error")
	fail := &retryExecutor{err: permanent}
	_, err = DeleteBatch(t.Context(), fail, c, time.Now(), 1)
	require.ErrorIs(t, err, permanent)
	require.Equal(t, 1, fail.calls)
	ctx, cancel := context.WithCancel(t.Context())
	stop := &retryExecutor{err: moerr.NewTxnNeedRetryNoCtx(), cancel: cancel}
	_, err = DeleteBatch(ctx, stop, c, time.Now(), 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, stop.calls)
	c.Policy.Seconds = 2
	_, err = DeleteBatch(t.Context(), fail, c, time.Now(), 1)
	require.Error(t, err)
	require.Equal(t, 1, fail.calls, "mismatched policy must not start a transaction")
	options := DefaultWorkerOptions()
	options.BatchSize = 0
	require.Error(t, Run(t.Context(), nil, time.Now(), options))
}
