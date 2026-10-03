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

package moerr

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

func TestISCPTimeoutCausesDoNotReportOnContextCreation(t *testing.T) {
	previousReporter := errutil.GetReportErrorFunc()
	var reports atomic.Int32
	errutil.SetErrorReporter(func(context.Context, error, int) {
		reports.Add(1)
	})
	t.Cleanup(func() {
		errutil.SetErrorReporter(previousReporter)
	})

	_ = NewInternalErrorNoCtx("construction-time reporting control")
	require.Equal(t, int32(1), reports.Load())
	reports.Store(0)

	causes := []error{
		CauseISCPIterationTimeout,
		CauseISCPFlushJobStatusTimeout,
		CauseISCPFlushPermanentErrorMessageTimeout,
		CauseISCPTransactionFinishTimeout,
		CauseISCPGetTaskRunnerTimeout,
	}
	for _, cause := range causes {
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Hour, cause)
		t.Cleanup(cancel)
		require.NoError(t, ctx.Err())
		require.Nil(t, context.Cause(ctx))
		cancel()

		ctx, cancel = context.WithTimeoutCause(context.Background(), time.Millisecond, cause)
		t.Cleanup(cancel)
		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		require.Same(t, cause, context.Cause(ctx))
		cancel()
	}

	require.Zero(t, reports.Load())
}
