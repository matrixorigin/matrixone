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

package connector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestConnectorResetReleasesDeferredSpoolMemoryAfterCleanupBarrier(t *testing.T) {
	oldSignalSendTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		process.PipelineSignalSendTimeout = oldSignalSendTimeout
	})

	proc := testutil.NewProcess(t)
	connector := NewArgument().WithReg(&process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)})
	require.NoError(t, connector.Prepare(proc))

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := testutil.NewBatch([]types.Type{types.New(types.T_int64, 0, 0)}, true, 1024, srcMP)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	for i := 0; i < 2; i++ {
		queryDone, err := connector.ctr.sp.SendBatch(context.Background(), 0, src, nil)
		require.NoError(t, err)
		require.False(t, queryDone)
	}

	require.Greater(t, proc.Mp().CurrNB(), int64(0))

	connector.Reset(proc, true, moerr.NewInternalErrorNoCtx("cleanup"))
	require.Greater(t, proc.Mp().CurrNB(), int64(0))

	connector.CleanupDeferredSpool()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
