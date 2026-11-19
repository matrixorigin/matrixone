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

package table_scan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Executor pooling can reuse an address even after Free. Count lifecycle calls
// on a non-pooled wrapper, rather than relying on pointer identity alone.
type resetObservedExecutor struct {
	colexec.ExpressionExecutor
	resets int
	frees  int
}

func (e *resetObservedExecutor) ResetForNextQuery() {
	e.resets++
	e.ExpressionExecutor.ResetForNextQuery()
}

func (e *resetObservedExecutor) Free() {
	e.frees++
	e.ExpressionExecutor.Free()
}

func TestTableScanResetRetainsFilterExecutor(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := orLikeScanArgument(orLikeOperatorOne(t, proc))
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		require.Zero(t, proc.Mp().CurrNB())
	})
	require.NoError(t, arg.Prepare(proc))
	executor := &resetObservedExecutor{ExpressionExecutor: arg.ctr.filterExecutors[0]}
	arg.ctr.filterExecutors[0] = executor
	arg.ctr.allFilterExecutors[0] = executor
	for i := range 2 {
		func() {
			input, _ := orLikeScanBatch(t, proc, 2, "pattern", false)
			defer input.Clean(proc.Mp())
			orLikeScanEval(t, proc, arg, input, false, []int64{0})
		}()
		arg.Reset(proc, false, nil)
		require.Equal(t, i+1, executor.resets)
		require.Zero(t, executor.frees, "Reset must not free a reusable filter executor")
		require.NoError(t, arg.Prepare(proc))
		require.Same(t, executor, arg.ctr.filterExecutors[0])
	}
	arg.Free(proc, false, nil)
	require.Equal(t, 1, executor.frees, "final cleanup must free the retained executor")
}
