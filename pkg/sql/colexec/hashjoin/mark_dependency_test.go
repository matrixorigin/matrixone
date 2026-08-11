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

package hashjoin

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

// TestHashMarkJoinBuildErrorStopsBeforeProbe ties the generic terminal
// HashBuild failure contract to MARK semantics: a failed build is not an empty
// set (which would produce FALSE markers), and no partial probe row may escape.
func TestHashMarkJoinBuildErrorStopsBeforeProbe(t *testing.T) {
	tc := newMarkSpillTestCase(t)
	probe := &countingBroadcastProbeChild{MockOperator: colexec.NewMockOperator()}
	tc.arg.Children = nil
	tc.arg.AppendChild(probe)
	require.NoError(t, probe.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	buildErr := moerr.NewOOM(tc.proc.Ctx)
	message.SendJoinMapResult(
		message.NewJoinMapBuildErrorResult(buildErr),
		tc.arg.JoinMapTag,
		tc.arg.IsShuffle,
		tc.arg.ShuffleIdx,
		tc.proc.GetMessageBoard(),
	)

	result, err := vm.Exec(tc.arg, tc.proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), err)
	require.Equal(t, buildErr.Error(), err.Error())
	require.Nil(t, result.Batch, "a terminal build error must not become empty-build FALSE rows")
	require.Zero(t, probe.calls.Load(), "MARK must fail before consuming probe input")

	tc.arg.Reset(tc.proc, true, err)
	probe.Reset(tc.proc, true, err)
	tc.arg.Free(tc.proc, true, err)
	probe.Free(tc.proc, true, err)
	tc.barg.Free(tc.proc, true, err)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

// TestHashMarkJoinWaitForBuildHonorsCancellation verifies the unhappy path in
// which the build never publishes a terminal JoinMap. Cancellation must abort
// the dependency wait without reading or emitting any probe row.
func TestHashMarkJoinWaitForBuildHonorsCancellation(t *testing.T) {
	tc := newMarkSpillTestCase(t)
	probe := &countingBroadcastProbeChild{MockOperator: colexec.NewMockOperator()}
	tc.arg.Children = nil
	tc.arg.AppendChild(probe)
	require.NoError(t, probe.Prepare(tc.proc))
	require.NoError(t, tc.arg.Prepare(tc.proc))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	tc.proc.BuildPipelineContext(ctx)
	started := time.Now()
	result, err := vm.Exec(tc.arg, tc.proc)
	elapsed := time.Since(started)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, result.Batch)
	require.Zero(t, probe.calls.Load())
	require.Less(t, elapsed, time.Second, "MARK dependency wait ignored cancellation")

	tc.arg.Reset(tc.proc, true, err)
	probe.Reset(tc.proc, true, err)
	tc.arg.Free(tc.proc, true, err)
	probe.Free(tc.proc, true, err)
	tc.barg.Free(tc.proc, true, err)
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}
