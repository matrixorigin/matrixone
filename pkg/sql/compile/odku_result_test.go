// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestODKUAttemptResultPublication(t *testing.T) {
	for _, rows := range []uint64{0, 7} {
		result := &odkuAttemptResult{}
		_, err := result.result()
		require.Error(t, err, "absence of EOF cannot masquerade as zero affected rows")
		start, done := make(chan struct{}), make(chan error, 1)
		go func() { <-start; done <- result.publish(rows) }()
		close(start)
		// Publication and reads can overlap. An incomplete result stays an error.
		if got, err := result.result(); err == nil {
			require.Equal(t, rows, got)
		}
		require.NoError(t, <-done)
		got, err := result.result()
		require.NoError(t, err)
		require.Equal(t, rows, got)
		require.Error(t, result.publish(rows+1))
		got, err = result.result()
		require.NoError(t, err)
		require.Equal(t, rows, got, "duplicate publication cannot replace the result")
	}
}

func TestODKUAttemptResultBindingAndRetry(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	owner := preinsertunique.NewArgument()
	scope := newScope(Normal)
	scope.Proc, scope.RootOp, scope.NodeInfo.Addr, scope.NodeInfo.Mcpu = proc, owner, "local", 1
	defer scope.release()
	owner.PreInsertCtx = odkuProtocolLayout()
	c := &Compile{proc: proc, addr: "local", scopes: []*Scope{scope}}
	require.NoError(t, c.bindODKUPriorData())
	first, delayed := c.odkuResult, owner.PublishAffectedRows
	require.NotNil(t, first)
	require.NoError(t, c.closeODKUPriorData())
	require.Nil(t, owner.PublishAffectedRows)
	require.NoError(t, c.bindODKUPriorData())
	require.NotSame(t, first, c.odkuResult)
	// Even an old callback cannot publish into a prepared/retried generation.
	require.NoError(t, delayed(11))
	_, err := c.odkuResult.result()
	require.Error(t, err)
	require.NoError(t, owner.PublishAffectedRows(3))
	got, err := c.odkuResult.result()
	require.NoError(t, err)
	require.Equal(t, uint64(3), got)
	require.NoError(t, c.closeODKUPriorData())
	scope.NodeInfo.Mcpu = 2
	require.Error(t, c.bindODKUPriorData(), "the action owner cannot be replicated")
}
