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

package compile

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestCompileClearReleasesBuildPlanClosure(t *testing.T) {
	captured := &plan.Plan{}
	c := &Compile{proc: testutil.NewProcess(t), MessageBoard: message.NewMessageBoard(), affectRows: new(atomic.Uint64)}
	t.Cleanup(func() {
		if c.proc != nil {
			c.clear()
		}
	})
	c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) { return captured, nil })
	c.SetIsPrepare(true)
	c.Release()
	require.NotNil(t, c.buildPlanFunc, "a cached prepared Compile still owns retry capability")
	got, err := c.buildPlanFunc(context.Background())
	require.NoError(t, err)
	require.Same(t, captured, got)
	c.SetIsPrepare(false)
	// Invoke the real pool reset seam on a private object, never inspect one after Put.
	c.clear()
	require.Nil(t, c.buildPlanFunc, "actual pool return must sever the session/history capture")
}

func TestDefinitionRetryBuildFailureInvalidatesGeneration(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("frozen=%t", frozen), func(t *testing.T) {
			c := &Compile{proc: testutil.NewProcess(t), pn: &plan.Plan{}, MessageBoard: message.NewMessageBoard(), affectRows: new(atomic.Uint64)}
			t.Cleanup(func() {
				if c.proc != nil {
					c.clear()
				}
			})
			original := c.pn
			sentinel := moerr.NewTxnNeedRetryWithDefChanged(context.Background())
			called := 0
			c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) { called++; return nil, sentinel })
			c.resultMetadataFrozen.Store(frozen)
			rebuilt, err := c.buildRetryCompile(true)
			require.ErrorIs(t, err, sentinel)
			require.Nil(t, rebuilt)
			require.Equal(t, 1, called)
			require.True(t, c.PlanGenerationRebuilt(), "even failed rebuild must invalidate the frontend's old generation")
			require.Same(t, original, c.pn, "failure cannot publish a different plan")
		})
	}
}
