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

package frontend

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestViewDescriptionCompilerContextIsIsolated(t *testing.T) {
	parentCtx := context.WithValue(t.Context(), struct{}{}, "parent")
	parent := InitTxnCompilerContext("parent_db")
	proc := process.NewTopProcess(parentCtx, mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer proc.Free()
	parent.SetExecCtx(&ExecCtx{reqCtx: parentCtx, proc: proc})
	parent.SetViews([]string{"parent_view"})
	parent.SetSnapshot(&planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}})
	parent.SetQueryingSubscription(&planpb.SubscriptionMeta{SubName: "parent_subscription"})
	childValue, closeChild, err := parent.NewViewDescriptionCompilerContext(t.Context())
	require.NoError(t, err)
	defer closeChild()
	child := childValue.(*TxnCompilerContext)
	require.NotSame(t, proc, child.GetProcess())
	require.NotSame(t, proc.Base, child.GetProcess().Base)
	require.Same(t, child, child.GetProcess().GetSessionInfo().CompilerContext)
	childCtx := context.WithValue(t.Context(), struct{}{}, "child")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			child.SetContext(childCtx)
			child.SetViews([]string{"child_view"})
			child.SetSnapshot(&planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 2}})
			child.SetQueryingSubscription(&planpb.SubscriptionMeta{SubName: "child_subscription"})
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			require.Same(t, parentCtx, parent.GetContext())
			require.Equal(t, []string{"parent_view"}, parent.GetViews())
			require.Equal(t, int64(1), parent.GetSnapshot().TS.PhysicalTime)
			require.Equal(t, "parent_subscription", parent.GetQueryingSubscription().SubName)
		}
	}()
	wg.Wait()
	require.Same(t, childCtx, child.GetProcess().GetTopContext())
	require.Same(t, parentCtx, parent.GetContext())
	require.Same(t, parentCtx, proc.GetTopContext())
	require.Equal(t, []string{"parent_view"}, parent.GetViews())
	require.Equal(t, int64(1), parent.GetSnapshot().TS.PhysicalTime)
	require.Equal(t, "parent_subscription", parent.GetQueryingSubscription().SubName)
}
