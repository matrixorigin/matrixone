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

package plan

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newPlanTestProcess owns memory-backed dependencies for the test lifetime.
func newPlanTestProcess(t testing.TB) *process.Process {
	t.Helper()
	fs := testutil.NewFS(nil)
	t.Cleanup(func() { fs.Close(context.Background()) })
	return testutil.NewProcess(t, testutil.WithFileService(fs))
}

func TestMockCompilerContextReusesProcess(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, tc := range []struct {
		name string
		ctx  *MockCompilerContext
	}{
		{name: "constructor", ctx: NewMockCompilerContext(false, proc)},
		{name: "literal", ctx: &MockCompilerContext{proc: proc}},
	} {
		t.Run(tc.name, func(t *testing.T) { assertMockCompilerContextReusesProcess(t, tc.ctx, proc) })
	}
	for _, tc := range []struct {
		name string
		ctx  *MockCompilerContext
	}{
		{name: "explicit nil", ctx: NewMockCompilerContext(false, nil)},
		{name: "empty nil", ctx: NewEmptyCompilerContext(nil)},
		{name: "zero value", ctx: &MockCompilerContext{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, tc.ctx.GetProcess())
			exec, ok := tc.ctx.getInternalSQLExecutor(nil)
			require.False(t, ok)
			require.Nil(t, exec)
		})
	}
}

func TestCopiedMockCompilerContextReusesProcess(t *testing.T) {
	proc := newPlanTestProcess(t)
	original := NewEmptyCompilerContext(proc)
	copied := *original
	require.NotNil(t, original.GetProcess())
	require.Same(t, proc, original.GetProcess())
	require.Same(t, proc, copied.GetProcess())
	exec, ok := original.getInternalSQLExecutor(proc)
	require.True(t, ok)
	require.NotNil(t, exec)
	copyExec, ok := copied.getInternalSQLExecutor(proc)
	require.True(t, ok)
	require.Same(t, exec, copyExec)
	unavailable, ok := copied.getInternalSQLExecutor(&process.Process{})
	require.False(t, ok)
	require.Nil(t, unavailable)
	calls := 0
	copied.GetProcessFunc = func() *process.Process { calls++; return nil }
	unavailable, ok = copied.getInternalSQLExecutor(proc)
	require.False(t, ok)
	require.Nil(t, unavailable)
	require.Zero(t, calls, "executor lookup must not invoke an override")
	require.Nil(t, copied.GetProcess())
	require.Equal(t, 1, calls)
	require.Same(t, proc, original.GetProcess())
}

func TestMockCompilerContextDoesNotLeakInternalSQLExecutor(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	runProducer := func(t *testing.T, preexisting executor.SQLExecutor) {
		t.Helper()
		oldExecutor, hadOldExecutor := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
		if hadOldExecutor {
			require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor))
		}
		if preexisting != nil {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, preexisting)
		}
		t.Cleanup(func() {
			if currentExecutor, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor); ok {
				require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, currentExecutor))
			}
			if hadOldExecutor {
				rt.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
			}
		})

		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
			"select row_number() over (order by n_name) from nation", 1)
		require.NoError(t, err)
		defer stmt.Free()

		// This is the same plan-building producer shape used by the frontend
		// named-window regression, using syntax supported by this branch.
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		queryPlan, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		require.NotNil(t, queryPlan.GetQuery())

		result, err := runSqlWithSnapshot(ctx, "select 1", nil)
		require.NoError(t, err)
		result.Close()

		newExecutor, stillHasExecutor := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
		if preexisting == nil {
			require.False(t, stillHasExecutor)
		} else {
			require.True(t, stillHasExecutor)
			require.Same(t, preexisting, newExecutor)
		}
	}

	t.Run("without preexisting executor", func(t *testing.T) {
		runProducer(t, nil)
	})
	t.Run("preserves preexisting executor", func(t *testing.T) {
		preexisting := executor.NewMemExecutor(func(string) (executor.Result, error) {
			return executor.Result{}, nil
		})
		runProducer(t, preexisting)
	})
}

func assertMockCompilerContextReusesProcess(t *testing.T, ctx *MockCompilerContext, expected *process.Process) {
	t.Helper()
	require.NotNil(t, expected)
	const workers = 2
	start := make(chan struct{})
	results := make(chan *process.Process, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; results <- ctx.GetProcess() }()
	}
	close(start)
	wg.Wait()
	close(results)
	require.Same(t, expected, ctx.GetProcess())
	for result := range results {
		require.Same(t, expected, result)
	}
}

func TestFullTextMockResolverObservesCurrentState(t *testing.T) {
	ctx := newFullTextJoinMockCompilerContext(t)
	resolver := ctx.proc.GetResolveVariableFunc()
	require.NotNil(t, resolver)
	value, err := resolver("fulltext_bloom_filter_pushdown", true, false)
	require.NoError(t, err)
	require.Equal(t, int8(0), value)
	ctx.fulltextBloomFilterPushdown = 1
	value, err = resolver("fulltext_bloom_filter_pushdown", true, false)
	require.NoError(t, err)
	require.Equal(t, int8(1), value)
	require.Same(t, ctx.proc, ctx.GetProcess())
}

func TestPlanFixtureReleasesPoolAtChildEnd(t *testing.T) {
	count := func() int {
		var pools []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(mpool.ReportMemUsage("must_new_zero_no_fixed")), &pools))
		return len(pools)
	}
	before := count()
	t.Run("owned compiler session", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		ctx := NewEmptyCompilerContext(proc)
		require.NotNil(t, ctx.GetProcess())
		require.Same(t, proc, ctx.GetProcess())
		require.Equal(t, before+1, count())
		t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
		for _, offHeap := range []bool{true, false} {
			block, err := proc.Mp().Alloc(16, offHeap)
			require.NoError(t, err)
			t.Cleanup(func() { proc.Mp().Free(block) })
		}
		require.Greater(t, proc.Mp().CurrNB(), int64(0))
	})
	require.Equal(t, before, count(), "zero byte use does not prove pool deregistration")
}
