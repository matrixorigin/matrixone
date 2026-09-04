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

// Coverage for the executor's plugin lookup, task metadata, constructor and
// re-entrancy guard. getTasksFunc, getTableDefFunc and run need a live engine and
// are not covered here.
package idxcron

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

// A plugin is reachable by the action token its SyncDescriptor declares; an unknown
// action resolves to nothing.
func TestFindReindexAlgo(t *testing.T) {
	indexplugin.Register(&mockReindexAlgoPlugin{
		algo: "findreindexalgo_test",
		desc: catalogplugin.SyncDescriptor{IdxcronAction: "findreindexalgo_test_action"},
	})

	got, ok := findReindexAlgo("findreindexalgo_test_action")
	require.True(t, ok)
	require.Equal(t, "findreindexalgo_test", got.Algo())

	_, ok = findReindexAlgo("no_such_action")
	require.False(t, ok)

	// The empty token matches any plugin that declares no action, so it is not a
	// valid lookup key; the executor only ever passes a token read from a task row.
}

func TestIndexUpdateTaskMetadata(t *testing.T) {
	md := IndexUpdateTaskMetadata(task.TaskCode_TestOnly, "a", "b")
	require.Equal(t, "IndexUpdateTask", md.ID)
	require.Equal(t, task.TaskCode_TestOnly, md.Executor)
	require.Equal(t, "a"+ParamSeparator+"b", string(md.Context))
	require.Equal(t, 1, int(md.Options.Concurrency), "idxcron runs one at a time")

	md = IndexUpdateTaskMetadata(task.TaskCode_TestOnly)
	require.Empty(t, md.Context)
}

// The constructor derives a cancellable context from its caller's.
func TestNewIndexUpdateTaskExecutor(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	exec, err := NewIndexUpdateTaskExecutor(parent, "uuid", nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "uuid", exec.cnUUID)
	require.NotNil(t, exec.cancelFunc)
	require.NoError(t, exec.ctx.Err())

	cancelParent()
	<-exec.ctx.Done()
	require.Error(t, exec.ctx.Err())
}

// A second firing while a run is in flight returns immediately.
func TestIndexUpdateTaskExecutorFactory_ReentrancyGuard(t *testing.T) {
	require.True(t, running.CompareAndSwap(false, true))
	t.Cleanup(func() { running.Store(false) })

	fn := IndexUpdateTaskExecutorFactory("uuid", nil, nil, nil)
	require.NoError(t, fn(context.Background(), nil), "the second firing is a no-op")
	require.True(t, running.Load(), "and it must not clear the in-flight flag")
}

// --- buildReindexSql -------------------------------------------------------

func TestBuildReindexSql(t *testing.T) {
	require.Equal(t,
		"ALTER TABLE `db`.`tbl` ALTER REINDEX `idx` ivfflat FORCE_SYNC",
		buildReindexSql("db", "tbl", "idx", "ivfflat", ""))
	require.Equal(t,
		"ALTER TABLE `db`.`tbl` ALTER REINDEX `idx` ivfflat LISTS=8 FORCE_SYNC",
		buildReindexSql("db", "tbl", "idx", "ivfflat", "LISTS=8"))
}
